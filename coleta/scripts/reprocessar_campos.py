"""
Reprocessador de campos — Fase 3 do requalificador iterativo.

Lê a matriz de qualidade e reprocessa apenas os campos marcados como REPROCESSAR.
Trabalha campo por campo, empresa por empresa.
"""

import sqlite3
import requests
import re
import os
import sys
import io
import json
import time
import logging
from collections import defaultdict
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
MATRIZ_PATH = os.path.join(PROJECT_ROOT, "config", "matriz_qualidade.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger()


def fetch(url, session):
    try:
        r = session.get(url, timeout=12, allow_redirects=True)
        if r.status_code == 200:
            return r.text, r.url
    except:
        pass
    return None, None


# ============================================================
# EXTRATORES POR CAMPO (versão que retorna o valor, não bool)
# ============================================================

def extrair_tipologia_valor(html, titulo):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()

    secao = ""
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        if any(kw in classes + " " + sid for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur", "dormit"]):
            secao += " " + section.get_text(separator=" ")

    texto = secao.lower() if secao.strip() else soup.get_text(separator=" ").lower()
    titulo_l = titulo.lower()

    flags = {"apto_studio": 0, "apto_1_dorm": 0, "apto_2_dorms": 0,
             "apto_3_dorms": 0, "apto_4_dorms": 0, "apto_suite": 0}

    for m in re.finditer(r"(\d)\s*a\s*(\d)\s*(?:dorm|quarto|su[ií]te)", texto):
        for d in range(int(m.group(1)), int(m.group(2)) + 1):
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    for m in re.finditer(r"((?:\d\s*(?:,|e)\s*)+\d)\s*(?:dorm|quarto)", texto):
        for n in re.findall(r"\d", m.group(1)):
            d = int(n)
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    if re.search(r"\b0?1\s*(?:dorm|quarto)", texto): flags["apto_1_dorm"] = 1
    if re.search(r"\b0?2\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_2_dorms"] = 1
    if re.search(r"\b0?3\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_3_dorms"] = 1
    if re.search(r"\b0?4\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_4_dorms"] = 1
    if re.search(r"su[ií]te", texto): flags["apto_suite"] = 1

    if ("studio" in titulo_l or
        ("studio" in secao.lower() if secao.strip() else False) or
        re.search(r"studio.{0,40}m[²2]|\d+m[²2].{0,40}studio", texto) or
        re.search(r"tipo\w*\s*:?\s*studio|studio\s*\|", texto)):
        flags["apto_studio"] = 1

    if not any(v == 1 for v in flags.values()):
        return None

    partes = []
    if flags["apto_studio"]: partes.append("Studio")
    if flags["apto_1_dorm"]: partes.append("1 dorm")
    if flags["apto_2_dorms"]: partes.append("2 dorms")
    if flags["apto_3_dorms"]: partes.append("3 dorms")
    if flags["apto_4_dorms"]: partes.append("4 dorms")
    if flags["apto_suite"]: partes.append("c/suíte")
    desc = " e ".join(partes)

    return {"flags": flags, "desc": desc}


def extrair_imagem_valor(html):
    soup = BeautifulSoup(html[:30000], "html.parser")
    og = soup.find("meta", {"property": "og:image"})
    if og and og.get("content"):
        url = og["content"]
        if url.startswith("http") and not url.endswith(".svg"):
            return url
    for img in soup.find_all("img", limit=15):
        src = img.get("src") or img.get("data-src") or ""
        if src and src.startswith("http") and not src.endswith(".svg") and "logo" not in src.lower() and "icon" not in src.lower():
            return src
    return None


def extrair_metragem_valor(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()
    texto = soup.get_text(separator=" ")

    matches = re.findall(r"(\d+(?:[.,]\d+)?)\s*(?:a|e|-|até)\s*(\d+(?:[.,]\d+)?)\s*m[²2]", texto)
    if matches:
        areas = []
        for a, b in matches:
            areas.extend([float(a.replace(",", ".")), float(b.replace(",", "."))])
        areas = [a for a in areas if 10 < a < 500]
        if areas:
            return min(areas), max(areas)

    singles = re.findall(r"(\d+(?:[.,]\d+)?)\s*m[²2]", texto)
    if singles:
        areas = [float(a.replace(",", ".")) for a in singles]
        areas = [a for a in areas if 10 < a < 500]
        if areas:
            return min(areas), max(areas)
    return None, None


def extrair_coords_valor(html):
    for pat in [r'@(-?\d+\.\d{4,}),(-?\d+\.\d{4,})', r'q=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
                r'll=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})', r'center=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})']:
        m = re.search(pat, html)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon

    m = re.search(r'"lat(?:itude)?":\s*(-?\d+\.\d{4,}).*?"l(?:ng|on(?:gitude)?)":\s*(-?\d+\.\d{4,})', html, re.DOTALL)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        if -35 < lat < 6 and -75 < lon < -30:
            return lat, lon

    soup = BeautifulSoup(html[:10000], "html.parser")
    lat_m = soup.find("meta", {"property": re.compile(r"latitude|place:location:latitude", re.I)})
    lon_m = soup.find("meta", {"property": re.compile(r"longitude|place:location:longitude", re.I)})
    if lat_m and lon_m:
        try:
            lat, lon = float(lat_m["content"]), float(lon_m["content"])
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon
        except:
            pass
    return None, None


def extrair_fase_valor(html, url):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header"]):
        tag.decompose()

    for tag in soup.find_all(["span", "div", "p", "label", "a", "li"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        combined = classes + " " + tag_id
        if any(kw in combined for kw in ["status", "fase", "stage", "badge", "tag-", "etapa"]):
            t = tag.get_text().strip().lower()
            if 0 < len(t) < 50:
                if re.search(r"100\s*%?\s*vendido|esgotado", t): return "100% Vendido"
                if re.search(r"breve|em\s*breve", t): return "Breve Lançamento"
                if re.search(r"constru|obra|andamento", t): return "Em Construção"
                if re.search(r"pronto|entregue|chaves", t): return "Pronto para Morar"
                if re.search(r"lan[çc]a", t): return "Lançamento"

    url_l = url.lower()
    if "/pronto" in url_l or "/entregue" in url_l: return "Pronto para Morar"
    if "/obra" in url_l or "/em-construcao" in url_l or "/construcao" in url_l: return "Em Construção"
    if "/vendido" in url_l: return "100% Vendido"

    texto = soup.get_text(separator=" ").lower()
    if re.search(r"100\s*%?\s*vendido|esgotado", texto): return "100% Vendido"
    if re.search(r"\d+\s*%\s*(?:da\s*obra|conclu|execut)", texto): return "Em Construção"
    return None


def extrair_endereco_valor(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict):
                addr = data.get("address", {})
                if isinstance(addr, dict) and addr.get("streetAddress"):
                    return addr["streetAddress"]
        except:
            pass

    for meta in soup.find_all("meta"):
        prop = (meta.get("property") or "").lower()
        if "street-address" in prop and meta.get("content"):
            return meta["content"]

    texto = soup.get_text(separator=" ")
    m = re.search(r"(?:endere[çc]o|localiza[çc][ãa]o)\s*:?\s*(.+?)(?:\n|<)", texto, re.I)
    if m and 10 < len(m.group(1).strip()) < 200:
        return m.group(1).strip()

    m = re.search(r"((?:Rua|Av\.|Avenida|R\.|Al\.|Alameda)\s+[^,\n]+(?:,\s*\d+)?)", texto)
    if m and 10 < len(m.group(1)) < 200:
        return m.group(1).strip()
    return None


def extrair_lazer_valor(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()

    LAZER_MAP = {
        "lazer_piscina": r"piscina",
        "lazer_churrasqueira": r"churrasqueir|churras\b|barbecue",
        "lazer_fitness": r"fitness|academia|gym",
        "lazer_playground": r"playground",
        "lazer_brinquedoteca": r"brinquedoteca",
        "lazer_salao_festas": r"sal[ãa]o\s*(?:de\s*)?festas?",
        "lazer_pet_care": r"pet\s*(?:care|place|garden|park)",
        "lazer_coworking": r"coworking|co-working",
        "lazer_bicicletario": r"biciclet[áa]rio",
        "lazer_quadra": r"quadra(?!\s*de\s*garagem)",
        "lazer_spa": r"\bspa\b",
        "lazer_sauna": r"sauna",
        "lazer_espaco_gourmet": r"gourmet|espa[çc]o\s*gourmet",
        "lazer_sala_jogos": r"sala\s*(?:de\s*)?jogos|game",
    }

    # Seção de lazer
    secao = ""
    for section in soup.find_all(["section", "div", "ul"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        if any(kw in classes + " " + sid for kw in ["lazer", "amenid", "comodid", "infraestr", "area-comum", "diferencia", "condomin"]):
            secao += " " + section.get_text(separator=" ")

    texto = secao.lower() if secao.strip() else soup.get_text(separator=" ").lower()

    flags = {}
    itens = []
    for campo, pattern in LAZER_MAP.items():
        if re.search(pattern, texto, re.I):
            flags[campo] = 1
            itens.append(campo.replace("lazer_", "").replace("_", " "))

    if not flags:
        return None, None
    return flags, ", ".join(itens)


# ============================================================
# PROCESSAMENTO
# ============================================================

def reprocessar_campo(campo, empresas, conn, session, delay=1.5):
    """Reprocessa um campo específico para uma lista de empresas."""
    cur = conn.cursor()
    total_atualiz = 0
    total_urls = 0
    total_erros = 0

    for emp_idx, empresa in enumerate(empresas, 1):
        cur.execute("""SELECT id, nome, url_fonte FROM empreendimentos
            WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
            ORDER BY nome""", (empresa,))
        rows = cur.fetchall()

        atualiz_emp = 0
        for row in rows:
            emp_id, nome, url = row
            total_urls += 1

            html, final_url = fetch(url, session)
            if not html:
                total_erros += 1
                continue

            titulo_tag = BeautifulSoup(html[:5000], "html.parser").find("title")
            titulo = titulo_tag.get_text().strip() if titulo_tag else ""

            if campo == "tipologia":
                result = extrair_tipologia_valor(html, titulo)
                if result:
                    for flag_campo, flag_val in result["flags"].items():
                        cur.execute(f"UPDATE empreendimentos SET {flag_campo}=? WHERE id=?", (flag_val, emp_id))
                    cur.execute("UPDATE empreendimentos SET dormitorios_descricao=? WHERE id=?", (result["desc"], emp_id))
                    atualiz_emp += 1

            elif campo == "imagem":
                img = extrair_imagem_valor(html)
                if img:
                    cur.execute("UPDATE empreendimentos SET imagem_url=? WHERE id=?", (img, emp_id))
                    atualiz_emp += 1

            elif campo == "metragem":
                amin, amax = extrair_metragem_valor(html)
                if amin:
                    cur.execute("UPDATE empreendimentos SET area_min_m2=?, area_max_m2=? WHERE id=?", (amin, amax, emp_id))
                    atualiz_emp += 1

            elif campo == "coords":
                lat, lon = extrair_coords_valor(html)
                if lat:
                    cur.execute("UPDATE empreendimentos SET latitude=?, longitude=? WHERE id=?", (lat, lon, emp_id))
                    atualiz_emp += 1

            elif campo == "fase":
                fase = extrair_fase_valor(html, final_url or url)
                if fase:
                    cur.execute("UPDATE empreendimentos SET fase=? WHERE id=?", (fase, emp_id))
                    atualiz_emp += 1

            elif campo == "endereco":
                end = extrair_endereco_valor(html)
                if end:
                    cur.execute("UPDATE empreendimentos SET endereco=? WHERE id=?", (end, emp_id))
                    atualiz_emp += 1

            elif campo == "lazer":
                flags, texto_lazer = extrair_lazer_valor(html)
                if flags:
                    for flag_campo, flag_val in flags.items():
                        cur.execute(f"UPDATE empreendimentos SET {flag_campo}=? WHERE id=?", (flag_val, emp_id))
                    if texto_lazer:
                        cur.execute("UPDATE empreendimentos SET itens_lazer=? WHERE id=?", (texto_lazer, emp_id))
                    atualiz_emp += 1

            time.sleep(delay)

        conn.commit()
        total_atualiz += atualiz_emp
        log.info(f"  [{emp_idx}/{len(empresas)}] {empresa:30s}: {atualiz_emp}/{len(rows)} atualizados")

    return total_urls, total_atualiz, total_erros


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--campo", type=str, required=True, choices=["tipologia", "imagem", "metragem", "coords", "fase", "endereco", "lazer"])
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--limite-empresas", type=int, default=999)
    args = parser.parse_args()

    with open(MATRIZ_PATH, "r", encoding="utf-8") as f:
        matriz = json.load(f)

    empresas = [emp for emp, dados in matriz.items()
                if dados.get(args.campo, {}).get("status") == "REPROCESSAR"][:args.limite_empresas]

    log.info(f"=== REPROCESSAR: {args.campo.upper()} ({len(empresas)} empresas) ===")

    conn = sqlite3.connect(DB_PATH, timeout=60)
    session = requests.Session()
    session.headers.update(HEADERS)

    total_urls, total_atualiz, total_erros = reprocessar_campo(
        args.campo, empresas, conn, session, args.delay
    )

    log.info(f"\n{'='*40}")
    log.info(f"URLs processadas: {total_urls}")
    log.info(f"Atualizados: {total_atualiz}")
    log.info(f"Erros: {total_erros}")

    conn.close()
    log.info("DONE")


if __name__ == "__main__":
    main()

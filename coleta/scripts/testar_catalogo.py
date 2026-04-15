"""
Testador do Catálogo — Fase 2 do requalificador iterativo.

Para cada empresa SSR (188):
1. Pega 5 URLs aleatórias
2. Para cada campo, tenta extrair o dado
3. Compara com o que tem no banco
4. Classifica: OK / REPROCESSAR / NA

Resultado: matriz empresa × campo salva em config/matriz_qualidade.json
"""

import sqlite3
import requests
import re
import os
import sys
import io
import json
import time
import random
import logging
from collections import defaultdict
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
CLASSIF_PATH = os.path.join(PROJECT_ROOT, "config", "classificacao_sites.json")
MATRIZ_PATH = os.path.join(PROJECT_ROOT, "config", "matriz_qualidade.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger()

CAMPOS_TESTAR = ["coords", "fase", "tipologia", "lazer", "endereco", "metragem", "imagem"]


def fetch(url, session):
    try:
        r = session.get(url, timeout=12, allow_redirects=True)
        if r.status_code == 200:
            return r.text, r.url
    except:
        pass
    return None, None


def extrair_coords(html):
    for pat in [r'@(-?\d+\.\d{4,}),(-?\d+\.\d{4,})', r'q=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
                r'll=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})', r'center=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})']:
        m = re.search(pat, html)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 < lat < 6 and -75 < lon < -30:
                return True
    if re.search(r'"lat(?:itude)?":\s*(-?\d+\.\d{4,})', html):
        return True
    soup = BeautifulSoup(html[:10000], "html.parser")
    if soup.find("meta", {"property": re.compile(r"latitude|place:location", re.I)}):
        return True
    # Coords brutas (padrão -XX.XXXXX)
    brutas = re.findall(r'(-?\d{1,2}\.\d{5,})', html)
    # Filtrar: precisam vir em pares (lat, lon) e estar no range Brasil
    for i in range(len(brutas) - 1):
        try:
            lat, lon = float(brutas[i]), float(brutas[i+1])
            if -35 < lat < 6 and -75 < lon < -30:
                return True
        except:
            pass
    return False


def extrair_fase(soup, texto, url):
    # Badges
    for tag in soup.find_all(["span", "div", "p", "label", "a", "li"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        combined = classes + " " + tag_id
        if any(kw in combined for kw in ["status", "fase", "stage", "badge", "tag-", "label-", "etapa"]):
            t = tag.get_text().strip()
            if 0 < len(t) < 50:
                return True
    # URL
    if re.search(r'/(?:pronto|obra|lancamento|vendido|entregue|construcao|em-construcao|breve)', url.lower()):
        return True
    # Texto
    if re.search(r'breve\s*lan[çc]amento|100\s*%?\s*vendido|pronto\s*(?:para|pra)\s*morar|em\s*constru[çc][ãa]o|\d+\s*%\s*(?:da\s*obra|conclu)', texto.lower()):
        return True
    return False


def extrair_tipologia(soup, titulo, texto):
    # Seção específica
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        if any(kw in classes + " " + sid for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur", "dormit"]):
            t = section.get_text(separator=" ").lower()
            if re.search(r"\d\s*(?:dorm|quarto|su[ií]te)|studio", t):
                return True
    # Título
    if re.search(r"\d\s*(?:dorm|quarto)|studio", titulo.lower()):
        return True
    # Texto geral (menos confiável)
    if re.search(r"\b0?[1-4]\s*(?:dorm|quarto)|studio", texto.lower()):
        return True
    return False


def extrair_lazer(soup, texto):
    # Seção específica
    for section in soup.find_all(["section", "div", "ul"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        if any(kw in classes + " " + sid for kw in ["lazer", "amenid", "comodid", "infraestr", "area-comum", "leisure", "diferencia", "condomin"]):
            t = section.get_text(separator=" ").lower()
            if re.search(r"piscina|churras|fitness|academia|playground|sal[ãa]o|pet|quadra", t):
                return True
    # Lista de itens
    for ul in soup.find_all("ul"):
        lis = ul.find_all("li")
        if len(lis) >= 3:
            textos = " ".join(li.get_text().strip().lower() for li in lis[:15])
            hits = len(re.findall(r"piscina|churras|fitness|academia|playground|sal[ãa]o|pet|quadra|biciclet|coworking", textos))
            if hits >= 2:
                return True
    # Texto geral
    hits = len(re.findall(r"piscina|churrasqueir|fitness|academia|playground|sal[ãa]o\s*(?:de\s*)?festas|pet\s*(?:care|place)|quadra|biciclet", texto.lower()))
    if hits >= 3:
        return True
    return False


def extrair_endereco(soup, html, texto):
    # Schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict) and data.get("address"):
                return True
        except:
            pass
    # Meta OG
    if soup.find("meta", {"property": re.compile(r"street-address|locality", re.I)}):
        return True
    # Texto
    if re.search(r"(?:endere[çc]o|localiza[çc][ãa]o)\s*:", texto, re.I):
        return True
    if re.search(r"(?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rod\.)\s+\w", texto):
        return True
    return False


def extrair_metragem(texto):
    if re.search(r"\d+(?:[.,]\d+)?\s*(?:a\s*\d+(?:[.,]\d+)?\s*)?m[²2]", texto):
        return True
    return False


def extrair_imagem(soup):
    if soup.find("meta", {"property": "og:image"}):
        return True
    for img in soup.find_all("img", limit=10):
        src = (img.get("src") or img.get("data-src") or "")
        if src and not src.endswith(".svg") and "logo" not in src.lower():
            return True
    return False


EXTRATORES = {
    "coords": lambda soup, html, titulo, texto, url: extrair_coords(html),
    "fase": lambda soup, html, titulo, texto, url: extrair_fase(soup, texto, url),
    "tipologia": lambda soup, html, titulo, texto, url: extrair_tipologia(soup, titulo, texto),
    "lazer": lambda soup, html, titulo, texto, url: extrair_lazer(soup, texto),
    "endereco": lambda soup, html, titulo, texto, url: extrair_endereco(soup, html, texto),
    "metragem": lambda soup, html, titulo, texto, url: extrair_metragem(texto),
    "imagem": lambda soup, html, titulo, texto, url: extrair_imagem(BeautifulSoup(html[:20000], "html.parser")),
}

DB_CAMPOS = {
    "coords": "latitude",
    "fase": "fase",
    "tipologia": "apto_2_dorms",  # proxy: se tem 2dorms preenchido, tem tipologia
    "lazer": "itens_lazer",
    "endereco": "endereco",
    "metragem": "area_min_m2",
    "imagem": "imagem_url",
}


def testar_empresa(empresa, conn, session, n_amostras=5):
    """Testa n amostras de uma empresa e retorna resultados por campo."""
    cur = conn.cursor()

    # Pegar amostras aleatórias
    campos_select = ", ".join(f"{v}" for v in DB_CAMPOS.values())
    cur.execute(f"""
        SELECT id, nome, url_fonte, {campos_select}
        FROM empreendimentos WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
        ORDER BY RANDOM() LIMIT ?
    """, (empresa, n_amostras))
    rows = cur.fetchall()

    if not rows:
        return None

    resultados = {campo: {"total": 0, "pagina_tem": 0, "banco_tem": 0, "ambos": 0, "pagina_sim_banco_nao": 0, "pagina_nao_banco_sim": 0} for campo in CAMPOS_TESTAR}

    for row in rows:
        emp_id, nome, url = row[0], row[1], row[2]
        db_vals = {}
        for i, campo in enumerate(DB_CAMPOS.keys()):
            db_vals[campo] = row[3 + i]

        html, final_url = fetch(url, session)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        soup_clean = BeautifulSoup(html, "html.parser")
        for tag in soup_clean.find_all(["nav", "footer", "header", "aside", "menu"]):
            tag.decompose()

        titulo_tag = soup.find("title")
        titulo = titulo_tag.get_text().strip() if titulo_tag else ""
        texto = soup_clean.get_text(separator=" ")

        for campo in CAMPOS_TESTAR:
            resultados[campo]["total"] += 1

            # Testar se a página tem o dado
            pagina_tem = EXTRATORES[campo](soup_clean, html, titulo, texto, final_url or url)

            # Checar se o banco tem o dado
            db_val = db_vals.get(campo)
            banco_tem = db_val is not None and db_val != "" and db_val != 0

            resultados[campo]["pagina_tem"] += int(pagina_tem)
            resultados[campo]["banco_tem"] += int(banco_tem)
            if pagina_tem and banco_tem:
                resultados[campo]["ambos"] += 1
            elif pagina_tem and not banco_tem:
                resultados[campo]["pagina_sim_banco_nao"] += 1
            elif not pagina_tem and banco_tem:
                resultados[campo]["pagina_nao_banco_sim"] += 1

        time.sleep(1.0)

    return resultados


def classificar_campo(resultado):
    """Classifica um campo como OK / REPROCESSAR / NA."""
    total = resultado["total"]
    if total == 0:
        return "NA"

    pagina_tem = resultado["pagina_tem"]
    banco_tem = resultado["banco_tem"]
    pagina_sim_banco_nao = resultado["pagina_sim_banco_nao"]
    pagina_nao_banco_sim = resultado["pagina_nao_banco_sim"]

    # Se a página não tem o dado em nenhuma amostra -> NA (site não publica)
    if pagina_tem == 0:
        if banco_tem > 0:
            return "VERIFICAR"  # banco tem mas página não -> pode ser dado de outra fonte ou erro
        return "NA"

    # Se página tem e banco tem na maioria -> OK
    if pagina_sim_banco_nao == 0 and pagina_nao_banco_sim == 0:
        return "OK"

    # Se página tem mas banco não -> REPROCESSAR
    if pagina_sim_banco_nao > 0:
        return "REPROCESSAR"

    # Se banco tem mas página não (em algumas) -> pode estar OK
    if pagina_nao_banco_sim > 0 and pagina_sim_banco_nao == 0:
        return "OK"  # banco tem de outra fonte

    return "VERIFICAR"


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", type=str, default=None)
    parser.add_argument("--limite", type=int, default=999)
    parser.add_argument("--amostras", type=int, default=5)
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    session = requests.Session()
    session.headers.update(HEADERS)

    # Carregar classificação
    with open(CLASSIF_PATH, "r", encoding="utf-8") as f:
        classif = json.load(f)

    # Só empresas SSR (e API conhecidas com páginas HTML)
    empresas_ssr = classif.get("ssr", []) + classif.get("api_conhecida", [])

    if args.empresa:
        empresas = [args.empresa]
    else:
        empresas = empresas_ssr[:args.limite]

    # Carregar matriz existente
    matriz = {}
    if os.path.exists(MATRIZ_PATH):
        with open(MATRIZ_PATH, "r", encoding="utf-8") as f:
            try:
                matriz = json.load(f)
            except:
                pass

    log.info(f"=== TESTE DO CATÁLOGO ===")
    log.info(f"Empresas: {len(empresas)} | Amostras: {args.amostras}/empresa")

    contadores = {campo: defaultdict(int) for campo in CAMPOS_TESTAR}

    for i, empresa in enumerate(empresas, 1):
        if empresa in matriz and not args.empresa:
            # Já testada, pular
            for campo in CAMPOS_TESTAR:
                status = matriz[empresa].get(campo, {}).get("status", "NA")
                contadores[campo][status] += 1
            if i % 50 == 0:
                log.info(f"  [{i}/{len(empresas)}] (pulando já testados)")
            continue

        resultado = testar_empresa(empresa, conn, session, args.amostras)
        if not resultado:
            continue

        # Classificar cada campo
        empresa_resultado = {}
        campos_status = []
        for campo in CAMPOS_TESTAR:
            status = classificar_campo(resultado[campo])
            empresa_resultado[campo] = {
                "status": status,
                "detalhes": resultado[campo],
            }
            contadores[campo][status] += 1
            campos_status.append(f"{campo[0].upper()}:{status[0]}")

        matriz[empresa] = empresa_resultado

        status_str = " | ".join(campos_status)
        log.info(f"  [{i}/{len(empresas)}] {empresa:30s}: {status_str}")

        # Salvar progresso a cada 20
        if i % 20 == 0:
            with open(MATRIZ_PATH, "w", encoding="utf-8") as f:
                json.dump(matriz, f, ensure_ascii=False, indent=2)

        time.sleep(0.5)

    # Salvar final
    with open(MATRIZ_PATH, "w", encoding="utf-8") as f:
        json.dump(matriz, f, ensure_ascii=False, indent=2)

    # Resumo
    log.info(f"\n{'='*60}")
    log.info(f"MATRIZ DE QUALIDADE")
    log.info(f"{'='*60}")
    log.info(f"{'Campo':15s} {'OK':>5s} {'REPROC':>7s} {'NA':>5s} {'VERIF':>6s}")
    log.info("-" * 40)
    for campo in CAMPOS_TESTAR:
        ok = contadores[campo]["OK"]
        rep = contadores[campo]["REPROCESSAR"]
        na = contadores[campo]["NA"]
        ver = contadores[campo]["VERIFICAR"]
        log.info(f"{campo:15s} {ok:5d} {rep:7d} {na:5d} {ver:6d}")

    total_reproc = sum(contadores[c]["REPROCESSAR"] for c in CAMPOS_TESTAR)
    log.info(f"\nTotal empresa×campo a reprocessar: {total_reproc}")

    conn.close()
    log.info(f"Matriz salva em: {MATRIZ_PATH}")
    log.info("DONE")


if __name__ == "__main__":
    main()

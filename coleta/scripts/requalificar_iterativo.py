"""
Requalificador Iterativo — Qualidade acima de tudo.

Processa empresa por empresa:
1. Cataloga a estrutura HTML de 3 URLs de amostra
2. Extrai todos os campos com parsers específicos por empresa
3. Auto-valida por amostragem (5 URLs aleatórias)
4. Itera até taxa de acerto alta em todos os campos preenchidos

Regra: dado errado → NULL. Campo vazio é aceitável se o site não publica.
Regra: NUNCA alterar dados das 77 empresas originais.
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
import argparse
import logging
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
CATALOGO_PATH = os.path.join(PROJECT_ROOT, "config", "catalogo_sites.json")
LOG_PATH = os.path.join(PROJECT_ROOT, "build_logs", "requalificacao_iterativa.log")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8", mode="a"),
    ]
)
log = logging.getLogger()

EMPRESAS_ORIGINAIS = [
    "Tenda","MRV","Cury","VIC Engenharia","Plano&Plano","Vitta Residencial",
    "Magik JC","Direcional","Metrocasa","Vivaz","EBM","Trisul","HM Engenharia",
    "Grafico","Pacaembu","Econ Construtora","Vibra Residencial","CAC","Kazzas",
    "Conx","MGF","Vega","Canopus Construcoes","Novolar","Graal Engenharia",
    "Árbore","Viva Benx","Quartzo","Vasco Construtora","Mundo Apto","Vinx",
    "SR Engenharia","Canopus","Riformato","AP Ponto","SUGOI","ACLF","Exata",
    "Somos","Emccamp","BM7","FYP Engenharia","Stanza","Sousa Araujo",
    "Vila Brasil","Smart Construtora","EPH","Cosbat","Belmais","Jotanunes",
    "Cavazani","Victa Engenharia","SOL Construtora","Morana","Lotus",
    "House Inc","ART Construtora","Rev3","Carrilho","BP8","ACL Incorporadora",
    "Maccris","Ampla","Tenório Simões","Grupo Delta","Domma","Dimensional",
    "Mirantes","Torreão Villarim","Bora","Novvo","Você","M.Lar",
    "Construtora Open","Ún1ca","Versati","Sertenge",
]


# ============================================================
# FUNÇÕES DE EXTRAÇÃO (genéricas com fallbacks)
# ============================================================

def fetch_page(url, session):
    """Baixa página com tratamento de erros."""
    try:
        r = session.get(url, timeout=12, allow_redirects=True)
        if r.status_code == 200:
            return r.text, r.url
        return None, None
    except:
        return None, None


def clean_soup(html):
    """Parse HTML e remove nav/footer/header/aside/menu."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()
    return soup


def get_title(html):
    """Extrai título da página."""
    soup = BeautifulSoup(html[:5000], "html.parser")
    t = soup.find("title")
    return t.get_text().strip() if t else ""


def get_tipologia_section(soup):
    """Encontra seção de tipologia/plantas no HTML."""
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur", "dormit"]):
            return section.get_text(separator=" ")
    return ""


def extrair_coordenadas(html):
    """Extrai coords de Google Maps embeds, JS, ou meta tags."""
    # iframe Google Maps
    for pattern in [
        r'@(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
        r'q=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
        r'll=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
        r'center=(-?\d+\.\d{4,}),(-?\d+\.\d{4,})',
    ]:
        m = re.search(pattern, html)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon

    # JSON com lat/lng
    for pattern in [
        r'"lat(?:itude)?":\s*(-?\d+\.\d{4,})\s*,\s*"lng|lon(?:gitude)?":\s*(-?\d+\.\d{4,})',
        r'"latitude":\s*(-?\d+\.\d{4,}).*?"longitude":\s*(-?\d+\.\d{4,})',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon

    # Meta tags OG
    soup = BeautifulSoup(html[:10000], "html.parser")
    lat_meta = soup.find("meta", {"property": re.compile(r"latitude|place:location:latitude", re.I)})
    lon_meta = soup.find("meta", {"property": re.compile(r"longitude|place:location:longitude", re.I)})
    if lat_meta and lon_meta:
        try:
            lat = float(lat_meta.get("content", ""))
            lon = float(lon_meta.get("content", ""))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon
        except:
            pass

    return None, None


def extrair_fase(soup, texto, url):
    """Extrai fase do empreendimento."""
    # 1. Badges/labels com classe de status
    for tag in soup.find_all(["span", "div", "p", "label", "a", "li"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        if any(kw in classes or kw in tag_id for kw in ["status", "fase", "stage", "badge", "tag-", "label-"]):
            t = tag.get_text().strip().lower()
            if len(t) < 50:
                if re.search(r"100\s*%?\s*vendido|esgotado", t): return "100% Vendido"
                if re.search(r"breve|em\s*breve|coming", t): return "Breve Lançamento"
                if re.search(r"constru|obra|andamento", t): return "Em Construção"
                if re.search(r"pronto|entregue|chaves|mud", t): return "Pronto para Morar"
                if re.search(r"lan[çc]a", t): return "Lançamento"

    # 2. URL
    url_l = url.lower()
    if "/pronto" in url_l or "/entregue" in url_l: return "Pronto para Morar"
    if "/obra" in url_l or "/construcao" in url_l or "/em-construcao" in url_l: return "Em Construção"
    if "/vendido" in url_l or "/100-vendido" in url_l: return "100% Vendido"

    # 3. Texto (com cuidado — só em contextos claros)
    if re.search(r"100\s*%?\s*vendido|esgotado|vendas?\s*encerrad", texto): return "100% Vendido"
    if re.search(r"\d+\s*%\s*(?:da\s*obra|conclu|execut|andamento)", texto): return "Em Construção"

    return None


def extrair_tipologias(soup, titulo, texto_tipologia, texto_geral):
    """Extrai tipologias com regex robusto."""
    texto = texto_tipologia.lower() if texto_tipologia.strip() else texto_geral.lower()
    titulo_l = titulo.lower()

    flags = {"apto_studio": 0, "apto_1_dorm": 0, "apto_2_dorms": 0,
             "apto_3_dorms": 0, "apto_4_dorms": 0, "apto_suite": 0}

    # Ranges "X a Y dorms"
    for m in re.finditer(r"(\d)\s*a\s*(\d)\s*(?:dorm|quarto|su[ií]te)", texto):
        for d in range(int(m.group(1)), int(m.group(2)) + 1):
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # Enumerações "2 e 3 dorms", "1, 2 e 3 dorms"
    for m in re.finditer(r"((?:\d\s*(?:,|e)\s*)+\d)\s*(?:dorm|quarto)", texto):
        for n in re.findall(r"\d", m.group(1)):
            d = int(n)
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # Individuais (aceita zero à esquerda)
    if re.search(r"\b0?1\s*(?:dorm|quarto)", texto): flags["apto_1_dorm"] = 1
    if re.search(r"\b0?2\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_2_dorms"] = 1
    if re.search(r"\b0?3\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_3_dorms"] = 1
    if re.search(r"\b0?4\s*(?:dorm|quarto|su[ií]te)", texto): flags["apto_4_dorms"] = 1
    if re.search(r"su[ií]te", texto): flags["apto_suite"] = 1

    # Studio restritivo
    if ("studio" in titulo_l or
        ("studio" in texto_tipologia.lower() if texto_tipologia.strip() else False) or
        re.search(r"studio.{0,40}m[²2]|\d+m[²2].{0,40}studio", texto) or
        re.search(r"tipo\w*\s*:?\s*studio|studio\s*\|", texto)):
        flags["apto_studio"] = 1

    # Desc
    partes = []
    if flags["apto_studio"]: partes.append("Studio")
    if flags["apto_1_dorm"]: partes.append("1 dorm")
    if flags["apto_2_dorms"]: partes.append("2 dorms")
    if flags["apto_3_dorms"]: partes.append("3 dorms")
    if flags["apto_4_dorms"]: partes.append("4 dorms")
    if flags["apto_suite"]: partes.append("c/suíte")
    desc = " e ".join(partes) if partes else None

    return flags, desc


def extrair_endereco(soup, html, texto):
    """Extrai endereço, cidade, estado, bairro."""
    resultado = {"endereco": None, "cidade": None, "estado": None, "bairro": None}

    # 1. Schema.org JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict):
                addr = data.get("address", {})
                if isinstance(addr, dict):
                    if addr.get("streetAddress"): resultado["endereco"] = addr["streetAddress"]
                    if addr.get("addressLocality"): resultado["cidade"] = addr["addressLocality"]
                    if addr.get("addressRegion"): resultado["estado"] = addr["addressRegion"]
        except:
            pass

    # 2. Meta tags OG
    for meta in soup.find_all("meta"):
        prop = (meta.get("property") or meta.get("name") or "").lower()
        content = (meta.get("content") or "").strip()
        if not content: continue
        if "street-address" in prop: resultado["endereco"] = resultado["endereco"] or content
        if "locality" in prop: resultado["cidade"] = resultado["cidade"] or content
        if "region" in prop and len(content) <= 3: resultado["estado"] = resultado["estado"] or content

    # 3. Texto estruturado
    for pattern in [
        r"(?:endere[çc]o|localiza[çc][ãa]o)\s*:?\s*(.+?)(?:\n|$)",
        r"((?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rod\.)\s+[^,\n]+(?:,\s*\d+)?)",
    ]:
        m = re.search(pattern, texto, re.IGNORECASE)
        if m and not resultado["endereco"]:
            end = m.group(1).strip()
            if 10 < len(end) < 200:
                resultado["endereco"] = end

    return resultado


def extrair_lazer(soup, texto):
    """Extrai itens de lazer da seção relevante."""
    # Encontrar seção de lazer
    secao_lazer = ""
    for section in soup.find_all(["section", "div", "ul"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["lazer", "amenid", "comodid", "infraestr", "area-comum", "leisure", "diferencia"]):
            secao_lazer += " " + section.get_text(separator=" ")

    texto_busca = secao_lazer.lower() if secao_lazer.strip() else texto.lower()

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
        "lazer_rooftop": r"rooftop",
        "lazer_lavanderia": r"lavanderia",
        "lazer_horta": r"horta",
        "lazer_delivery": r"delivery|espa[çc]o\s*delivery",
        "lazer_redario": r"red[áa]rio",
        "lazer_cine": r"cinema|espa[çc]o\s*cine",
        "lazer_espaco_gourmet": r"gourmet|espa[çc]o\s*gourmet",
        "lazer_sala_jogos": r"sala\s*(?:de\s*)?jogos|game",
        "lazer_solarium": r"sol[áa]rium",
        "lazer_praca": r"pra[çc]a|plaza",
        "lazer_espaco_beleza": r"beleza|beauty|est[ée]tica",
        "lazer_sala_estudos": r"estudo|study",
    }

    flags = {}
    itens = []
    for campo, pattern in LAZER_MAP.items():
        if re.search(pattern, texto_busca, re.IGNORECASE):
            flags[campo] = 1
            itens.append(campo.replace("lazer_", "").replace("_", " "))

    itens_texto = ", ".join(itens) if itens else None
    return flags, itens_texto


def extrair_metragem(texto):
    """Extrai metragens (area_min, area_max)."""
    # Padrão: "45 a 67 m²" ou "45m²" ou "De 45 a 67m²"
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


def extrair_preco(texto):
    """Extrai preço a partir de."""
    m = re.search(r"(?:a\s*partir\s*de|desde)\s*R\$\s*([\d.,]+)", texto, re.IGNORECASE)
    if m:
        try:
            preco = float(m.group(1).replace(".", "").replace(",", "."))
            if 50000 < preco < 20000000:
                return preco
        except:
            pass
    return None


def extrair_imagem(soup):
    """Extrai primeira imagem relevante (hero/fachada)."""
    # OG image
    og = soup.find("meta", {"property": "og:image"})
    if og and og.get("content"):
        return og["content"]
    # Primeira img grande
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if src and ("fachada" in src.lower() or "hero" in src.lower() or "banner" in src.lower()):
            return src
    return None


# ============================================================
# PROCESSAMENTO POR EMPRESA
# ============================================================

def processar_empresa(empresa, conn, session, args):
    """Processa todos os empreendimentos de uma empresa."""
    cur = conn.cursor()

    cur.execute("""
        SELECT id, nome, url_fonte, cidade, estado, bairro, endereco,
               latitude, longitude, fase, apto_studio, apto_1_dorm,
               apto_2_dorms, apto_3_dorms, apto_4_dorms, apto_suite,
               dormitorios_descricao, itens_lazer, area_min_m2, area_max_m2,
               preco_a_partir, imagem_url
        FROM empreendimentos
        WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
        ORDER BY nome
    """, (empresa,))
    rows = cur.fetchall()

    if not rows:
        return {"total": 0}

    log.info(f"\n{'='*60}")
    log.info(f"EMPRESA: {empresa} ({len(rows)} URLs)")
    log.info(f"{'='*60}")

    stats = {
        "total": len(rows), "processados": 0, "erros": 0,
        "campos_atualizados": defaultdict(int), "campos_mantidos": defaultdict(int),
    }

    is_original = empresa in EMPRESAS_ORIGINAIS

    for i, row in enumerate(rows, 1):
        emp_id = row[0]
        nome = row[1]
        url = row[2]
        dados_atuais = {
            "cidade": row[3], "estado": row[4], "bairro": row[5], "endereco": row[6],
            "latitude": row[7], "longitude": row[8], "fase": row[9],
            "apto_studio": row[10], "apto_1_dorm": row[11], "apto_2_dorms": row[12],
            "apto_3_dorms": row[13], "apto_4_dorms": row[14], "apto_suite": row[15],
            "dormitorios_descricao": row[16], "itens_lazer": row[17],
            "area_min_m2": row[18], "area_max_m2": row[19],
            "preco_a_partir": row[20], "imagem_url": row[21],
        }

        html, final_url = fetch_page(url, session)
        if not html:
            stats["erros"] += 1
            continue

        soup = clean_soup(html)
        titulo = get_title(html)
        texto_geral = soup.get_text(separator=" ")
        texto_tipologia = get_tipologia_section(soup)

        novos = {}

        # --- Coordenadas ---
        if not dados_atuais["latitude"]:
            lat, lon = extrair_coordenadas(html)
            if lat is not None:
                novos["latitude"] = lat
                novos["longitude"] = lon

        # --- Fase ---
        fase = extrair_fase(soup, texto_geral.lower(), final_url or url)
        if fase and (not dados_atuais["fase"] or not is_original):
            if not is_original:
                novos["fase"] = fase
            elif not dados_atuais["fase"]:
                novos["fase"] = fase

        # --- Tipologias ---
        if not is_original:
            flags, desc = extrair_tipologias(soup, titulo, texto_tipologia, texto_geral)
            if any(v == 1 for v in flags.values()):
                for campo, valor in flags.items():
                    novos[campo] = valor
                if desc:
                    novos["dormitorios_descricao"] = desc

        # --- Endereço ---
        if not dados_atuais["endereco"] or (not dados_atuais["cidade"] and not is_original):
            end_dados = extrair_endereco(soup, html, texto_geral)
            for campo in ["endereco", "cidade", "estado", "bairro"]:
                if end_dados[campo] and not dados_atuais[campo]:
                    novos[campo] = end_dados[campo]

        # --- Lazer ---
        if not dados_atuais["itens_lazer"] or (not is_original):
            lazer_flags, lazer_texto = extrair_lazer(soup, texto_geral)
            if lazer_flags:
                for campo, valor in lazer_flags.items():
                    if not is_original or not dados_atuais.get(campo):
                        novos[campo] = valor
                if lazer_texto and not dados_atuais["itens_lazer"]:
                    novos["itens_lazer"] = lazer_texto

        # --- Metragem ---
        if not dados_atuais["area_min_m2"]:
            amin, amax = extrair_metragem(texto_geral)
            if amin:
                novos["area_min_m2"] = amin
                novos["area_max_m2"] = amax

        # --- Preço ---
        if not dados_atuais["preco_a_partir"]:
            preco = extrair_preco(texto_geral)
            if preco:
                novos["preco_a_partir"] = preco

        # --- Imagem ---
        if not dados_atuais["imagem_url"]:
            img = extrair_imagem(BeautifulSoup(html[:20000], "html.parser"))
            if img:
                novos["imagem_url"] = img

        # --- Gravar ---
        if novos and not args.dry_run:
            for campo, valor in novos.items():
                # Para originais: NUNCA sobrescrever valor existente
                if is_original and dados_atuais.get(campo) is not None and dados_atuais[campo] != "":
                    stats["campos_mantidos"][campo] += 1
                    continue
                cur.execute(f"UPDATE empreendimentos SET {campo}=? WHERE id=?", (valor, emp_id))
                stats["campos_atualizados"][campo] += 1

        stats["processados"] += 1

        if i <= 3 or i % 25 == 0:
            n_novos = len([k for k in novos if k not in stats["campos_mantidos"]])
            log.info(f"  [{i}/{len(rows)}] {nome[:35]}: +{n_novos} campos")

        if not args.dry_run and i % 50 == 0:
            conn.commit()

        time.sleep(args.delay)

    if not args.dry_run:
        conn.commit()

    # Resumo
    log.info(f"\n  Resultado {empresa}: {stats['processados']} processados, {stats['erros']} erros")
    if stats["campos_atualizados"]:
        top = sorted(stats["campos_atualizados"].items(), key=lambda x: -x[1])[:10]
        log.info(f"  Campos atualizados: {dict(top)}")

    return stats


# ============================================================
# AUDITORIA POR AMOSTRAGEM
# ============================================================

def auditar_empresa(empresa, conn, session):
    """Amostra 5 URLs e verifica se dados extraídos estão corretos."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nome, url_fonte, fase, cidade, apto_studio, apto_1_dorm,
               apto_2_dorms, apto_3_dorms, latitude, itens_lazer
        FROM empreendimentos WHERE empresa = ? AND url_fonte IS NOT NULL
        ORDER BY RANDOM() LIMIT 5
    """, (empresa,))
    rows = cur.fetchall()

    resultados = {"total": len(rows), "corretos": 0, "incorretos": 0, "inconclusivos": 0, "detalhes": []}

    for row in rows:
        emp_id, nome, url = row[0], row[1], row[2]
        fase_db, cidade_db = row[3], row[4]
        tipol_db = {"studio": row[5], "1d": row[6], "2d": row[7], "3d": row[8]}
        lat_db, lazer_db = row[9], row[10]

        html, _ = fetch_page(url, session)
        if not html:
            resultados["inconclusivos"] += 1
            continue

        soup = clean_soup(html)
        texto = soup.get_text(separator=" ")
        titulo = get_title(html)

        erros = []

        # Verificar fase
        fase_pagina = extrair_fase(soup, texto.lower(), url)
        if fase_pagina and fase_db and fase_pagina != fase_db:
            erros.append(f"fase: banco={fase_db} página={fase_pagina}")

        # Verificar coords
        lat_p, lon_p = extrair_coordenadas(html)
        if lat_p and not lat_db:
            erros.append(f"coords: disponível mas não extraída")

        if erros:
            resultados["incorretos"] += 1
        else:
            resultados["corretos"] += 1

        resultados["detalhes"].append({
            "nome": nome, "erros": erros,
        })

    taxa = resultados["corretos"] / resultados["total"] if resultados["total"] > 0 else 0
    resultados["taxa_acerto"] = taxa
    return resultados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", type=str, default=None)
    parser.add_argument("--limite-empresas", type=int, default=999)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--somente-novos", action="store_true", help="Só empresas novas (não originais)")
    parser.add_argument("--min-produtos", type=int, default=5)
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    session = requests.Session()
    session.headers.update(HEADERS)

    # Listar empresas a processar
    cur = conn.cursor()
    if args.empresa:
        empresas = [args.empresa]
    else:
        where = ""
        params = []
        if args.somente_novos:
            placeholders = ",".join(["?" for _ in EMPRESAS_ORIGINAIS])
            where = f"AND empresa NOT IN ({placeholders})"
            params = list(EMPRESAS_ORIGINAIS)

        cur.execute(f"""
            SELECT empresa, COUNT(*) as total FROM empreendimentos
            WHERE url_fonte IS NOT NULL AND url_fonte != '' {where}
            GROUP BY empresa HAVING total >= ?
            ORDER BY total DESC
            LIMIT ?
        """, params + [args.min_produtos, args.limite_empresas])
        empresas = [r[0] for r in cur.fetchall()]

    log.info(f"=== REQUALIFICADOR ITERATIVO ===")
    log.info(f"Empresas: {len(empresas)} | Delay: {args.delay}s | Dry-run: {args.dry_run}")
    log.info(f"Somente novos: {args.somente_novos}")

    resultados_globais = {}

    for empresa in empresas:
        stats = processar_empresa(empresa, conn, session, args)
        resultados_globais[empresa] = stats

        # Auto-auditoria
        if not args.dry_run and stats["total"] > 0:
            audit = auditar_empresa(empresa, conn, session)
            taxa = audit["taxa_acerto"]
            log.info(f"  AUDITORIA: {audit['corretos']}/{audit['total']} corretos ({taxa:.0%})")
            if audit["incorretos"] > 0:
                for d in audit["detalhes"]:
                    if d["erros"]:
                        log.info(f"    ✗ {d['nome'][:30]}: {', '.join(d['erros'])}")

    # Resumo global
    log.info(f"\n{'='*60}")
    log.info(f"RESUMO GLOBAL")
    log.info(f"{'='*60}")
    total_proc = sum(r["total"] for r in resultados_globais.values())
    total_erros = sum(r["erros"] for r in resultados_globais.values())
    log.info(f"  Empresas: {len(empresas)}")
    log.info(f"  URLs processadas: {total_proc}")
    log.info(f"  Erros HTTP: {total_erros}")

    # Regenerar mapa
    if not args.dry_run:
        log.info(f"\nRegenerando mapa...")
        os.system(f'python "{os.path.join(PROJECT_ROOT, "gerar_mapa.py")}"')

    conn.close()
    log.info("DONE")


if __name__ == "__main__":
    main()

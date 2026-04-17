"""
Enriquecimento Autonomo Completo — Todas as 218 empresas, produto a produto.
=============================================================================
Visita cada URL e extrai todos os campos faltantes de uma vez:
  - tipologias_detalhadas (ex: "2D 42m2 | 3D 55m2")
  - fase (Lancamento, Em Construcao, Pronto para Morar, etc.)
  - area_min_m2, area_max_m2
  - coordenadas (lat/lon de Google Maps embarcado)
  - imagem_url (og:image ou hero image)
  - itens_lazer, itens_lazer_raw, itens_marketizaveis
  - endereco, cidade, estado, bairro
  - dormitorios_descricao + flags binarios
  - preco_a_partir
  - total_unidades, numero_vagas

Resumivel: salva progresso em JSON a cada empresa concluida.
Validacao amostral: verifica 5 URLs aleatorias apos cada empresa.
Build log: gera relatorio ao final.

Uso:
    python scripts/enriquecimento_autonomo.py
    python scripts/enriquecimento_autonomo.py --empresa MRV
    python scripts/enriquecimento_autonomo.py --dry-run
    python scripts/enriquecimento_autonomo.py --resumir
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

# Encoding fix for Windows
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
PROGRESSO_PATH = os.path.join(PROJECT_ROOT, "build_logs", "enriquecimento_autonomo_progresso.json")
LOG_PATH = os.path.join(PROJECT_ROOT, "build_logs", "enriquecimento_autonomo.log")

os.makedirs(os.path.join(PROJECT_ROOT, "build_logs"), exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
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


# ============================================================
# FUNCOES DE EXTRACAO (auto-contidas para robustez)
# ============================================================

def fetch_page(url, session, timeout=15):
    """Baixa pagina HTML com tratamento de erros."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.text, r.url
        if r.status_code in (301, 302, 307, 308):
            return r.text, r.url
        return None, None
    except Exception:
        return None, None


def clean_soup(html):
    """Parse HTML e remove nav/footer/header/aside/menu."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()
    return soup


def get_title(html):
    """Extrai titulo da pagina."""
    soup = BeautifulSoup(html[:5000], "html.parser")
    t = soup.find("title")
    return t.get_text().strip() if t else ""


# --- TIPOLOGIAS ---

def _normalizar_tipo(raw):
    """Normaliza tipo: '2 dormitorios' -> '2D', 'studio' -> 'Studio'."""
    raw = raw.strip().lower()
    if raw in ("studio", "loft", "kitnet"):
        return raw.capitalize()
    m = re.match(r'(\d)\s*d\s*(?:\+\s*su[ií]te)?', raw, re.I)
    if m:
        base = f"{m.group(1)}D"
        if "suite" in raw or "suíte" in raw:
            return f"{base}+Suite"
        return base
    m = re.match(r'(\d)\s*(?:dorm|quarto|suite|suíte)', raw, re.I)
    if m:
        num = m.group(1)
        if "suite" in raw or "suíte" in raw:
            return f"{num}D+Suite"
        return f"{num}D"
    # "1" solo (de pattern "1 suíte - 55m²")
    if raw.isdigit() and int(raw) <= 6:
        return f"{raw}D"
    return None


def _add_tipologia(encontrados, tipologias, tipo_norm, metragem_str):
    """Adiciona tipologia ao conjunto se valida."""
    if not tipo_norm or not metragem_str:
        return
    m_val = float(metragem_str.replace(",", "."))
    if 10 < m_val < 500:
        key = f"{tipo_norm} {metragem_str}m\u00b2"
        if key not in encontrados:
            encontrados.add(key)
            tipologias.append(key)


def extrair_tipologias_detalhadas(soup, texto):
    """Extrai tipologias com metragem: 'Studio 26m2 | 2D 42m2 | 2D+Suite 55m2'."""
    tipologias = []

    # Procurar secoes HTML de tipologia/ficha tecnica
    secoes = soup.find_all(attrs={"class": re.compile(r"tipolog|planta|ficha|tipo|plant|floor|configur|imovel|apart", re.I)})
    secoes += soup.find_all(attrs={"id": re.compile(r"tipolog|planta|ficha|tipo|plant|floor|configur|imovel|apart", re.I)})
    secao_texto = ""
    for sec in secoes:
        secao_texto += " " + sec.get_text(separator=" ", strip=True)
    # Buscar em AMBOS: secao + texto completo (secao pode ter ficha-tecnica sem metragens)
    texto_busca = (secao_texto + " " + texto) if secao_texto.strip() else texto

    # === FASE 1: Patterns tipo + metragem simples ===
    patterns_simples = [
        r'(studio|loft|kitnet|(?:\d)\s*(?:dorm(?:it[oó]rio)?s?\.?|quartos?|suites?)(?:\s*\+?\s*su[ií]te)?)\s*[:\-\u2013|]?\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        r'(\d)\s*(?:dorm(?:it[oó]rio)?s?\.?|quartos?)\s*(?:de|com)?\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        r'(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]\s*[:\-\u2013|]?\s*(\d)\s*(?:dorm(?:it[oó]rio)?s?\.?|quartos?)',
        r'(?:planta\s+)?(\d[dD]\s*(?:\+\s*su[ií]te)?)\s*[:\-\u2013|]?\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        r'(studio|loft|kitnet)\s*(?:\||\\n|\s)+\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        # "Studios - 28,93m²" (Tibério style)
        r'(studios?)\s*[\-\u2013]\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        # "1 suíte - 55m²"
        r'(\d)\s*su[ií]tes?\s*[\-\u2013|:]\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
    ]

    encontrados = set()
    for pat in patterns_simples:
        for m in re.finditer(pat, texto_busca, re.IGNORECASE):
            g1, g2 = m.group(1).strip(), m.group(2).strip()
            if g1.isdigit() and len(g1) >= 2:
                metragem, tipo_raw = g1, g2
            elif g2.isdigit() and len(g2) >= 2 and int(g2) > 15:
                metragem, tipo_raw = g2, g1
            elif g1.isdigit() and int(g1) <= 6:
                tipo_raw, metragem = g1, g2
            else:
                tipo_raw, metragem = g1, g2

            tipo_norm = _normalizar_tipo(tipo_raw)
            _add_tipologia(encontrados, tipologias, tipo_norm, metragem)

    # === FASE 2: Ranges "Studio - 21 m² a 24 m²", "1 Dorm. - 24 a 34 m²", "3 dorms. 69 e 71 m²" ===
    range_patterns = [
        # "Studio - 21 m² a 24 m²" ou "Studio - 21 a 24 m²"
        r'(studio|loft|kitnet)\s*[\-\u2013:]\s*(\d{2,4}(?:[.,]\d+)?)\s*(?:m[\u00b22])?\s*(?:a|e|at\u00e9)\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        # "1 Dorm. - 24 m² a 34 m²" ou "2 dorms 30 a 55 m²"
        r'(\d)\s*(?:dorm(?:it[oó]rio)?s?\.?|quartos?|su[ií]tes?)\s*[\-\u2013:.]?\s*(\d{2,4}(?:[.,]\d+)?)\s*(?:m[\u00b22])?\s*(?:a|e|at\u00e9)\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
        # "2D - 30 a 55m²"
        r'(\d[dD](?:\s*\+?\s*su[ií]te)?)\s*[\-\u2013:.]?\s*(\d{2,4}(?:[.,]\d+)?)\s*(?:m[\u00b22])?\s*(?:a|e|at\u00e9)\s*(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]',
    ]

    for pat in range_patterns:
        for m in re.finditer(pat, texto_busca, re.IGNORECASE):
            tipo_raw = m.group(1).strip()
            metro_min = m.group(2).strip()
            metro_max = m.group(3).strip()
            tipo_norm = _normalizar_tipo(tipo_raw)
            _add_tipologia(encontrados, tipologias, tipo_norm, metro_min)
            _add_tipologia(encontrados, tipologias, tipo_norm, metro_max)

    # === FASE 3: Near-proximity "3 dorms. 69 e 71 m²" (tipo e depois metragens) ===
    for m in re.finditer(
        r'(\d)\s*(?:dorm(?:it[oó]rio)?s?\.?|quartos?|su[ií]tes?)\s*.{0,15}?(\d{2,3}(?:[.,]\d+)?)\s*(?:e|,)\s*(\d{2,3}(?:[.,]\d+)?)\s*m[\u00b22]',
        texto_busca, re.IGNORECASE
    ):
        tipo_norm = _normalizar_tipo(m.group(1))
        _add_tipologia(encontrados, tipologias, tipo_norm, m.group(2).strip())
        _add_tipologia(encontrados, tipologias, tipo_norm, m.group(3).strip())

    # === FASE 4: JSON-LD fallback ===
    if not tipologias:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    data = data[0] if data else {}
                floor_plans = data.get("floorSize", [])
                if isinstance(floor_plans, dict):
                    floor_plans = [floor_plans]
                for fp in floor_plans:
                    val = fp.get("value", "")
                    if val:
                        tipologias.append(f"{val}m\u00b2")
            except (json.JSONDecodeError, AttributeError):
                pass

    return " | ".join(tipologias) if tipologias else None


# --- TIPOLOGIAS FLAGS (binarios) ---

def extrair_tipologias_flags(soup, titulo, texto):
    """Extrai flags binarios de tipologia + dormitorios_descricao."""
    # Procurar secao de tipologia
    secao = ""
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur", "dormit"]):
            secao = section.get_text(separator=" ")
            break
    texto_busca = secao.lower() if secao.strip() else texto.lower()
    titulo_l = titulo.lower()

    flags = {"apto_studio": 0, "apto_1_dorm": 0, "apto_2_dorms": 0,
             "apto_3_dorms": 0, "apto_4_dorms": 0, "apto_suite": 0}

    # Ranges
    for m in re.finditer(r"(\d)\s*a\s*(\d)\s*(?:dorm|quarto|su[ií]te)", texto_busca):
        for d in range(int(m.group(1)), int(m.group(2)) + 1):
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # Enumeracoes
    for m in re.finditer(r"((?:\d\s*(?:,|e)\s*)+\d)\s*(?:dorm|quarto)", texto_busca):
        for n in re.findall(r"\d", m.group(1)):
            d = int(n)
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # Individuais
    if re.search(r"\b0?1\s*(?:dorm|quarto)", texto_busca): flags["apto_1_dorm"] = 1
    if re.search(r"\b0?2\s*(?:dorm|quarto|su[ií]te)", texto_busca): flags["apto_2_dorms"] = 1
    if re.search(r"\b0?3\s*(?:dorm|quarto|su[ií]te)", texto_busca): flags["apto_3_dorms"] = 1
    if re.search(r"\b0?4\s*(?:dorm|quarto|su[ií]te)", texto_busca): flags["apto_4_dorms"] = 1
    if re.search(r"su[ií]te", texto_busca): flags["apto_suite"] = 1

    # Studio
    if ("studio" in titulo_l or
        ("studio" in (secao.lower() if secao.strip() else "")) or
        re.search(r"studio.{0,40}m[\u00b22]|\d+m[\u00b22].{0,40}studio", texto_busca) or
        re.search(r"tipo\w*\s*:?\s*studio|studio\s*\|", texto_busca)):
        flags["apto_studio"] = 1

    # Descricao
    partes = []
    if flags["apto_studio"]: partes.append("Studio")
    if flags["apto_1_dorm"]: partes.append("1 dorm")
    if flags["apto_2_dorms"]: partes.append("2 dorms")
    if flags["apto_3_dorms"]: partes.append("3 dorms")
    if flags["apto_4_dorms"]: partes.append("4 dorms")
    if flags["apto_suite"]: partes.append("c/su\u00edte")
    desc = " e ".join(partes) if partes else None

    return flags, desc


# --- COORDENADAS ---

def extrair_coordenadas(html):
    """Extrai coords de Google Maps embeds, JS, ou meta tags."""
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

    for pattern in [
        r'"lat(?:itude)?":\s*(-?\d+\.\d{4,})\s*,\s*"(?:lng|lon(?:gitude)?)"\s*:\s*(-?\d+\.\d{4,})',
        r'"latitude"\s*:\s*(-?\d+\.\d{4,}).*?"longitude"\s*:\s*(-?\d+\.\d{4,})',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon

    soup = BeautifulSoup(html[:10000], "html.parser")
    lat_meta = soup.find("meta", {"property": re.compile(r"latitude|place:location:latitude", re.I)})
    lon_meta = soup.find("meta", {"property": re.compile(r"longitude|place:location:longitude", re.I)})
    if lat_meta and lon_meta:
        try:
            lat = float(lat_meta.get("content", ""))
            lon = float(lon_meta.get("content", ""))
            if -35 < lat < 6 and -75 < lon < -30:
                return lat, lon
        except (ValueError, TypeError):
            pass

    return None, None


# --- FASE ---

def extrair_fase(soup, texto_lower, url):
    """Extrai fase do empreendimento."""
    for tag in soup.find_all(["span", "div", "p", "label", "a", "li"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        if any(kw in classes or kw in tag_id for kw in ["status", "fase", "stage", "badge", "tag-", "label-"]):
            t = tag.get_text().strip().lower()
            if len(t) < 50:
                if re.search(r"100\s*%?\s*vendido|esgotado", t): return "100% Vendido"
                if re.search(r"breve|em\s*breve|coming", t): return "Breve Lan\u00e7amento"
                if re.search(r"constru|obra|andamento", t): return "Em Constru\u00e7\u00e3o"
                if re.search(r"pronto|entregue|chaves|mud", t): return "Pronto para Morar"
                if re.search(r"lan[çc]a", t): return "Lan\u00e7amento"

    url_l = url.lower()
    if "/pronto" in url_l or "/entregue" in url_l: return "Pronto para Morar"
    if "/obra" in url_l or "/construcao" in url_l or "/em-construcao" in url_l: return "Em Constru\u00e7\u00e3o"
    if "/vendido" in url_l or "/100-vendido" in url_l: return "100% Vendido"

    if re.search(r"100\s*%?\s*vendido|esgotado|vendas?\s*encerrad", texto_lower): return "100% Vendido"
    if re.search(r"\d+\s*%\s*(?:da\s*obra|conclu|execut|andamento)", texto_lower): return "Em Constru\u00e7\u00e3o"

    return None


# --- ENDERECO ---

def extrair_endereco(soup, html, texto):
    """Extrai endereco, cidade, estado, bairro."""
    resultado = {"endereco": None, "cidade": None, "estado": None, "bairro": None}

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
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    for meta in soup.find_all("meta"):
        prop = (meta.get("property") or meta.get("name") or "").lower()
        content = (meta.get("content") or "").strip()
        if not content: continue
        if "street-address" in prop: resultado["endereco"] = resultado["endereco"] or content
        if "locality" in prop: resultado["cidade"] = resultado["cidade"] or content
        if "region" in prop and len(content) <= 3: resultado["estado"] = resultado["estado"] or content

    for pattern in [
        r"(?:endere[çc]o|localiza[çc][ãa]o)\s*:?\s*(.+?)(?:\n|$)",
        r"((?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rod\.)\s+[^\n,]+(?:,\s*\d+)?)",
    ]:
        m = re.search(pattern, texto, re.IGNORECASE)
        if m and not resultado["endereco"]:
            end = m.group(1).strip()
            if 10 < len(end) < 200:
                resultado["endereco"] = end

    return resultado


# --- LAZER ---

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

LAZER_KEYWORDS = [
    "piscina", "churrasqueira", "fitness", "academia", "sal\u00e3o de festas",
    "playground", "brinquedoteca", "pet place", "pet care", "coworking",
    "biciclet\u00e1rio", "quadra", "sauna", "spa", "ofur\u00f4", "deck",
    "solarium", "rooftop", "espa\u00e7o gourmet", "lounge", "cinema",
    "game", "jogos", "lavanderia", "mini market", "beauty",
    "pilates", "yoga", "crossfit", "funcional", "pra\u00e7a",
    "jardim", "horta", "fire pit", "hidro", "massagem",
    "sports bar", "bar", "adega", "estudo", "biblioteca",
    "delivery", "car wash", "medita\u00e7\u00e3o",
    "parquinho", "baby", "kids", "teen", "infantil",
    "quadra poliesportiva", "beach tennis", "t\u00eanis",
    "piscina coberta", "piscina aquecida",
    "red\u00e1rio", "gazebo", "pergolado", "mirante",
    "dog run", "dog place",
]


def extrair_lazer(soup, texto):
    """Extrai lazer com flags + itens raw."""
    secao_lazer = ""
    for section in soup.find_all(["section", "div", "ul"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["lazer", "amenid", "comodid", "infraestr", "area-comum", "leisure", "diferencia"]):
            secao_lazer += " " + section.get_text(separator=" ")

    texto_busca = secao_lazer.lower() if secao_lazer.strip() else texto.lower()

    flags = {}
    itens = []
    for campo, pattern in LAZER_MAP.items():
        if re.search(pattern, texto_busca, re.IGNORECASE):
            flags[campo] = 1
            itens.append(campo.replace("lazer_", "").replace("_", " "))

    itens_texto = ", ".join(itens) if itens else None
    return flags, itens_texto


def extrair_itens_lazer_raw(soup, texto):
    """Extracao exaustiva de itens de lazer do site."""
    itens = []
    seen = set()

    def _add(item):
        item = item.strip().strip("\u2022\u2013-\u00b7").strip()
        item = re.sub(r'\s+', ' ', item)
        if len(item) < 3 or len(item) > 80: return
        if re.match(r'^[\d\s\(\)\-\+\.]+$', item): return
        if re.search(r'\(\d{2}\)\s*\d{4}', item): return
        if '@' in item or 'http' in item.lower(): return
        key = item.lower()
        if key not in seen:
            seen.add(key)
            itens.append(item)

    secoes = soup.find_all(attrs={"class": re.compile(r"lazer|amenid|infraestrutura|diferenc|leisure|amenities|facilities|common", re.I)})
    secoes += soup.find_all(attrs={"id": re.compile(r"lazer|amenid|infraestrutura|diferenc|leisure|amenities", re.I)})
    for sec in secoes:
        for li in sec.find_all("li"):
            txt = li.get_text(strip=True)
            if txt: _add(txt)
        for tag in sec.find_all(["h3", "h4", "h5", "span", "p", "strong"]):
            txt = tag.get_text(strip=True)
            if txt and len(txt) < 60: _add(txt)

    for img in soup.find_all("img"):
        alt = (img.get("alt", "") or "").strip()
        if alt:
            alt_lower = alt.lower()
            if any(kw in alt_lower for kw in LAZER_KEYWORDS[:30]):
                _add(alt)

    if len(itens) < 3:
        texto_busca = texto.lower()
        for kw in LAZER_KEYWORDS:
            if kw in texto_busca:
                _add(kw.title())

    return " | ".join(itens) if itens else None


# --- METRAGEM ---

def extrair_metragem(texto):
    """Extrai metragens (area_min, area_max)."""
    matches = re.findall(r"(\d+(?:[.,]\d+)?)\s*(?:a|e|-|at\u00e9)\s*(\d+(?:[.,]\d+)?)\s*m[\u00b22]", texto)
    if matches:
        areas = []
        for a, b in matches:
            areas.extend([float(a.replace(",", ".")), float(b.replace(",", "."))])
        areas = [a for a in areas if 10 < a < 500]
        if areas:
            return min(areas), max(areas)

    singles = re.findall(r"(\d+(?:[.,]\d+)?)\s*m[\u00b22]", texto)
    if singles:
        areas = [float(a.replace(",", ".")) for a in singles]
        areas = [a for a in areas if 10 < a < 500]
        if areas:
            return min(areas), max(areas)

    return None, None


# --- PRECO ---

def extrair_preco(texto):
    """Extrai preco a partir de."""
    patterns = [
        r'(?:a\s*partir\s*(?:de)?|desde|pre[cç]o)\s*(?:de\s*)?R\$\s*([\d.,]+)',
        r'R\$\s*([\d]{2,3}(?:[.,]\d{3})+(?:[.,]\d{2})?)',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = m.group(1).replace(".", "").replace(",", ".")
            try:
                preco = float(val)
                if 50000 < preco < 20000000:
                    return preco
            except ValueError:
                pass
    return None


# --- IMAGEM ---

def extrair_imagem(html):
    """Extrai URL da imagem principal (og:image, hero, fachada, perspectiva, etc.)."""
    # 1. og:image (scan apenas head)
    soup_head = BeautifulSoup(html[:20000], "html.parser")
    og = soup_head.find("meta", {"property": "og:image"})
    if og and og.get("content"):
        url = og["content"].strip()
        if url and len(url) > 10 and not url.endswith(".ico"):
            return url

    # 2. Scan HTML mais amplo para imagens com keywords
    soup = BeautifulSoup(html[:80000], "html.parser")
    # Remover nav/footer para evitar logos
    for tag in soup.find_all(["nav", "footer", "header"]):
        tag.decompose()

    IMG_KEYWORDS = [
        "fachada", "hero", "banner", "perspectiva", "capa", "render",
        "empreendimento", "destaque", "principal", "cover", "featured",
        "thumb", "produto", "external", "exterior",
    ]

    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy") or img.get("data-original") or ""
        alt = (img.get("alt") or "").lower()
        src_lower = src.lower()
        if not src or src.startswith("data:") or src.endswith((".svg", ".gif", ".ico")):
            continue
        if any(kw in src_lower or kw in alt for kw in IMG_KEYWORDS):
            return src

    # 3. Fallback: primeira imagem grande (jpg/png/webp) no conteudo principal
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy") or img.get("data-original") or ""
        if not src or src.startswith("data:") or src.endswith((".svg", ".gif", ".ico")):
            continue
        src_lower = src.lower()
        # Pular icones, logos, avatars
        if any(skip in src_lower for skip in ["logo", "icon", "avatar", "whatsapp", "favicon", "placeholder", "blank.gif"]):
            continue
        if any(ext in src_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            return src

    return None


# --- UNIDADES / VAGAS ---

def extrair_unidades_vagas(texto):
    """Extrai total de unidades e vagas."""
    dados = {}
    for pat in [r'(\d+)\s*(?:unidades|apartamentos|aptos)', r'(?:unidades|apartamentos)\s*[:=]?\s*(\d+)']:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 5 <= val <= 10000:
                dados["total_unidades"] = val
                break

    for pat in [r'(\d+)\s*vagas?\b', r'vagas?\s*[:=]?\s*(\d+)']:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = m.group(1)
            if val and int(val) >= 1:
                dados["numero_vagas"] = val
                break

    return dados


# --- MARKETIZAVEIS ---

MARKETIZAVEIS = [
    ("Cortina blackout", ["blackout", "cortina integrada"]),
    ("Veneziana integrada", ["veneziana integrada"]),
    ("Tomada USB", ["tomada usb", "usb"]),
    ("Ponto grill varanda", ["ponto grill", "prepara\u00e7\u00e3o churrasqueira"]),
    ("Prep ar condicionado", ["prepara\u00e7\u00e3o ar", "infra split", "infra vrf", "ar condicionado"]),
    ("Aquecimento g\u00e1s/solar", ["aquecimento a g\u00e1s", "aquecedor solar", "aquecimento solar"]),
    ("Medi\u00e7\u00e3o individualizada", ["medi\u00e7\u00e3o individual", "hidr\u00f4metro individual"]),
    ("Portaria com clausura", ["clausura", "eclusa pedestres"]),
    ("Lazer no rooftop", ["lazer rooftop", "rooftop"]),
    ("Contrapiso nivelado", ["contrapiso nivelado"]),
    ("Esquadria ac\u00fastica", ["ac\u00fastic", "vidro ac\u00fastic"]),
    ("Reuso de \u00e1gua", ["reuso \u00e1gua", "reaproveitamento \u00e1gua"]),
    ("Energia solar", ["energia solar", "fotovoltaic", "painel solar"]),
    ("Smart home", ["smart home", "automa\u00e7\u00e3o", "casa inteligente"]),
    ("Varanda gourmet", ["varanda gourmet", "varanda integrada", "terra\u00e7o gourmet"]),
    ("Carregador VE", ["carregador ve\u00edculo", "carro el\u00e9trico", "eletroposto"]),
]


def extrair_itens_marketizaveis(texto):
    """Detecta diferenciais premium."""
    texto_lower = texto.lower()
    encontrados = []
    for nome, keywords in MARKETIZAVEIS:
        for kw in keywords:
            if kw.lower() in texto_lower:
                encontrados.append(nome)
                break
    return " | ".join(encontrados) if encontrados else None


# ============================================================
# PROGRESSO (resumivel)
# ============================================================

def carregar_progresso():
    if os.path.exists(PROGRESSO_PATH):
        try:
            with open(PROGRESSO_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"empresas_concluidas": [], "snapshot_antes": {}, "stats": {}}


def salvar_progresso(prog):
    with open(PROGRESSO_PATH, "w", encoding="utf-8") as f:
        json.dump(prog, f, indent=2, ensure_ascii=False)


# ============================================================
# SNAPSHOT ANTES (para build log comparativo)
# ============================================================

def snapshot_completude(conn):
    """Tira foto da completude atual por campo."""
    cur = conn.cursor()
    campos = [
        "tipologias_detalhadas", "fase", "area_min_m2", "area_max_m2",
        "latitude", "longitude", "endereco", "cidade",
        "imagem_url", "itens_lazer", "itens_lazer_raw", "itens_marketizaveis",
        "dormitorios_descricao", "preco_a_partir", "total_unidades", "numero_vagas",
    ]
    snap = {}
    for campo in campos:
        try:
            if campo in ("area_min_m2", "area_max_m2", "preco_a_partir"):
                cur.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {campo} IS NOT NULL AND {campo} > 0")
            else:
                cur.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {campo} IS NOT NULL AND {campo} != ''")
            snap[campo] = cur.fetchone()[0]
        except Exception:
            snap[campo] = 0
    snap["total"] = cur.execute("SELECT COUNT(*) FROM empreendimentos").fetchone()[0]
    return snap


# ============================================================
# CONVERSORES OFFLINE (sem visitar URL)
# ============================================================

def converter_metragens_para_tipologias(metragens_desc):
    """Converte metragens_descricao para tipologias_detalhadas.
    Ex: '2 Quartos (PCD): 42.02m² | 2 Quartos: 39.3m²' -> '2D 42m² | 2D 39m²'
    """
    if not metragens_desc:
        return None

    tipologias = []
    encontrados = set()

    for parte in metragens_desc.split("|"):
        parte = parte.strip()
        if not parte:
            continue

        # Pattern: "2 Quartos (PCD): 42.02m²" ou "Studio: 26m²" ou "3 Dorms c/suite: 85m²"
        m = re.match(
            r'(studio|loft|\d\s*(?:quartos?|dorm(?:it[oó]rio)?s?\.?|su[ií]tes?)(?:\s*(?:c/|com\s*)?su[ií]te)?)'
            r'(?:\s*\([^)]*\))?'  # "(PCD)" opcional
            r'\s*[:\-\u2013]?\s*'
            r'(\d{2,4}(?:[.,]\d+)?)\s*m[\u00b22]?',
            parte, re.IGNORECASE
        )
        if m:
            tipo_raw = m.group(1).strip()
            metragem = m.group(2).strip()
            tipo_norm = _normalizar_tipo(tipo_raw)
            if tipo_norm:
                m_val = float(metragem.replace(",", "."))
                if 10 < m_val < 500:
                    metragem_int = str(int(round(m_val)))
                    key = f"{tipo_norm} {metragem_int}m\u00b2"
                    if key not in encontrados:
                        encontrados.add(key)
                        tipologias.append(key)

    return " | ".join(tipologias) if tipologias else None


def extrair_dados_next_data(html):
    """Extrai dados de __NEXT_DATA__ para sites Next.js (Pride, etc.)."""
    soup = BeautifulSoup(html[:200000], "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return {}

    try:
        data = json.loads(script.string)
    except (json.JSONDecodeError, TypeError):
        return {}

    resultado = {}

    # Navegar pela estrutura de props
    props = data.get("props", {}).get("pageProps", {})

    # Tentar encontrar dados do empreendimento em varias estruturas
    emp_data = None
    for key in ["empreendimento", "product", "data", "imovel", "property"]:
        if key in props and isinstance(props[key], dict):
            emp_data = props[key]
            break

    if not emp_data:
        # Tentar primeiro nivel de props
        emp_data = props

    if not isinstance(emp_data, dict):
        return {}

    # Extrair campos comuns
    # Tipologia/metragem
    for key in ["titulo", "title", "nome", "name"]:
        val = emp_data.get(key, "")
        if isinstance(val, str):
            m = re.search(r'(\d{2,3}(?:[.,]\d+)?)\s*m', val)
            if m:
                resultado["_metragem_titulo"] = m.group(1)

    # Quartos
    for key in ["quartos", "dormitorios", "bedrooms", "rooms", "initialRooms"]:
        val = emp_data.get(key, "")
        if isinstance(val, str):
            m = re.search(r'(\d)', val)
            if m:
                resultado["_quartos"] = m.group(1)

    # Fase/status
    for key in ["status", "fase", "stage", "situacao"]:
        val = emp_data.get(key, "")
        if isinstance(val, str) and val:
            resultado["_fase_raw"] = val

    # Area
    for key in ["area", "areaUtil", "areaPrivativa", "metragem", "area_min", "area_max"]:
        val = emp_data.get(key)
        if val:
            try:
                v = float(str(val).replace(",", ".").replace("m²", "").strip())
                if 10 < v < 500:
                    resultado["_area"] = v
            except (ValueError, TypeError):
                pass

    # Imagem
    for key in ["imagem", "image", "thumbnail", "foto", "capa", "cover"]:
        val = emp_data.get(key)
        if isinstance(val, str) and val and len(val) > 10:
            resultado["_imagem"] = val
            break
        elif isinstance(val, dict):
            for sub_key in ["url", "src", "link"]:
                if sub_key in val and val[sub_key]:
                    resultado["_imagem"] = val[sub_key]
                    break

    # Endereço
    for key in ["endereco", "address", "localizacao", "location"]:
        val = emp_data.get(key)
        if isinstance(val, str) and val:
            resultado["_endereco"] = val
        elif isinstance(val, dict):
            resultado["_endereco"] = val.get("rua", val.get("street", val.get("logradouro", "")))
            resultado["_cidade"] = val.get("cidade", val.get("city", ""))
            resultado["_bairro"] = val.get("bairro", val.get("neighborhood", ""))

    # Coordenadas
    for key in ["latitude", "lat"]:
        val = emp_data.get(key)
        if val:
            try:
                lat = float(val)
                if -35 < lat < 6:
                    resultado["_latitude"] = lat
            except (ValueError, TypeError):
                pass
    for key in ["longitude", "lng", "lon"]:
        val = emp_data.get(key)
        if val:
            try:
                lon = float(val)
                if -75 < lon < -30:
                    resultado["_longitude"] = lon
            except (ValueError, TypeError):
                pass

    # === Sub-objetos: EmpreendimentosItem (Pride, etc.) ===
    item_data = emp_data.get("EmpreendimentosItem", {})
    if isinstance(item_data, dict):
        ps = item_data.get("projectStatus")
        if isinstance(ps, list) and len(ps) >= 2:
            resultado["_fase_raw"] = resultado.get("_fase_raw") or ps[1]

        ir = item_data.get("initialRooms", "")
        if ir and "_quartos" not in resultado:
            mq = re.search(r'(\d)', str(ir))
            if mq:
                resultado["_quartos"] = mq.group(1)

        for img_key in ["imagemDestaque", "bannerDestaque"]:
            img = item_data.get(img_key, {})
            if isinstance(img, dict) and img.get("guid"):
                resultado["_imagem"] = resultado.get("_imagem") or img["guid"]

        end = item_data.get("endereco", {})
        if isinstance(end, dict) and end.get("bairro"):
            resultado["_bairro"] = resultado.get("_bairro") or end["bairro"]

        loc = item_data.get("localizacao", {})
        if isinstance(loc, dict):
            iframe = loc.get("iframe", "")
            if iframe and "_latitude" not in resultado:
                mc = re.search(r'!2d(-?\d+\.\d+).*?!3d(-?\d+\.\d+)', iframe)
                if mc:
                    lon_v, lat_v = float(mc.group(1)), float(mc.group(2))
                    if -35 < lat_v < 6 and -75 < lon_v < -30:
                        resultado["_latitude"] = lat_v
                        resultado["_longitude"] = lon_v

    # === Sub-objetos: CorpoDaPagina sections ===
    corpo = emp_data.get("CorpoDaPagina", {})
    if isinstance(corpo, dict):
        sections = corpo.get("sections", [])
        if isinstance(sections, list):
            for sec in sections:
                if not isinstance(sec, dict):
                    continue
                for k, v in sec.items():
                    if isinstance(v, str) and "m" in v.lower() and "_metragem_titulo" not in resultado:
                        mt = re.search(r'(\d{2,3}(?:[.,]\d+)?)\s*m', v)
                        if mt:
                            resultado["_metragem_titulo"] = mt.group(1)
                    if isinstance(v, list):
                        for sub in v:
                            if isinstance(sub, dict):
                                t = sub.get("titulo", "")
                                q = sub.get("quartos", "")
                                if isinstance(t, str) and "m" in t.lower() and "_metragem_titulo" not in resultado:
                                    mt2 = re.search(r'(\d{2,3}(?:[.,]\d+)?)\s*m', t)
                                    if mt2:
                                        resultado["_metragem_titulo"] = mt2.group(1)
                                if isinstance(q, str) and q and "_quartos" not in resultado:
                                    mq2 = re.search(r'(\d)', q)
                                    if mq2:
                                        resultado["_quartos"] = mq2.group(1)

    return resultado


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_empresa(empresa, conn, session, dry_run=False, delay=1.0):
    """Processa todos os produtos de uma empresa, URL por URL."""
    cur = conn.cursor()

    cur.execute("""
        SELECT id, nome, url_fonte, empresa, slug,
               tipologias_detalhadas, fase, area_min_m2, area_max_m2,
               latitude, longitude, endereco, cidade, estado, bairro,
               imagem_url, itens_lazer, itens_lazer_raw, itens_marketizaveis,
               dormitorios_descricao, preco_a_partir, total_unidades, numero_vagas,
               apto_studio, apto_1_dorm, apto_2_dorms, apto_3_dorms, apto_4_dorms, apto_suite,
               metragens_descricao
        FROM empreendimentos
        WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
        ORDER BY nome
    """, (empresa,))
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    if not rows:
        return {"total": 0, "processados": 0, "erros": 0, "atualizados": defaultdict(int)}

    log.info(f"\n{'='*60}")
    log.info(f"EMPRESA: {empresa} ({len(rows)} produtos)")
    log.info(f"{'='*60}")

    stats = {"total": len(rows), "processados": 0, "erros": 0,
             "atualizados": defaultdict(int), "sem_html": 0}

    for i, row in enumerate(rows, 1):
        dados_atuais = dict(zip(col_names, row))
        emp_id = dados_atuais["id"]
        nome = dados_atuais["nome"]
        url = dados_atuais["url_fonte"]

        try:
            novos = {}

            # === PRE-FETCH: Conversoes offline (sem precisar visitar URL) ===

            # Converter metragens_descricao -> tipologias_detalhadas
            if not dados_atuais.get("tipologias_detalhadas") and dados_atuais.get("metragens_descricao"):
                val = converter_metragens_para_tipologias(dados_atuais["metragens_descricao"])
                if val:
                    novos["tipologias_detalhadas"] = val

            # === FETCH: Visitar URL e extrair dados ===
            html, final_url = fetch_page(url, session)
            if not html:
                stats["sem_html"] += 1
                stats["erros"] += 1
                # Mesmo sem HTML, salvar conversoes offline
                if novos and not dry_run:
                    sets = ", ".join([f"{k}=?" for k in novos.keys()])
                    vals = list(novos.values()) + [emp_id]
                    cur.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
                    for campo in novos:
                        stats["atualizados"][campo] += 1
                    stats["processados"] += 1
                elif i <= 3:
                    log.info(f"  [{i}/{len(rows)}] {nome[:40]}: SEM HTML")
                continue

            soup = clean_soup(html)
            titulo = get_title(html)
            texto = soup.get_text(separator=" ", strip=True)
            texto_lower = texto.lower()

            # === Tentar __NEXT_DATA__ para sites Next.js ===
            next_data = extrair_dados_next_data(html)

            # --- Tipologias detalhadas ---
            if not novos.get("tipologias_detalhadas") and not dados_atuais.get("tipologias_detalhadas"):
                val = extrair_tipologias_detalhadas(soup, texto)
                if not val and next_data:
                    # Tentar montar de __NEXT_DATA__
                    quartos = next_data.get("_quartos")
                    metr = next_data.get("_metragem_titulo") or (str(int(next_data["_area"])) if next_data.get("_area") else None)
                    if quartos and metr:
                        tipo_norm = _normalizar_tipo(quartos)
                        if tipo_norm:
                            val = f"{tipo_norm} {metr}m\u00b2"
                if val:
                    novos["tipologias_detalhadas"] = val

            # --- Tipologias flags + dormitorios ---
            if not dados_atuais.get("dormitorios_descricao"):
                flags, desc = extrair_tipologias_flags(soup, titulo, texto)
                if any(v == 1 for v in flags.values()):
                    for campo, valor in flags.items():
                        if not dados_atuais.get(campo):
                            novos[campo] = valor
                    if desc:
                        novos["dormitorios_descricao"] = desc

            # --- Fase ---
            if not dados_atuais.get("fase"):
                fase = extrair_fase(soup, texto_lower, final_url or url)
                if not fase and next_data.get("_fase_raw"):
                    raw = next_data["_fase_raw"].lower()
                    if "pronto" in raw or "entregue" in raw: fase = "Pronto para Morar"
                    elif "constru" in raw or "obra" in raw: fase = "Em Constru\u00e7\u00e3o"
                    elif "lan\u00e7a" in raw or "lanca" in raw: fase = "Lan\u00e7amento"
                    elif "breve" in raw: fase = "Breve Lan\u00e7amento"
                    elif "vendido" in raw or "esgotado" in raw: fase = "100% Vendido"
                if fase:
                    novos["fase"] = fase

            # --- Metragem ---
            if not dados_atuais.get("area_min_m2") or dados_atuais.get("area_min_m2") == 0:
                amin, amax = extrair_metragem(texto)
                if not amin and next_data.get("_area"):
                    amin = amax = next_data["_area"]
                if amin:
                    novos["area_min_m2"] = amin
                    novos["area_max_m2"] = amax

            # --- Coordenadas ---
            if not dados_atuais.get("latitude"):
                lat, lon = extrair_coordenadas(html)
                if lat is None and next_data.get("_latitude"):
                    lat = next_data["_latitude"]
                    lon = next_data.get("_longitude")
                if lat is not None:
                    novos["latitude"] = str(lat)
                    novos["longitude"] = str(lon)

            # --- Endereco ---
            if not dados_atuais.get("endereco") or not dados_atuais.get("cidade"):
                end_dados = extrair_endereco(soup, html, texto)
                # Fallback __NEXT_DATA__
                if not end_dados["endereco"] and next_data.get("_endereco"):
                    end_dados["endereco"] = next_data["_endereco"]
                if not end_dados["cidade"] and next_data.get("_cidade"):
                    end_dados["cidade"] = next_data["_cidade"]
                if not end_dados["bairro"] and next_data.get("_bairro"):
                    end_dados["bairro"] = next_data["_bairro"]
                for campo in ["endereco", "cidade", "estado", "bairro"]:
                    if end_dados[campo] and not dados_atuais.get(campo):
                        novos[campo] = end_dados[campo]

            # --- Imagem ---
            if not dados_atuais.get("imagem_url"):
                img = extrair_imagem(html)
                if not img and next_data.get("_imagem"):
                    img = next_data["_imagem"]
                if img:
                    novos["imagem_url"] = img

            # --- Lazer ---
            if not dados_atuais.get("itens_lazer"):
                lazer_flags, lazer_texto = extrair_lazer(soup, texto)
                if lazer_flags:
                    for campo, valor in lazer_flags.items():
                        novos[campo] = valor
                    if lazer_texto:
                        novos["itens_lazer"] = lazer_texto

            if not dados_atuais.get("itens_lazer_raw"):
                val = extrair_itens_lazer_raw(soup, texto)
                if val:
                    novos["itens_lazer_raw"] = val

            if not dados_atuais.get("itens_marketizaveis"):
                val = extrair_itens_marketizaveis(texto)
                if val:
                    novos["itens_marketizaveis"] = val

            # --- Preco ---
            if not dados_atuais.get("preco_a_partir") or dados_atuais.get("preco_a_partir") == 0:
                preco = extrair_preco(texto)
                if preco:
                    novos["preco_a_partir"] = preco

            # --- Unidades e Vagas ---
            if not dados_atuais.get("total_unidades") or not dados_atuais.get("numero_vagas"):
                uv = extrair_unidades_vagas(texto)
                for campo, valor in uv.items():
                    if not dados_atuais.get(campo) or dados_atuais.get(campo) == 0:
                        novos[campo] = valor

            # --- Gravar ---
            if novos and not dry_run:
                sets = ", ".join([f"{k}=?" for k in novos.keys()])
                vals = list(novos.values()) + [emp_id]
                cur.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)

                for campo in novos:
                    stats["atualizados"][campo] += 1

            stats["processados"] += 1

            if i <= 3 or i % 50 == 0 or i == len(rows):
                n_novos = len(novos)
                campos_str = ", ".join(sorted(novos.keys())[:5]) if novos else "-"
                log.info(f"  [{i}/{len(rows)}] {nome[:40]}: +{n_novos} ({campos_str})")

        except Exception as e:
            stats["erros"] += 1
            if i <= 5:
                log.info(f"  [{i}/{len(rows)}] {nome[:40]}: ERRO: {e}")

        if not dry_run and i % 50 == 0:
            conn.commit()

        time.sleep(delay)

    if not dry_run:
        conn.commit()

    # Resumo
    log.info(f"\n  Resultado {empresa}: {stats['processados']}/{stats['total']} processados, "
             f"{stats['erros']} erros, {stats['sem_html']} sem HTML")
    if stats["atualizados"]:
        top = sorted(stats["atualizados"].items(), key=lambda x: -x[1])[:8]
        log.info(f"  Campos atualizados: {dict(top)}")

    return stats


# ============================================================
# AUDITORIA POR AMOSTRAGEM
# ============================================================

def auditar_empresa(empresa, conn, session):
    """Amostra 5 URLs e verifica consistencia."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nome, url_fonte, fase, latitude, tipologias_detalhadas, imagem_url
        FROM empreendimentos WHERE empresa = ? AND url_fonte IS NOT NULL
        ORDER BY RANDOM() LIMIT 5
    """, (empresa,))
    rows = cur.fetchall()

    resultados = {"total": len(rows), "ok": 0, "problemas": 0, "inacessiveis": 0, "detalhes": []}

    for row in rows:
        emp_id, nome, url, fase_db, lat_db, tip_db, img_db = row
        html, final_url = fetch_page(url, session)
        if not html:
            resultados["inacessiveis"] += 1
            continue

        alertas = []
        soup = clean_soup(html)
        texto = soup.get_text(separator=" ", strip=True)
        titulo = get_title(html)

        # Verificar imagem: se nao tem no banco mas tem og:image na pagina
        if not img_db:
            img_pag = extrair_imagem(html)
            if img_pag:
                alertas.append("imagem: disponivel mas nao extraida")

        # Verificar fase
        fase_pag = extrair_fase(soup, texto.lower(), final_url or url)
        if fase_pag and fase_db and fase_pag != fase_db:
            alertas.append(f"fase: banco={fase_db} pagina={fase_pag}")

        # Verificar coords
        lat_p, _ = extrair_coordenadas(html)
        if lat_p and not lat_db:
            alertas.append("coords: disponivel mas nao extraida")

        # Verificar tipologia
        tip_pag = extrair_tipologias_detalhadas(soup, texto)
        if tip_pag and not tip_db:
            alertas.append("tipologia: disponivel mas nao extraida")

        if alertas:
            resultados["problemas"] += 1
        else:
            resultados["ok"] += 1

        resultados["detalhes"].append({"nome": nome[:35], "alertas": alertas})
        time.sleep(0.5)

    return resultados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento autonomo completo")
    parser.add_argument("--empresa", type=str, default=None, help="Processar apenas essa empresa")
    parser.add_argument("--dry-run", action="store_true", help="Nao gravar no banco")
    parser.add_argument("--resumir", action="store_true", help="Retomar de onde parou")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay entre requests (segundos)")
    parser.add_argument("--timeout-empresa", type=int, default=600, help="Timeout por empresa (segundos)")
    parser.add_argument("--skip-auditoria", action="store_true", help="Pular auditoria amostral")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    session = requests.Session()
    session.headers.update(HEADERS)

    progresso = carregar_progresso() if args.resumir else {"empresas_concluidas": [], "snapshot_antes": {}, "stats": {}}

    # Snapshot antes
    if not progresso.get("snapshot_antes"):
        progresso["snapshot_antes"] = snapshot_completude(conn)
        salvar_progresso(progresso)

    # Listar empresas
    cur = conn.cursor()
    if args.empresa:
        empresas = [args.empresa]
    else:
        cur.execute("""
            SELECT empresa, COUNT(*) as total
            FROM empreendimentos
            WHERE url_fonte IS NOT NULL AND url_fonte != ''
            GROUP BY empresa
            ORDER BY total DESC
        """)
        empresas = [r[0] for r in cur.fetchall()]

    # Filtrar ja concluidas
    if args.resumir:
        empresas = [e for e in empresas if e not in progresso["empresas_concluidas"]]

    log.info(f"{'='*60}")
    log.info(f"ENRIQUECIMENTO AUTONOMO — {len(empresas)} empresas")
    log.info(f"Dry-run: {args.dry_run} | Delay: {args.delay}s | Resumir: {args.resumir}")
    log.info(f"{'='*60}")

    total_global = {"empresas": 0, "produtos": 0, "atualizacoes": 0, "erros": 0}
    inicio = time.time()

    for idx, empresa in enumerate(empresas, 1):
        t0 = time.time()
        log.info(f"\n[{idx}/{len(empresas)}] Processando: {empresa}")

        try:
            stats = processar_empresa(empresa, conn, session, dry_run=args.dry_run, delay=args.delay)

            total_global["empresas"] += 1
            total_global["produtos"] += stats.get("processados", 0)
            total_global["atualizacoes"] += sum(stats.get("atualizados", {}).values())
            total_global["erros"] += stats.get("erros", 0)

            # Auditoria amostral (se nao pulada e empresa tem >5 produtos)
            if not args.skip_auditoria and not args.dry_run and stats.get("total", 0) >= 5:
                audit = auditar_empresa(empresa, conn, session)
                taxa = audit["ok"] / audit["total"] if audit["total"] > 0 else 0
                log.info(f"  AUDITORIA: {audit['ok']}/{audit['total']} OK ({taxa:.0%})")
                if audit["problemas"] > 0:
                    for d in audit["detalhes"]:
                        if d["alertas"]:
                            log.info(f"    ! {d['nome']}: {', '.join(d['alertas'])}")

            # Salvar progresso
            progresso["empresas_concluidas"].append(empresa)
            progresso["stats"][empresa] = {
                "total": stats.get("total", 0),
                "processados": stats.get("processados", 0),
                "erros": stats.get("erros", 0),
                "atualizados": dict(stats.get("atualizados", {})),
            }
            salvar_progresso(progresso)

            elapsed = time.time() - t0
            log.info(f"  Tempo: {elapsed:.0f}s")

        except Exception as e:
            log.info(f"  ERRO FATAL em {empresa}: {e}")
            total_global["erros"] += 1

    # Snapshot depois
    snapshot_depois = snapshot_completude(conn)
    conn.close()

    # Resumo final
    elapsed_total = time.time() - inicio
    log.info(f"\n{'='*60}")
    log.info(f"RESUMO FINAL")
    log.info(f"{'='*60}")
    log.info(f"Empresas processadas: {total_global['empresas']}")
    log.info(f"Produtos processados: {total_global['produtos']}")
    log.info(f"Atualizacoes totais: {total_global['atualizacoes']}")
    log.info(f"Erros totais: {total_global['erros']}")
    log.info(f"Tempo total: {elapsed_total/60:.1f} minutos")
    log.info(f"\nCOMPLETUDE ANTES vs DEPOIS:")
    for campo in sorted(snapshot_depois.keys()):
        if campo == "total": continue
        antes = progresso["snapshot_antes"].get(campo, 0)
        depois = snapshot_depois[campo]
        delta = depois - antes
        total = snapshot_depois["total"]
        pct = (depois / total) * 100
        sinal = f"+{delta}" if delta > 0 else str(delta)
        log.info(f"  {campo}: {antes} -> {depois} ({sinal}) [{pct:.1f}%]")

    # Salvar build log
    build_log_path = os.path.join(PROJECT_ROOT, "build_logs", f"enriquecimento_autonomo_{datetime.now().strftime('%Y%m%d_%H%M')}.md")
    with open(build_log_path, "w", encoding="utf-8") as f:
        f.write(f"# Enriquecimento Aut\u00f4nomo — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"## Resumo\n")
        f.write(f"- Empresas: {total_global['empresas']}\n")
        f.write(f"- Produtos processados: {total_global['produtos']}\n")
        f.write(f"- Atualiza\u00e7\u00f5es: {total_global['atualizacoes']}\n")
        f.write(f"- Erros: {total_global['erros']}\n")
        f.write(f"- Tempo: {elapsed_total/60:.1f} min\n\n")
        f.write(f"## Completude Antes vs Depois\n\n")
        f.write(f"| Campo | Antes | Depois | Delta | % |\n")
        f.write(f"|-------|-------|--------|-------|---|\n")
        for campo in sorted(snapshot_depois.keys()):
            if campo == "total": continue
            antes = progresso["snapshot_antes"].get(campo, 0)
            depois = snapshot_depois[campo]
            delta = depois - antes
            total = snapshot_depois["total"]
            pct = (depois / total) * 100
            f.write(f"| {campo} | {antes} | {depois} | +{delta} | {pct:.1f}% |\n")
        f.write(f"\n## Detalhes por Empresa\n\n")
        for emp, st in progresso.get("stats", {}).items():
            if st.get("atualizados"):
                f.write(f"### {emp}\n")
                f.write(f"- Processados: {st['processados']}/{st['total']}, Erros: {st['erros']}\n")
                f.write(f"- Campos: {st['atualizados']}\n\n")

    log.info(f"\nBuild log salvo em: {build_log_path}")
    log.info("CONCLUIDO!")


if __name__ == "__main__":
    main()

"""
Enriquecimento de dados de empreendimentos.
============================================
Varre todos os empreendimentos no banco de dados e tenta preencher:
  - data_lancamento (data do lancamento ou R.I.)
  - endereco (se vazio)
  - latitude / longitude (de mapas embarcados ou geocoding)

Estrategias por empresa:
  - API (MRV, Vivaz): consulta endpoints JSON
  - Requests (Plano&Plano, Direcional, generico): fetch HTML
  - Selenium (Cury, Metrocasa): renderiza SPA

Uso:
    python enriquecer_dados.py
    python enriquecer_dados.py --empresa "Vivaz"
    python enriquecer_dados.py --campo coordenadas
    python enriquecer_dados.py --limite 5 --forcar
    python enriquecer_dados.py --sem-geocoding
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import REQUESTS as REQ_CONFIG, LOGS_DIR, SELENIUM
from data.database import (
    get_connection,
    garantir_coluna,
    atualizar_empreendimento,
)

# ============================================================
# CONFIGURACAO
# ============================================================

EMPRESA_CONFIG = {
    "MRV":               {"tipo_acesso": "api_mrv",   "download_key": "mrv"},
    "Vivaz":             {"tipo_acesso": "api_vivaz",  "download_key": "vivaz"},
    "Plano&Plano":       {"tipo_acesso": "requests",   "download_key": "planoeplano"},
    "Direcional":        {"tipo_acesso": "requests",   "download_key": "direcional"},
    "Magik JC":          {"tipo_acesso": "requests",   "download_key": "magik_jc"},
    "Kazzas":            {"tipo_acesso": "requests",   "download_key": "kazzas"},
    "Vibra Residencial": {"tipo_acesso": "requests",   "download_key": "vibra"},
    "Pacaembu":          {"tipo_acesso": "requests",   "download_key": "pacaembu"},
    "Mundo Apto":        {"tipo_acesso": "requests",   "download_key": "mundo_apto"},
    "Conx":              {"tipo_acesso": "requests",   "download_key": "conx"},
    "Benx":              {"tipo_acesso": "requests",   "download_key": "benx"},
    "Cury":              {"tipo_acesso": "selenium",   "download_key": "cury"},
    "Metrocasa":         {"tipo_acesso": "selenium",   "download_key": "metrocasa"},
    # Construtoras do scraper genérico
    "VIC Engenharia":    {"tipo_acesso": "requests",   "download_key": "vic"},
    "Vasco Construtora": {"tipo_acesso": "requests",   "download_key": "vasco"},
    "Vinx":              {"tipo_acesso": "requests",   "download_key": "vinx"},
    "Riformato":         {"tipo_acesso": "requests",   "download_key": "riformato"},
    "ACLF":              {"tipo_acesso": "requests",   "download_key": "aclf"},
    "BM7":               {"tipo_acesso": "requests",   "download_key": "bm7"},
    "FYP Engenharia":    {"tipo_acesso": "requests",   "download_key": "fyp"},
    "Smart Construtora": {"tipo_acesso": "requests",   "download_key": "smart"},
    "Jotanunes":         {"tipo_acesso": "requests",   "download_key": "jotanunes"},
    "Cavazani":          {"tipo_acesso": "requests",   "download_key": "cavazani"},
    "Carrilho":          {"tipo_acesso": "requests",   "download_key": "carrilho"},
    "BP8":               {"tipo_acesso": "requests",   "download_key": "bp8"},
    "ACL Incorporadora": {"tipo_acesso": "requests",   "download_key": "acl"},
    "Novolar":           {"tipo_acesso": "requests",   "download_key": "novolar"},
    "Árbore":            {"tipo_acesso": "requests",   "download_key": "arbore"},
    "SUGOI":             {"tipo_acesso": "requests",   "download_key": "sugoi"},
    "Emccamp":           {"tipo_acesso": "requests",   "download_key": "emccamp"},
    "EPH":               {"tipo_acesso": "requests",   "download_key": "eph"},
    "Ampla":             {"tipo_acesso": "requests",   "download_key": "ampla"},
    "Novvo":             {"tipo_acesso": "requests",   "download_key": "novvo"},
    "M.Lar":             {"tipo_acesso": "requests",   "download_key": "mlar"},
    "Ún1ca":             {"tipo_acesso": "requests",   "download_key": "unica"},
    "Viva Benx":         {"tipo_acesso": "requests",   "download_key": "vivabenx"},
}

HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

PROGRESSO_FILE = os.path.join(LOGS_DIR, "enriquecer_dados_progresso.json")

# Meses em portugues
MESES_PT = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
    "abril": "04", "maio": "05", "junho": "06", "julho": "07",
    "agosto": "08", "setembro": "09", "outubro": "10",
    "novembro": "11", "dezembro": "12",
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("enriquecer_dados")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "enriquecer_dados.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ============================================================
# PROGRESSO
# ============================================================

def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": [], "erros": []}


def salvar_progresso(progresso):
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(progresso, f, indent=2, ensure_ascii=False)


# ============================================================
# BUSCA DE REGISTROS
# ============================================================

def buscar_registros(empresa, campo="todos", forcar=False, limite=0):
    """Busca registros que precisam de enriquecimento."""
    conn = get_connection()
    cursor = conn.cursor()

    # Garantir que colunas existem
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}

    conditions = ["empresa = ?"]
    params = [empresa]

    if not forcar:
        filtros = []
        if campo in ("todos", "data_lancamento"):
            if "data_lancamento" in colunas_existentes:
                filtros.append("(data_lancamento IS NULL OR data_lancamento = '')")
            else:
                filtros.append("1=1")  # coluna nem existe, todos precisam
        if campo in ("todos", "endereco"):
            filtros.append("(endereco IS NULL OR endereco = '')")
        if campo in ("todos", "coordenadas"):
            if "latitude" in colunas_existentes:
                filtros.append("(latitude IS NULL OR latitude = '')")
            else:
                filtros.append("1=1")
        if filtros:
            conditions.append(f"({' OR '.join(filtros)})")

    where = " AND ".join(conditions)

    # Selecionar campos disponiveis
    cols_select = ["id", "nome", "slug", "url_fonte", "endereco", "cidade", "estado", "bairro"]
    for extra in ["latitude", "longitude", "data_lancamento", "registro_incorporacao", "cep"]:
        if extra in colunas_existentes:
            cols_select.append(extra)

    query = f"SELECT {', '.join(cols_select)} FROM empreendimentos WHERE {where}"
    if limite > 0:
        query += f" LIMIT {limite}"

    rows = cursor.execute(query, params).fetchall()
    conn.close()
    return rows, cols_select


# ============================================================
# ACESSO A PAGINAS
# ============================================================

def fetch_html_requests(url, session):
    """Faz GET e retorna HTML."""
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"  HTTP {resp.status_code} para {url}")
    except Exception as e:
        logger.warning(f"  Erro requests: {e}")
    return None


def fetch_html_selenium(url, driver):
    """Carrega pagina com Selenium e retorna HTML."""
    try:
        driver.get(url)
        time.sleep(4)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)
        return driver.page_source
    except Exception as e:
        logger.warning(f"  Erro selenium: {e}")
    return None


def criar_driver():
    """Cria instancia do Chrome headless."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={SELENIUM['user_agent']}")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_page_load_timeout(30)
    return driver


# ============================================================
# API VIVAZ
# ============================================================

def fetch_api_vivaz(slug):
    """Busca dados detalhados via API Vivaz."""
    VIVAZ_BASE = "https://www.meuvivaz.com.br"
    headers = {
        "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": f"{VIVAZ_BASE}/",
        "Origin": VIVAZ_BASE,
    }

    resultado = {}

    try:
        resp = requests.post(
            f"{VIVAZ_BASE}/imovel/informacoes/",
            headers=headers, json={"Url": slug}, timeout=20
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success"):
                imovel = data.get("imovel", {})

                loc = imovel.get("Localizacao", {})
                if loc:
                    lat = loc.get("Latitude") or loc.get("latitude")
                    lng = loc.get("Longitude") or loc.get("longitude")
                    if lat and lng:
                        try:
                            resultado["latitude"] = str(float(lat))
                            resultado["longitude"] = str(float(lng))
                        except (ValueError, TypeError):
                            pass

                    rua = loc.get("Rua", "")
                    numero = loc.get("Numero", "")
                    if rua:
                        end = f"{rua}, {numero}".strip(", ") if numero else rua
                        resultado["endereco_encontrado"] = end

                    bairro = loc.get("Bairro", "")
                    if bairro:
                        resultado["bairro"] = bairro

                # Data de lancamento
                for key in ["DataLancamento", "DataCriacao", "DataPublicacao", "dataLancamento"]:
                    val = imovel.get(key)
                    if val and str(val).strip():
                        resultado["data_lancamento"] = str(val)[:10]
                        break

                # Vagas, quartos, varanda
                vagas = imovel.get("VagasGaragem")
                if vagas is not None:
                    resultado["numero_vagas"] = str(vagas)

                if imovel.get("OpcaoDeVaranda"):
                    resultado["apto_terraco"] = 1
                    resultado["lazer_varanda"] = 1

                quartos_str = imovel.get("Quartos", "")
                if quartos_str:
                    resultado["dormitorios_descricao"] = quartos_str

    except Exception as e:
        logger.warning(f"  Vivaz API erro: {e}")

    return resultado


# ============================================================
# API MRV
# ============================================================

def fetch_api_mrv(slug, estado, cidade):
    """Busca dados detalhados via API MRV GraphQL."""
    import unicodedata

    def slugify(text):
        if not text:
            return ""
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        text = re.sub(r"[^\w\s-]", "", text.lower())
        return re.sub(r"[\s_]+", "-", text).strip("-")

    estado_slug = slugify(estado) if estado else ""
    cidade_slug = slugify(cidade) if cidade else ""

    tipos = ["apartamentos", "casas", "lotes"]
    resultado = {}

    for tipo in tipos:
        url = (
            "https://mrv.com.br/graphql/execute.json/mrv/properties-details"
            f";path=/content/dam/mrv/content-fragments/detalhe-empreendimento"
            f"/{estado_slug}/{tipo}/{cidade_slug}/{slug}/{slug}"
        )
        try:
            resp = requests.get(url, headers={
                "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
                "Accept": "application/json",
                "Referer": "https://mrv.com.br/",
            }, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                item = data.get("data", {}).get("detalheEmpreendimentoByPath", {}).get("item", {})
                if item:
                    # Coordenadas
                    lat = item.get("latitude")
                    lng = item.get("longitude")
                    if lat and lng:
                        try:
                            resultado["latitude"] = str(float(str(lat).replace(",", ".")))
                            resultado["longitude"] = str(float(str(lng).replace(",", ".")))
                        except (ValueError, TypeError):
                            pass

                    # Endereco
                    end = item.get("endereco") or item.get("enderecoCompleto", "")
                    if end:
                        from html import unescape
                        resultado["endereco_encontrado"] = unescape(str(end))

                    # Data
                    for key in ["dataLancamento", "dataCadastro", "dataPublicacao"]:
                        val = item.get(key)
                        if val and str(val).strip():
                            resultado["data_lancamento"] = str(val)[:10]
                            break

                    break  # Encontrou, nao precisa tentar outros tipos
        except Exception:
            continue

    return resultado


# ============================================================
# EXTRACAO DE DATA DE LANCAMENTO (HTML)
# ============================================================

def extrair_data_lancamento(soup, texto_completo, url):
    """Tenta extrair data de lancamento de uma pagina HTML."""

    # 1. Meta tags
    for prop in ["article:published_time", "og:updated_time", "datePublished"]:
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content"):
            data = tag["content"].strip()[:10]
            if re.match(r"\d{4}-\d{2}", data):
                return data

    # 2. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                for key in ["datePublished", "dateCreated", "dateModified"]:
                    val = item.get(key)
                    if val and re.match(r"\d{4}-\d{2}", str(val)):
                        return str(val)[:10]
        except (json.JSONDecodeError, TypeError):
            pass

    # 3. Texto: padroes de lancamento
    patterns = [
        r"lan[çc]amento\s+em\s+(\w+)\s+(?:de\s+)?(\d{4})",
        r"lan[çc]ado\s+em\s+(\w+)\s+(?:de\s+)?(\d{4})",
        r"lan[çc]amento\s*[:\s]+(\d{2})[/\-](\d{4})",
        r"lan[çc]amento\s+em\s+(\d{2})[/\-](\d{4})",
    ]
    for pat in patterns:
        match = re.search(pat, texto_completo, re.IGNORECASE)
        if match:
            mes, ano = match.group(1), match.group(2)
            if mes.lower() in MESES_PT:
                return f"{ano}-{MESES_PT[mes.lower()]}"
            elif re.match(r"\d{2}$", mes):
                return f"{ano}-{mes}"

    # 4. Datas proximas a "incorporacao" ou "R.I."
    ri_pattern = r"(?:incorpora[çc][ãa]o|R\.?\s*I\.?)[^\d]*(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})"
    match = re.search(ri_pattern, texto_completo, re.IGNORECASE)
    if match:
        dia, mes, ano = match.group(1).zfill(2), match.group(2).zfill(2), match.group(3)
        if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31:
            return f"{ano}-{mes}-{dia}"

    # 5. Formato d/m/yyyy ou dd/mm/yyyy perto de "registro" ou "incorpora"
    ri_pattern2 = r"(?:registro|incorpora)[^\d]{0,50}(\d{1,2})[/\-](\d{4})"
    match = re.search(ri_pattern2, texto_completo, re.IGNORECASE)
    if match:
        mes, ano = match.group(1).zfill(2), match.group(2)
        if 1 <= int(mes) <= 12:
            return f"{ano}-{mes}"

    return None


def extrair_data_de_ri(registro_incorporacao):
    """Tenta extrair data do campo registro_incorporacao ja no banco."""
    if not registro_incorporacao:
        return None
    # Procurar datas no texto do R.I. (aceita 1 ou 2 digitos em dia/mes)
    match = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", registro_incorporacao)
    if match:
        dia, mes, ano = match.group(1).zfill(2), match.group(2).zfill(2), match.group(3)
        if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31:
            return f"{ano}-{mes}-{dia}"
    match = re.search(r"(\d{1,2})[/\-](\d{4})", registro_incorporacao)
    if match:
        mes, ano = match.group(1).zfill(2), match.group(2)
        if 1 <= int(mes) <= 12:
            return f"{ano}-{mes}"
    return None


# ============================================================
# EXTRACAO DE COORDENADAS (HTML)
# ============================================================

def extrair_coordenadas(soup, html_raw):
    """Extrai lat/lng de iframes Google Maps, JS, data-attrs, JSON-LD."""

    # 1. Google Maps iframe
    for iframe in soup.find_all("iframe", src=True):
        src = iframe["src"]
        if "google" in src and "map" in src.lower():
            # q=LAT,LNG
            m = re.search(r"[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # center=LAT,LNG
            m = re.search(r"center=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # @LAT,LNG
            m = re.search(r"@(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # !2dLNG!3dLAT (ordem invertida!)
            m = re.search(r"!2d(-?\d+\.?\d+)!3d(-?\d+\.?\d+)", src)
            if m:
                lng, lat = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)

    # 2. JS variables
    for script in soup.find_all("script"):
        text = script.string or ""
        if not text or len(text) < 10:
            continue

        # LatLng(LAT, LNG)
        m = re.search(r"LatLng\(\s*(-?\d+\.?\d+)\s*,\s*(-?\d+\.?\d+)\s*\)", text)
        if m:
            lat, lng = float(m.group(1)), float(m.group(2))
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)

        # lat: VAL, lng: VAL ou latitude/longitude
        lat_m = re.search(r"""(?:lat|latitude)\s*[:=]\s*['"]?(-?\d+\.?\d+)['"]?""", text)
        lng_m = re.search(r"""(?:lng|longitude|lon)\s*[:=]\s*['"]?(-?\d+\.?\d+)['"]?""", text)
        if lat_m and lng_m:
            lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)

    # 3. Data attributes
    for attr_lat, attr_lng in [("data-lat", "data-lng"), ("data-latitude", "data-longitude")]:
        elem = soup.find(True, attrs={attr_lat: True, attr_lng: True})
        if elem:
            try:
                lat, lng = float(elem[attr_lat]), float(elem[attr_lng])
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            except (ValueError, TypeError):
                pass

    # 4. JSON-LD geo
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                geo = item.get("geo", {})
                if isinstance(geo, dict) and geo.get("latitude") and geo.get("longitude"):
                    lat, lng = float(geo["latitude"]), float(geo["longitude"])
                    if _validar_coords_brasil(lat, lng):
                        return str(lat), str(lng)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # 5. Resolver links curtos do Google Maps (maps.app.goo.gl)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "maps.app.goo.gl" in href or "goo.gl/maps" in href:
            try:
                resp = requests.head(href, allow_redirects=True, timeout=10,
                                     headers={"User-Agent": "Mozilla/5.0"})
                final_url = resp.url
                # @LAT,LNG
                m = re.search(r"@(-?\d+\.?\d+),\s*(-?\d+\.?\d+)", final_url)
                if m:
                    lat, lng = float(m.group(1)), float(m.group(2))
                    if _validar_coords_brasil(lat, lng):
                        return str(lat), str(lng)
                # !3dLAT!4dLNG
                m = re.search(r"!3d(-?\d+\.?\d+)!4d(-?\d+\.?\d+)", final_url)
                if m:
                    lat, lng = float(m.group(1)), float(m.group(2))
                    if _validar_coords_brasil(lat, lng):
                        return str(lat), str(lng)
            except Exception:
                pass

    return None, None


def _validar_coords_brasil(lat, lng):
    """Verifica se coordenadas estao dentro do Brasil."""
    return -35.0 < lat < 6.0 and -75.0 < lng < -30.0


# ============================================================
# EXTRACAO DE ENDERECO (HTML)
# ============================================================

def extrair_endereco_de_pagina(soup, texto_completo):
    """Extrai endereco de pagina HTML se ausente no DB."""

    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                addr = item.get("address", {})
                if isinstance(addr, dict):
                    rua = addr.get("streetAddress", "")
                    if rua and len(rua) > 5:
                        return rua.strip()
        except (json.JSONDecodeError, TypeError):
            pass

    # 2. Regex para enderecos brasileiros
    patterns = [
        r"((?:Rua|R\.|Av\.|Avenida|Al\.|Alameda|Pça\.|Praça|Estrada|Rodovia|Travessa)\s+[^<\n]{10,100})",
    ]
    for pat in patterns:
        matches = re.findall(pat, texto_completo)
        for match in matches:
            endereco = match.strip()
            # Deve conter um numero para ser endereco valido
            if re.search(r"\d", endereco):
                # Limpar lixo no final
                endereco = re.sub(r"\s*[-–|]\s*$", "", endereco).strip()
                if len(endereco) > 10:
                    return endereco

    return None


# ============================================================
# GEOCODING NOMINATIM
# ============================================================

CEP_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ceps_brasil.db")


def extrair_cep_de_html(soup, html_raw):
    """Extrai CEP de texto da pagina HTML."""
    # Padrão: 5 dígitos, hífen, 3 dígitos
    ceps = re.findall(r"\b(\d{5})-?(\d{3})\b", html_raw)
    for d5, d3 in ceps:
        cep = d5 + d3
        # Filtrar CEPs invalidos (00000xxx, 99999xxx)
        if cep[:5] not in ("00000", "99999") and int(d5) > 0:
            return cep
    return None


def geocodificar_por_cep(cep):
    """Geocodifica CEP usando base local de CEPs (basedosdados.org)."""
    if not cep or not os.path.exists(CEP_DB_PATH):
        return None, None
    cep_limpo = re.sub(r"\D", "", cep)
    if len(cep_limpo) != 8:
        return None, None
    try:
        conn = sqlite3.connect(CEP_DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT latitude, longitude FROM ceps WHERE cep = ?", (cep_limpo,))
        row = cur.fetchone()
        conn.close()
        if row and row[0] and row[1]:
            lat, lng = float(row[0]), float(row[1])
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)
    except Exception:
        pass
    return None, None


_ultimo_geocode = 0

def geocodificar_nominatim(endereco, cidade=None, estado=None):
    """Geocodifica endereco via Nominatim (free, 1 req/sec)."""
    global _ultimo_geocode

    # Rate limit
    agora = time.time()
    espera = max(0, 1.2 - (agora - _ultimo_geocode))
    if espera > 0:
        time.sleep(espera)

    query_parts = [endereco]
    if cidade:
        query_parts.append(cidade)
    if estado:
        query_parts.append(estado)
    query_parts.append("Brasil")
    query = ", ".join(p for p in query_parts if p)

    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "br",
    }
    nominatim_headers = {
        "User-Agent": "Coleta-Empreendimentos/1.0 (educational-project)"
    }

    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params=params, headers=nominatim_headers, timeout=10
        )
        _ultimo_geocode = time.time()

        if resp.status_code == 200:
            results = resp.json()
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                if _validar_coords_brasil(lat, lon):
                    return str(lat), str(lon)
    except Exception as e:
        logger.debug(f"  Geocoding erro: {e}")

    return None, None


# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def enriquecer_empresa(empresa, config, campo, forcar, limite, sem_geocoding, progresso):
    """Processa todos os registros de uma empresa."""
    tipo_acesso = config["tipo_acesso"]

    registros, cols = buscar_registros(empresa, campo, forcar, limite)
    total = len(registros)

    if total == 0:
        logger.info(f"  {empresa}: nenhum registro para enriquecer")
        return {"atualizados": 0, "erros": 0, "sem_dados": 0}

    logger.info(f"  {empresa}: {total} registros para processar ({tipo_acesso})")

    driver = None
    session = None

    if tipo_acesso == "selenium":
        logger.info(f"  Iniciando Chrome para {empresa}...")
        driver = criar_driver()
    elif tipo_acesso == "requests":
        session = requests.Session()
        session.headers.update(HEADERS)

    atualizados = 0
    erros = 0
    sem_dados = 0

    try:
        for i, row in enumerate(registros, 1):
            # Mapear colunas
            rec = dict(zip(cols, row))
            nome = rec["nome"]
            slug = rec.get("slug", "")
            url_fonte = rec.get("url_fonte", "")
            endereco = rec.get("endereco", "")
            cidade = rec.get("cidade", "")
            estado = rec.get("estado", "")
            lat = rec.get("latitude", "")
            lng = rec.get("longitude", "")
            data_lanc = rec.get("data_lancamento", "")
            ri = rec.get("registro_incorporacao", "")

            chave = f"{empresa}:{nome}"
            if chave in progresso.get("processados", []) and not forcar:
                continue

            logger.info(f"  [{i}/{total}] {nome}")

            dados_novos = {}
            html = None
            soup = None

            try:
                if tipo_acesso == "api_mrv":
                    dados_api = fetch_api_mrv(slug, estado, cidade)
                    if dados_api.get("data_lancamento") and not data_lanc:
                        dados_novos["data_lancamento"] = dados_api["data_lancamento"]
                    if dados_api.get("latitude") and not lat:
                        dados_novos["latitude"] = dados_api["latitude"]
                        dados_novos["longitude"] = dados_api.get("longitude", "")
                    if dados_api.get("endereco_encontrado") and not endereco:
                        dados_novos["endereco"] = dados_api["endereco_encontrado"]

                elif tipo_acesso == "api_vivaz":
                    dados_api = fetch_api_vivaz(slug)
                    if dados_api.get("data_lancamento") and not data_lanc:
                        dados_novos["data_lancamento"] = dados_api["data_lancamento"]
                    if dados_api.get("latitude") and not lat:
                        dados_novos["latitude"] = dados_api["latitude"]
                        dados_novos["longitude"] = dados_api.get("longitude", "")
                    if dados_api.get("endereco_encontrado") and not endereco:
                        dados_novos["endereco"] = dados_api["endereco_encontrado"]

                elif tipo_acesso in ("requests", "selenium") and url_fonte:
                    if tipo_acesso == "requests":
                        html = fetch_html_requests(url_fonte, session)
                    else:
                        html = fetch_html_selenium(url_fonte, driver)

                    if html:
                        soup = BeautifulSoup(html, "html.parser")
                        texto = soup.get_text(separator="\n", strip=True)

                        # Data lancamento
                        if campo in ("todos", "data_lancamento") and not data_lanc:
                            data_found = extrair_data_lancamento(soup, texto, url_fonte)
                            if data_found:
                                dados_novos["data_lancamento"] = data_found

                        # Endereco
                        if campo in ("todos", "endereco") and not endereco:
                            end_found = extrair_endereco_de_pagina(soup, texto)
                            if end_found:
                                dados_novos["endereco"] = end_found

                        # Coordenadas
                        if campo in ("todos", "coordenadas") and not lat:
                            lat_f, lng_f = extrair_coordenadas(soup, html)
                            if lat_f and lng_f:
                                dados_novos["latitude"] = lat_f
                                dados_novos["longitude"] = lng_f

                # Tentar data do R.I. ja no banco se ainda nao achou data
                if not dados_novos.get("data_lancamento") and not data_lanc and ri:
                    data_ri = extrair_data_de_ri(ri)
                    if data_ri:
                        dados_novos["data_lancamento"] = data_ri

                # Geocoding fallback: CEP local > Nominatim
                if campo in ("todos", "coordenadas"):
                    lat_final = dados_novos.get("latitude") or lat
                    if not lat_final:
                        # Tentar extrair CEP da página para geocodificar
                        if html:
                            cep_encontrado = extrair_cep_de_html(soup, html)
                            if cep_encontrado:
                                lat_c, lng_c = geocodificar_por_cep(cep_encontrado)
                                if lat_c and lng_c:
                                    dados_novos["latitude"] = lat_c
                                    dados_novos["longitude"] = lng_c
                                    if not rec.get("cep"):
                                        dados_novos["cep"] = cep_encontrado

                    lat_final = dados_novos.get("latitude") or lat
                    if not lat_final and not sem_geocoding:
                        end_final = dados_novos.get("endereco") or endereco
                        if end_final:
                            lat_g, lng_g = geocodificar_nominatim(end_final, cidade, estado)
                            if lat_g and lng_g:
                                dados_novos["latitude"] = lat_g
                                dados_novos["longitude"] = lng_g

                # Atualizar banco
                if dados_novos:
                    atualizar_empreendimento(empresa, nome, dados_novos)
                    atualizados += 1
                    campos_atualizados = list(dados_novos.keys())
                    logger.info(f"    Atualizado: {campos_atualizados}")
                else:
                    sem_dados += 1

                progresso.setdefault("processados", []).append(chave)

            except Exception as e:
                logger.error(f"    Erro: {e}")
                erros += 1
                progresso.setdefault("erros", []).append(chave)

            salvar_progresso(progresso)

            # Delay
            if tipo_acesso == "selenium":
                time.sleep(3)
            elif tipo_acesso == "requests":
                time.sleep(1.5)
            else:
                time.sleep(1)

    finally:
        if driver:
            driver.quit()
            logger.info(f"  Chrome fechado para {empresa}")

    return {"atualizados": atualizados, "erros": erros, "sem_dados": sem_dados}


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento de dados de empreendimentos")
    parser.add_argument("--empresa", type=str, default="todas",
                        help="Empresa a processar (nome no banco ou 'todas')")
    parser.add_argument("--campo", type=str, default="todos",
                        choices=["todos", "data_lancamento", "endereco", "coordenadas"],
                        help="Qual campo enriquecer")
    parser.add_argument("--limite", type=int, default=0,
                        help="Limite de registros por empresa (0=todos)")
    parser.add_argument("--forcar", action="store_true",
                        help="Reprocessar registros que ja tem dados")
    parser.add_argument("--reset-progresso", action="store_true",
                        help="Resetar progresso")
    parser.add_argument("--sem-geocoding", action="store_true",
                        help="Pular geocodificacao Nominatim")
    args = parser.parse_args()

    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            logger.info("Progresso resetado.")
        return

    # Garantir coluna data_lancamento
    garantir_coluna("data_lancamento", "TEXT")
    garantir_coluna("latitude", "TEXT")
    garantir_coluna("longitude", "TEXT")

    progresso = carregar_progresso()

    logger.info("=" * 60)
    logger.info("ENRIQUECIMENTO DE DADOS")
    logger.info(f"Campo: {args.campo} | Empresa: {args.empresa} | Limite: {args.limite}")
    logger.info("=" * 60)

    # Ordenar: API primeiro (rapido), depois requests, depois selenium
    ordem = {"api_mrv": 0, "api_vivaz": 1, "requests": 2, "selenium": 3}

    if args.empresa.lower() == "todas":
        empresas = sorted(EMPRESA_CONFIG.items(), key=lambda x: ordem.get(x[1]["tipo_acesso"], 9))
    else:
        if args.empresa in EMPRESA_CONFIG:
            empresas = [(args.empresa, EMPRESA_CONFIG[args.empresa])]
        else:
            logger.error(f"Empresa desconhecida: {args.empresa}")
            logger.info(f"Disponiveis: {', '.join(EMPRESA_CONFIG.keys())}")
            return

    total_atualizado = 0
    total_erros = 0
    total_sem = 0

    for empresa_nome, config in empresas:
        logger.info(f"\n--- {empresa_nome} ---")
        resultado = enriquecer_empresa(
            empresa_nome, config, args.campo, args.forcar,
            args.limite, args.sem_geocoding, progresso
        )
        total_atualizado += resultado["atualizados"]
        total_erros += resultado["erros"]
        total_sem += resultado["sem_dados"]

    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL - ENRIQUECIMENTO")
    logger.info(f"  Atualizados: {total_atualizado}")
    logger.info(f"  Sem dados novos: {total_sem}")
    logger.info(f"  Erros: {total_erros}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

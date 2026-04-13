"""
Scraper Batch M5 — 8 Incorporadoras de Complexidade Media (ultimo batch medios)
================================================================================
Sites medios: Next.js, Angular, Duda CMS, sitemaps XML.

Empresas:
    itajai       — construtoraitajai.com.br (Duda CMS, SSR, listing /encontreseuimovel, ~5 props, Americana/SP)
    carozzo      — carozzo.com.br (Next.js, /portfolio/{slug}, ~5 props, Salvador/BA)
    helbor       — helbor.com.br (Next.js, __NEXT_DATA__ c/ coords/dados ricos, multi-estado, B3, nao MCMV)
    lyall        — lyall.com.br (SSR, /projetos listing, /projeto/{slug}, ~8 props, Lajeado/RS)
    lcm          — meulcm.com.br (WordPress?, /empreendimento/{slug}, ~11 props, BH/MG)
    alianca      — aliancaempreendimentossc.com.br (Next.js, __NEXT_DATA__ c/ coords, ~3 props, Itapema/SC, nao MCMV)
    nbr          — nbrempreendimentos.com.br (Certificado SSL expirado — precisa revisao manual)
    bild         — bild.com.br (Angular SPA, sitemap.xml c/ 125+ props, SSR individual, Ribeirao Preto, nao MCMV)

Uso:
    python scrapers/generico_novas_empresas_m5.py --empresa helbor
    python scrapers/generico_novas_empresas_m5.py --empresa todas
    python scrapers/generico_novas_empresas_m5.py --listar
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import REQUESTS, LOGS_DIR, DOWNLOADS_DIR
from data.database import (
    inserir_empreendimento,
    empreendimento_existe,
    atualizar_empreendimento,
    detectar_atributos_binarios,
    contar_empreendimentos,
)

# Importar funcoes core do generico existente
from scrapers.generico_empreendimentos import (
    fetch_html,
    extrair_por_parser,
    extrair_por_css,
    extrair_dados_empreendimento,
    coletar_links_empreendimentos,
    detectar_fase,
    extrair_preco,
    extrair_itens_lazer,
    extrair_cidade_estado,
    extrair_bairro,
    download_imagens,
    HEADERS,
    DELAY,
)


# ============================================================
# LOGGER
# ============================================================
def setup_logger(empresa_key):
    logger = logging.getLogger(f"scraper.m5.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"m5_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# HEADERS customizados
# ============================================================
HEADERS_M5 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch M5
# ============================================================
EMPRESAS = {
    "itajai": {
        "nome_banco": "Construtora Itajaí",
        "base_url": "https://www.construtoraitajai.com.br",
        "estado_default": "SP",
        "cidade_default": "Americana",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada
        "padrao_link": r"construtoraitajai\.com\.br/(residencial-[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt][eé]rreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "carozzo": {
        "nome_banco": "Carozzo",
        "base_url": "https://www.carozzo.com.br",
        "estado_default": "BA",
        "cidade_default": "Salvador",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Short-stay/investimento, nao MCMV
        "urls_listagem": [],  # Coleta customizada
        "padrao_link": r"carozzo\.com\.br/portfolio/([\w%-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\blofts?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "helbor": {
        "nome_banco": "Helbor",
        "base_url": "https://www.helbor.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # B3, medio/alto padrao
        "urls_listagem": [],  # Coleta via __NEXT_DATA__
        "padrao_link": r"helbor\.com\.br/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lyall": {
        "nome_banco": "Lyall",
        "base_url": "https://www.lyall.com.br",
        "estado_default": "RS",
        "cidade_default": "Lajeado",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada
        "padrao_link": r"lyall\.com\.br/projeto/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|salas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt][eé]rreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lcm": {
        "nome_banco": "LCM",
        "base_url": "https://meulcm.com.br",
        "estado_default": "MG",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada
        "padrao_link": r"meulcm\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|casas?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "alianca": {
        "nome_banco": "Aliança",
        "base_url": "https://aliancaempreendimentossc.com.br",
        "estado_default": "SC",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Itapema litoral, alto padrao
        "urls_listagem": [],  # Coleta via __NEXT_DATA__
        "padrao_link": r"aliancaempreendimentossc\.com\.br/empreendimentos/([\w%-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "nbr": {
        "nome_banco": "NBR",
        "base_url": "https://nbrempreendimentos.com.br",
        "estado_default": "MA",
        "cidade_default": "São Luís",
        "nome_from_title": True,
        "urls_listagem": [],
        "padrao_link": r"nbrempreendimentos\.com\.br/([\w-]+)",
        "ssl_expired": True,  # Certificado SSL expirado
        "parsers": {},
    },

    "bild": {
        "nome_banco": "Bild",
        "base_url": "https://bild.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Medio/alto padrao, Ribeirao Preto
        "urls_listagem": [],  # Coleta via sitemap.xml
        "padrao_link": r"bild\.com\.br/imoveis/([\w-]+)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .nome-imovel", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|salas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt][eé]rreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:[Aa]\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCAO DE EXTRACAO DE COORDENADAS
# ============================================================

def extrair_coordenadas(html):
    """Extrai coordenadas de Google Maps embeds ou JS no HTML."""
    if not html:
        return None, None

    # 1. Padrao iframe Google Maps com q=lat,lon
    match = re.search(r'maps.*?[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)', html)
    if match:
        return match.group(1), match.group(2)

    # 2. Padrao @lat,lon no Google Maps URL
    match = re.search(r'google\.com/maps.*?@(-?\d+\.\d+),(-?\d+\.\d+)', html)
    if match:
        return match.group(1), match.group(2)

    # 3. Padrao ll=lat,lon
    match = re.search(r'll=(-?\d+\.\d+),(-?\d+\.\d+)', html)
    if match:
        return match.group(1), match.group(2)

    # 4. Padrao center=lat,lon
    match = re.search(r'center=(-?\d+\.\d+),(-?\d+\.\d+)', html)
    if match:
        return match.group(1), match.group(2)

    # 5. Padrao em JS: lat: -XX.XXX, lng: -XX.XXX
    match = re.search(r'lat["\']?\s*[:=]\s*(-?\d+\.\d{4,})\s*[,;]\s*l(?:ng|on)["\']?\s*[:=]\s*(-?\d+\.\d{4,})', html)
    if match:
        return match.group(1), match.group(2)

    # 6. Padrao embed pb com coords (!2d=lon, !3d=lat)
    match = re.search(r'!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)', html)
    if match:
        return match.group(2), match.group(1)

    # 7. Padrao latitude/longitude com valor numerico proximo (JSON, escaped ou nao)
    match = re.search(r'latitude[^-\d]*?(-?\d+\.\d{4,})[^-\d]*?longitude[^-\d]*?(-?\d+\.\d{4,})', html)
    if match:
        return match.group(1), match.group(2)

    # 8. Padrao "lat":-XX.XXX,"lng":-XX.XXX (JSON, ex: APIs)
    match = re.search(r'"lat"\s*:\s*(-?\d+\.\d{4,})\s*,\s*"lng"\s*:\s*(-?\d+\.\d{4,})', html)
    if match:
        return match.group(1), match.group(2)

    # 9. OpenGraph place:location meta tags
    lat_match = re.search(r'property="place:location:latitude"\s+content="(-?\d+\.\d{4,})"', html)
    lon_match = re.search(r'property="place:location:longitude"\s+content="(-?\d+\.\d{4,})"', html)
    if lat_match and lon_match:
        return lat_match.group(1), lon_match.group(1)

    # 10. data-latitude/data-longitude HTML attributes
    lat_match = re.search(r'data-latitude="(-?\d+\.\d{4,})"', html)
    lon_match = re.search(r'data-longitude="(-?\d+\.\d{4,})"', html)
    if lat_match and lon_match:
        return lat_match.group(1), lon_match.group(1)

    return None, None


# ============================================================
# FUNCOES AUXILIARES
# ============================================================

def _fetch_html_m5(url, logger, verify_ssl=True):
    """Fetch HTML com headers customizados."""
    try:
        resp = requests.get(url, headers=HEADERS_M5, timeout=15, allow_redirects=True, verify=verify_ssl)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None


def _normalizar_fase(fase_raw):
    """Normaliza texto de fase para os valores padrao."""
    if not fase_raw:
        return None
    fase_lower = fase_raw.lower().strip()

    if any(k in fase_lower for k in ["breve", "em breve", "futuro", "brevemente"]):
        return "Breve Lançamento"
    if any(k in fase_lower for k in ["lançamento", "lancamento", "lanc"]):
        return "Lançamento"
    if any(k in fase_lower for k in ["em obra", "em constru", "construção", "construcao"]):
        return "Em Construção"
    if any(k in fase_lower for k in ["pronto", "entregue", "conclu", "pronto para morar"]):
        return "Imóvel Pronto"
    if any(k in fase_lower for k in ["100% vendido", "esgotado"]):
        return "Imóvel Pronto"
    if "últimas" in fase_lower or "ultimas" in fase_lower:
        return "Lançamento"

    return fase_raw.strip()


def _extrair_nome_from_title(html, empresa_key):
    """Extrai nome limpo do <title> da pagina."""
    if not html:
        return None
    match = re.search(r'<title>([^<]+)</title>', html)
    if not match:
        return None
    title = match.group(1).strip()

    # Remover sufixos comuns por empresa
    sufixos = [
        " | Construtora Itajaí", " - Construtora Itajaí",
        " | Carozzo", " - Carozzo",
        " | Helbor", " - Helbor", " | HELBOR",
        " | Lyall", " - Lyall",
        " | LCM", " - LCM", " | LCM Construção",
        " | Aliança", " - Aliança",
        " | NBR", " - NBR",
        " | Bild", " - Bild", " | BILD",
        " - Bild Desenvolvimento Imobiliário",
    ]
    for sufixo in sufixos:
        if title.lower().endswith(sufixo.lower()):
            title = title[:-len(sufixo)].strip()
            break

    # Limpar pipe ou dash no final
    title = re.sub(r'\s*[|–-]\s*$', '', title).strip()

    if len(title) < 3 or len(title) > 80:
        return None

    return title


# ============================================================
# COLETA DE LINKS POR EMPRESA
# ============================================================

def _coletar_links_itajai(config, logger):
    """Coleta links da Construtora Itajai da pagina /encontreseuimovel."""
    links = {}
    base = config["base_url"]

    url = f"{base}/encontreseuimovel"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    # Links residenciais na pagina de listagem
    found = set(re.findall(r'href=["\'](/residencial-[\w-]+)', html))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.lstrip("/")
        if slug and slug != "encontreseuimovel":
            full_url = f"{base}/{slug}"
            links[slug] = full_url

    # Tambem buscar links externos para residenciais com dominio proprio
    ext_links = re.findall(r'href=["\'](https?://residencial[\w]+\.com\.br/?)', html)
    for ext_url in ext_links:
        slug = re.sub(r'https?://(.*?)\.com\.br/?', r'\1', ext_url)
        if slug:
            links[slug] = ext_url.rstrip("/")

    logger.info(f"Total de links Itajai: {len(links)}")
    return links


def _coletar_links_carozzo(config, logger):
    """Coleta links da Carozzo da homepage (portfolio links)."""
    links = {}
    base = config["base_url"]

    # Buscar na homepage
    url = base
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    found = set(re.findall(r'href=["\']\./portfolio/([\w%-]+)', html))
    if not found:
        found = set(re.findall(r'href=["\'](?:\.)?/portfolio/([\w%-]+)', html))
    if not found:
        found = set(re.findall(r'href=["\']((?:https?://(?:www\.)?carozzo\.com\.br)?/portfolio/[\w%-]+)', html))
        found = {u.split("/portfolio/")[-1] for u in found if "/portfolio/" in u}

    for slug in found:
        slug_clean = slug.strip("/")
        if slug_clean:
            full_url = f"{base}/portfolio/{slug_clean}"
            links[slug_clean] = full_url

    logger.info(f"Total de links Carozzo: {len(links)}")
    return links


# Cache para dados __NEXT_DATA__ da Helbor
_helbor_cache = {}


def _coletar_links_helbor(config, logger):
    """Coleta links e dados da Helbor via __NEXT_DATA__ da homepage."""
    links = {}
    _helbor_cache.clear()

    url = "https://www.helbor.com.br"
    logger.info(f"Coletando dados via __NEXT_DATA__: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        logger.warning("__NEXT_DATA__ nao encontrado na homepage da Helbor")
        return links

    try:
        data = json.loads(script.string)

        # Buscar recursivamente por empreendimentos
        empreendimentos = []

        def find_items(obj):
            if isinstance(obj, dict):
                # Procurar por objetos com 'url' e 'nome' que parecem empreendimentos
                if "url" in obj and "nome" in obj and "localizacao" in obj:
                    empreendimentos.append(obj)
                for v in obj.values():
                    find_items(v)
            elif isinstance(obj, list):
                for item in obj:
                    find_items(item)

        find_items(data)

        for emp in empreendimentos:
            slug = emp.get("url", "")
            nome = emp.get("nome", "")
            if slug and nome:
                slug_clean = slug.strip("/")
                full_url = f"https://www.helbor.com.br/{slug_clean}"
                links[slug_clean] = full_url
                _helbor_cache[slug_clean] = emp

        logger.info(f"Total de empreendimentos Helbor (__NEXT_DATA__): {len(links)}")

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__ da Helbor: {e}")

    return links


def _extrair_dados_helbor_cache(slug, logger):
    """Extrai dados ricos do cache __NEXT_DATA__ da Helbor."""
    if slug not in _helbor_cache:
        return {}

    emp = _helbor_cache[slug]
    dados = {}

    # Nome
    nome = emp.get("nome", "")
    if nome:
        dados["nome"] = nome

    # Localizacao
    loc = emp.get("localizacao", {})
    if isinstance(loc, dict):
        cidade = loc.get("cidade", "")
        estado = loc.get("estado", "")
        bairro = loc.get("bairro", "")
        rua = loc.get("rua", "")
        numero = loc.get("numero", "")
        cep = loc.get("cep", "")

        if cidade:
            dados["cidade"] = cidade
        if estado:
            dados["estado"] = estado
        if bairro:
            dados["bairro"] = bairro
        if rua:
            endereco = rua
            if numero:
                endereco += f", {numero}"
            dados["endereco"] = endereco

        # Coordenadas
        lat = loc.get("latitude") or loc.get("lat")
        lng = loc.get("longitude") or loc.get("lng") or loc.get("lon")
        if lat and lng:
            dados["latitude"] = str(lat)
            dados["longitude"] = str(lng)

    # Status
    status = emp.get("status", "")
    if isinstance(status, dict):
        status = status.get("nome", "") or status.get("name", "")
    if status:
        dados["fase"] = _normalizar_fase(status)

    # Totalmente vendido
    if emp.get("totalmenteVendido"):
        dados["fase"] = "Imóvel Pronto"

    # Metragem
    met_min = emp.get("metragemMinima")
    met_max = emp.get("metragemMaxima")
    if met_min and met_max:
        if met_min == met_max:
            dados["metragens_descricao"] = f"{met_min}m²"
        else:
            dados["metragens_descricao"] = f"{met_min} a {met_max}m²"
        try:
            dados["area_min_m2"] = float(met_min)
            dados["area_max_m2"] = float(met_max)
        except (ValueError, TypeError):
            pass

    # Dormitorios
    dorms = emp.get("quantidadeDormitorios")
    if dorms:
        if isinstance(dorms, list):
            dados["dormitorios_descricao"] = " e ".join(str(d) for d in dorms) + " dorms"
        elif isinstance(dorms, str):
            dados["dormitorios_descricao"] = dorms

    # Tipo
    tipo = emp.get("tipo", [])
    if isinstance(tipo, list) and tipo:
        tipo_names = [t.get("nome", "") if isinstance(t, dict) else str(t) for t in tipo]
        tipo_names = [t for t in tipo_names if t]
        if tipo_names and not dados.get("dormitorios_descricao"):
            dados["dormitorios_descricao"] = " | ".join(tipo_names)

    # Valor
    valor = emp.get("valor")
    if valor and valor > 0:
        dados["preco_a_partir"] = valor

    return dados


def _coletar_links_lyall(config, logger):
    """Coleta links da Lyall da pagina /projetos."""
    links = {}
    base = config["base_url"]

    url = f"{base}/projetos"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    found = set(re.findall(r'href=["\']((?:https?://www\.lyall\.com\.br)?/projeto/[\w-]+)', html))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/projeto/")[-1] if "/projeto/" in href_clean else href_clean.split("/")[-1]
        if slug and len(slug) > 2:
            full_url = f"{base}/projeto/{slug}"
            links[slug] = full_url

    logger.info(f"Total de links Lyall: {len(links)}")
    return links


def _coletar_links_lcm(config, logger):
    """Coleta links da LCM de meulcm.com.br."""
    links = {}
    base = config["base_url"]

    url = f"{base}/empreendimentos/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        # Fallback: homepage
        url = base
        logger.info(f"Tentando homepage: {url}")
        html = _fetch_html_m5(url, logger)
    if not html:
        return links

    found = set(re.findall(r'href=["\'](https?://meulcm\.com\.br/empreendimento/[\w-]+/?)', html))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/empreendimento/")[-1]
        if slug and slug != "empreendimentos":
            links[slug] = href_clean + "/"

    # Se nao encontrou na listagem, tentar homepage
    if not links and url != base:
        html_home = _fetch_html_m5(base, logger)
        if html_home:
            found2 = set(re.findall(r'href=["\'](https?://meulcm\.com\.br/empreendimento/[\w-]+/?)', html_home))
            for href in found2:
                href_clean = href.rstrip("/")
                slug = href_clean.split("/empreendimento/")[-1]
                if slug:
                    links[slug] = href_clean + "/"

    logger.info(f"Total de links LCM: {len(links)}")
    return links


# Cache para dados __NEXT_DATA__ da Alianca
_alianca_cache = {}


def _coletar_links_alianca(config, logger):
    """Coleta links e dados da Alianca via __NEXT_DATA__."""
    links = {}
    _alianca_cache.clear()

    url = "https://aliancaempreendimentossc.com.br"
    logger.info(f"Coletando dados via __NEXT_DATA__: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        logger.warning("__NEXT_DATA__ nao encontrado na Alianca")
        # Fallback: buscar links no HTML
        found = set(re.findall(
            r'href=["\'](/empreendimentos/[\w%-]+)',
            html
        ))
        for href in found:
            slug = href.split("/empreendimentos/")[-1]
            if slug:
                full_url = f"https://aliancaempreendimentossc.com.br{href}"
                links[slug] = full_url
        logger.info(f"Total de links Alianca (HTML): {len(links)}")
        return links

    try:
        data = json.loads(script.string)
        empreendimentos = []

        def find_items(obj):
            if isinstance(obj, dict):
                # Procurar objetos com campos tipicos de empreendimento
                if ("nome" in obj or "name" in obj) and ("endereco" in obj or "address" in obj or "latitude" in obj or "quartos" in obj):
                    empreendimentos.append(obj)
                for v in obj.values():
                    find_items(v)
            elif isinstance(obj, list):
                for item in obj:
                    find_items(item)

        find_items(data)

        for emp in empreendimentos:
            nome = emp.get("nome") or emp.get("name", "")
            slug_id = emp.get("slug") or emp.get("id", "")
            if nome:
                # Montar slug como no site: Nome_ID
                if slug_id:
                    slug_key = f"{nome.replace(' ', '-')}_{slug_id}" if "_" not in str(slug_id) else str(slug_id)
                else:
                    slug_key = nome.replace(" ", "-")

                # Verificar se ja tem URL
                url_emp = emp.get("url") or emp.get("link", "")
                if url_emp:
                    links[slug_key] = url_emp if url_emp.startswith("http") else f"https://aliancaempreendimentossc.com.br{url_emp}"
                else:
                    links[slug_key] = f"https://aliancaempreendimentossc.com.br/empreendimentos/{slug_key}"

                _alianca_cache[slug_key] = emp

        # Se nao encontrou via busca recursiva, buscar links no HTML
        if not links:
            found = set(re.findall(
                r'href=["\'](/empreendimentos/[\w%-]+)',
                html
            ))
            for href in found:
                slug = href.split("/empreendimentos/")[-1]
                if slug:
                    full_url = f"https://aliancaempreendimentossc.com.br{href}"
                    links[slug] = full_url

        logger.info(f"Total de empreendimentos Alianca: {len(links)}")

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__ da Alianca: {e}")

    return links


def _extrair_dados_alianca_cache(slug, logger):
    """Extrai dados do cache __NEXT_DATA__ da Alianca."""
    if slug not in _alianca_cache:
        return {}

    emp = _alianca_cache[slug]
    dados = {}

    nome = emp.get("nome") or emp.get("name", "")
    if nome:
        dados["nome"] = nome

    # Endereco
    endereco = emp.get("endereco") or emp.get("address", "")
    if endereco:
        dados["endereco"] = endereco

    # Cidade
    cidade = emp.get("cidade") or emp.get("city", "")
    if cidade:
        dados["cidade"] = cidade

    # Estado
    estado = emp.get("estado") or emp.get("state", "")
    if estado:
        dados["estado"] = estado

    # Coordenadas
    lat = emp.get("latitude") or emp.get("lat")
    lng = emp.get("longitude") or emp.get("lng") or emp.get("lon")
    if lat and lng:
        dados["latitude"] = str(lat)
        dados["longitude"] = str(lng)

    # Quartos
    quartos = emp.get("quartos") or emp.get("rooms")
    if quartos:
        dados["dormitorios_descricao"] = f"{quartos} quartos"

    # Suites
    suites = emp.get("suites")
    if suites:
        dados["dormitorios_descricao"] = f"{quartos} quartos, {suites} suítes" if quartos else f"{suites} suítes"

    # Area
    area_min = emp.get("areaMinima") or emp.get("area_min")
    area_max = emp.get("areaMaxima") or emp.get("area_max")
    if area_min and area_max:
        dados["metragens_descricao"] = f"{area_min} a {area_max}m²"
    elif area_min:
        dados["metragens_descricao"] = f"{area_min}m²"

    # Vagas
    vagas = emp.get("vagas") or emp.get("garages")
    if vagas:
        dados["numero_vagas"] = str(vagas)

    # Status
    status = emp.get("status") or emp.get("fase", "")
    if isinstance(status, dict):
        status = status.get("nome", "") or status.get("name", "")
    if status:
        dados["fase"] = _normalizar_fase(status)

    return dados


def _coletar_links_bild(config, logger):
    """Coleta links da Bild via sitemap.xml."""
    links = {}

    url = "https://bild.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m5(url, logger)
    if not html:
        return links

    # URLs de imoveis: /imoveis/{cidade}/{nome}
    urls_sitemap = re.findall(r'<loc>(https?://bild\.com\.br/imoveis/[\w-]+/[\w&-]+/?)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        parts = u_clean.split("/imoveis/")[-1].split("/")
        if len(parts) == 2:
            cidade_slug, nome_slug = parts
            slug_key = f"{cidade_slug}/{nome_slug}"
            links[slug_key] = u_clean

    logger.info(f"Total de links Bild (sitemap): {len(links)}")
    return links


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    # Verificar se tem SSL expirado
    if config.get("ssl_expired"):
        logger.warning(f"{nome_banco}: certificado SSL expirado. Pulando.")
        print(f"  NOTA: {nome_banco} ({config['base_url']}) tem certificado SSL expirado — nao acessivel")
        return {"novos": 0, "atualizados": 0, "erros": 0, "nota": "SSL expirado"}

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper M5: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    coletores = {
        "itajai": _coletar_links_itajai,
        "carozzo": _coletar_links_carozzo,
        "helbor": _coletar_links_helbor,
        "lyall": _coletar_links_lyall,
        "lcm": _coletar_links_lcm,
        "alianca": _coletar_links_alianca,
        "bild": _coletar_links_bild,
    }

    coletor = coletores.get(empresa_key)
    if coletor:
        links = coletor(config, logger)
    else:
        links = {}

    if not links:
        logger.warning("Nenhum link de empreendimento encontrado!")
        return {"novos": 0, "atualizados": 0, "erros": 0}

    logger.info(f"Links coletados: {len(links)}")

    # Filtrar ja processados (a menos que --atualizar)
    if not atualizar:
        links_novos = {}
        for slug, url in links.items():
            # Para Helbor, verificar pelo nome do cache
            if empresa_key == "helbor" and slug in _helbor_cache:
                nome_cache = _helbor_cache[slug].get("nome", "")
                if nome_cache and empreendimento_existe(nome_banco, nome_cache):
                    continue

            # Verificar pelo slug como nome
            nome_teste = slug.replace("-", " ").replace("/", " - ").title()
            if not empreendimento_existe(nome_banco, nome_teste):
                if not empreendimento_existe(nome_banco, slug):
                    links_novos[slug] = url
        logger.info(f"Novos para processar: {len(links_novos)} (ja no banco: {len(links) - len(links_novos)})")
        links = links_novos

    if limite:
        links = dict(list(links.items())[:limite])
        logger.info(f"Limitando a {limite} empreendimentos")

    # Fase 2: Processar cada empreendimento
    novos = 0
    atualizados = 0
    erros = 0
    total = len(links)

    for i, (slug, url) in enumerate(links.items(), 1):
        logger.info(f"[{i}/{total}] Processando: {slug}")
        logger.info(f"  URL: {url}")

        # === Helbor: tentar dados do cache primeiro ===
        if empresa_key == "helbor":
            dados_cache = _extrair_dados_helbor_cache(slug, logger)
            nome_cache = dados_cache.get("nome", "")
            if nome_cache and not atualizar and empreendimento_existe(nome_banco, nome_cache):
                logger.info(f"  Ja existe (cache): {nome_cache}")
                continue

        # === Alianca: dados do cache ===
        if empresa_key == "alianca":
            dados_cache_ali = _extrair_dados_alianca_cache(slug, logger)

        # Fetch pagina individual
        html = _fetch_html_m5(url, logger)
        if not html:
            # Para Helbor, podemos usar so o cache
            if empresa_key == "helbor" and slug in _helbor_cache:
                logger.info(f"  Usando apenas dados do cache __NEXT_DATA__")
                dados = _extrair_dados_helbor_cache(slug, logger)
                if dados.get("nome"):
                    dados["empresa"] = nome_banco
                    dados["url_fonte"] = url
                    dados["data_coleta"] = datetime.now().isoformat()
                    dados["prog_mcmv"] = config.get("prog_mcmv", 0)

                    nome = dados["nome"]
                    existe = empreendimento_existe(nome_banco, nome)
                    if existe and atualizar:
                        atualizar_empreendimento(nome_banco, nome, dados)
                        atualizados += 1
                        logger.info(f"  Atualizado (cache): {nome}")
                    elif not existe:
                        inserir_empreendimento(dados)
                        novos += 1
                        logger.info(f"  Inserido (cache): {nome} | {dados.get('cidade', 'N/A')} | {dados.get('fase', 'N/A')}")
                    time.sleep(DELAY)
                    continue
            erros += 1
            time.sleep(DELAY)
            continue

        try:
            dados = extrair_dados_empreendimento(html, url, config, logger)

            # === Enriquecimento por empresa ===

            # Nome: tentar do title
            nome_title = _extrair_nome_from_title(html, empresa_key)
            if nome_title and (not dados.get("nome") or len(dados.get("nome", "")) < 3):
                dados["nome"] = nome_title

            # Helbor: merge cache com dados da pagina
            if empresa_key == "helbor" and slug in _helbor_cache:
                dados_cache = _extrair_dados_helbor_cache(slug, logger)
                for k, v in dados_cache.items():
                    if k == "nome":
                        # Cache tem nome correto
                        if v and len(v) >= 3:
                            dados["nome"] = v
                    elif v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

            # Helbor: extrair coords de __NEXT_DATA__ da pagina individual tambem
            if empresa_key == "helbor" and not dados.get("latitude"):
                soup_hb = BeautifulSoup(html, "html.parser")
                script_hb = soup_hb.find("script", id="__NEXT_DATA__")
                if script_hb and script_hb.string:
                    try:
                        data_hb = json.loads(script_hb.string)
                        # Buscar localizacao recursivamente
                        def find_loc(obj):
                            if isinstance(obj, dict):
                                if "latitude" in obj and "longitude" in obj:
                                    lat = obj.get("latitude")
                                    lng = obj.get("longitude")
                                    if lat and lng:
                                        return str(lat), str(lng)
                                if "lat" in obj and "lng" in obj:
                                    lat = obj.get("lat")
                                    lng = obj.get("lng")
                                    if lat and lng:
                                        return str(lat), str(lng)
                                for v in obj.values():
                                    result = find_loc(v)
                                    if result:
                                        return result
                            elif isinstance(obj, list):
                                for item in obj:
                                    result = find_loc(item)
                                    if result:
                                        return result
                            return None
                        coords = find_loc(data_hb)
                        if coords:
                            dados["latitude"], dados["longitude"] = coords
                    except (json.JSONDecodeError, KeyError):
                        pass

            # Alianca: merge cache com dados da pagina
            if empresa_key == "alianca" and slug in _alianca_cache:
                dados_cache_ali = _extrair_dados_alianca_cache(slug, logger)
                for k, v in dados_cache_ali.items():
                    if k == "nome" and v:
                        dados["nome"] = v
                    elif v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

            # Alianca: extrair dados de __NEXT_DATA__ da pagina individual
            if empresa_key == "alianca" and not dados.get("latitude"):
                soup_ali = BeautifulSoup(html, "html.parser")
                script_ali = soup_ali.find("script", id="__NEXT_DATA__")
                if script_ali and script_ali.string:
                    try:
                        data_ali = json.loads(script_ali.string)
                        # Buscar coords e dados
                        def find_emp_data(obj):
                            if isinstance(obj, dict):
                                if "latitude" in obj and obj.get("latitude"):
                                    return obj
                                for v in obj.values():
                                    result = find_emp_data(v)
                                    if result:
                                        return result
                            elif isinstance(obj, list):
                                for item in obj:
                                    result = find_emp_data(item)
                                    if result:
                                        return result
                            return None

                        emp_data = find_emp_data(data_ali)
                        if emp_data:
                            lat = emp_data.get("latitude")
                            lng = emp_data.get("longitude")
                            if lat and lng:
                                dados["latitude"] = str(lat)
                                dados["longitude"] = str(lng)
                            if emp_data.get("endereco") and not dados.get("endereco"):
                                dados["endereco"] = emp_data["endereco"]
                            if emp_data.get("cidade") and not dados.get("cidade"):
                                dados["cidade"] = emp_data["cidade"]
                    except (json.JSONDecodeError, KeyError):
                        pass

            # Coordenadas do HTML (fallback generico)
            if not dados.get("latitude"):
                lat, lon = extrair_coordenadas(html)
                if lat and lon:
                    dados["latitude"] = lat
                    dados["longitude"] = lon

            # Itajai: extrair localidade do texto (Bairro X - Americana - SP)
            if empresa_key == "itajai":
                soup_it = BeautifulSoup(html, "html.parser")
                text_it = soup_it.get_text(separator="\n", strip=True)
                # Buscar padrao "Bairro • Cidade • UF" ou "Bairro - Cidade"
                m_loc = re.search(r'([\w\s]+?)\s*[•·]\s*([\w\s]+?)\s*[•·]\s*(SP|SC|PR|MG|RJ|BA|RS|GO)', text_it)
                if m_loc:
                    dados["bairro"] = m_loc.group(1).strip()
                    dados["cidade"] = m_loc.group(2).strip()
                    dados["estado"] = m_loc.group(3).strip()

                # Nome do H1 (mais confiavel que title para Itajai)
                h1_it = soup_it.find("h1")
                if h1_it:
                    nome_h1 = h1_it.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1

                # Extrair total unidades e detalhes do corpo
                m_units = re.search(r'(\d+)\s*(?:apartamentos?|unidades?)', text_it, re.IGNORECASE)
                if m_units and not dados.get("total_unidades"):
                    try:
                        dados["total_unidades"] = int(m_units.group(1))
                    except ValueError:
                        pass

                # Evolucao de obra
                m_obra = re.search(r'(\d+[.,]\d+)\s*%', text_it)
                if m_obra:
                    try:
                        pct = float(m_obra.group(1).replace(",", "."))
                        dados["evolucao_obra_pct"] = pct
                        if pct >= 100:
                            dados["fase"] = "Imóvel Pronto"
                        elif pct > 0:
                            dados["fase"] = "Em Construção"
                    except ValueError:
                        pass

            # Carozzo: extrair cidade do texto (Salvador-BA ou Feira de Santana-BA)
            if empresa_key == "carozzo":
                soup_cz = BeautifulSoup(html, "html.parser")
                text_cz = soup_cz.get_text(separator="\n", strip=True)

                # Buscar padrao Cidade-UF ou Cidade - UF
                cidades_ba = {"Salvador", "Feira de Santana", "Lauro de Freitas", "Camaçari"}
                for cidade_cand in cidades_ba:
                    if cidade_cand.lower() in text_cz.lower():
                        dados["cidade"] = cidade_cand
                        dados["estado"] = "BA"
                        break

                # Nome: H1 ou titulo
                h1_cz = soup_cz.find("h1")
                if h1_cz:
                    nome_h1 = h1_cz.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1

                # Bairro
                m_bairro = re.search(r'(?:Porto da Barra|Jardim Armação|Praia do Flamengo|Barra|Armação|Ondina|Pituba|Itapuã)', text_cz)
                if m_bairro:
                    dados["bairro"] = m_bairro.group(0)

                # Status da obra
                if re.search(r'Serviços Preliminares.*?0%', text_cz, re.DOTALL):
                    dados["fase"] = "Breve Lançamento"
                elif re.search(r'(\d+)%.*(?:conclu|entregue)', text_cz, re.IGNORECASE):
                    dados["fase"] = "Imóvel Pronto"

            # Lyall: dados da pagina individual
            if empresa_key == "lyall":
                soup_ly = BeautifulSoup(html, "html.parser")
                text_ly = soup_ly.get_text(separator="\n", strip=True)

                # Nome do H1
                h1_ly = soup_ly.find("h1")
                if h1_ly:
                    nome_h1 = h1_ly.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1

                # Cidade/Bairro do texto
                m_loc_ly = re.search(r'([\w\s]+?)\s*[-/]\s*(Lajeado|Estrela|Arroio do Meio|Teutônia|Encantado)', text_ly)
                if m_loc_ly:
                    dados["bairro"] = m_loc_ly.group(1).strip()
                    dados["cidade"] = m_loc_ly.group(2).strip()
                elif not dados.get("cidade"):
                    # Buscar Lajeado ou outra cidade no texto
                    for cidade_ly in ["Lajeado", "Estrela", "Arroio do Meio", "Teutônia", "Encantado"]:
                        if cidade_ly.lower() in text_ly.lower():
                            dados["cidade"] = cidade_ly
                            break

                # Fase a partir de datas/texto
                m_inicio = re.search(r'[Ii]n[ií]cio.*?(\w+\s+\d{4})', text_ly)
                m_conclusao = re.search(r'[Cc]onclus[ãa]o.*?(\w+\s+\d{4})', text_ly)
                if "lançamento" in text_ly.lower() or "lancamento" in text_ly.lower():
                    dados["fase"] = "Lançamento"
                elif "em obras" in text_ly.lower() or "em construção" in text_ly.lower():
                    dados["fase"] = "Em Construção"
                elif "concluído" in text_ly.lower() or "entregue" in text_ly.lower():
                    dados["fase"] = "Imóvel Pronto"

            # LCM: dados da pagina
            if empresa_key == "lcm":
                soup_lcm = BeautifulSoup(html, "html.parser")
                text_lcm = soup_lcm.get_text(separator="\n", strip=True)

                # Nome do H1
                h1_lcm = soup_lcm.find("h1")
                if h1_lcm:
                    nome_h1 = h1_lcm.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1

                # Cidade do texto
                cidades_mg = {
                    "Belo Horizonte": "MG", "Betim": "MG", "Contagem": "MG",
                    "Nova Lima": "MG", "Ribeirão das Neves": "MG", "Santa Luzia": "MG",
                    "Sete Lagoas": "MG", "Ibirité": "MG", "Sabará": "MG",
                    "Porto Seguro": "BA", "Arraial d'Ajuda": "BA",
                }
                for cidade_cand, estado_cand in cidades_mg.items():
                    if cidade_cand.lower() in text_lcm.lower():
                        dados["cidade"] = cidade_cand
                        dados["estado"] = estado_cand
                        break

                # Fase
                if "breve lançamento" in text_lcm.lower() or "em breve" in text_lcm.lower():
                    dados["fase"] = "Breve Lançamento"
                elif "em obras" in text_lcm.lower() or "em construção" in text_lcm.lower():
                    dados["fase"] = "Em Construção"
                elif "pronto" in text_lcm.lower() or "entregue" in text_lcm.lower():
                    dados["fase"] = "Imóvel Pronto"
                elif "lançamento" in text_lcm.lower():
                    dados["fase"] = "Lançamento"

            # Bild: extrair cidade do slug da URL e dados da pagina
            if empresa_key == "bild":
                # Cidade do slug
                slug_parts = slug.split("/")
                if len(slug_parts) == 2:
                    cidade_slug = slug_parts[0]
                    cidade_bild = cidade_slug.replace("-", " ").title()
                    # Correcoes
                    cidade_bild = cidade_bild.replace("Sao Paulo", "São Paulo")
                    cidade_bild = cidade_bild.replace("Sao Jose Do Rio Preto", "São José do Rio Preto")
                    cidade_bild = cidade_bild.replace("Ribeirao Preto", "Ribeirão Preto")
                    dados["cidade"] = cidade_bild

                    # Estado a partir da cidade
                    estados_bild = {
                        "Ribeirão Preto": "SP", "Franca": "SP", "Bauru": "SP",
                        "Araraquara": "SP", "São José Do Rio Preto": "SP",
                        "Campinas": "SP", "Piracicaba": "SP", "Marília": "SP",
                        "Sorocaba": "SP", "Paulínia": "SP", "São Paulo": "SP",
                        "Americana": "SP", "Londrina": "PR", "Cuiabá": "MT",
                        "Uberlândia": "MG", "Uberaba": "MG",
                    }
                    dados["estado"] = estados_bild.get(cidade_bild, "SP")

                # Extrair dados da pagina individual
                soup_bild = BeautifulSoup(html, "html.parser")
                text_bild = soup_bild.get_text(separator="\n", strip=True)

                # Nome: H1 ou titulo limpo
                h1_bild = soup_bild.find("h1")
                if h1_bild:
                    nome_h1 = h1_bild.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1

                # Bairro
                m_bairro_bild = re.search(r'(?:Bairro|Localização):\s*([\w\s\'"]+)', text_bild)
                if m_bairro_bild:
                    dados["bairro"] = m_bairro_bild.group(1).strip()
                else:
                    # Tentar extrair bairro do endereco
                    m_bairro2 = re.search(r'(?:R\.|Rua|Av\.|Avenida)[^,]+,\s*\d+\s*[-–]\s*([\w\s]+?)(?:\s*[-–]|\s*$)', text_bild)
                    if m_bairro2:
                        dados["bairro"] = m_bairro2.group(1).strip()

                # Status
                if "pronto para morar" in text_bild.lower() or "pronto pra morar" in text_bild.lower():
                    dados["fase"] = "Imóvel Pronto"
                elif "100%" in text_bild and ("conclu" in text_bild.lower() or "pronto" in text_bild.lower()):
                    dados["fase"] = "Imóvel Pronto"
                elif "em obras" in text_bild.lower() or "em construção" in text_bild.lower():
                    dados["fase"] = "Em Construção"
                elif "lançamento" in text_bild.lower() or "lancamento" in text_bild.lower():
                    dados["fase"] = "Lançamento"

                # Vagas
                m_vagas = re.search(r'(\d+)\s*(?:vagas?|vaga)', text_bild, re.IGNORECASE)
                if m_vagas and not dados.get("numero_vagas"):
                    dados["numero_vagas"] = m_vagas.group(1)

                # Evolucao obra
                fases_obra = re.findall(r'(?:Fundação|Estrutura|Alvenaria|Acabamento)\s*(\d+)%', text_bild)
                if fases_obra:
                    try:
                        pcts = [int(p) for p in fases_obra]
                        media = sum(pcts) / len(pcts)
                        dados["evolucao_obra_pct"] = round(media, 1)
                        if all(p == 100 for p in pcts) and not dados.get("fase"):
                            dados["fase"] = "Imóvel Pronto"
                        elif any(p > 0 for p in pcts) and not dados.get("fase"):
                            dados["fase"] = "Em Construção"
                    except (ValueError, ZeroDivisionError):
                        pass

            # Estado default
            if not dados.get("estado") and config.get("estado_default"):
                dados["estado"] = config["estado_default"]

            # Cidade default
            if not dados.get("cidade") and config.get("cidade_default"):
                dados["cidade"] = config["cidade_default"]

            # Fase: normalizar
            if dados.get("fase"):
                dados["fase"] = _normalizar_fase(dados["fase"])

            # Validar nome
            if not dados.get("nome") or dados["nome"].strip() == "":
                # Tentar nome do slug
                nome_slug = slug.split("/")[-1].replace("-", " ").title()
                if len(nome_slug) >= 3:
                    dados["nome"] = nome_slug
                else:
                    logger.warning(f"  Nome nao encontrado, pulando")
                    erros += 1
                    time.sleep(DELAY)
                    continue

            nome = dados["nome"]

            # MCMV: verificar preco e config
            if config.get("prog_mcmv") is not None:
                dados["prog_mcmv"] = config["prog_mcmv"]
            else:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # Data coleta
            dados["data_coleta"] = datetime.now().isoformat()

            # Verificar se existe
            existe = empreendimento_existe(nome_banco, nome)

            if existe and atualizar:
                atualizar_empreendimento(nome_banco, nome, dados)
                atualizados += 1
                logger.info(f"  Atualizado: {nome}")
            elif not existe:
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {nome} | {dados.get('cidade', 'N/A')} | {dados.get('fase', 'N/A')} | {dados.get('dormitorios_descricao', 'N/A')}")
            else:
                logger.info(f"  Ja existe: {nome}")

            # Download de imagens
            if not sem_imagens:
                download_imagens(html, url, empresa_key, slug.replace("/", "_"), logger)

        except Exception as e:
            logger.error(f"  Erro: {e}", exc_info=True)
            erros += 1

        time.sleep(DELAY)

    # Relatorio
    logger.info("=" * 60)
    logger.info(f"RELATORIO - {nome_banco}")
    logger.info(f"  Total processado: {total}")
    logger.info(f"  Novos inseridos: {novos}")
    logger.info(f"  Atualizados: {atualizados}")
    logger.info(f"  Erros: {erros}")
    logger.info(f"  Total no banco: {contar_empreendimentos(nome_banco)}")
    logger.info("=" * 60)

    return {"novos": novos, "atualizados": atualizados, "erros": erros}


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper Batch M5 - 8 incorporadoras medias (ultimo batch)")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: helbor, bild, todas)")
    parser.add_argument("--atualizar", action="store_true",
                       help="Reatualizar empreendimentos ja existentes")
    parser.add_argument("--limite", type=int, default=None,
                       help="Limitar a N empreendimentos")
    parser.add_argument("--sem-imagens", action="store_true",
                       help="Nao baixar imagens")
    parser.add_argument("--listar", action="store_true",
                       help="Listar empresas configuradas")
    args = parser.parse_args()

    if args.listar:
        print("\nEmpresas configuradas (Batch M5):")
        for key, cfg in EMPRESAS.items():
            nota = ""
            if cfg.get("ssl_expired"):
                nota = " [SSL EXPIRADO]"
            elif cfg.get("prog_mcmv") == 0:
                nota = " [NAO MCMV]"
            print(f"  {key:18s} -> {cfg['nome_banco']}{nota}")
        print(f"\nNOTA: NBR (nbrempreendimentos.com.br) tem certificado SSL expirado")
        print(f"\nUso: python scrapers/generico_novas_empresas_m5.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_m5.py --empresa todas")
        return

    if not args.empresa:
        parser.print_help()
        return

    if args.empresa.lower() == "todas":
        empresas = list(EMPRESAS.keys())
    else:
        empresa_key = args.empresa.lower()
        if empresa_key not in EMPRESAS:
            print(f"Empresa '{empresa_key}' nao encontrada. Disponiveis:")
            for key in EMPRESAS:
                print(f"  {key}")
            return
        empresas = [empresa_key]

    resultados = {}
    for empresa_key in empresas:
        try:
            resultado = processar_empresa(
                empresa_key,
                atualizar=args.atualizar,
                limite=args.limite,
                sem_imagens=args.sem_imagens,
            )
            resultados[empresa_key] = resultado
        except Exception as e:
            print(f"ERRO ao processar {empresa_key}: {e}")
            import traceback
            traceback.print_exc()
            resultados[empresa_key] = {"novos": 0, "atualizados": 0, "erros": -1}

    if len(resultados) > 1:
        print("\n" + "=" * 60)
        print("  RESUMO GERAL — Batch M5")
        print("=" * 60)
        total_novos = 0
        total_erros = 0
        for key, r in resultados.items():
            if r:
                nota = r.get("nota", "")
                if nota:
                    status = nota
                elif r["erros"] >= 0:
                    status = "OK"
                else:
                    status = "FALHA"
                print(f"  {EMPRESAS[key]['nome_banco']:20s} +{r['novos']} novos, {r['erros']} erros [{status}]")
                total_novos += r["novos"]
                total_erros += max(r["erros"], 0)
        print(f"  {'TOTAL':20s} +{total_novos} novos, {total_erros} erros")
        print("=" * 60)


if __name__ == "__main__":
    main()

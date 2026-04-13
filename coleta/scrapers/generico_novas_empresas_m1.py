"""
Scraper Batch M1 — 10 Incorporadoras de Complexidade Media
============================================================
Sites medios: JS/React, APIs internas, paginacao complexa.

Empresas:
    living       — meuliving.com.br (Drupal SSR, paginacao ?page=N)
    riva         — rivaincorporadora.com.br (SSR, listagem direta)
    oneinnovation — oneinnovation.com.br (Nuxt.js, sitemap.xml)
    youinc       — youinc.com.br (Nuxt.js, sitemap.xml, 126 URLs)
    engelux      — engelux.com.br (Next.js 13+, sitemap.xml, 8 props)
    vitacon      — vitacon.com.br (Next.js __NEXT_DATA__ na listagem)
    jeronimo     — jeronimodaveiga.com.br (SSR, cidade/bairro na URL)
    kallas       — grupokallas.com.br (WP REST API empreendimento)
    eztec        — eztec.com.br (WP, links da homepage)
    brz          — brzempreendimentos.com (Blazor SPA — precisa Selenium)

Uso:
    python scrapers/generico_novas_empresas_m1.py --empresa living
    python scrapers/generico_novas_empresas_m1.py --empresa todas
    python scrapers/generico_novas_empresas_m1.py --listar
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
    logger = logging.getLogger(f"scraper.m1.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"m1_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# HEADERS customizados (alguns sites bloqueiam sem Accept)
# ============================================================
HEADERS_M1 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch M1
# ============================================================
EMPRESAS = {
    "living": {
        "nome_banco": "Living",
        "base_url": "https://meuliving.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Marca Cyrela, medio/alto padrao
        "urls_listagem": [],  # Coleta customizada via paginacao
        "padrao_link": r"meuliving\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.field-content", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:a\s+\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:a\s+\d+(?:[.,]\d+)?\s*)?m[²2]"},
        },
    },

    "riva": {
        "nome_banco": "Riva",
        "base_url": "https://www.rivaincorporadora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.rivaincorporadora.com.br/empreendimentos/",
        ],
        "padrao_link": r"rivaincorporadora\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "oneinnovation": {
        "nome_banco": "One Innovation",
        "base_url": "https://oneinnovation.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP top 10, medio/alto padrao
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"oneinnovation\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "youinc": {
        "nome_banco": "You,inc",
        "base_url": "https://www.youinc.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP compactos, medio padrao
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"youinc\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*residenciais?|unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt]erreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "engelux": {
        "nome_banco": "Engelux",
        "base_url": "https://www.engelux.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"engelux\.com\.br/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vitacon": {
        "nome_banco": "Vitacon",
        "base_url": "https://vitacon.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Studios SP, medio/alto padrao
        "urls_listagem": [],  # Coleta via __NEXT_DATA__ da listagem
        "padrao_link": r"vitacon\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "jeronimo": {
        "nome_banco": "Jerônimo da Veiga",
        "base_url": "https://jeronimodaveiga.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://jeronimodaveiga.com.br/imoveis/",
        ],
        "padrao_link": r"jeronimodaveiga\.com\.br/(?:apartamento|casa|lotes|area-industrial|laje-corporativa)/[^/]+/[^/]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:,\s*\d+\s*)?(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\blofts?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "kallas": {
        "nome_banco": "Kallas",
        "base_url": "https://grupokallas.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP medio/alto padrao
        "wp_api_url": "https://grupokallas.com.br/wp-json/wp/v2/empreendimento?per_page=100&_embed",
        "urls_listagem": [],  # Coleta via WP API
        "padrao_link": r"grupokallas\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt]erreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*(?:vagas?\s*(?:de\s*)?(?:auto|garagem|estacionamento))"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "eztec": {
        "nome_banco": "Eztec",
        "base_url": "https://www.eztec.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "prog_mcmv": 0,  # B3, medio/alto padrao
        "urls_listagem": [
            "https://www.eztec.com.br/",
        ],
        "padrao_link": r"eztec\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "brz": {
        "nome_banco": "BRZ",
        "base_url": "https://brzempreendimentos.com",
        "nome_from_title": True,
        "urls_listagem": [],
        "padrao_link": r"brzempreendimentos\.com/([\w-]+)",
        "selenium_required": True,  # Blazor SPA
        "parsers": {},
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

    # 8. Padrao "lat":-XX.XXX,"lng":-XX.XXX (JSON, ex: Vitacon, APIs)
    match = re.search(r'"lat"\s*:\s*(-?\d+\.\d{4,})\s*,\s*"lng"\s*:\s*(-?\d+\.\d{4,})', html)
    if match:
        return match.group(1), match.group(2)

    # 9. OpenGraph place:location meta tags (ex: Living/Drupal)
    lat_match = re.search(r'property="place:location:latitude"\s+content="(-?\d+\.\d{4,})"', html)
    lon_match = re.search(r'property="place:location:longitude"\s+content="(-?\d+\.\d{4,})"', html)
    if lat_match and lon_match:
        return lat_match.group(1), lon_match.group(1)

    # 10. data-latitude/data-longitude HTML attributes (ex: Kallas)
    lat_match = re.search(r'data-latitude="(-?\d+\.\d{4,})"', html)
    lon_match = re.search(r'data-longitude="(-?\d+\.\d{4,})"', html)
    if lat_match and lon_match:
        return lat_match.group(1), lon_match.group(1)

    return None, None


# ============================================================
# FUNCOES CUSTOMIZADAS DE COLETA DE LINKS
# ============================================================

def _fetch_html_m1(url, logger):
    """Fetch HTML com headers customizados para sites que bloqueiam."""
    try:
        resp = requests.get(url, headers=HEADERS_M1, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None


def _coletar_links_living(config, logger):
    """Coleta links da Living via paginacao Drupal (?page=0, ?page=1, ...)."""
    links = {}
    base = config["base_url"]

    for page in range(0, 15):
        url = f"{base}/empreendimentos?page={page}"
        logger.info(f"Coletando links de: {url}")
        html = _fetch_html_m1(url, logger)
        if not html:
            break

        found = set(re.findall(r'href=["\'](/empreendimentos/[\w-]+)', html))
        new_count = 0
        for href in found:
            slug = href.split("/")[-1]
            full_url = f"{base}{href}"
            if slug not in links:
                links[slug] = full_url
                new_count += 1

        logger.info(f"  Pagina {page}: {len(found)} links, {new_count} novos")
        if new_count == 0:
            break
        time.sleep(DELAY)

    logger.info(f"Total de links Living: {len(links)}")
    return links


def _coletar_links_riva(config, logger):
    """Coleta links da Riva da pagina de empreendimentos."""
    links = {}
    base = config["base_url"]

    url = f"{base}/empreendimentos/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    found = set(re.findall(
        r'href=["\'](https?://www\.rivaincorporadora\.com\.br/empreendimentos/[\w-]+/?)',
        html
    ))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/")[-1]
        if slug and slug != "empreendimentos":
            links[slug] = href_clean + "/"

    logger.info(f"Total de links Riva: {len(links)}")
    return links


def _coletar_links_oneinnovation(config, logger):
    """Coleta links da One Innovation via sitemap.xml."""
    links = {}

    url = "https://oneinnovation.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://oneinnovation\.com\.br/imoveis/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug:
            links[slug] = u_clean + "/"

    logger.info(f"Total de links One Innovation (sitemap): {len(links)}")
    return links


def _coletar_links_youinc(config, logger):
    """Coleta links da You,inc via sitemap.xml."""
    links = {}

    url = "https://www.youinc.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://www\.youinc\.com\.br/imovel/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug:
            links[slug] = u_clean

    logger.info(f"Total de links You,inc (sitemap): {len(links)}")
    return links


def _coletar_links_engelux(config, logger):
    """Coleta links da Engelux via sitemap.xml."""
    links = {}

    url = "https://engelux.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://www\.engelux\.com\.br/[^<]+)</loc>', html)
    # Filtrar apenas paginas de empreendimentos (excluir paginas institucionais)
    paginas_inst = {
        "imoveis-a-venda", "simulador", "a-engelux", "parcerias",
        "fale-conosco", "his-hmp", "portfolio", "politica-de-privacidade", ""
    }
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        # Excluir paginas que nao sao empreendimentos
        if slug and slug not in paginas_inst and "?" not in u:
            links[slug] = u_clean

    logger.info(f"Total de links Engelux (sitemap): {len(links)}")
    return links


def _coletar_links_vitacon(config, logger):
    """Coleta links e dados da Vitacon via __NEXT_DATA__ da pagina de listagem."""
    links = {}

    url = "https://vitacon.com.br/empreendimentos"
    logger.info(f"Coletando dados via __NEXT_DATA__: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        logger.warning("__NEXT_DATA__ nao encontrado na pagina da Vitacon")
        return links

    try:
        data = json.loads(script.string)
        # Armazenar dados completos para uso posterior
        _vitacon_cache.clear()

        nodes = data.get("props", {}).get("pageProps", {}).get("node", {})
        # A listagem pode estar em diferentes campos
        empreendimentos = []

        # Buscar recursivamente por items com 'uri'
        def find_items(obj):
            if isinstance(obj, dict):
                if "uri" in obj and "titulo" in obj:
                    empreendimentos.append(obj)
                for v in obj.values():
                    find_items(v)
            elif isinstance(obj, list):
                for item in obj:
                    find_items(item)

        find_items(data)

        for emp in empreendimentos:
            uri = emp.get("uri", "")
            titulo = emp.get("titulo", "")
            if uri and "/empreendimentos/" in uri:
                slug = uri.rstrip("/").split("/")[-1]
                full_url = f"https://vitacon.com.br{uri}"
                links[slug] = full_url
                _vitacon_cache[slug] = emp

        logger.info(f"Total de links Vitacon (__NEXT_DATA__): {len(links)}")

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__: {e}")

    return links

# Cache para dados __NEXT_DATA__ da Vitacon
_vitacon_cache = {}


def _coletar_links_jeronimo(config, logger):
    """Coleta links da Jeronimo da Veiga da pagina /imoveis/."""
    links = {}
    base = config["base_url"]

    url = f"{base}/imoveis/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    # Links seguem: /apartamento/{cidade}/{bairro}/{nome} ou /casa/... etc
    found = set(re.findall(
        r'href=["\'](/(?:apartamento|casa|lotes|area-industrial|laje-corporativa)/[^"\']+)',
        html
    ))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/")[-1]
        if slug and len(slug) > 2:
            full_url = f"{base}{href_clean}"
            links[slug] = full_url

    logger.info(f"Total de links Jeronimo da Veiga: {len(links)}")
    return links


def _coletar_links_kallas(config, logger):
    """Coleta links da Kallas via WP REST API."""
    links = {}

    wp_api_url = config.get("wp_api_url")
    if not wp_api_url:
        return links

    logger.info(f"Coletando links via WP REST API: {wp_api_url}")
    try:
        resp = requests.get(wp_api_url, headers=HEADERS_M1, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # Armazenar dados da API para uso posterior
            _kallas_cache.clear()
            for item in data:
                slug = item.get("slug", "")
                title = item.get("title", {}).get("rendered", "")
                if slug:
                    url = f"https://grupokallas.com.br/empreendimento/{slug}"
                    links[slug] = url
                    _kallas_cache[slug] = item
            logger.info(f"WP API Kallas: {len(links)} empreendimentos")
    except Exception as e:
        logger.error(f"Erro na WP API Kallas: {e}")

    return links

# Cache para dados WP API do Kallas
_kallas_cache = {}


def _coletar_links_eztec(config, logger):
    """Coleta links da Eztec da homepage."""
    links = {}
    base = config["base_url"]

    url = f"{base}/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m1(url, logger)
    if not html:
        return links

    found = set(re.findall(r'href=["\'](https?://www\.eztec\.com\.br/imovel/[\w-]+/?)', html))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/")[-1]
        if slug:
            links[slug] = href_clean + "/"

    logger.info(f"Total de links Eztec (homepage): {len(links)}")
    return links


# ============================================================
# FUNCOES DE EXTRACAO DE DADOS ESPECIFICAS
# ============================================================

def _extrair_nome_from_title(html, empresa_key):
    """Extrai nome limpo do <title> da pagina."""
    if not html:
        return None
    match = re.search(r'<title>([^<]+)</title>', html)
    if not match:
        return None
    title = match.group(1).strip()

    # Remover sufixos comuns
    for sufixo in [
        " | Living", " | Cyrela", " - Living", " - Cyrela",
        " | Riva", " - Riva", " | Riva Incorporadora",
        " | One Innovation", " - One Innovation",
        " | You,inc", " - You,inc", " | youinc", " - youinc",
        " | Engelux", " - Engelux",
        " | Vitacon", " - Vitacon",
        " | Jerônimo da Veiga", " - Jerônimo da Veiga",
        " | Kallas", " - Kallas", " | Grupo Kallas",
        " | Eztec", " - Eztec", " | EZTEC",
        " | BRZ", " - BRZ",
    ]:
        if title.lower().endswith(sufixo.lower()):
            title = title[:-len(sufixo)].strip()
            break

    # Limpar pipe ou dash no final
    title = re.sub(r'\s*[|–-]\s*$', '', title).strip()

    if len(title) < 3 or len(title) > 80:
        return None

    return title


def _extrair_cidade_estado_da_url(url):
    """Extrai cidade e estado de URLs com padrao /{tipo}/{cidade}/{bairro}/{nome}."""
    # Padrao youinc: /imovel/apartamentos-venda-{bairro}-{cidade}-{estado}-{nome}
    match = re.search(r'youinc\.com\.br/imovel/[\w-]+-venda-([\w-]+)-(sao-paulo|campinas|sao-caetano-do-sul|santo-andre|sao-bernardo)-(sp|rj|mg)-', url)
    if match:
        cidade = match.group(2).replace("-", " ").title()
        estado = match.group(3).upper()
        # Correcoes
        cidade = cidade.replace("Sao Paulo", "São Paulo").replace("Sao Caetano Do Sul", "São Caetano do Sul").replace("Sao Bernardo", "São Bernardo do Campo")
        return cidade, estado

    # Padrao youinc alternativo simples
    match = re.search(r'youinc\.com\.br/imovel/[\w-]+-(sao-paulo|campinas|sao-caetano-do-sul|santo-andre)-(sp|rj|mg)', url)
    if match:
        cidade = match.group(1).replace("-", " ").title()
        estado = match.group(2).upper()
        cidade = cidade.replace("Sao Paulo", "São Paulo").replace("Sao Caetano Do Sul", "São Caetano do Sul")
        return cidade, estado

    # Padrao oneinnovation: /imoveis/apartamento-{nome}-{cidade}-{estado}/
    # URL: apartamento-nex-one-estacao-eucaliptos-sao-paulo-sp
    match = re.search(r'oneinnovation\.com\.br/imoveis/[\w-]+-(sao-paulo|campinas)-(sp)/?$', url.rstrip("/"))
    if match:
        cidade = match.group(1).replace("-", " ").title()
        estado = match.group(2).upper()
        cidade = cidade.replace("Sao Paulo", "São Paulo")
        return cidade, estado

    # Padrao jeronimo: /{tipo}/{cidade}/{bairro}/{nome}
    match = re.search(r'jeronimodaveiga\.com\.br/(?:apartamento|casa|lotes|area-industrial|laje-corporativa)/([\w-]+)/', url)
    if match:
        cidade_slug = match.group(1)
        # Remover sufixo -sp, -rj etc
        estado = None
        if cidade_slug.endswith("-sp"):
            estado = "SP"
            cidade_slug = cidade_slug[:-3]
        elif cidade_slug.endswith("-rj"):
            estado = "RJ"
            cidade_slug = cidade_slug[:-3]

        cidade = cidade_slug.replace("-", " ").title()
        # Correcoes
        cidade = cidade.replace("Sao Paulo", "São Paulo").replace("Nova Iguacu", "Nova Iguaçu")
        cidade = cidade.replace("Taboao Da Serra", "Taboão da Serra").replace("Poa", "Poá")
        cidade = cidade.replace("Itaborai", "Itaboraí").replace("Niteroi", "Niterói")
        cidade = cidade.replace("Rio De Janeiro", "Rio de Janeiro").replace("Santo Andre", "Santo André")

        if not estado:
            # Inferir estado pela cidade
            cidades_rj = {"Rio de Janeiro", "Nova Iguaçu", "Niterói", "Belford Roxo", "Itaboraí"}
            estado = "RJ" if cidade in cidades_rj else "SP"

        return cidade, estado

    return None, None


def _extrair_bairro_da_url_jeronimo(url):
    """Extrai bairro da URL do Jeronimo da Veiga."""
    match = re.search(r'jeronimodaveiga\.com\.br/\w+/[\w-]+/([\w-]+)/', url)
    if match:
        bairro_slug = match.group(1)
        bairro = bairro_slug.replace("-", " ").title()
        # Correcoes
        bairro = bairro.replace("Na Avenida Paulista", "Avenida Paulista")
        return bairro
    return None


def _extrair_bairro_da_url_youinc(url):
    """Extrai bairro da URL da You,inc."""
    match = re.search(r'youinc\.com\.br/imovel/apartamentos-venda-([\w-]+)-(?:sao-paulo|campinas|sao-caetano)', url)
    if match:
        bairro_slug = match.group(1)
        bairro = bairro_slug.replace("-", " ").title()
        bairro = bairro.replace("Sao Paulo", "São Paulo")
        return bairro
    return None


def _extrair_dados_vitacon_cache(slug, logger):
    """Extrai dados do cache __NEXT_DATA__ da Vitacon."""
    if slug not in _vitacon_cache:
        return {}

    emp = _vitacon_cache[slug]
    dados = {}

    titulo = emp.get("titulo", "")
    if titulo:
        dados["nome"] = titulo

    # Bairro
    bairro_obj = emp.get("bairro", {})
    if isinstance(bairro_obj, dict) and bairro_obj.get("name"):
        dados["bairro"] = bairro_obj["name"]
    elif isinstance(bairro_obj, str) and bairro_obj:
        dados["bairro"] = bairro_obj

    # Status
    status = emp.get("status_imovel", "")
    if isinstance(status, dict):
        status = status.get("name", "")
    if status:
        dados["fase"] = _normalizar_fase(status)

    # Tipologia
    tipo_list = emp.get("tipo_imovel_list", [])
    if tipo_list:
        nomes_tipo = []
        for t in tipo_list:
            if isinstance(t, dict):
                nomes_tipo.append(t.get("name", ""))
            elif isinstance(t, str):
                nomes_tipo.append(t)
        nomes_tipo = [n for n in nomes_tipo if n]
        if nomes_tipo:
            dados["dormitorios_descricao"] = " e ".join(nomes_tipo)

    # Coordenadas
    loc = emp.get("localizacao", {})
    if isinstance(loc, dict):
        lat = loc.get("lat")
        lng = loc.get("lng")
        if lat and lng:
            dados["latitude"] = str(lat)
            dados["longitude"] = str(lng)
        endereco = loc.get("address", "")
        if endereco:
            dados["endereco"] = endereco

    # Siglas HIS/HMP
    sigla = emp.get("exibir_sigla", "")
    if sigla and ("HIS" in sigla or "HMP" in sigla):
        dados["prog_mcmv"] = 1

    return dados


def _extrair_dados_kallas_api(slug, api_data, logger):
    """Extrai dados do cache WP API do Kallas."""
    dados = {}

    title = api_data.get("title", {}).get("rendered", "")
    if title:
        # Limpar HTML entities
        dados["nome"] = BeautifulSoup(title, "html.parser").get_text(strip=True)

    # Status via taxonomia embeddida
    embedded = api_data.get("_embedded", {})
    wp_terms = embedded.get("wp:term", [])
    for term_group in wp_terms:
        if isinstance(term_group, list):
            for term in term_group:
                tax = term.get("taxonomy", "")
                name = term.get("name", "")
                if tax == "status-do-empreendimento" and name:
                    dados["fase"] = _normalizar_fase(name)
                elif tax == "opcao-de-dormitorio" and name:
                    if "dormitorios_descricao" not in dados:
                        dados["dormitorios_descricao"] = name
                    else:
                        dados["dormitorios_descricao"] += " e " + name

    # Itens de lazer via taxonomia
    lazer_items = []
    for term_group in wp_terms:
        if isinstance(term_group, list):
            for term in term_group:
                if term.get("taxonomy") == "itens-de-lazer":
                    lazer_items.append(term.get("name", ""))
    if lazer_items:
        dados["itens_lazer"] = " | ".join(lazer_items)

    return dados


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
    if any(k in fase_lower for k in ["pronto", "entregue", "conclu"]):
        return "Imóvel Pronto"
    if "últimas" in fase_lower or "ultimas" in fase_lower:
        return "Lançamento"

    return fase_raw.strip()


def _extrair_nome_oneinnovation_from_title(title_raw):
    """Extrai nome limpo do title da One Innovation."""
    if not title_raw:
        return None
    # Title format: "Nex One Estacao Eucaliptos | Studio e 1 Dorm."
    nome = title_raw.split("|")[0].strip()
    nome = nome.split(" - ")[0].strip()
    # Limpar sufixos
    for suf in [" One Innovation", " | One Innovation"]:
        if nome.endswith(suf):
            nome = nome[:-len(suf)].strip()
    return nome if len(nome) >= 3 else None


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    # Verificar se precisa Selenium
    if config.get("selenium_required"):
        logger.warning(f"{nome_banco}: precisa Selenium (Blazor SPA). Pulando.")
        print(f"  NOTA: {nome_banco} ({config['base_url']}) precisa Selenium — Blazor SPA, sem API acessivel")
        return {"novos": 0, "atualizados": 0, "erros": 0, "nota": "Precisa Selenium"}

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper M1: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    coletores = {
        "living": _coletar_links_living,
        "riva": _coletar_links_riva,
        "oneinnovation": _coletar_links_oneinnovation,
        "youinc": _coletar_links_youinc,
        "engelux": _coletar_links_engelux,
        "vitacon": _coletar_links_vitacon,
        "jeronimo": _coletar_links_jeronimo,
        "kallas": _coletar_links_kallas,
        "eztec": _coletar_links_eztec,
    }

    coletor = coletores.get(empresa_key)
    if coletor:
        links = coletor(config, logger)
    else:
        links = coletar_links_empreendimentos(config, logger)

    if not links:
        logger.warning("Nenhum link de empreendimento encontrado!")
        return {"novos": 0, "atualizados": 0, "erros": 0}

    logger.info(f"Links coletados: {len(links)}")

    # Filtrar ja processados (a menos que --atualizar)
    if not atualizar:
        links_novos = {}
        for slug, url in links.items():
            nome_teste = slug.replace("-", " ").title()
            if not empreendimento_existe(nome_banco, nome_teste):
                links_novos[slug] = url
            elif not empreendimento_existe(nome_banco, slug):
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

        # === Vitacon: usar dados do cache __NEXT_DATA__ + pagina individual ===
        if empresa_key == "vitacon":
            dados_cache = _extrair_dados_vitacon_cache(slug, logger)
            nome = dados_cache.get("nome", "")
            if nome and not atualizar:
                if empreendimento_existe(nome_banco, nome):
                    logger.info(f"  Ja existe (cache): {nome}")
                    continue

        html = _fetch_html_m1(url, logger)
        if not html:
            erros += 1
            time.sleep(DELAY)
            continue

        try:
            dados = extrair_dados_empreendimento(html, url, config, logger)

            # === Enriquecimento por empresa ===

            # Nome: tentar do title primeiro
            nome_title = _extrair_nome_from_title(html, empresa_key)
            if nome_title and (not dados.get("nome") or len(dados.get("nome", "")) < 3):
                dados["nome"] = nome_title

            # Vitacon: extrair nome do title (padrao "NOME - Bairro - Vitacon")
            if empresa_key == "vitacon":
                title_match = re.search(r'<title>([^<]+)</title>', html)
                if title_match:
                    parts = title_match.group(1).strip().split(" - ")
                    if len(parts) >= 2:
                        # Primeiro segmento e o nome
                        nome_vit = parts[0].strip()
                        if nome_vit and len(nome_vit) >= 3:
                            dados["nome"] = nome_vit

            # Vitacon: merge cache com dados extraidos (cache complementa, nao sobrescreve)
            if empresa_key == "vitacon":
                dados_cache = _extrair_dados_vitacon_cache(slug, logger)
                for k, v in dados_cache.items():
                    if k == "nome":
                        # Para nome, preferir o da pagina (mais completo) se disponivel
                        if not dados.get("nome") or len(dados.get("nome", "")) < 3:
                            dados["nome"] = v
                    elif v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

            # Kallas: merge dados da API
            if empresa_key == "kallas" and slug in _kallas_cache:
                dados_api = _extrair_dados_kallas_api(slug, _kallas_cache[slug], logger)
                for k, v in dados_api.items():
                    if v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

            # Coordenadas do HTML
            lat, lon = extrair_coordenadas(html)
            if lat and lon:
                dados["latitude"] = lat
                dados["longitude"] = lon

            # Cidade/Estado da URL (youinc, oneinnovation, jeronimo)
            # Para oneinnovation: SEMPRE usar cidade da URL (generico extrai bairro como cidade)
            if empresa_key == "oneinnovation":
                cidade_url, estado_url = _extrair_cidade_estado_da_url(url)
                if cidade_url:
                    # Salvar cidade antiga como bairro se nao tinha bairro
                    if dados.get("cidade") and not dados.get("bairro"):
                        dados["bairro"] = dados["cidade"]
                    dados["cidade"] = cidade_url
                if estado_url:
                    dados["estado"] = estado_url
            elif not dados.get("cidade"):
                cidade_url, estado_url = _extrair_cidade_estado_da_url(url)
                if cidade_url:
                    dados["cidade"] = cidade_url
                if estado_url and not dados.get("estado"):
                    dados["estado"] = estado_url

            # Bairro da URL
            if not dados.get("bairro"):
                if empresa_key == "jeronimo":
                    bairro = _extrair_bairro_da_url_jeronimo(url)
                    if bairro:
                        dados["bairro"] = bairro
                elif empresa_key == "youinc":
                    bairro = _extrair_bairro_da_url_youinc(url)
                    if bairro:
                        dados["bairro"] = bairro

            # Estado default
            if not dados.get("estado") and config.get("estado_default"):
                dados["estado"] = config["estado_default"]

            # Cidade default
            if not dados.get("cidade") and config.get("cidade_default"):
                dados["cidade"] = config["cidade_default"]

            # One Innovation: nome do title
            if empresa_key == "oneinnovation":
                title_raw = re.search(r'<title>([^<]+)</title>', html)
                if title_raw:
                    nome_one = _extrair_nome_oneinnovation_from_title(title_raw.group(1))
                    if nome_one:
                        dados["nome"] = nome_one

            # Engelux: nome do title tem sufixo "| Apartamentos a Venda..."
            if empresa_key == "engelux":
                title_match = re.search(r'<title>([^<]+)</title>', html)
                if title_match:
                    nome_eng = title_match.group(1).strip()
                    # Remover tudo apos " |" (sufixo padrao Engelux)
                    nome_eng = nome_eng.split("|")[0].strip()
                    nome_eng = nome_eng.split(" - ")[0].strip()
                    if nome_eng and len(nome_eng) >= 3:
                        dados["nome"] = nome_eng

            # Engelux: extrair coords de __next_f chunks
            if empresa_key == "engelux":
                # Pattern coords in __next_f JSON: lat and lng as floats
                lat_m = re.search(r'"lat"\s*:\s*(-?\d+\.\d{4,})', html)
                lng_m = re.search(r'"lng"\s*:\s*(-?\d+\.\d{4,})', html)
                if lat_m and lng_m:
                    dados["latitude"] = lat_m.group(1)
                    dados["longitude"] = lng_m.group(1)
                # Also try "latitude" and "longitude" keys
                if not dados.get("latitude"):
                    lat_m2 = re.search(r'latitude["\s:]+(-?\d+\.\d{4,})', html)
                    lng_m2 = re.search(r'longitude["\s:]+(-?\d+\.\d{4,})', html)
                    if lat_m2 and lng_m2:
                        dados["latitude"] = lat_m2.group(1)
                        dados["longitude"] = lng_m2.group(1)

            # Living: H1 tem o nome correto (title contem "Living" que confunde o parser)
            if empresa_key == "living":
                soup_living = BeautifulSoup(html, "html.parser")
                h1_living = soup_living.find("h1")
                if h1_living:
                    nome_h1 = h1_living.get_text(strip=True)
                    if nome_h1 and len(nome_h1) >= 3 and len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

            # Living: extrair dados de meta tags OpenGraph (Drupal)
            if empresa_key == "living":
                soup_tmp = BeautifulSoup(html, "html.parser")
                # Endereco do og:street_address
                og_addr = soup_tmp.find("meta", property="og:street_address")
                if og_addr and og_addr.get("content") and not dados.get("endereco"):
                    dados["endereco"] = og_addr["content"]
                # Cidade/Estado do og:region e og:locality
                og_locality = soup_tmp.find("meta", property="og:locality")
                og_region = soup_tmp.find("meta", property="og:region")
                if og_locality and og_locality.get("content"):
                    dados["cidade"] = og_locality["content"]
                if og_region and og_region.get("content"):
                    dados["estado"] = og_region["content"]
                # Fallback: procurar no texto
                if not dados.get("cidade"):
                    text_page = soup_tmp.get_text(separator="\n", strip=True)
                    match_ce = re.search(r'([\w\s]+?)\s*[-–]\s*(SP|RJ|MG|BA|PR|SC|RS|GO|DF|CE|PE|PA|AM|MA|MT|MS|ES|PB|RN|SE|AL|PI|TO|RO|AC|AP|RR)\b', text_page)
                    if match_ce:
                        cidade_cand = match_ce.group(1).strip()
                        if len(cidade_cand) > 2 and len(cidade_cand) < 30:
                            dados["cidade"] = cidade_cand
                            dados["estado"] = match_ce.group(2)

            # Riva: cidade do titulo/texto
            if empresa_key == "riva" and not dados.get("cidade"):
                soup_riva = BeautifulSoup(html, "html.parser")
                text_riva = soup_riva.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_riva)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Eztec: nome do H1 (title tem tagline de marketing)
            if empresa_key == "eztec":
                soup_ez_name = BeautifulSoup(html, "html.parser")
                h1_ez = soup_ez_name.find("h1")
                if h1_ez:
                    nome_ez = h1_ez.get_text(strip=True)
                    if nome_ez and len(nome_ez) >= 3 and len(nome_ez) <= 60:
                        dados["nome"] = nome_ez
                # Se nome ainda muito longo, truncar no primeiro separador
                if dados.get("nome") and len(dados["nome"]) > 50:
                    for sep in [",", ".", ":", "|", "–", " - "]:
                        if sep in dados["nome"]:
                            candidate = dados["nome"].split(sep)[0].strip()
                            if len(candidate) >= 5:
                                dados["nome"] = candidate
                                break

            # Eztec: cidade do texto
            if empresa_key == "eztec" and not dados.get("cidade"):
                soup_ez = BeautifulSoup(html, "html.parser")
                text_ez = soup_ez.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_ez)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Fase: normalizar
            if dados.get("fase"):
                dados["fase"] = _normalizar_fase(dados["fase"])

            # Validar nome
            if not dados.get("nome") or dados["nome"].strip() == "":
                logger.warning(f"  Nome nao encontrado, pulando")
                erros += 1
                time.sleep(DELAY)
                continue

            nome = dados["nome"]

            # MCMV: empresas de medio/alto padrao nao sao MCMV
            if config.get("prog_mcmv") is not None:
                dados["prog_mcmv"] = config["prog_mcmv"]
            else:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # Vitacon: HIS/HMP sao MCMV
            if empresa_key == "vitacon" and dados.get("prog_mcmv") == 1:
                pass  # Ja setado pelo cache
            elif empresa_key == "engelux":
                # Engelux: verificar preco para determinar MCMV
                preco = dados.get("preco_a_partir")
                if preco and preco <= 350000:
                    dados["prog_mcmv"] = 1
                else:
                    dados["prog_mcmv"] = 0

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
                download_imagens(html, url, empresa_key, slug, logger)

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
    parser = argparse.ArgumentParser(description="Scraper Batch M1 - 10 incorporadoras medias")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: living, kallas, todas)")
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
        print("\nEmpresas configuradas (Batch M1):")
        for key, cfg in EMPRESAS.items():
            nota = " [SELENIUM]" if cfg.get("selenium_required") else ""
            print(f"  {key:18s} -> {cfg['nome_banco']}{nota}")
        print(f"\nNOTA: BRZ (brzempreendimentos.com) precisa Selenium — Blazor SPA")
        print(f"\nUso: python scrapers/generico_novas_empresas_m1.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_m1.py --empresa todas")
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
        print("  RESUMO GERAL — Batch M1")
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

"""
Scraper Batch M4 — 10 Incorporadoras de Complexidade Media
============================================================
Sites medios: Next.js, WordPress, Laravel, sitemaps XML, APIs internas.

Empresas:
    pernambuco   — pernambucoconstrutora.com.br (Next.js SSR, listing, ~14 props, PE)
    due          — dueinc.com.br (WordPress, listing, ~12 props, PE)
    rni          — rni.com.br (Next.js, sitemap + __NEXT_DATA__ c/ coords/dados ricos, multi-estado)
    mitre        — mitrerealty.com.br (Next.js, __NEXT_DATA__ c/ WP backend, coords/dados, SP, B3)
    brava        — somosbrava.com.br (WordPress/Elementor, 3 props incorporacao, CE — sem paginas individuais)
    tarraf       — tarraf.com.br (Laravel/Livewire, listing, ~12 props, SP interior)
    zarin        — grupozarin.com.br (WordPress, portfolio-sitemap.xml, ~16 props, Indaiatuba/SP)
    gavea        — gavea.eng.br (WordPress — construtora, NAO incorporadora. Projetos sao gerenciamento de obra)
    vrv          — grupovrv.com.br (WordPress, listing, ~6 props, Campinas/SP)
    hacasa       — hacasa.com.br (WordPress, listing paginada, ~12 props, Joinville/SC)

Uso:
    python scrapers/generico_novas_empresas_m4.py --empresa pernambuco
    python scrapers/generico_novas_empresas_m4.py --empresa todas
    python scrapers/generico_novas_empresas_m4.py --listar
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
    logger = logging.getLogger(f"scraper.m4.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"m4_{empresa_key}.log"), encoding="utf-8"
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
HEADERS_M4 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch M4
# ============================================================
EMPRESAS = {
    "pernambuco": {
        "nome_banco": "Pernambuco",
        "base_url": "https://www.pernambucoconstrutora.com.br",
        "estado_default": "PE",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Medio/alto padrao, Recife
        "urls_listagem": [],
        "padrao_link": r"pernambucoconstrutora\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "due": {
        "nome_banco": "Due",
        "base_url": "https://www.dueinc.com.br",
        "estado_default": "PE",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Resort/praia, R$1bi VGV
        "urls_listagem": [
            "https://www.dueinc.com.br/empreendimentos/",
        ],
        "padrao_link": r"dueinc\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "rni": {
        "nome_banco": "RNI",
        "base_url": "https://rni.com.br",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via sitemap + __NEXT_DATA__
        "padrao_link": r"rni\.com\.br/imoveis/(?:apartamentos|casas)/\w+/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "mitre": {
        "nome_banco": "Mitre",
        "base_url": "https://www.mitrerealty.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "prog_mcmv": 0,  # B3, medio/alto padrao
        "urls_listagem": [],  # Coleta via __NEXT_DATA__
        "padrao_link": r"mitrerealty\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {},  # Dados vem da API __NEXT_DATA__
    },

    "brava": {
        "nome_banco": "Brava",
        "base_url": "https://www.somosbrava.com.br",
        "estado_default": "CE",
        "nome_from_title": True,
        "urls_listagem": [],  # Dados extraidos direto da pagina /incorporacao/
        "padrao_link": r"somosbrava\.com\.br/([\w-]+)",
        "no_individual_pages": True,  # Empreendimentos estao todos na mesma pagina
        "parsers": {},
    },

    "tarraf": {
        "nome_banco": "Tarraf",
        "base_url": "https://tarraf.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "prog_mcmv": 0,  # S.J. Rio Preto, medio/alto padrao
        "urls_listagem": [
            "https://tarraf.com.br/empreendimentos",
        ],
        "padrao_link": r"tarraf\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "zarin": {
        "nome_banco": "Zarin",
        "base_url": "https://grupozarin.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via portfolio-sitemap.xml
        "padrao_link": r"grupozarin\.com\.br/portfolio/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "gavea": {
        "nome_banco": "Gávea",
        "base_url": "https://www.gavea.eng.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [],
        "padrao_link": r"gavea\.eng\.br/([\w-]+)",
        "selenium_required": True,  # Site de construtora, NAO incorporadora. Projetos sao gerenciamento de obra, nao venda
        "parsers": {},
    },

    "vrv": {
        "nome_banco": "VRV",
        "base_url": "https://grupovrv.com.br",
        "estado_default": "SP",
        "cidade_default": "Campinas",
        "nome_from_title": True,
        "urls_listagem": [
            "https://grupovrv.com.br/empreendimentos/",
        ],
        "padrao_link": r"grupovrv\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "hacasa": {
        "nome_banco": "Hacasa",
        "base_url": "https://www.hacasa.com.br",
        "estado_default": "SC",
        "cidade_default": "Joinville",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via paginacao /comprar-imoveis/
        "padrao_link": r"hacasa\.com\.br/para-comprar/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
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

    # 8. Padrao "lat":-XX.XXX,"lng":-XX.XXX (JSON)
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
# FUNCOES CUSTOMIZADAS DE COLETA DE LINKS
# ============================================================

def _fetch_html_m4(url, logger):
    """Fetch HTML com headers customizados."""
    try:
        resp = requests.get(url, headers=HEADERS_M4, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            # Forcar UTF-8 se o servidor reporta errado (ex: Tarraf reporta ISO-8859-1)
            if resp.encoding and resp.encoding.lower() != 'utf-8':
                if resp.apparent_encoding and resp.apparent_encoding.lower() == 'utf-8':
                    resp.encoding = 'utf-8'
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None


def _coletar_links_pernambuco(config, logger):
    """Coleta links da Pernambuco Construtora via pagina de listagem."""
    links = {}
    base = config["base_url"]

    url = f"{base}/empreendimentos"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    # Links absolutos
    found = set(re.findall(
        r'href=["\'](https?://www\.pernambucoconstrutora\.com\.br/empreendimentos/([\w-]+))',
        html
    ))
    for full_url, slug in found:
        if slug and slug not in ["empreendimentos", "pagina", ""] and not slug.startswith("listagem-") and not slug.startswith("destaque-"):
            links[slug] = full_url

    # Links relativos (fallback)
    found2 = set(re.findall(
        r'href=["\'](/empreendimentos/([\w-]+))',
        html
    ))
    for href, slug in found2:
        if slug and slug not in links and slug not in ["empreendimentos", "pagina", ""] and not slug.startswith("listagem-") and not slug.startswith("destaque-"):
            links[slug] = f"{base}{href}"

    logger.info(f"Total de links Pernambuco: {len(links)}")
    return links


def _coletar_links_due(config, logger):
    """Coleta links da Due Incorporadora via EmpreendimentosData JSON na pagina."""
    links = {}
    base = config["base_url"]

    url = f"{base}/empreendimentos/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    # Extrair todos os slugs reais do HTML
    all_slugs = set(re.findall(r'/empreendimento/([\w-]+)/', html))
    all_slugs.discard("")

    # Extrair JSON de EmpreendimentosData para dados ricos
    _due_cache.clear()
    match = re.search(r'var\s+EmpreendimentosData\s*=\s*(\{.+?\});\s*(?:var|let|const|<)', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            emps = data.get("empreendimentos", [])
            # Mapear nome -> emp para cross-reference com slugs
            name_to_emp = {}
            for emp in emps:
                name = emp.get("name", "")
                if name:
                    name_to_emp[name.lower()] = emp

            for slug in all_slugs:
                full_url = f"{base}/empreendimento/{slug}/"
                links[slug] = full_url

                # Tentar encontrar o emp correspondente no JSON pelo slug
                slug_lower = slug.replace("-", " ")
                for name, emp in name_to_emp.items():
                    # Normalizar nome para slug
                    import unicodedata
                    name_norm = unicodedata.normalize('NFD', name)
                    name_norm = ''.join(c for c in name_norm if unicodedata.category(c) != 'Mn')
                    name_slug = name_norm.replace(' ', '-').replace("'", "").lower()
                    name_slug = re.sub(r'[^a-z0-9-]', '', name_slug)
                    name_slug = re.sub(r'-+', '-', name_slug).strip('-')
                    if name_slug == slug or slug_lower == name.lower().replace("-", " "):
                        _due_cache[slug] = emp
                        break

            logger.info(f"EmpreendimentosData Due: {len(links)} links, {len(_due_cache)} com dados JSON")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Erro ao parsear EmpreendimentosData: {e}")

    if not links:
        # Fallback: usar todos os slugs encontrados
        for slug in all_slugs:
            links[slug] = f"{base}/empreendimento/{slug}/"

    logger.info(f"Total de links Due: {len(links)}")
    return links


# Cache para dados EmpreendimentosData da Due
_due_cache = {}


def _coletar_links_rni(config, logger):
    """Coleta links da RNI via sitemap.xml."""
    links = {}

    url = "https://rni.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://rni\.com\.br/imoveis/(?:apartamentos|casas)/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug:
            links[slug] = u_clean

    logger.info(f"Total de links RNI (sitemap): {len(links)}")
    return links


# Cache para dados __NEXT_DATA__ da Mitre
_mitre_cache = {}


def _coletar_links_mitre(config, logger):
    """Coleta links e dados da Mitre via __NEXT_DATA__ da pagina de listagem."""
    links = {}

    url = "https://www.mitrerealty.com.br/empreendimentos"
    logger.info(f"Coletando dados via __NEXT_DATA__: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html)
    if not match:
        logger.warning("__NEXT_DATA__ nao encontrado na pagina da Mitre")
        return links

    try:
        data = json.loads(match.group(1))
        _mitre_cache.clear()

        real_estates = data.get("props", {}).get("pageProps", {}).get("realEstates", [])
        for emp in real_estates:
            slug = emp.get("slug", "")
            title = emp.get("title", {}).get("rendered", "")
            if slug:
                full_url = f"https://www.mitrerealty.com.br/empreendimentos/{slug}"
                links[slug] = full_url
                _mitre_cache[slug] = emp

        logger.info(f"Total de links Mitre (__NEXT_DATA__): {len(links)}")

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__ Mitre: {e}")

    return links


def _coletar_links_tarraf(config, logger):
    """Coleta links da Tarraf via pagina de empreendimentos."""
    links = {}

    url = "https://tarraf.com.br/empreendimentos"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    found = set(re.findall(
        r'href=["\'](https?://tarraf\.com\.br/empreendimentos/([\w-]+))',
        html
    ))
    for full_url, slug in found:
        if slug and slug not in ["empreendimentos", ""]:
            links[slug] = full_url

    # Tambem links relativos
    found2 = set(re.findall(
        r'href=["\'](/empreendimentos/([\w-]+))',
        html
    ))
    for href, slug in found2:
        if slug and slug not in links and slug != "empreendimentos":
            links[slug] = f"https://tarraf.com.br{href}"

    logger.info(f"Total de links Tarraf: {len(links)}")
    return links


def _coletar_links_zarin(config, logger):
    """Coleta links do Grupo Zarin via portfolio-sitemap.xml."""
    links = {}

    url = "https://grupozarin.com.br/portfolio-sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://grupozarin\.com\.br/portfolio/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        # Filtrar pagina index
        if slug and slug != "portfolio":
            links[slug] = u_clean + "/"

    logger.info(f"Total de links Zarin (sitemap): {len(links)}")
    return links


def _coletar_links_vrv(config, logger):
    """Coleta links do Grupo VRV via pagina de empreendimentos."""
    links = {}
    base = config["base_url"]

    url = f"{base}/empreendimentos/"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return links

    slugs_ignorar = {"empreendimentos", "feed", "page", "category", "tag", ""}

    found = set(re.findall(
        r'href=["\'](https?://grupovrv\.com\.br/empreendimentos/([\w-]+)/?)',
        html
    ))
    for full_url, slug in found:
        if slug and slug not in slugs_ignorar:
            links[slug] = full_url.rstrip("/") + "/"

    # Tambem buscar na homepage
    html2 = _fetch_html_m4(f"{base}/", logger)
    if html2:
        found2 = set(re.findall(
            r'href=["\'](https?://grupovrv\.com\.br/empreendimentos/([\w-]+)/?)',
            html2
        ))
        for full_url, slug in found2:
            if slug and slug not in links and slug not in slugs_ignorar:
                links[slug] = full_url.rstrip("/") + "/"

    logger.info(f"Total de links VRV: {len(links)}")
    return links


def _coletar_links_hacasa(config, logger):
    """Coleta links da Hacasa via paginacao /comprar-imoveis/."""
    links = {}
    base = config["base_url"]

    for page in range(1, 10):
        if page == 1:
            url = f"{base}/comprar-imoveis/"
        else:
            url = f"{base}/comprar-imoveis/?paged={page}"

        logger.info(f"Coletando links de: {url}")
        html = _fetch_html_m4(url, logger)
        if not html:
            break

        found = set(re.findall(
            r'href=["\'](https?://www\.hacasa\.com\.br/para-comprar/([\w-]+)/?)',
            html
        ))
        new_count = 0
        for full_url, slug in found:
            if slug and slug not in links:
                links[slug] = full_url.rstrip("/") + "/"
                new_count += 1

        logger.info(f"  Pagina {page}: {len(found)} links, {new_count} novos")
        if new_count == 0:
            break
        time.sleep(DELAY)

    logger.info(f"Total de links Hacasa: {len(links)}")
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

    # Remover sufixos comuns por empresa
    sufixos = [
        " - Empreendimentos - Pernambuco Construtora",
        " - Pernambuco Construtora", " | Pernambuco Construtora",
        " - Due Incorporadora", " | Due Incorporadora", " | Due Inc",
        " - RNI", " | RNI",
        " - Mitre", " | Mitre", " | Mitre Realty",
        " - Brava", " | Brava", " | Somos Brava",
        " - Tarraf", " | Tarraf",
        " - Grupo Zarin", " | Grupo Zarin", " | Zarin",
        " - Gávea", " | Gávea", " | Gávea Engenharia",
        " - Grupo VRV", " | Grupo VRV", " | VRV",
        " - Hacasa", " | Hacasa",
        " - Apartamentos em Joinville",
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


def _normalizar_fase(fase_raw):
    """Normaliza texto de fase para os valores padrao."""
    if not fase_raw:
        return None
    fase_lower = fase_raw.lower().strip()

    if any(k in fase_lower for k in ["breve", "em breve", "futuro", "brevemente", "soon"]):
        return "Breve Lançamento"
    if any(k in fase_lower for k in ["lançamento", "lancamento", "lanc", "release"]):
        return "Lançamento"
    if any(k in fase_lower for k in ["em obra", "em constru", "construção", "construcao", "construction"]):
        return "Em Construção"
    if any(k in fase_lower for k in ["pronto", "entregue", "conclu", "ready", "delivered"]):
        return "Imóvel Pronto"
    if "vendido" in fase_lower or "100%" in fase_lower:
        return "Imóvel Pronto"
    if "últimas" in fase_lower or "ultimas" in fase_lower:
        return "Lançamento"

    return fase_raw.strip()


def _extrair_dados_rni_nextdata(url, html, logger):
    """Extrai dados ricos da RNI via __NEXT_DATA__."""
    dados = {}

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html)
    if not match:
        return dados

    try:
        data = json.loads(match.group(1))
        attrs = data.get("props", {}).get("pageProps", {}).get("undertaking", {}).get("attributes", {})
        if not attrs:
            return dados

        # Nome
        title = attrs.get("title", "")
        if title:
            dados["nome"] = title

        # MCMV
        if attrs.get("mcmvActive"):
            dados["prog_mcmv"] = 1
        else:
            dados["prog_mcmv"] = 0

        # Fase (stage)
        stage = attrs.get("stage", {})
        if isinstance(stage, dict):
            stage_data = stage.get("data", {})
            if isinstance(stage_data, dict):
                stage_name = stage_data.get("attributes", {}).get("name", "")
                if stage_name:
                    dados["fase"] = _normalizar_fase(stage_name)

        # Status (vendido, etc)
        status = attrs.get("status", {})
        if isinstance(status, dict):
            status_data = status.get("data", {})
            if isinstance(status_data, dict):
                status_name = status_data.get("attributes", {}).get("name", "")
                if "vendido" in status_name.lower():
                    dados["fase"] = "Imóvel Pronto"

        # Endereco
        addr = attrs.get("address", {})
        if isinstance(addr, dict):
            bairro = addr.get("neighborhood", "")
            public_place = addr.get("publicPlace", "")
            zipcode = addr.get("zipcode", "")
            lat = addr.get("latitude")
            lon = addr.get("longitude")

            if public_place:
                dados["endereco"] = public_place
            if bairro:
                dados["bairro"] = bairro
            if zipcode:
                dados["cep"] = zipcode
            if lat and lon:
                dados["latitude"] = str(lat)
                dados["longitude"] = str(lon)

            # Estado
            state_data = addr.get("state", {})
            if isinstance(state_data, dict):
                s_attrs = state_data.get("data", {}).get("attributes", {})
                if s_attrs:
                    dados["estado"] = s_attrs.get("uf", "")
                    # Cidade da URL
                    # /imoveis/apartamentos/sp/piracicaba/slug
                    city_match = re.search(r'/imoveis/\w+/\w+/([\w-]+)/', url)
                    if city_match:
                        cidade_slug = city_match.group(1)
                        dados["cidade"] = _normalizar_cidade(cidade_slug)

        # Diferenciais (dorms, vagas, area)
        diff = attrs.get("differentials", {})
        if isinstance(diff, dict):
            dorms = diff.get("dormitories", {})
            if isinstance(dorms, dict):
                d_val = dorms.get("dormitories")
                if d_val:
                    dados["dormitorios_descricao"] = f"{d_val} dorms"

            parking = diff.get("parkingSpaces", {})
            if isinstance(parking, dict):
                p_val = parking.get("parkingSpaces")
                if p_val:
                    dados["numero_vagas"] = str(p_val)

            prop_size = diff.get("propertySize", {})
            if isinstance(prop_size, dict):
                size = prop_size.get("propertySize")
                if size:
                    dados["area_min_m2"] = float(size)
                    dados["area_max_m2"] = float(size)
                    dados["metragens_descricao"] = f"{size}m²"

            floor = diff.get("floor")
            if floor:
                dados["numero_andares"] = str(floor)

        # Amenidades (propertyFeatures)
        features = attrs.get("propertyFeatures", {})
        if isinstance(features, dict):
            feat_data = features.get("data", [])
            if isinstance(feat_data, list):
                feat_names = []
                for f in feat_data:
                    name = f.get("attributes", {}).get("name", "")
                    if name:
                        feat_names.append(name)
                if feat_names:
                    dados["itens_lazer"] = " | ".join(feat_names)

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__ RNI: {e}")

    return dados


def _extrair_dados_mitre_nextdata(slug, html, logger):
    """Extrai dados da Mitre via __NEXT_DATA__ da pagina individual."""
    dados = {}

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html)
    if not match:
        return dados

    try:
        data = json.loads(match.group(1))
        emp = data.get("props", {}).get("pageProps", {}).get("empreendimento", {})
        if not emp:
            return dados

        # Nome
        title = emp.get("title", {}).get("rendered", "")
        if title:
            dados["nome"] = BeautifulSoup(title, "html.parser").get_text(strip=True)

        acf = emp.get("acf", {})
        if not acf:
            return dados

        # Status
        status_obj = acf.get("empreendimentoStatus", {})
        if isinstance(status_obj, dict):
            status = status_obj.get("status", "")
            pct = status_obj.get("porcentagem", "")
            status_map = {
                "soon": "Breve Lançamento",
                "release": "Lançamento",
                "construction": "Em Construção",
                "ready": "Imóvel Pronto",
            }
            if status in status_map:
                dados["fase"] = status_map[status]

            if pct:
                try:
                    dados["evolucao_obra_pct"] = float(pct.replace("%", "").replace(",", "."))
                except ValueError:
                    pass

        # Localizacao (coords e endereco)
        loc = acf.get("localizacao", {})
        if isinstance(loc, dict):
            lat = loc.get("lat")
            lng = loc.get("lng")
            address = loc.get("address", "")
            if lat and lng:
                dados["latitude"] = str(lat)
                dados["longitude"] = str(lng)
            if address:
                dados["endereco"] = address
                # Extrair bairro e cidade do endereco
                # Formato: "Rua X, 123 - Bairro, Cidade - UF, Brasil"
                addr_match = re.search(r'-\s*([^,]+),\s*([^-]+)\s*-\s*(\w{2})', address)
                if addr_match:
                    dados["bairro"] = addr_match.group(1).strip()
                    dados["cidade"] = addr_match.group(2).strip()
                    dados["estado"] = addr_match.group(3).strip()

        # Sections: ficha tecnica
        sections = acf.get("sections", [])
        for section in sections:
            layout = section.get("acf_fc_layout", "")

            if layout == "empreendimentoItemFichaTenica":
                area_terreno = section.get("areaTerreno", "")
                if area_terreno:
                    try:
                        dados["area_terreno_m2"] = float(area_terreno.replace(",", "."))
                    except ValueError:
                        pass

                arq = section.get("projetoArquitetonico", "")
                if arq:
                    dados["arquitetura"] = arq

                paisagismo = section.get("projetoPaisagistico", "")
                if paisagismo:
                    dados["paisagismo"] = paisagismo

                decoracao = section.get("projetoDecoracaoAreasComuns", "")
                if decoracao:
                    dados["decoracao"] = decoracao

        # Linha (Origem = MCMV)
        linha = acf.get("linha_do_empreendimento", "") or acf.get("linhaDoEmpreendimento", "")
        if linha and "origem" in linha.lower():
            dados["prog_mcmv"] = 1

        # Categorias
        categorias = acf.get("categorias", [])
        if isinstance(categorias, list):
            for cat in categorias:
                if isinstance(cat, dict):
                    cat_name = cat.get("name", "")
                    # Pode ter info de regiao
                    pass

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.error(f"Erro ao parsear __NEXT_DATA__ Mitre: {e}")

    return dados


def _processar_brava(config, logger, atualizar=False):
    """Processa Brava de forma especial — 3 empreendimentos na mesma pagina, sem paginas individuais."""
    nome_banco = config["nome_banco"]
    novos = 0
    atualizados = 0
    erros = 0

    url = "https://www.somosbrava.com.br/incorporacao/"
    logger.info(f"Coletando dados de: {url}")
    html = _fetch_html_m4(url, logger)
    if not html:
        return {"novos": 0, "atualizados": 0, "erros": 1}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Empreendimentos conhecidos da pagina /incorporacao/
    empreendimentos_brava = [
        {
            "nome": "Villa Vento Cumbuco",
            "cidade": "Caucaia",
            "estado": "CE",
            "bairro": "Cumbuco",
            "total_unidades": 14,
            "dormitorios_descricao": "Casas duplex",
            "area_min_m2": 85.0,
            "area_max_m2": 85.0,
            "metragens_descricao": "85m²",
        },
        {
            "nome": "Bravize Eusébio",
            "cidade": "Eusébio",
            "estado": "CE",
            "bairro": "Mangabeira",
            "total_unidades": 280,
            "dormitorios_descricao": "2 dorms",
            "area_min_m2": 43.9,
            "area_max_m2": 48.6,
            "metragens_descricao": "43,9m² a 48,6m²",
        },
        {
            "nome": "Talassa Residence",
            "cidade": "Itaitinga",
            "estado": "CE",
            "dormitorios_descricao": "2 quartos",
            "area_min_m2": 46.0,
            "area_max_m2": 46.0,
            "metragens_descricao": "46m²",
        },
    ]

    for emp_data in empreendimentos_brava:
        nome = emp_data["nome"]
        existe = empreendimento_existe(nome_banco, nome)

        if existe and not atualizar:
            logger.info(f"  Ja existe: {nome}")
            continue

        dados = {
            "empresa": nome_banco,
            "url_fonte": url,
            "data_coleta": datetime.now().isoformat(),
            "prog_mcmv": 1,  # MCMV (Caixa, popular)
        }
        dados.update(emp_data)

        if existe and atualizar:
            atualizar_empreendimento(nome_banco, nome, dados)
            atualizados += 1
            logger.info(f"  Atualizado: {nome}")
        elif not existe:
            inserir_empreendimento(dados)
            novos += 1
            logger.info(f"  Inserido: {nome} | {dados.get('cidade', 'N/A')}")

    return {"novos": novos, "atualizados": atualizados, "erros": erros}


def _normalizar_cidade(cidade_slug):
    """Normaliza slug de cidade para nome legivel."""
    cidade = cidade_slug.replace("-", " ").title()
    correcoes = {
        "Sao Paulo": "São Paulo",
        "Sao Jose Do Rio Preto": "São José do Rio Preto",
        "Sao Bernardo Do Campo": "São Bernardo do Campo",
        "Sao Caetano Do Sul": "São Caetano do Sul",
        "Rio De Janeiro": "Rio de Janeiro",
        "Aparecida De Goiania": "Aparecida de Goiânia",
        "Varzea Grande": "Várzea Grande",
        "Rondonopolis": "Rondonópolis",
        "Cuiaba": "Cuiabá",
        "Goiania": "Goiânia",
        "Gravatai": "Gravataí",
        "Palhoca": "Palhoça",
        "Cachoeirinha": "Cachoeirinha",
        "Bady Bassitt": "Bady Bassitt",
        "Campo Grande": "Campo Grande",
    }
    for wrong, right in correcoes.items():
        if cidade == wrong:
            return right
    return cidade


def _extrair_cidade_rni_url(url):
    """Extrai cidade e estado da URL da RNI.
    Formato: /imoveis/{tipo}/{estado}/{cidade}/{slug}
    """
    match = re.search(r'/imoveis/\w+/(\w+)/([\w-]+)/', url)
    if match:
        estado = match.group(1).upper()
        cidade_slug = match.group(2)
        cidade = _normalizar_cidade(cidade_slug)
        return cidade, estado
    return None, None


def _extrair_dados_pernambuco(html, url, logger):
    """Extrai dados da Pernambuco Construtora do HTML da pagina."""
    dados = {}
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # CEP
    cep_match = re.search(r'CEP\s*(\d{5}-?\d{3})', text)
    if cep_match:
        dados["cep"] = cep_match.group(1)

    # Bairro e cidade do texto (formato: "Bairro, Cidade/UF")
    loc_match = re.search(r'([\w\s]+),\s*(Recife|Jaboatão|Olinda|Cabo|Paulista|Ipojuca)/(PE)', text)
    if loc_match:
        dados["bairro"] = loc_match.group(1).strip()
        dados["cidade"] = loc_match.group(2).strip()
        dados["estado"] = loc_match.group(3).strip()
    else:
        # Formato alternativo: "Bairro, Recife/PE" ou "Muro Alto"
        loc2 = re.search(r'([\w\s]+?),\s*([\w\s]+?)/(PE|BA|CE)', text)
        if loc2:
            dados["bairro"] = loc2.group(1).strip()
            dados["cidade"] = loc2.group(2).strip()
            dados["estado"] = loc2.group(3).strip()

    # Fase
    fase_match = re.search(r'(?:Status\s*(?:das?\s*)?obras?|Fase)[:\s]*(Lan[çc]amento|Em\s*[Oo]bra|Pronto|Breve|Em\s*[Cc]onstru[çc][ãa]o)', text)
    if fase_match:
        dados["fase"] = _normalizar_fase(fase_match.group(1))

    # Vagas
    vagas_match = re.search(r'(\d+)\s*(?:e\s*\d+\s*)?vagas?', text)
    if vagas_match:
        dados["numero_vagas"] = vagas_match.group(0)

    return dados


def _extrair_dados_due(html, url, slug, logger):
    """Extrai dados da Due Incorporadora do HTML e cache."""
    dados = {}

    # Dados do cache (EmpreendimentosData JSON)
    if slug in _due_cache:
        emp = _due_cache[slug]
        name = emp.get("name", "")
        if name:
            dados["nome"] = name

        location = emp.get("location", "")
        if location:
            # Formato: "Praia dos Carneiros (PE)" ou "Recife (PE)"
            loc_match = re.match(r'([\w\s]+?)\s*\((\w{2})\)', location)
            if loc_match:
                local = loc_match.group(1).strip()
                estado = loc_match.group(2).upper()
                dados["estado"] = estado

                locais_cidade = {
                    "praia dos carneiros": ("Tamandaré", "Praia dos Carneiros"),
                    "muro alto": ("Ipojuca", "Muro Alto"),
                    "recife": ("Recife", None),
                    "praia do cupe": ("Ipojuca", "Praia do Cupe"),
                }
                local_lower = local.lower()
                if local_lower in locais_cidade:
                    dados["cidade"] = locais_cidade[local_lower][0]
                    if locais_cidade[local_lower][1]:
                        dados["bairro"] = locais_cidade[local_lower][1]
                else:
                    dados["cidade"] = local

        status = emp.get("status", "")
        if status:
            dados["fase"] = _normalizar_fase(status)

        rooms = emp.get("rooms", [])
        if rooms:
            dados["dormitorios_descricao"] = " e ".join(rooms) + " dorms"

        sizes = emp.get("size", [])
        if sizes and isinstance(sizes, list) and len(sizes) > 0:
            min_m = sizes[0].get("metragem_minima", "")
            max_m = sizes[0].get("metragem_maxima", "") if len(sizes) == 1 else sizes[-1].get("metragem_maxima", "")
            if min_m:
                try:
                    dados["area_min_m2"] = float(min_m.replace(",", "."))
                except ValueError:
                    pass
            if max_m:
                try:
                    dados["area_max_m2"] = float(max_m.replace(",", "."))
                except ValueError:
                    pass
            if min_m and max_m:
                dados["metragens_descricao"] = f"{min_m}m\u00b2 a {max_m}m\u00b2"

        is_studio = emp.get("isStudio", False)
        if is_studio:
            if dados.get("dormitorios_descricao"):
                dados["dormitorios_descricao"] = "Studio e " + dados["dormitorios_descricao"]
            else:
                dados["dormitorios_descricao"] = "Studio"

    # Complementar com dados do HTML
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Fase do texto (fallback)
    if not dados.get("fase"):
        if "em obra" in text.lower():
            dados["fase"] = "Em Construção"
        elif "pronto" in text.lower() or "entregue" in text.lower():
            dados["fase"] = "Imóvel Pronto"
        elif "lançamento" in text.lower():
            dados["fase"] = "Lançamento"

    # Localizacao do texto (fallback)
    if not dados.get("cidade"):
        loc_match = re.search(r'([\w\s]+?)\s*\((\w{2})\)', text[:3000])
        if loc_match:
            local = loc_match.group(1).strip()
            estado = loc_match.group(2).upper()
            dados["estado"] = estado
            dados["cidade"] = local

    return dados


def _extrair_cidade_tarraf(html, url, logger):
    """Extrai cidade/estado da Tarraf do texto da pagina."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Cidades conhecidas da Tarraf
    cidades_tarraf = {
        "São José do Rio Preto": "SP",
        "Ribeirão Preto": "SP",
        "Araçatuba": "SP",
    }
    for cidade, estado in cidades_tarraf.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    # Padrao generico: "Cidade - SP" ou "Cidade/SP"
    match = re.search(r'([\w\s\.]+?)\s*[-–/]\s*(SP|RJ|MG)', text[:5000])
    if match:
        cidade = match.group(1).strip()
        estado = match.group(2)
        if len(cidade) > 3 and len(cidade) < 40:
            return cidade, estado

    return None, None


def _extrair_cidade_zarin(html, url, logger):
    """Extrai cidade/estado do Zarin. Atua em Indaiatuba, Salto e regiao."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    cidades_zarin = {
        "Indaiatuba": "SP",
        "Salto": "SP",
        "Itu": "SP",
        "Sorocaba": "SP",
        "Campinas": "SP",
    }
    for cidade, estado in cidades_zarin.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    return None, None


def _extrair_cidade_hacasa(html, url, logger):
    """Extrai cidade/estado da Hacasa do texto ou title."""
    # Title format: "Nome - Apartamentos em Joinville - Hacasa"
    title_match = re.search(r'<title>[^<]*em\s+([\w\s]+?)\s*[-|]', html[:2000])
    if title_match:
        cidade = title_match.group(1).strip()
        cidades_sc = {
            "Joinville": "SC", "Florianópolis": "SC", "Blumenau": "SC",
            "Jaraguá do Sul": "SC", "São José": "SC", "Itajaí": "SC",
        }
        estado = cidades_sc.get(cidade, "SC")
        return cidade, estado

    # Texto da pagina
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Formato: "Rua X - Bairro | Cidade/SC"
    loc_match = re.search(r'(Joinville|Florianópolis|Blumenau|Jaraguá do Sul|São José|Itajaí)/(SC)', text)
    if loc_match:
        return loc_match.group(1), loc_match.group(2)

    return None, None


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
        nota = {
            "gavea": "Construtora/gerenciadora de obras, NAO incorporadora. Projetos sao gerenciamento, nao venda",
        }.get(empresa_key, "Precisa Selenium")
        logger.warning(f"{nome_banco}: {nota}. Pulando.")
        print(f"  NOTA: {nome_banco} ({config['base_url']}) — {nota}")
        return {"novos": 0, "atualizados": 0, "erros": 0, "nota": nota}

    # Brava: processamento especial (todos os empreendimentos na mesma pagina)
    if empresa_key == "brava":
        logger.info("=" * 60)
        logger.info(f"Iniciando scraper M4: {nome_banco}")
        logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
        logger.info("=" * 60)
        resultado = _processar_brava(config, logger, atualizar)
        logger.info("=" * 60)
        logger.info(f"RELATORIO - {nome_banco}")
        logger.info(f"  Novos inseridos: {resultado['novos']}")
        logger.info(f"  Atualizados: {resultado['atualizados']}")
        logger.info(f"  Erros: {resultado['erros']}")
        logger.info(f"  Total no banco: {contar_empreendimentos(nome_banco)}")
        logger.info("=" * 60)
        return resultado

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper M4: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    coletores = {
        "pernambuco": _coletar_links_pernambuco,
        "due": _coletar_links_due,
        "rni": _coletar_links_rni,
        "mitre": _coletar_links_mitre,
        "tarraf": _coletar_links_tarraf,
        "zarin": _coletar_links_zarin,
        "vrv": _coletar_links_vrv,
        "hacasa": _coletar_links_hacasa,
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

        # === Mitre: verificar existencia pelo cache primeiro ===
        if empresa_key == "mitre" and slug in _mitre_cache:
            emp_cache = _mitre_cache[slug]
            nome_cache = BeautifulSoup(
                emp_cache.get("title", {}).get("rendered", ""),
                "html.parser"
            ).get_text(strip=True)
            if nome_cache and not atualizar:
                if empreendimento_existe(nome_banco, nome_cache):
                    logger.info(f"  Ja existe (cache): {nome_cache}")
                    continue

        html = _fetch_html_m4(url, logger)
        if not html:
            # Due: se temos dados do cache JSON, inserir mesmo sem HTML
            if empresa_key == "due" and slug in _due_cache:
                logger.info(f"  URL inacessivel, usando dados do cache JSON")
                try:
                    dados = _extrair_dados_due("", url, slug, logger)
                    if dados.get("nome"):
                        dados["empresa"] = nome_banco
                        dados["url_fonte"] = url
                        dados["data_coleta"] = datetime.now().isoformat()
                        if config.get("prog_mcmv") is not None:
                            dados["prog_mcmv"] = config["prog_mcmv"]
                        if not dados.get("estado") and config.get("estado_default"):
                            dados["estado"] = config["estado_default"]
                        nome = dados["nome"]
                        existe = empreendimento_existe(nome_banco, nome)
                        if not existe:
                            inserir_empreendimento(dados)
                            novos += 1
                            logger.info(f"  Inserido (cache): {nome} | {dados.get('cidade', 'N/A')} | {dados.get('fase', 'N/A')}")
                        elif atualizar:
                            atualizar_empreendimento(nome_banco, nome, dados)
                            atualizados += 1
                            logger.info(f"  Atualizado (cache): {nome}")
                        else:
                            logger.info(f"  Ja existe: {nome}")
                        time.sleep(DELAY)
                        continue
                except Exception as e:
                    logger.error(f"  Erro com cache: {e}")
            erros += 1
            time.sleep(DELAY)
            continue

        try:
            # === RNI: extrair dados da __NEXT_DATA__ (dados ricos) ===
            if empresa_key == "rni":
                dados = _extrair_dados_rni_nextdata(url, html, logger)
                # Complementar com dados do HTML
                dados_html = extrair_dados_empreendimento(html, url, config, logger)
                for k, v in dados_html.items():
                    if v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

                # Cidade/estado da URL
                if not dados.get("cidade"):
                    cidade_url, estado_url = _extrair_cidade_rni_url(url)
                    if cidade_url:
                        dados["cidade"] = cidade_url
                    if estado_url and not dados.get("estado"):
                        dados["estado"] = estado_url

            # === Mitre: extrair dados da __NEXT_DATA__ (dados ricos) ===
            elif empresa_key == "mitre":
                dados = _extrair_dados_mitre_nextdata(slug, html, logger)
                # Complementar com dados do HTML
                dados_html = extrair_dados_empreendimento(html, url, config, logger)
                for k, v in dados_html.items():
                    if v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

            # === Outros: extrar generico ===
            else:
                dados = extrair_dados_empreendimento(html, url, config, logger)

            # === Enriquecimento por empresa ===

            # Nome: tentar do title
            nome_title = _extrair_nome_from_title(html, empresa_key)
            if nome_title and (not dados.get("nome") or len(dados.get("nome", "")) < 3):
                dados["nome"] = nome_title

            # Coordenadas do HTML (se nao veio da API)
            if not dados.get("latitude"):
                lat, lon = extrair_coordenadas(html)
                if lat and lon:
                    dados["latitude"] = lat
                    dados["longitude"] = lon

            # === Pernambuco ===
            if empresa_key == "pernambuco":
                dados_pe = _extrair_dados_pernambuco(html, url, logger)
                for k, v in dados_pe.items():
                    if v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v

                # H1 como nome
                soup_pe = BeautifulSoup(html, "html.parser")
                h1 = soup_pe.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

                # Locais de praia: Muro Alto, Serrambi, Carneiros = Ipojuca/PE
                if not dados.get("cidade"):
                    text_pe = soup_pe.get_text(separator=" ", strip=True).lower()
                    if "muro alto" in text_pe:
                        dados["cidade"] = "Ipojuca"
                        dados["bairro"] = dados.get("bairro") or "Muro Alto"
                    elif "serrambi" in text_pe:
                        dados["cidade"] = "Ipojuca"
                        dados["bairro"] = dados.get("bairro") or "Serrambi"
                    elif "candeias" in text_pe:
                        dados["cidade"] = "Jaboatão dos Guararapes"
                        dados["bairro"] = dados.get("bairro") or "Candeias"

            # === Due ===
            if empresa_key == "due":
                dados_due = _extrair_dados_due(html, url, slug, logger)
                for k, v in dados_due.items():
                    if v and (not dados.get(k) or dados.get(k) in [None, "", "0"]):
                        dados[k] = v
                # Nome do cache tem prioridade (melhor que H1 generico)
                if dados_due.get("nome"):
                    dados["nome"] = dados_due["nome"]

                # Nome: H1 (fallback)
                if not dados.get("nome") or len(dados.get("nome", "")) < 3:
                    soup_due = BeautifulSoup(html, "html.parser")
                    h1 = soup_due.find("h1")
                    if h1:
                        nome_h1 = h1.get_text(strip=True)
                        if nome_h1 and 3 <= len(nome_h1) <= 80:
                            dados["nome"] = nome_h1

            # === Tarraf ===
            if empresa_key == "tarraf":
                # Nome: title format "Name | Description | TARRAF"
                title_match = re.search(r'<title>([^<]+)</title>', html)
                if title_match:
                    title_parts = title_match.group(1).strip().split("|")
                    if len(title_parts) >= 2:
                        nome_tarraf = title_parts[0].strip()
                        if nome_tarraf and 3 <= len(nome_tarraf) <= 60:
                            dados["nome"] = nome_tarraf

                # Cidade do titulo/h1: formato "Type em Cidade / UF" ou "Type em Cidade | TARRAF"
                soup_t = BeautifulSoup(html, "html.parser")

                # H1 tem formato: "Apartamento em Araçatuba / SP"
                h1_t = soup_t.find("h1")
                h1_text = h1_t.get_text(strip=True) if h1_t else ""
                cidade_h1 = re.search(r'em\s+([\w\s]+?)\s*/\s*(\w{2})', h1_text)
                if cidade_h1:
                    dados["cidade"] = cidade_h1.group(1).strip()
                    dados["estado"] = cidade_h1.group(2).strip()
                else:
                    # Title tem formato: "Name | Type em Cidade | TARRAF"
                    title_match2 = re.search(r'<title>[^<]*em\s+([\w\sé]+?)\s*[|<]', html)
                    if title_match2:
                        cidade_cand = title_match2.group(1).strip()
                        if len(cidade_cand) > 3:
                            dados["cidade"] = cidade_cand
                            dados["estado"] = "SP"

                # Bairro: formato "Bairro, Cidade - SP" no texto
                text_t = soup_t.get_text(separator="\n", strip=True)
                if dados.get("cidade"):
                    bairro_match = re.search(
                        r'([\w\s]+?),\s*' + re.escape(dados["cidade"]) + r'\s*-\s*\w{2}',
                        text_t
                    )
                    if bairro_match:
                        bairro_cand = bairro_match.group(1).strip()
                        if bairro_cand and len(bairro_cand) > 2 and len(bairro_cand) < 40:
                            dados["bairro"] = bairro_cand

                # CEP
                cep_match = re.search(r'(\d{5}-?\d{3})', html[:10000])
                if cep_match:
                    dados["cep"] = cep_match.group(1)

            # === Zarin ===
            if empresa_key == "zarin":
                # Cidade do texto
                if not dados.get("cidade"):
                    cidade_z, estado_z = _extrair_cidade_zarin(html, url, logger)
                    if cidade_z:
                        dados["cidade"] = cidade_z
                    if estado_z:
                        dados["estado"] = estado_z

                # Nome: slug como fallback (paginas Elementor podem nao ter h1 visivel)
                if not dados.get("nome") or len(dados.get("nome", "")) < 3:
                    dados["nome"] = slug.replace("-", " ").title()

            # === VRV ===
            if empresa_key == "vrv":
                # Nome: H1
                soup_v = BeautifulSoup(html, "html.parser")
                h1 = soup_v.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

                # VRV atua em Campinas
                if not dados.get("cidade"):
                    text_v = soup_v.get_text(separator=" ", strip=True).lower()
                    if "campinas" in text_v:
                        dados["cidade"] = "Campinas"

            # === Hacasa ===
            if empresa_key == "hacasa":
                # Cidade do title ou texto
                if not dados.get("cidade"):
                    cidade_h, estado_h = _extrair_cidade_hacasa(html, url, logger)
                    if cidade_h:
                        dados["cidade"] = cidade_h
                    if estado_h:
                        dados["estado"] = estado_h

                # Nome: H1
                soup_h = BeautifulSoup(html, "html.parser")
                h1 = soup_h.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

                # CEP
                cep_match = re.search(r'(\d{5}-?\d{3})', html[:10000])
                if cep_match and not dados.get("cep"):
                    dados["cep"] = cep_match.group(1)

                # Fase do texto
                text_h = soup_h.get_text(separator=" ", strip=True).lower()
                if not dados.get("fase"):
                    if "em obras" in text_h or "em construção" in text_h:
                        dados["fase"] = "Em Construção"
                    elif "breve lançamento" in text_h or "breve" in text_h:
                        dados["fase"] = "Breve Lançamento"
                    elif "lançamento" in text_h:
                        dados["fase"] = "Lançamento"
                    elif "pronto" in text_h:
                        dados["fase"] = "Imóvel Pronto"

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
                # Ultimo fallback: slug
                dados["nome"] = slug.replace("-", " ").title()

            nome = dados["nome"]

            # MCMV
            if config.get("prog_mcmv") is not None:
                dados["prog_mcmv"] = config["prog_mcmv"]
            else:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # RNI: respeitar flag MCMV da API
            if empresa_key == "rni" and "prog_mcmv" in dados:
                pass  # Ja setado pelo __NEXT_DATA__

            # Mitre: linha Origem = MCMV, resto nao
            if empresa_key == "mitre":
                if dados.get("prog_mcmv") != 1:
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
    parser = argparse.ArgumentParser(description="Scraper Batch M4 - 10 incorporadoras medias")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: pernambuco, rni, todas)")
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
        print("\nEmpresas configuradas (Batch M4):")
        for key, cfg in EMPRESAS.items():
            nota = ""
            if cfg.get("selenium_required"):
                nota = " [CONSTRUTORA, NAO INCORPORADORA]"
            elif cfg.get("no_individual_pages"):
                nota = " [SEM PAGINAS INDIVIDUAIS]"
            print(f"  {key:18s} -> {cfg['nome_banco']}{nota}")
        print(f"\nNOTA: Gavea (gavea.eng.br) e construtora/gerenciadora, NAO incorporadora")
        print(f"NOTA: Brava (somosbrava.com.br) tem 3 projetos na pagina /incorporacao/ sem paginas individuais")
        print(f"\nUso: python scrapers/generico_novas_empresas_m4.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_m4.py --empresa todas")
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
        print("  RESUMO GERAL — Batch M4")
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
                total_novos += r.get("novos", 0)
                total_erros += max(r.get("erros", 0), 0)
        print("-" * 60)
        print(f"  {'TOTAL':20s} +{total_novos} novos, {total_erros} erros")
        print("=" * 60)


if __name__ == "__main__":
    main()

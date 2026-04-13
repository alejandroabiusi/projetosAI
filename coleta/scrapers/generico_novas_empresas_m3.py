"""
Scraper Batch M3 — 10 Incorporadoras de Complexidade Media
============================================================
Sites medios: WordPress, SSR, Next.js, Laravel, sitemaps XML.

Empresas:
    hsm          — hsmconstrutora.com (SSR, sitemap.xml, ~30 props, PE litoral)
    calper       — calper.com.br (WordPress, empreendimentos-sitemap.xml, RJ)
    kaslik       — kaslik.com.br (WordPress/Elementor SPA — precisa Selenium)
    tiberio      — tiberio.com.br (SSR, listing c/ cidade/bairro/fase na URL, SP)
    emais        — emais.com (Laravel/Livewire, listing pages, S.J. Rio Preto)
    prohidro     — prohidro.com (SSR, listing page residencial, Sorocaba/SP)
    abf          — abfdevelopments.com.br (Wix SPA — precisa Selenium)
    mbigucci     — mbigucci.com.br (WordPress/Elementor SSR, listing c/ dados ricos, ABC/SP)
    lavvi        — lavvi.com.br (Next.js SSR, sitemap.xml, coords no HTML, SP)
    seven        — incorporadoraseven.com (Sem listagem de empreendimentos — site institucional)

Uso:
    python scrapers/generico_novas_empresas_m3.py --empresa hsm
    python scrapers/generico_novas_empresas_m3.py --empresa todas
    python scrapers/generico_novas_empresas_m3.py --listar
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
    logger = logging.getLogger(f"scraper.m3.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"m3_{empresa_key}.log"), encoding="utf-8"
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
HEADERS_M3 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch M3
# ============================================================
EMPRESAS = {
    "hsm": {
        "nome_banco": "HSM",
        "base_url": "https://www.hsmconstrutora.com",
        "estado_default": "PE",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via sitemap — paginas JS-rendered (GTM only)
        "padrao_link": r"hsmconstrutora\.com/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|flats?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bflats?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
    # NOTA: HSM paginas sao JS-rendered (so GTM tag visivel). Nomes serao do slug.
    # Dados detalhados (cidade, coords, fase) serao preenchidos pelo enriquecer_dados.

    "calper": {
        "nome_banco": "Calper",
        "base_url": "https://www.calper.com.br",
        "estado_default": "RJ",
        "cidade_default": "Rio de Janeiro",
        "nome_from_title": True,
        "prog_mcmv": 0,  # RJ medio/alto padrao
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"calper\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\btownhouse\b|\bduplex\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "kaslik": {
        "nome_banco": "Kaslik",
        "base_url": "https://kaslik.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP medio/alto padrao, Vila Mariana
        "urls_listagem": [],
        "padrao_link": r"kaslik\.com\.br/([\w-]+)",
        "selenium_required": True,  # WordPress/Elementor SPA, conteudo JS-rendered
        "parsers": {},
    },

    "tiberio": {
        "nome_banco": "Tibério",
        "base_url": "https://www.tiberio.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP medio/alto padrao, 61 anos
        "urls_listagem": [],  # Coleta customizada via listing pages
        "padrao_link": r"tiberio\.com\.br/imoveis/[^\"]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "emais": {
        "nome_banco": "Emais",
        "base_url": "https://emais.com",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada via listing pages
        "padrao_link": r"emais\.com/imoveis/(?:apartamentos|casas)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "prohidro": {
        "nome_banco": "Prohidro",
        "base_url": "https://prohidro.com",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada via listing page
        "padrao_link": r"prohidro\.com/atuacao/residencial/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "abf": {
        "nome_banco": "ABF",
        "base_url": "https://www.abfdevelopments.com.br",
        "estado_default": "RS",
        "cidade_default": "Porto Alegre",
        "nome_from_title": True,
        "prog_mcmv": 0,  # POA alto padrao, R$1bi
        "urls_listagem": [],
        "padrao_link": r"abfdevelopments\.com\.br/([\w-]+)",
        "selenium_required": True,  # Wix SPA, conteudo JS-rendered
        "parsers": {},
    },

    "mbigucci": {
        "nome_banco": "MBigucci",
        "base_url": "https://mbigucci.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada via listing page
        "padrao_link": r"mbigucci\.com\.br/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lavvi": {
        "nome_banco": "Lavvi",
        "base_url": "https://www.lavvi.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "prog_mcmv": 0,  # Cyrela, B3, medio/alto padrao
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"lavvi\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "seven": {
        "nome_banco": "Seven",
        "base_url": "https://incorporadoraseven.com",
        "estado_default": "SP",
        "cidade_default": "Jundiaí",
        "nome_from_title": True,
        "urls_listagem": [],
        "padrao_link": r"incorporadoraseven\.com/([\w-]+)",
        "selenium_required": True,  # Site institucional, sem listagem de empreendimentos acessivel via requests
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

def _fetch_html_m3(url, logger):
    """Fetch HTML com headers customizados."""
    try:
        resp = requests.get(url, headers=HEADERS_M3, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None


def _coletar_links_hsm(config, logger):
    """Coleta links da HSM via sitemap.xml."""
    links = {}

    url = "https://www.hsmconstrutora.com/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m3(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://www\.hsmconstrutora\.com/empreendimento/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug:
            links[slug] = u_clean

    logger.info(f"Total de links HSM (sitemap): {len(links)}")
    return links


def _coletar_links_calper(config, logger):
    """Coleta links da Calper via empreendimentos-sitemap.xml (WordPress)."""
    links = {}

    url = "https://www.calper.com.br/empreendimentos-sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m3(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://www\.calper\.com\.br/empreendimentos/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        # Filtrar versoes em ingles/russo (sufixo -2, -3 e prefixo /en/, /ru/)
        if slug and "/en/" not in u and "/ru/" not in u:
            # Pular offices (comerciais)
            if "offices" in slug.lower():
                continue
            links[slug] = u_clean + "/"

    logger.info(f"Total de links Calper (sitemap, filtrado PT): {len(links)}")
    return links


def _fetch_html_m3_long(url, logger, timeout=30):
    """Fetch HTML com timeout mais longo para sites lentos."""
    try:
        resp = requests.get(url, headers=HEADERS_M3, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None


def _coletar_links_tiberio(config, logger):
    """Coleta links do Tiberio via paginas de listagem por fase."""
    links = {}
    base = config["base_url"]

    # Categorias de fase
    fases_listagem = [
        "lancamento",
        "em-construcao",
        "pronto-para-morar",
        "futuro-lancamento",
    ]

    # Tambem usar a pagina principal /imoveis/ que lista tudo
    urls_coleta = [f"{base}/imoveis/"]
    for fase in fases_listagem:
        urls_coleta.append(f"{base}/imoveis/todos-os-estados/todas-as-cidades/todos-os-bairros/{fase}/")

    # Slugs a ignorar (listagens, nao empreendimentos)
    slugs_ignorar = set(fases_listagem + [
        "imoveis", "todos-os-estados", "todas-as-cidades", "todos-os-bairros",
        "portfolio", "lojas",
    ])

    for url in urls_coleta:
        logger.info(f"Coletando links de: {url}")
        html = _fetch_html_m3_long(url, logger, timeout=30)
        if not html:
            continue

        # Links seguem: /imoveis/sp/sao-paulo/bairro/fase/slug/
        found = set(re.findall(
            r'href=["\'](https?://www\.tiberio\.com\.br/imoveis/\w+/[\w-]+/[\w-]+/[\w-]+/[\w-]+/?)["\']',
            html
        ))
        new_count = 0
        for href in found:
            href_clean = href.rstrip("/")
            slug = href_clean.split("/")[-1]
            # Evitar paginas de listagem e lojas
            if slug and slug not in slugs_ignorar and "/lojas/" not in href:
                if slug not in links:
                    links[slug] = href_clean + "/"
                    new_count += 1

        logger.info(f"  {url.split('/')[-2]}: {len(found)} links brutos, {new_count} novos")
        time.sleep(DELAY)

    logger.info(f"Total de links Tiberio: {len(links)}")
    return links


def _coletar_links_emais(config, logger):
    """Coleta links da Emais via paginas de listagem (apartamentos + casas)."""
    links = {}
    base = config["base_url"]

    categorias = [
        f"{base}/imoveis/apartamentos",
        f"{base}/imoveis/casas",
    ]

    for url in categorias:
        logger.info(f"Coletando links de: {url}")
        html = _fetch_html_m3(url, logger)
        if not html:
            continue

        found = set(re.findall(
            r'href=["\'](https?://emais\.com/imoveis/(?:apartamentos|casas)/[\w-]+)',
            html
        ))
        for href in found:
            href_clean = href.rstrip("/")
            slug = href_clean.split("/")[-1]
            if slug and slug not in ["apartamentos", "casas"]:
                links[slug] = href_clean

        logger.info(f"  {url.split('/')[-1]}: {len(found)} links")
        time.sleep(DELAY)

    logger.info(f"Total de links Emais: {len(links)}")
    return links


def _coletar_links_prohidro(config, logger):
    """Coleta links da Prohidro da pagina /atuacao/residencial."""
    links = {}

    url = "https://prohidro.com/atuacao/residencial"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m3(url, logger)
    if not html:
        return links

    found = set(re.findall(
        r'href=["\'](https?://prohidro\.com/atuacao/residencial/[\w-]+)',
        html
    ))
    for href in found:
        href_clean = href.rstrip("/")
        slug = href_clean.split("/")[-1]
        if slug and slug != "residencial":
            links[slug] = href_clean

    logger.info(f"Total de links Prohidro: {len(links)}")
    return links


def _coletar_links_mbigucci(config, logger):
    """Coleta links da MBigucci da pagina /imoveis-a-venda/ e homepage."""
    links = {}
    base = config["base_url"]

    urls_listagem = [
        f"{base}/imoveis-a-venda/",
        f"{base}/",
    ]

    # Paginas institucionais a ignorar
    paginas_ignorar = {
        "imoveis-a-venda", "sobre-a-mbigucci", "contato", "trabalhe-conosco",
        "politica-de-privacidade", "blog", "corretor", "corretores", "mapa-do-site",
        "assistencia-tecnica", "fase-da-obra", "estilod-e-vida",
        "", "mbigucci.com.br", "www.mbigucci.com.br",
        "portfolio", "empreendimentos", "feed", "wp-json", "wp-admin",
        "wp-login", "wp-content", "wp-includes", "xmlrpc", "wp-cron",
        "pacto-global", "responsabilidade-ambiental", "imprensa",
        "mbigucci-news", "turma-biguccino", "clube-de-descontos",
        "wp-sitemap", "sitemap", "robots", "favicon",
        "regulamento", "regulamento-de-promocoes", "regulamento-promocoes",
        "marcozeroone",  # duplica marcozeropremier
        "institucional", "como-funciona", "nossas-obras", "servicos",
    }
    # Ignorar tambem URLs que parecem blog posts (muito longas com muitos hifens)
    MBIGUCCI_MAX_SLUG_LEN = 45

    for url in urls_listagem:
        logger.info(f"Coletando links de: {url}")
        html = _fetch_html_m3(url, logger)
        if not html:
            continue

        found = set(re.findall(
            r'href=["\'](https?://(?:www\.)?mbigucci\.com\.br/([\w-]+)/?)["\']',
            html
        ))
        for full_url, slug in found:
            if slug and slug.lower() not in paginas_ignorar:
                # Ignorar URLs com subpaths (paginas de categoria)
                if slug.count("/") == 0 and len(slug) <= MBIGUCCI_MAX_SLUG_LEN:
                    links[slug] = f"{base}/{slug}/"

        time.sleep(DELAY)

    logger.info(f"Total de links MBigucci: {len(links)}")
    return links


def _coletar_links_lavvi(config, logger):
    """Coleta links da Lavvi via sitemap.xml."""
    links = {}

    url = "https://www.lavvi.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m3(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https://www\.lavvi\.com\.br/empreendimentos/[^<]+)</loc>', html)

    # Paginas que nao sao empreendimentos
    paginas_ignorar = {"empreendimentos", ""}

    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug and slug not in paginas_ignorar:
            links[slug] = u_clean

    logger.info(f"Total de links Lavvi (sitemap): {len(links)}")
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
        " | HSM Construtora", " - HSM Construtora", " | HSM", " - HSM",
        " - Calper", " | Calper",
        " | Kaslik", " - Kaslik",
        " | Tibério", " - Tibério", " | Tiberio", " - Tiberio",
        " | Emais", " - Emais", " | Emais Urbanismo",
        " | Prohidro", " - Prohidro",
        " | ABF", " - ABF", " | ABF Developments",
        " | MBigucci", " - MBigucci",
        " | Lavvi", " - Lavvi",
        " | Seven", " - Seven",
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

    if any(k in fase_lower for k in ["breve", "em breve", "futuro", "brevemente"]):
        return "Breve Lançamento"
    if any(k in fase_lower for k in ["lançamento", "lancamento", "lanc"]):
        return "Lançamento"
    if any(k in fase_lower for k in ["em obra", "em constru", "construção", "construcao"]):
        return "Em Construção"
    if any(k in fase_lower for k in ["pronto", "entregue", "conclu", "pronto para morar"]):
        return "Imóvel Pronto"
    if "vendido" in fase_lower or "100%" in fase_lower:
        return "Imóvel Pronto"
    if "últimas" in fase_lower or "ultimas" in fase_lower:
        return "Lançamento"
    if "liberado" in fase_lower:
        return "Imóvel Pronto"

    return fase_raw.strip()


def _extrair_cidade_estado_tiberio(url):
    """Extrai cidade, estado e bairro da URL do Tiberio.
    Formato: /imoveis/{estado}/{cidade}/{bairro}/{fase}/{slug}/
    """
    match = re.search(r'tiberio\.com\.br/imoveis/(\w+)/([\w-]+)/([\w-]+)/([\w-]+)/', url)
    if not match:
        return None, None, None, None

    estado = match.group(1).upper()
    cidade_slug = match.group(2)
    bairro_slug = match.group(3)
    fase_slug = match.group(4)

    cidade = cidade_slug.replace("-", " ").title()
    bairro = bairro_slug.replace("-", " ").title()

    # Correcoes de cidade
    cidade = cidade.replace("Sao Paulo", "São Paulo")
    cidade = cidade.replace("Santo Andre", "Santo André")
    cidade = cidade.replace("Sao Bernardo Do Campo", "São Bernardo do Campo")
    cidade = cidade.replace("Sao Caetano Do Sul", "São Caetano do Sul")

    # Correcoes de bairro
    bairro = bairro.replace("Higienopolis", "Higienópolis")
    bairro = bairro.replace("Saude", "Saúde")
    bairro = bairro.replace("Tatuape", "Tatuapé")
    bairro = bairro.replace("Belem", "Belém")
    bairro = bairro.replace("Bras", "Brás")
    bairro = bairro.replace("Carrao", "Carrão")
    bairro = bairro.replace("Pompeia", "Pompeia")
    bairro = bairro.replace("Republica", "República")
    bairro = bairro.replace("Vila Mariana", "Vila Mariana")

    # Fase da URL
    fase_map = {
        "lancamento": "Lançamento",
        "em-construcao": "Em Construção",
        "pronto-para-morar": "Imóvel Pronto",
        "futuro-lancamento": "Breve Lançamento",
        "portfolio": "Imóvel Pronto",
        "lojas": None,
    }
    fase = fase_map.get(fase_slug, None)

    return cidade, estado, bairro, fase


def _extrair_cidade_emais(html, url):
    """Extrai cidade/estado da Emais do texto da pagina."""
    if not html:
        return None, None

    # Padrao: "Cidade - UF" ou "Cidade/UF"
    match = re.search(r'([\w\s\.]+?)\s*[-–/]\s*(SP|RJ|MG|BA|PR|SC|RS|GO|DF|PE)\b', html[:5000])
    if match:
        cidade = match.group(1).strip()
        estado = match.group(2)
        # Validar que parece uma cidade
        if len(cidade) > 3 and len(cidade) < 40 and not any(c in cidade.lower() for c in ["http", "www", "class", "style"]):
            return cidade, estado

    # Fallback: cidades conhecidas da Emais
    cidades_emais = {
        "São José do Rio Preto": "SP",
        "Barretos": "SP",
        "Votuporanga": "SP",
        "Sorocaba": "SP",
        "Urânia": "SP",
    }
    text = html[:10000].lower()
    for cidade, estado in cidades_emais.items():
        if cidade.lower() in text:
            return cidade, estado

    return None, None


def _extrair_dados_mbigucci_listing(html, url, logger):
    """Extrai dados da MBigucci da pagina de listagem (dados ricos nos cards)."""
    dados_por_slug = {}

    soup = BeautifulSoup(html, "html.parser")

    # Procurar cards com dados
    # Os cards tipicamente contem: nome, cidade, bairro, area, dorms, preco
    links = soup.find_all("a", href=re.compile(r'mbigucci\.com\.br/[\w-]+/?$'))

    for link in links:
        href = link.get("href", "").rstrip("/")
        slug = href.split("/")[-1]
        if not slug:
            continue

        # Tentar extrair dados do card pai
        card = link.find_parent(["div", "article", "section"])
        if not card:
            continue

        card_text = card.get_text(separator="\n", strip=True)

        dados = {}

        # Preco
        preco_match = re.search(r'R\$\s*([\d.,]+)', card_text)
        if preco_match:
            preco_str = preco_match.group(1).replace(".", "").replace(",", ".")
            try:
                dados["preco_a_partir"] = float(preco_str)
            except ValueError:
                pass

        # Area
        area_match = re.search(r'(\d+)\s*(?:a|e)\s*(\d+)\s*m[²2]', card_text)
        if area_match:
            dados["area_min_m2"] = float(area_match.group(1))
            dados["area_max_m2"] = float(area_match.group(2))
        else:
            area_match2 = re.search(r'(\d+)\s*m[²2]', card_text)
            if area_match2:
                dados["area_min_m2"] = float(area_match2.group(1))
                dados["area_max_m2"] = float(area_match2.group(1))

        if dados:
            dados_por_slug[slug] = dados

    return dados_por_slug


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
        logger.warning(f"{nome_banco}: precisa Selenium. Pulando.")
        nota = {
            "kaslik": "WordPress/Elementor SPA, conteudo dinamico JS",
            "abf": "Wix SPA, conteudo dinamico JS",
            "seven": "Site institucional, sem listagem de empreendimentos",
        }.get(empresa_key, "Precisa Selenium")
        print(f"  NOTA: {nome_banco} ({config['base_url']}) — {nota}")
        return {"novos": 0, "atualizados": 0, "erros": 0, "nota": nota}

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper M3: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    coletores = {
        "hsm": _coletar_links_hsm,
        "calper": _coletar_links_calper,
        "tiberio": _coletar_links_tiberio,
        "emais": _coletar_links_emais,
        "prohidro": _coletar_links_prohidro,
        "mbigucci": _coletar_links_mbigucci,
        "lavvi": _coletar_links_lavvi,
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

        html = _fetch_html_m3(url, logger)
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

            # Coordenadas do HTML
            lat, lon = extrair_coordenadas(html)
            if lat and lon:
                dados["latitude"] = lat
                dados["longitude"] = lon

            # === HSM ===
            if empresa_key == "hsm":
                # Nome do H1 ou title
                soup_hsm = BeautifulSoup(html, "html.parser")
                h1 = soup_hsm.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 60:
                        dados["nome"] = nome_h1
                elif nome_title:
                    dados["nome"] = nome_title

                # Cidade/estado do texto
                if not dados.get("cidade"):
                    text_page = soup_hsm.get_text(separator="\n", strip=True)
                    ce = extrair_cidade_estado(text_page)
                    if ce:
                        dados["cidade"] = ce[0]
                        dados["estado"] = ce[1]

                # HSM atua em PE litoral — fallback
                if not dados.get("estado"):
                    dados["estado"] = "PE"

            # === Calper ===
            if empresa_key == "calper":
                # Nome do H1 (title as vezes tem sufixo)
                soup_calp = BeautifulSoup(html, "html.parser")
                h1 = soup_calp.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

                # Bairro da Barra Olimpica, Recreio etc
                if not dados.get("cidade"):
                    text_page = soup_calp.get_text(separator="\n", strip=True)
                    ce = extrair_cidade_estado(text_page)
                    if ce:
                        dados["cidade"] = ce[0]
                        dados["estado"] = ce[1]

            # === Tiberio ===
            if empresa_key == "tiberio":
                cidade_url, estado_url, bairro_url, fase_url = _extrair_cidade_estado_tiberio(url)
                if cidade_url:
                    dados["cidade"] = cidade_url
                if estado_url:
                    dados["estado"] = estado_url
                if bairro_url and not dados.get("bairro"):
                    dados["bairro"] = bairro_url
                if fase_url and not dados.get("fase"):
                    dados["fase"] = fase_url

                # Nome: title varia muito:
                # "Apartamentos a apenas 150 m da Estacao Belem - Hub Belem"
                # "Blend Saude: Studio, 1, 2 e 3 Dormitorios"
                # Estrategia: Schema.org, ou title apos " - " / antes de ":", ou slug
                soup_tib = BeautifulSoup(html, "html.parser")

                # 1. Tentar Schema.org JSON-LD
                nome_schema = None
                for sc in soup_tib.find_all("script", type="application/ld+json"):
                    try:
                        ld = json.loads(sc.string)
                        if isinstance(ld, dict) and ld.get("name"):
                            cand = ld["name"].strip()
                            if 3 <= len(cand) <= 60 and not cand.lower().startswith("apartamento"):
                                nome_schema = cand
                                break
                    except Exception:
                        pass

                if nome_schema:
                    dados["nome"] = nome_schema
                elif nome_title:
                    if " - " in nome_title:
                        candidate = nome_title.split(" - ")[-1].strip()
                        if candidate and 3 <= len(candidate) <= 60 and not candidate.lower().startswith("apartamento"):
                            dados["nome"] = candidate
                        else:
                            dados["nome"] = slug.replace("-", " ").title()
                    elif ":" in nome_title:
                        dados["nome"] = nome_title.split(":")[0].strip()
                    elif not nome_title.lower().startswith("apartamento"):
                        dados["nome"] = nome_title
                    else:
                        dados["nome"] = slug.replace("-", " ").title()
                else:
                    dados["nome"] = slug.replace("-", " ").title()

                # Tiberio: extrair coords do Waze/Google Maps links no HTML
                waze_match = re.search(r'waze\.com/ul\?ll=(-?\d+\.\d+),(-?\d+\.\d+)', html)
                if waze_match:
                    dados["latitude"] = waze_match.group(1)
                    dados["longitude"] = waze_match.group(2)

            # === Emais ===
            if empresa_key == "emais":
                # Nome do H1
                soup_emais = BeautifulSoup(html, "html.parser")
                h1 = soup_emais.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        dados["nome"] = nome_h1

                # Cidade/estado
                if not dados.get("cidade"):
                    cidade_e, estado_e = _extrair_cidade_emais(html, url)
                    if cidade_e:
                        dados["cidade"] = cidade_e
                    if estado_e:
                        dados["estado"] = estado_e

                # CEP
                cep_match = re.search(r'(\d{5}-?\d{3})', html[:10000])
                if cep_match:
                    dados["cep"] = cep_match.group(1)

                # Total unidades (mais especifico: "352 apartamentos" no texto)
                soup_text = soup_emais.get_text(separator=" ", strip=True)
                units_match = re.search(r'(\d+)\s*(?:apartamentos|unidades|casas)', soup_text, re.IGNORECASE)
                if units_match:
                    dados["total_unidades"] = int(units_match.group(1))

                # Torres/blocos
                torres_match = re.search(r'(\d+)\s*(?:torres?|blocos?)', soup_text, re.IGNORECASE)
                if torres_match:
                    dados["numero_torres"] = int(torres_match.group(1))

            # === Prohidro ===
            if empresa_key == "prohidro":
                # Prohidro: H1 e generico ("Residencial"), title e generico ("Construtora Prohidro")
                # Usar slug como nome (mais confiavel)
                nome_slug = slug.replace("-", " ").title()
                if nome_slug and len(nome_slug) >= 3:
                    dados["nome"] = nome_slug

                # Tentar extrair nome melhor do texto da pagina
                soup_pro = BeautifulSoup(html, "html.parser")

                # Prohidro: coords no footer sao da sede (~Sorocaba), NAO do empreendimento
                # Remover coords genericas
                if dados.get("latitude") and dados.get("longitude"):
                    lat_val = float(dados["latitude"])
                    lon_val = float(dados["longitude"])
                    # Coords da sede Prohidro: -23.4896, -47.450x
                    if abs(lat_val - (-23.4896)) < 0.001 and abs(lon_val - (-47.450)) < 0.01:
                        dados.pop("latitude", None)
                        dados.pop("longitude", None)

                # Prohidro atua em Sorocaba/SP e SP capital
                # Tentar extrair do texto
                text_pro = soup_pro.get_text(separator="\n", strip=True)
                if not dados.get("cidade"):
                    cidades_pro = {
                        "Sorocaba": "SP",
                        "São Paulo": "SP",
                        "Votorantim": "SP",
                    }
                    for cidade, estado in cidades_pro.items():
                        if cidade.lower() in text_pro.lower():
                            dados["cidade"] = cidade
                            dados["estado"] = estado
                            break

                # Total unidades
                units_match = re.search(r'(\d+)\s*(?:apartamentos|unidades)', text_pro, re.IGNORECASE)
                if units_match:
                    dados["total_unidades"] = int(units_match.group(1))

            # === MBigucci ===
            if empresa_key == "mbigucci":
                soup_mb = BeautifulSoup(html, "html.parser")

                # Nome do title (title: "Apartamento a venda Jacana: MBigucci Easy")
                if nome_title:
                    nome_mb = nome_title
                    # Remover sufixo " - MBigucci Construtora"
                    for suf in [" - MBigucci Construtora", " - MBigucci", " | MBigucci"]:
                        if nome_mb.endswith(suf):
                            nome_mb = nome_mb[:-len(suf)].strip()
                            break
                    # Se contem ":" pode ser "Apartamento a venda Jacana: MBigucci Easy"
                    if ":" in nome_mb:
                        parts = nome_mb.split(":")
                        # O nome do empreendimento e geralmente a parte apos ":"
                        candidate = parts[-1].strip()
                        if candidate and 3 <= len(candidate) <= 60:
                            nome_mb = candidate
                    # Se contem " | " pode ser "Area m2 | Nome"
                    if " | " in nome_mb:
                        parts = nome_mb.split(" | ")
                        # Pegar a parte mais curta e que nao comeca com "Apartamento"
                        for p in parts:
                            p = p.strip()
                            if p and not p.startswith("Apartamento") and 3 <= len(p) <= 50:
                                nome_mb = p
                                break
                    if nome_mb and 3 <= len(nome_mb) <= 60:
                        dados["nome"] = nome_mb
                    else:
                        dados["nome"] = nome_title

                # MBigucci: coords no HTML sao sempre da sede (-23.6693561)
                # Remover coords genericas
                if dados.get("latitude") and dados.get("longitude"):
                    try:
                        lat_val = float(dados["latitude"])
                        # Sede MBigucci: -23.6693561
                        if abs(lat_val - (-23.6693561)) < 0.001:
                            dados.pop("latitude", None)
                            dados.pop("longitude", None)
                    except ValueError:
                        pass

                # Cidade/Bairro: extrair do texto
                text_mb = soup_mb.get_text(separator="\n", strip=True)

                # Procurar endereco com bairro e cidade
                addr_match = re.search(r'(?:R\.|Rua|Av\.|Avenida)[^,\n]+,\s*\d+\s*[-–]\s*([\w\s]+),\s*([\w\s]+)', text_mb)
                if addr_match and not dados.get("bairro"):
                    dados["bairro"] = addr_match.group(1).strip()
                    if not dados.get("cidade"):
                        dados["cidade"] = addr_match.group(2).strip()

                if not dados.get("cidade"):
                    # Cidades MBigucci
                    cidades_mb = {
                        "São Bernardo do Campo": "SP",
                        "São Caetano do Sul": "SP",
                        "São Paulo": "SP",
                        "Guarulhos": "SP",
                        "Guarujá": "SP",
                        "Santo André": "SP",
                        "Diadema": "SP",
                        "Mauá": "SP",
                    }
                    for cidade, estado in cidades_mb.items():
                        if cidade.lower() in text_mb.lower():
                            dados["cidade"] = cidade
                            dados["estado"] = estado
                            break

                # Itens de lazer
                lazer_text = soup_mb.get_text(separator=" | ", strip=True)
                lazer_match = re.findall(
                    r'(?:Espaço\s+\w+|Piscina[\w\s]*|Churrasqueira|Fitness|Playground|Brinquedoteca|'
                    r'Salão\s+de\s+\w+|Pet\s+\w+|Coworking|Bicicletário|Quadra[\w\s]*|'
                    r'Lavanderia[\w\s]*|Minimercado|Redário|Mini\s+\w+)',
                    lazer_text, re.IGNORECASE
                )
                if lazer_match and not dados.get("itens_lazer"):
                    dados["itens_lazer"] = " | ".join(sorted(set(lazer_match)))

            # === Lavvi ===
            if empresa_key == "lavvi":
                # Nome do title (format: "Nome | Lavvi")
                if nome_title:
                    # Limpar prefixo "Empreendimento " e sufixo " - Bairro"
                    nome_lavvi = nome_title
                    if nome_lavvi.startswith("Empreendimento "):
                        nome_lavvi = nome_lavvi[len("Empreendimento "):].strip()
                    # Remover " - Bairro" do final (o bairro ja e extraido separadamente)
                    if " - " in nome_lavvi:
                        nome_lavvi = nome_lavvi.split(" - ")[0].strip()
                    if nome_lavvi and len(nome_lavvi) >= 3:
                        dados["nome"] = nome_lavvi
                    else:
                        dados["nome"] = nome_title

                # Coordenadas: Lavvi tem lat/lon no JS do HTML
                lat_m = re.search(r'"lat(?:itude)?"\s*:\s*(-?\d+\.\d{4,})', html)
                lng_m = re.search(r'"lng|longitude"\s*:\s*(-?\d+\.\d{4,})', html)
                if lat_m and lng_m:
                    dados["latitude"] = lat_m.group(1)
                    dados["longitude"] = lng_m.group(1)

                # Bairro/endereco do texto
                soup_lavvi = BeautifulSoup(html, "html.parser")
                text_lavvi = soup_lavvi.get_text(separator="\n", strip=True)

                # Bairro: extrair de padrao "Bairro, Cidade"
                bairro_match = re.search(r'(?:bairro|localização)\s*:?\s*([\w\s]+)', text_lavvi[:3000], re.IGNORECASE)
                if bairro_match and not dados.get("bairro"):
                    bairro_cand = bairro_match.group(1).strip()
                    if len(bairro_cand) > 2 and len(bairro_cand) < 30:
                        dados["bairro"] = bairro_cand

                # Dormitorios: "2 a 4 suites" ou "Studios e 1 Dorm"
                dorm_match = re.search(
                    r'(\d+)\s*(?:a|e)\s*(\d+)\s*(?:su[ií]tes?|dorms?\.?|dormit[oó]rios?)',
                    text_lavvi[:5000], re.IGNORECASE
                )
                if dorm_match and not dados.get("dormitorios_descricao"):
                    dados["dormitorios_descricao"] = f"{dorm_match.group(1)} e {dorm_match.group(2)} dorms"

                # Area: "84 m2 a 276 m2"
                area_match = re.search(
                    r'(\d+)\s*m[²2]\s*(?:a|até)\s*(\d+)\s*m[²2]',
                    text_lavvi[:5000], re.IGNORECASE
                )
                if area_match:
                    if not dados.get("area_min_m2"):
                        dados["area_min_m2"] = float(area_match.group(1))
                    if not dados.get("area_max_m2"):
                        dados["area_max_m2"] = float(area_match.group(2))

                # Status/fase
                fase_match = re.search(
                    r'(?:Breve\s+Lançamento|Lançamento|Em\s+Obras?|Em\s+Construção|Pronto\s+Para\s+Morar|Imóvel\s+Pronto)',
                    text_lavvi[:5000], re.IGNORECASE
                )
                if fase_match and not dados.get("fase"):
                    dados["fase"] = _normalizar_fase(fase_match.group(0))

            # === Estado/cidade defaults ===
            if not dados.get("estado") and config.get("estado_default"):
                dados["estado"] = config["estado_default"]

            if not dados.get("cidade") and config.get("cidade_default"):
                dados["cidade"] = config["cidade_default"]

            # Fase: normalizar
            if dados.get("fase"):
                dados["fase"] = _normalizar_fase(dados["fase"])

            # Validar nome
            if not dados.get("nome") or dados["nome"].strip() == "":
                # Fallback: slug como nome
                nome_fallback = slug.replace("-", " ").title()
                if len(nome_fallback) >= 3:
                    dados["nome"] = nome_fallback
                else:
                    logger.warning(f"  Nome nao encontrado, pulando")
                    erros += 1
                    time.sleep(DELAY)
                    continue

            nome = dados["nome"]

            # MCMV: determinar por preco ou config
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
    parser = argparse.ArgumentParser(description="Scraper Batch M3 - 10 incorporadoras medias")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: hsm, lavvi, todas)")
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
        print("\nEmpresas configuradas (Batch M3):")
        for key, cfg in EMPRESAS.items():
            nota = " [SELENIUM]" if cfg.get("selenium_required") else ""
            print(f"  {key:18s} -> {cfg['nome_banco']}{nota}")
        print(f"\nNOTA: Kaslik (WordPress/Elementor SPA), ABF (Wix SPA), Seven (sem listagem) precisam Selenium")
        print(f"\nUso: python scrapers/generico_novas_empresas_m3.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_m3.py --empresa todas")
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
        print("  RESUMO GERAL — Batch M3")
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

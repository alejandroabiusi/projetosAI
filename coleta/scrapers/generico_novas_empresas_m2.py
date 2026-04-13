"""
Scraper Batch M2 — 10 Incorporadoras de Complexidade Media
============================================================
Sites medios: Next.js, WordPress, APIs internas, sitemaps XML.

Empresas:
    inc          — meuinc.com.br (Next.js, sitemap.xml, 24 props)
    lidera       — grupolidera.com.br (PHP custom, listagem direta)
    hlts         — hlts.com.br (403 bloqueado — precisa Selenium)
    rsf          — rsf.com.br (WordPress, WP pages API)
    yticon       — yticon.com.br (Custom, sitemap.xml, 44+ props)
    dialogo      — dialogo.com.br (SSR, JSON embarcado na listagem)
    baptistaleal — grupobaptistaleal.com.br (SPA client-side — precisa Selenium)
    mouradubeux  — mouradubeux.com.br (Next.js SPA, state selector — precisa Selenium)
    morar        — morar.com.br (WordPress/Elementor, sitemap imoveis)
    seazone      — institucional.seazone.com.br (WordPress/Elementor, sitemap lancamentos)

Uso:
    python scrapers/generico_novas_empresas_m2.py --empresa inc
    python scrapers/generico_novas_empresas_m2.py --empresa todas
    python scrapers/generico_novas_empresas_m2.py --listar
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
    logger = logging.getLogger(f"scraper.m2.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"m2_{empresa_key}.log"), encoding="utf-8"
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
HEADERS_M2 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch M2
# ============================================================
EMPRESAS = {
    "inc": {
        "nome_banco": "INC",
        "base_url": "https://www.meuinc.com.br",
        "nome_from_title": True,
        "prog_mcmv": 1,  # Maioria MCMV (JF, MG, SP popular)
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"meuinc\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lidera": {
        "nome_banco": "Lidera",
        "base_url": "https://grupolidera.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SP/Barueri, medio/alto padrao (condominios resort)
        "urls_listagem": [
            "https://grupolidera.com.br/",
        ],
        "padrao_link": r"grupolidera\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:a\s+\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "hlts": {
        "nome_banco": "HLTS",
        "base_url": "https://hlts.com.br",
        "nome_from_title": True,
        "estado_default": "MG",
        "cidade_default": "Uberlândia",
        "urls_listagem": [],
        "padrao_link": r"hlts\.com\.br/([\w-]+)",
        "selenium_required": True,  # 403 em requests puro
        "parsers": {},
    },

    "rsf": {
        "nome_banco": "RSF",
        "base_url": "https://www.rsf.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "prog_mcmv": 0,  # RMSP, medio/alto padrao
        "urls_listagem": [],  # Coleta via WP API + sitemap
        "padrao_link": r"rsf\.com\.br/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "yticon": {
        "nome_banco": "Yticon",
        "base_url": "https://www.yticon.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [],  # Coleta via sitemap
        "padrao_link": r"yticon\.com\.br/empreendimentos/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:(?:ou\s+\d+\s*)?dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "dialogo": {
        "nome_banco": "Diálogo",
        "base_url": "https://www.dialogo.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta via pagina de listagem com JSON embarcado
        "padrao_link": r"dialogo\.com\.br/imoveis/[\w-]+/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "baptistaleal": {
        "nome_banco": "Baptista Leal",
        "base_url": "https://www.grupobaptistaleal.com.br",
        "estado_default": "PE",
        "cidade_default": "Recife",
        "nome_from_title": True,
        "urls_listagem": [],
        "padrao_link": r"grupobaptistaleal\.com\.br/([\w-]+)",
        "selenium_required": True,  # SPA client-side, conteudo nao renderiza sem JS
        "parsers": {},
    },

    "mouradubeux": {
        "nome_banco": "Moura Dubeux",
        "base_url": "https://www.mouradubeux.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # NE, B3, medio/alto padrao
        "urls_listagem": [],
        "padrao_link": r"mouradubeux\.com\.br/empreendimentos?/([\w-]+)",
        "selenium_required": True,  # Next.js SPA, state selector, dados carregam client-side
        "parsers": {},
    },

    "morar": {
        "nome_banco": "Morar",
        "base_url": "https://morar.com.br",
        "nome_from_title": True,
        "estado_default": "ES",
        "urls_listagem": [],  # Coleta via sitemap imoveis
        "padrao_link": r"morar\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "seazone": {
        "nome_banco": "Seazone",
        "base_url": "https://institucional.seazone.com.br",
        "nome_from_title": True,
        "prog_mcmv": 0,  # SC short-rental, investimento
        "urls_listagem": [],  # Coleta via sitemap lancamentos
        "padrao_link": r"seazone\.com\.br/lancamentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n<]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n<]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?|flats?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCAO DE EXTRACAO DE COORDENADAS (replicada do M1)
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
# FUNCOES AUXILIARES
# ============================================================

def _fetch_html_m2(url, logger):
    """Fetch HTML com headers customizados."""
    try:
        resp = requests.get(url, headers=HEADERS_M2, timeout=15, allow_redirects=True)
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

    if any(k in fase_lower for k in ["breve", "em breve", "futuro", "brevemente", "futuros"]):
        return "Breve Lançamento"
    if any(k in fase_lower for k in ["lançamento", "lancamento", "lanc", "pré-lançamento", "pre-lancamento"]):
        return "Lançamento"
    if any(k in fase_lower for k in ["em obra", "em constru", "construção", "construcao"]):
        return "Em Construção"
    if any(k in fase_lower for k in ["pronto", "entregue", "conclu", "100%"]):
        return "Imóvel Pronto"
    if "últimas" in fase_lower or "ultimas" in fase_lower:
        return "Lançamento"

    return fase_raw.strip()


def _extrair_nome_from_title(html, sufixos_empresa):
    """Extrai nome limpo do <title> da pagina."""
    if not html:
        return None
    match = re.search(r'<title>([^<]+)</title>', html)
    if not match:
        return None
    title = match.group(1).strip()

    for sufixo in sufixos_empresa:
        if title.lower().endswith(sufixo.lower()):
            title = title[:-len(sufixo)].strip()
            break

    # Limpar pipe ou dash no final
    title = re.sub(r'\s*[|–-]\s*$', '', title).strip()

    if len(title) < 3 or len(title) > 80:
        return None

    return title


# ============================================================
# COLETORES DE LINKS POR EMPRESA
# ============================================================

def _coletar_links_inc(config, logger):
    """Coleta links da INC via sitemap.xml."""
    links = {}

    url = "https://www.meuinc.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m2(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https?://(?:www\.)?meuinc\.com\.br/imoveis/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug and slug != "imoveis":
            links[slug] = u_clean

    logger.info(f"Total de links INC (sitemap): {len(links)}")
    return links


def _coletar_links_lidera(config, logger):
    """Coleta links da Lidera da homepage e pagina de imoveis."""
    links = {}
    base = config["base_url"]

    # Paginas .php institucionais a excluir
    paginas_inst_lidera = {
        "index", "lancamentos", "obras", "brevelancamento", "entregues",
        "pontosdevenda",
    }

    for page_url in [f"{base}/", f"{base}/imoveis/"]:
        logger.info(f"Coletando links de: {page_url}")
        html = _fetch_html_m2(page_url, logger)
        if not html:
            continue

        # Case-insensitive: Lidera usa HREF maiusculo
        found = set(re.findall(
            r'(?i)href=["\'](/imoveis/[\w-]+/?)',
            html
        ))
        for href in found:
            href_clean = href.rstrip("/")
            slug = href_clean.split("/")[-1]
            # Remover extensao .php se presente
            slug_clean = slug.replace(".php", "")
            if slug_clean and slug_clean != "imoveis" and slug_clean not in paginas_inst_lidera and len(slug_clean) > 2:
                full_url = f"{base}{href_clean}/"
                links[slug_clean] = full_url

        # Tambem buscar links absolutos
        found2 = set(re.findall(
            r'(?i)href=["\'](https?://grupolidera\.com\.br/imoveis/[\w-]+/?)',
            html
        ))
        for href in found2:
            href_clean = href.rstrip("/")
            slug = href_clean.split("/")[-1]
            slug_clean = slug.replace(".php", "")
            if slug_clean and slug_clean != "imoveis" and slug_clean not in paginas_inst_lidera and len(slug_clean) > 2:
                links[slug_clean] = href_clean + "/"

        time.sleep(DELAY)

    logger.info(f"Total de links Lidera: {len(links)}")
    return links


def _coletar_links_rsf(config, logger):
    """Coleta links da RSF via WP API pages + sitemap."""
    links = {}

    # 1. WP API: pages com template de empreendimento
    for page_num in range(1, 10):
        api_url = f"https://www.rsf.com.br/wp-json/wp/v2/pages?per_page=100&page={page_num}"
        logger.info(f"Coletando via WP API: {api_url}")
        try:
            resp = requests.get(api_url, headers=HEADERS_M2, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            for item in data:
                link = item.get("link", "")
                slug = item.get("slug", "")
                template = item.get("template", "")
                # Filtrar apenas paginas de empreendimento
                if "empreendimento" in template.lower() or "loop-empreendimentos" in template.lower():
                    if slug and link:
                        links[slug] = link.rstrip("/") + "/"
        except Exception as e:
            logger.error(f"Erro na WP API RSF: {e}")
            break
        time.sleep(1)

    # 2. Se API nao trouxe, varrer empreendimentos da pagina
    if len(links) < 5:
        logger.info("WP API trouxe poucos resultados, varrendo pagina de empreendimentos")
        html = _fetch_html_m2("https://www.rsf.com.br/empreendimentos/", logger)
        if html:
            # Padroes de URL: /alphaville/slug/, /barueri/slug/, /sao-paulo/slug/ etc
            cidades_rsf = [
                "alphaville", "barueri", "sao-paulo", "osasco", "carapicuiba",
                "alto-padrao", "apartamentos", "premium", "casas", "comerciais",
            ]
            for cidade in cidades_rsf:
                found = re.findall(
                    rf'href=["\'](https?://www\.rsf\.com\.br/{cidade}/[\w-]+/?)',
                    html
                )
                for href in found:
                    href_clean = href.rstrip("/")
                    slug = href_clean.split("/")[-1]
                    if slug and slug != cidade and len(slug) > 2:
                        links[slug] = href_clean + "/"

        # Sitemap page-sitemap.xml tambem pode ter
        html_sitemap = _fetch_html_m2("https://www.rsf.com.br/page-sitemap.xml", logger)
        if html_sitemap:
            all_urls = re.findall(r'<loc>(https?://www\.rsf\.com\.br/[\w-]+/[\w-]+/?)</loc>', html_sitemap)
            # Filtrar URLs institucionais
            paginas_inst = {
                "empreendimentos", "portal-do-cliente", "contato", "trabalhe-conosco",
                "buscar", "buscar-regiao", "inovacoes", "vantagens", "sobre-a-rsf",
                "parceiros", "quero-ser-parceiro", "politica-privacidade", "certificados",
                "blog-pesquisa", "confirmacao", "newsletter", "financiamento",
            }
            for u in all_urls:
                u_clean = u.rstrip("/")
                parts = u_clean.replace("https://www.rsf.com.br/", "").split("/")
                if len(parts) == 2:
                    cat, slug = parts
                    if slug not in paginas_inst and cat not in paginas_inst:
                        links[slug] = u_clean + "/"

    logger.info(f"Total de links RSF: {len(links)}")
    return links


def _coletar_links_yticon(config, logger):
    """Coleta links da Yticon via sitemap.xml."""
    links = {}

    url = "https://www.yticon.com.br/sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m2(url, logger)
    if not html:
        return links

    # Padrao: /empreendimentos/{cidade}/{nome}
    urls_sitemap = re.findall(
        r'<loc>(https?://(?:www\.)?yticon\.com\.br/empreendimentos/[\w-]+/[\w-]+)</loc>',
        html
    )
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug and len(slug) > 1:
            links[slug] = u_clean

    logger.info(f"Total de links Yticon (sitemap): {len(links)}")
    return links


def _coletar_links_dialogo(config, logger):
    """Coleta links da Dialogo da pagina de imoveis (que tem JSON embarcado)."""
    links = {}

    # A pagina de listagem /imoveis tem links para todos os empreendimentos
    url = "https://www.dialogo.com.br/imoveis"
    logger.info(f"Coletando links de: {url}")
    html = _fetch_html_m2(url, logger)
    if not html:
        return links

    # Dialogo usa URLs absolutas: href="https://www.dialogo.com.br/imoveis/bairro/tipo/nome"
    found = set(re.findall(
        r'href=["\'](https?://www\.dialogo\.com\.br/imoveis/[\w-]+/[\w-]+/[\w-]+)',
        html
    ))
    # Tambem buscar paths relativos caso existam
    found_rel = set(re.findall(
        r'href=["\'](/imoveis/[\w-]+/[\w-]+/[\w-]+)',
        html
    ))
    for href in found_rel:
        found.add(f"https://www.dialogo.com.br{href}")

    for full_url in found:
        full_url_clean = full_url.rstrip("/")
        slug = full_url_clean.split("/")[-1]
        if slug and len(slug) > 2:
            links[slug] = full_url_clean

    logger.info(f"Total de links Dialogo: {len(links)}")
    return links


def _coletar_links_morar(config, logger):
    """Coleta links da Morar via sitemap de imoveis."""
    links = {}

    url = "https://morar.com.br/imoveis-sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m2(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(r'<loc>(https?://morar\.com\.br/imoveis/[^<]+)</loc>', html)
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        if slug and slug != "imoveis":
            links[slug] = u_clean + "/"

    logger.info(f"Total de links Morar (sitemap): {len(links)}")
    return links


def _coletar_links_seazone(config, logger):
    """Coleta links da Seazone via sitemap de lancamentos."""
    links = {}

    url = "https://institucional.seazone.com.br/lancamentos-sitemap.xml"
    logger.info(f"Coletando links via sitemap: {url}")
    html = _fetch_html_m2(url, logger)
    if not html:
        return links

    urls_sitemap = re.findall(
        r'<loc>(https?://institucional\.seazone\.com\.br/lancamentos/[^<]+)</loc>',
        html
    )
    for u in urls_sitemap:
        u_clean = u.rstrip("/")
        slug = u_clean.split("/")[-1]
        # Excluir a pagina indice de lancamentos
        if slug and slug != "lancamentos" and len(slug) > 2:
            links[slug] = u_clean + "/"

    logger.info(f"Total de links Seazone (sitemap): {len(links)}")
    return links


# ============================================================
# FUNCOES DE EXTRACAO DE DADOS ESPECIFICAS
# ============================================================

_SUFIXOS_TITLE = {
    "inc": [" | INC", " - INC", " | INC Incorporadora", " - INC Incorporadora", " | Meu INC"],
    "lidera": [" | Lidera", " - Lidera", " | Grupo Lidera"],
    "rsf": [" - RSF Empreendimentos", " | RSF Empreendimentos", " | RSF", " - RSF"],
    "yticon": [" | Yticon Construção e Incorporação", " | Yticon Construtora", " | Yticon", " - Yticon"],
    "dialogo": [" | Diálogo Engenharia", " | Diálogo", " - Diálogo", " - Dialogo"],
    "morar": [" | Morar", " - Morar", " | Morar Construtora"],
    "seazone": [" - Seazone", " | Seazone", " | Seazone Investimentos"],
}


def _extrair_cidade_yticon_url(url):
    """Extrai cidade da URL da Yticon (padrao /empreendimentos/{cidade}/{nome})."""
    match = re.search(r'yticon\.com\.br/empreendimentos/([\w-]+)/', url)
    if match:
        cidade_slug = match.group(1)
        # Mapear cidades conhecidas
        mapa_cidades = {
            "londrina": ("Londrina", "PR"),
            "maringa": ("Maringá", "PR"),
            "campinas": ("Campinas", "SP"),
            "cambe": ("Cambé", "PR"),
            "presidenteprudente": ("Presidente Prudente", "SP"),
            "presidente-prudente": ("Presidente Prudente", "SP"),
        }
        if cidade_slug in mapa_cidades:
            return mapa_cidades[cidade_slug]
        # Fallback: tentar converter slug
        cidade = cidade_slug.replace("-", " ").title()
        return cidade, "PR"  # Default PR para Yticon
    return None, None


def _extrair_bairro_dialogo_url(url):
    """Extrai bairro da URL da Dialogo (padrao /imoveis/{bairro}/{tipo}/{nome})."""
    match = re.search(r'dialogo\.com\.br/imoveis/([\w-]+)/', url)
    if match:
        bairro_slug = match.group(1)
        bairro = bairro_slug.replace("-", " ").title()
        # Correcoes comuns
        bairro = bairro.replace("Alto Do Ipiranga", "Alto do Ipiranga")
        bairro = bairro.replace("Vila Ema", "Vila Ema")
        bairro = bairro.replace("Analia Franco", "Anália Franco")
        bairro = bairro.replace("Alto Da Boa Vista", "Alto da Boa Vista")
        bairro = bairro.replace("Jardim Prudencia", "Jardim Prudência")
        bairro = bairro.replace("Vila Guilhermina", "Vila Guilhermina")
        return bairro
    return None


def _extrair_cidade_rsf_url(url):
    """Extrai cidade da URL da RSF (padrao /{cidade-ou-categoria}/{nome}/)."""
    match = re.search(r'rsf\.com\.br/([\w-]+)/', url)
    if match:
        cat = match.group(1)
        mapa = {
            "alphaville": ("Barueri", "SP"),  # Alphaville fica em Barueri
            "barueri": ("Barueri", "SP"),
            "sao-paulo": ("São Paulo", "SP"),
            "osasco": ("Osasco", "SP"),
            "carapicuiba": ("Carapicuíba", "SP"),
            "alto-padrao": (None, "SP"),
            "apartamentos": (None, "SP"),
            "premium": (None, "SP"),
            "casas": (None, "SP"),
            "comerciais": (None, "SP"),
        }
        if cat in mapa:
            return mapa[cat]
    return None, None


def _extrair_cidade_morar_html(html, logger):
    """Extrai cidade/estado do HTML da Morar (ES)."""
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Cidades do ES onde a Morar atua
    cidades_es = {
        "Vila Velha": "ES",
        "Vitória": "ES",
        "Serra": "ES",
        "Campos dos Goytacazes": "RJ",
        "Cariacica": "ES",
        "Guarapari": "ES",
    }
    for cidade, estado in cidades_es.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    # Buscar padrao "Cidade - ES" ou "Cidade/ES"
    match = re.search(r'([\w\s]+?)\s*[-–/]\s*(ES|RJ)\b', text)
    if match:
        cidade = match.group(1).strip()
        if len(cidade) > 2 and len(cidade) < 30:
            return cidade, match.group(2)

    return None, None


def _extrair_cidade_seazone_html(html, logger):
    """Extrai cidade/estado do HTML da Seazone."""
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Cidades onde Seazone atua
    cidades = {
        "Florianópolis": "SC", "Florianopolis": "SC",
        "Balneário Camboriú": "SC", "Balneario Camboriu": "SC",
        "Itapema": "SC",
        "Porto Belo": "SC",
        "Bombinhas": "SC",
        "Garopaba": "SC",
        "Palhoça": "SC", "Palhoca": "SC",
        "Jurerê": "SC", "Jurere": "SC",
        "Campeche": "SC",
        "Canasvieiras": "SC",
        "Ponta das Canas": "SC",
        "Natal": "RN",
        "Porto Seguro": "BA",
        "Caraguatatuba": "SP",
        "Bonito": "MS",
        "Itacaré": "BA", "Itacare": "BA",
        "Barra Grande": "BA",
    }
    for cidade, estado in cidades.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    # Buscar padrao "Cidade - SC/RN/BA"
    match = re.search(r'([\w\s]+?)\s*[-–/,]\s*(SC|RN|BA|SP|MS|RJ)\b', text)
    if match:
        cidade = match.group(1).strip()
        if len(cidade) > 2 and len(cidade) < 30:
            return cidade, match.group(2)

    return None, None


def _extrair_cidade_inc_html(html, logger):
    """Extrai cidade/estado do HTML da INC."""
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Cidades onde INC atua (JF, MG, SP)
    cidades = {
        "Juiz de Fora": "MG",
        "São Paulo": "SP",
        "Uberaba": "MG",
        "Uberlândia": "MG", "Uberlandia": "MG",
        "Belo Horizonte": "MG",
        "Contagem": "MG",
        "Betim": "MG",
        "São José dos Campos": "SP",
        "Campinas": "SP",
        "Ribeirão Preto": "SP", "Ribeirao Preto": "SP",
    }
    for cidade, estado in cidades.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    # Buscar padrao generico "Cidade - MG/SP"
    match = re.search(r'([\w\s]+?)\s*[-–/,]\s*(MG|SP|RJ|ES)\b', text)
    if match:
        cidade = match.group(1).strip()
        if len(cidade) > 2 and len(cidade) < 30:
            return cidade, match.group(2)

    return None, None


def _extrair_cidade_lidera_html(html, logger):
    """Extrai cidade/estado do HTML da Lidera."""
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    cidades = {
        "Barueri": "SP",
        "Alphaville": "SP",
        "São Paulo": "SP",
        "Guarulhos": "SP",
        "Mogi das Cruzes": "SP",
        "Caraguatatuba": "SP",
        "Bertioga": "SP",
        "Riviera de São Lourenço": "SP",
    }
    for cidade, estado in cidades.items():
        if cidade.lower() in text.lower():
            return cidade, estado

    match = re.search(r'([\w\s]+?)\s*[-–/,]\s*(SP|RJ|MG)\b', text)
    if match:
        cidade = match.group(1).strip()
        if len(cidade) > 2 and len(cidade) < 30:
            return cidade, match.group(2)

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
        logger.warning(f"{nome_banco}: precisa Selenium. Pulando.")
        print(f"  NOTA: {nome_banco} ({config['base_url']}) precisa Selenium — conteudo nao acessivel via requests")
        return {"novos": 0, "atualizados": 0, "erros": 0, "nota": "Precisa Selenium"}

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper M2: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    coletores = {
        "inc": _coletar_links_inc,
        "lidera": _coletar_links_lidera,
        "rsf": _coletar_links_rsf,
        "yticon": _coletar_links_yticon,
        "dialogo": _coletar_links_dialogo,
        "morar": _coletar_links_morar,
        "seazone": _coletar_links_seazone,
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

        html = _fetch_html_m2(url, logger)
        if not html:
            erros += 1
            time.sleep(DELAY)
            continue

        try:
            dados = extrair_dados_empreendimento(html, url, config, logger)

            # === Nome: tentar do title ===
            sufixos = _SUFIXOS_TITLE.get(empresa_key, [])
            nome_title = _extrair_nome_from_title(html, sufixos)

            # Empresas onde o title e a fonte principal do nome
            # (H1 tem nome generico ou marketing text)
            empresas_nome_do_title = {"yticon", "seazone", "rsf", "dialogo"}

            if empresa_key in empresas_nome_do_title:
                # Title e a fonte primaria
                if nome_title:
                    dados["nome"] = nome_title
            else:
                # H1 e a fonte primaria, title e fallback
                soup_nome = BeautifulSoup(html, "html.parser")
                h1 = soup_nome.find("h1")
                if h1:
                    nome_h1 = h1.get_text(strip=True)
                    if nome_h1 and 3 <= len(nome_h1) <= 80:
                        if empresa_key in ("inc", "morar"):
                            dados["nome"] = nome_h1
                        elif not dados.get("nome") or len(dados.get("nome", "")) < 3:
                            dados["nome"] = nome_h1

                # Title como fallback se H1 nao funcionou
                if nome_title and (not dados.get("nome") or len(dados.get("nome", "")) < 3):
                    dados["nome"] = nome_title

            # === Coordenadas do HTML ===
            lat, lon = extrair_coordenadas(html)
            if lat and lon:
                dados["latitude"] = lat
                dados["longitude"] = lon

            # === Cidade/Estado especifico por empresa ===
            if empresa_key == "yticon":
                cidade_url, estado_url = _extrair_cidade_yticon_url(url)
                if cidade_url:
                    dados["cidade"] = cidade_url
                if estado_url:
                    dados["estado"] = estado_url

            elif empresa_key == "rsf":
                cidade_url, estado_url = _extrair_cidade_rsf_url(url)
                if cidade_url:
                    dados["cidade"] = cidade_url
                if estado_url and not dados.get("estado"):
                    dados["estado"] = estado_url
                # Tentar extrair cidade do texto se nao veio da URL
                if not dados.get("cidade"):
                    soup_rsf = BeautifulSoup(html, "html.parser")
                    text_rsf = soup_rsf.get_text(separator="\n", strip=True)
                    ce = extrair_cidade_estado(text_rsf)
                    if ce:
                        dados["cidade"] = ce[0]
                        dados["estado"] = ce[1]

            elif empresa_key == "dialogo":
                bairro = _extrair_bairro_dialogo_url(url)
                if bairro and not dados.get("bairro"):
                    dados["bairro"] = bairro
                # Dialogo e sempre SP
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"

                # Dialogo: nome do title tem prefixo "Apartamento à venda na Mooca -"
                # Extrair so a parte apos o dash
                if dados.get("nome"):
                    nome_dialogo = dados["nome"]
                    # Padroes: "Apartamento à venda na Mooca -Nome" ou "Studio à venda no Belém -Nome"
                    match_dialogo = re.search(r'(?:venda|investir)\s+(?:n[oa]s?\s+)?[\w\s]+\s*[-–]\s*(.+)', nome_dialogo)
                    if match_dialogo:
                        nome_limpo = match_dialogo.group(1).strip()
                        if nome_limpo and len(nome_limpo) >= 3:
                            dados["nome"] = nome_limpo
                    # Se nome ainda tem prefixo de tipo, usar slug como fallback
                    if dados["nome"].startswith(("Apartamento", "Studio", "Diálogo Mall", "Diálogo Office")):
                        nome_slug = slug.replace("-", " ").title()
                        # Correcoes
                        nome_slug = nome_slug.replace("By Dialogo", "by Diálogo")
                        nome_slug = nome_slug.replace("Grandialogo", "GranDiálogo")
                        if len(nome_slug) >= 3:
                            dados["nome"] = nome_slug

                # Dialogo: dormitorios_descricao fica contaminado pelos filtros da pagina
                # Limpar: pegar so o primeiro match de dormitorios do texto da pagina
                # que nao seja do menu de filtros
                if dados.get("dormitorios_descricao"):
                    dorms_raw = dados["dormitorios_descricao"]
                    # Se tem muitos pipes, esta contaminado
                    if dorms_raw.count("|") > 3:
                        # Extrair do slug ou resetar
                        dados["dormitorios_descricao"] = None

            elif empresa_key == "inc":
                cidade, estado = _extrair_cidade_inc_html(html, logger)
                if cidade:
                    dados["cidade"] = cidade
                if estado:
                    dados["estado"] = estado

            elif empresa_key == "lidera":
                cidade, estado = _extrair_cidade_lidera_html(html, logger)
                if cidade:
                    dados["cidade"] = cidade
                if estado:
                    dados["estado"] = estado

            elif empresa_key == "morar":
                cidade, estado = _extrair_cidade_morar_html(html, logger)
                if cidade:
                    dados["cidade"] = cidade
                if estado:
                    dados["estado"] = estado

            elif empresa_key == "seazone":
                cidade, estado = _extrair_cidade_seazone_html(html, logger)
                if cidade:
                    dados["cidade"] = cidade
                if estado:
                    dados["estado"] = estado
                # Seazone: tentar extrair cidade do nome/slug do empreendimento
                if not dados.get("cidade"):
                    nome_tmp = dados.get("nome", slug)
                    slug_cidades = {
                        "jurere": ("Florianópolis", "SC"),
                        "campeche": ("Florianópolis", "SC"),
                        "ponta-das-canas": ("Florianópolis", "SC"),
                        "natal": ("Natal", "RN"),
                        "bonito": ("Bonito", "MS"),
                        "caragua": ("Caraguatatuba", "SP"),
                        "itacare": ("Itacaré", "BA"),
                        "barra-grande": ("Maraú", "BA"),
                        "marista": ("Goiânia", "GO"),
                    }
                    for key_slug, (c, e) in slug_cidades.items():
                        if key_slug in slug.lower():
                            dados["cidade"] = c
                            dados["estado"] = e
                            break

            # === Fase do HTML ===
            if not dados.get("fase"):
                fase_detectada = detectar_fase(html)
                if fase_detectada:
                    dados["fase"] = fase_detectada

            # === Normalizar fase ===
            if dados.get("fase"):
                dados["fase"] = _normalizar_fase(dados["fase"])

            # === Preco ===
            if not dados.get("preco_a_partir"):
                soup_preco = BeautifulSoup(html, "html.parser")
                texto_preco = soup_preco.get_text(separator="\n", strip=True)
                preco = extrair_preco(texto_preco)
                if preco:
                    dados["preco_a_partir"] = preco

            # === Itens de lazer ===
            if not dados.get("itens_lazer"):
                soup_lazer = BeautifulSoup(html, "html.parser")
                texto_lazer = soup_lazer.get_text(separator="\n", strip=True)
                lazer = extrair_itens_lazer(soup_lazer, texto_lazer)
                if lazer:
                    dados["itens_lazer"] = lazer

            # === Estado default ===
            if not dados.get("estado") and config.get("estado_default"):
                dados["estado"] = config["estado_default"]

            # === Cidade default ===
            if not dados.get("cidade") and config.get("cidade_default"):
                dados["cidade"] = config["cidade_default"]

            # === Validar nome ===
            if not dados.get("nome") or dados["nome"].strip() == "":
                logger.warning(f"  Nome nao encontrado, pulando")
                erros += 1
                time.sleep(DELAY)
                continue

            nome = dados["nome"]

            # === MCMV ===
            if config.get("prog_mcmv") is not None:
                dados["prog_mcmv"] = config["prog_mcmv"]
            else:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # === Data coleta ===
            dados["data_coleta"] = datetime.now().isoformat()

            # === Verificar se existe ===
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

            # === Download de imagens ===
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
    parser = argparse.ArgumentParser(description="Scraper Batch M2 - 10 incorporadoras medias")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: inc, yticon, todas)")
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
        print("\nEmpresas configuradas (Batch M2):")
        for key, cfg in EMPRESAS.items():
            nota = " [SELENIUM]" if cfg.get("selenium_required") else ""
            print(f"  {key:18s} -> {cfg['nome_banco']}{nota}")
        print(f"\nNOTA: HLTS (hlts.com.br) — 403 bloqueado, precisa Selenium")
        print(f"NOTA: Baptista Leal (grupobaptistaleal.com.br) — SPA client-side, precisa Selenium")
        print(f"NOTA: Moura Dubeux (mouradubeux.com.br) — Next.js SPA, state selector, precisa Selenium")
        print(f"\nUso: python scrapers/generico_novas_empresas_m2.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_m2.py --empresa todas")
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
        print("  RESUMO GERAL — Batch M2")
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

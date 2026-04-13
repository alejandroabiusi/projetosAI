"""
Scraper Batch F6 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F6.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    coevo, anton, bracon, prm, midas, catagua, engefortes, grp, engeplan, baliza

Notas:
    - Anton: site JS-only (Next.js/React), sitemap nao lista empreendimentos individuais
    - PRM: site retorna 403, coleta falha
    - Engefortes: Google Sites, padrao /empreendimentos/{slug}
    - GRP: links homepage apontam errado (todos para /wit), coleta via slugs conhecidos
    - Engeplan: site PHP antigo, padrao /obras/{slug}.php
    - Catagua: WP com Elementor, padrao /imoveis/{cidade}/{slug}

Uso:
    python scrapers/generico_novas_empresas_f6.py --empresa coevo
    python scrapers/generico_novas_empresas_f6.py --empresa todas
    python scrapers/generico_novas_empresas_f6.py --listar
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
    logger = logging.getLogger(f"scraper.f6.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f6_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F6
# ============================================================
EMPRESAS = {
    "coevo": {
        "nome_banco": "Coevo",
        "base_url": "https://coevoconstrutora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://coevoconstrutora.com.br/todos-os-imoveis/",
            "https://coevoconstrutora.com.br/",
        ],
        # Padrao: /imoveis/{slug}/
        "padrao_link": r"coevoconstrutora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "anton": {
        "nome_banco": "Anton",
        "base_url": "https://www.antonincorporacoes.com.br",
        "nome_from_title": True,
        # Anton: site JS-only (React/Next.js), sitemap nao lista empreendimentos individuais
        # Coleta anotada como falha — site nao renderiza via requests
        "urls_listagem": [
            "https://www.antonincorporacoes.com.br/empreendimentos",
            "https://www.antonincorporacoes.com.br/",
        ],
        "padrao_link": r"antonincorporacoes\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "bracon": {
        "nome_banco": "Bracon",
        "base_url": "https://bracon.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://bracon.com.br/empreendimentos",
            "https://bracon.com.br/",
        ],
        # Padrao: /empreendimento/{slug}/  (singular!)
        "padrao_link": r"bracon\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "prm": {
        "nome_banco": "PRM",
        "base_url": "https://prmempreendimentos.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "cidade_default": "Curitiba",
        # PRM: site retorna 403 — coleta anotada como falha
        "urls_listagem": [
            "https://prmempreendimentos.com.br/empreendimentos",
            "https://prmempreendimentos.com.br/",
        ],
        "padrao_link": r"prmempreendimentos\.com\.br/empreendimentos?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "midas": {
        "nome_banco": "Midas",
        "base_url": "https://www.midasconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "SC",
        "cidade_default": "Blumenau",
        "urls_listagem": [
            "https://www.midasconstrutora.com.br/empreendimentos",
            "https://www.midasconstrutora.com.br/empreendimentos/lancamentos",
            "https://www.midasconstrutora.com.br/empreendimentos/construcao",
            "https://www.midasconstrutora.com.br/empreendimentos/concluidos",
        ],
        # Padrao: /empreendimentos/{status}/{slug}
        "padrao_link": r"midasconstrutora\.com\.br/empreendimentos/(?:lancamentos|construcao|concluidos)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\blofts?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "catagua": {
        "nome_banco": "Catagua",
        "base_url": "https://catagua.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        # Catagua: WP com Elementor. Sitemap WP: /page-sitemap.xml
        # Padrao: /imoveis/{cidade}/{slug}
        "sitemap_url": "https://catagua.com.br/page-sitemap.xml",
        "urls_listagem": [
            "https://catagua.com.br/imoveis",
            "https://catagua.com.br/",
        ],
        "padrao_link": r"catagua\.com\.br/imoveis/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "engefortes": {
        "nome_banco": "Engefortes",
        "base_url": "https://www.engefortes.com.br",
        "nome_from_title": True,
        "estado_default": "RS",
        "cidade_default": "Canoas",
        # Engefortes: Google Sites hospedado
        "urls_listagem": [
            "https://www.engefortes.com.br/empreendimentos",
            "https://www.engefortes.com.br/",
        ],
        "padrao_link": r"engefortes\.com\.br/empreendimentos/([\w%-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "grp": {
        "nome_banco": "GRP",
        "base_url": "https://construtoragrp.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "cidade_default": "Maringá",
        # GRP: homepage links sao bugados (varias apontam pra /wit)
        # Coleta via slugs conhecidos + pagina individual
        "urls_listagem": [
            "https://construtoragrp.com.br/",
        ],
        "padrao_link": r"construtoragrp\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\blofts?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "engeplan": {
        "nome_banco": "Engeplan",
        "base_url": "http://www.engeplanconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        # Engeplan: site PHP antigo. Padrao: /obras/{slug}.php
        "urls_listagem": [
            "http://www.engeplanconstrutora.com.br/construcao.php",
            "http://www.engeplanconstrutora.com.br/lancamento.php",
            "http://www.engeplanconstrutora.com.br/entregues.php",
            "http://www.engeplanconstrutora.com.br/",
        ],
        "padrao_link": r"engeplanconstrutora\.com\.br/obras/([\w-]+)\.php",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3, .titulo", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*(?:residenciais?)?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "baliza": {
        "nome_banco": "Baliza",
        "base_url": "https://balizaconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "RS",
        # Baliza: WP com Elementor. MCMV.
        "urls_listagem": [
            "https://balizaconstrutora.com.br/empreendimentos/",
            "https://balizaconstrutora.com.br/",
        ],
        "padrao_link": r"balizaconstrutora\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_catagua(config, logger):
    """Coleta links da Catagua via sitemap WP + listagem HTML."""
    links = {}
    base = config["base_url"]

    # 1. Sitemap WP (fonte principal — contem todas as paginas individuais)
    sitemap_url = config.get("sitemap_url")
    if sitemap_url:
        logger.info(f"Coletando links via sitemap: {sitemap_url}")
        try:
            resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for loc in soup.find_all("loc"):
                    url = loc.get_text(strip=True)
                    # Filtrar apenas URLs de imoveis individuais: /imoveis/{cidade}/{slug}
                    match = re.search(r'catagua\.com\.br/imoveis/([\w-]+)/([\w-]+)', url)
                    if match:
                        cidade_slug = match.group(1)
                        slug = match.group(2)
                        # Excluir paginas de cidade (sem slug de empreendimento)
                        if slug:
                            links[slug] = url.rstrip("/")
                logger.info(f"Sitemap: {len(links)} empreendimentos encontrados")
        except Exception as e:
            logger.error(f"Erro ao acessar sitemap: {e}")

    # 2. Complementar com listagem HTML
    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(config["padrao_link"], href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Catagua: {len(links)}")
    return links


def _coletar_links_grp(config, logger):
    """Coleta links da GRP via homepage + slugs conhecidos."""
    links = {}
    base = config["base_url"]

    # 1. Coletar links da homepage
    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("./"):
                href = base + href[1:]
            elif href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(config["padrao_link"], href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    # 2. Slugs conhecidos (homepage tem links bugados apontando tudo para /wit)
    slugs_conhecidos = {
        "wit": f"{base}/empreendimento/wit",
        "nura": f"{base}/empreendimento/nura",
        "omni": f"{base}/empreendimento/omni",
    }
    # Lumme e Axis sao sites externos (lummegrp.com.br, axisgrp.com.br)
    # Vamos tentar incluir se existirem como paginas no dominio principal
    slugs_extras = ["lumme", "axis", "morada-do-park", "moradapark"]
    for slug in slugs_extras:
        url_teste = f"{base}/empreendimento/{slug}"
        slugs_conhecidos[slug] = url_teste

    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    logger.info(f"Total de links GRP: {len(links)}")
    return links


def _coletar_links_engeplan(config, logger):
    """Coleta links da Engeplan de paginas PHP de listagem."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Links relativos (obras/slug.php)
            if href.startswith("obras/"):
                href = base + "/" + href
            elif href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0]
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    # Slugs conhecidos (fallback)
    slugs_conhecidos = {
        "ultramarino": f"{base}/obras/ultramarino.php",
        "lagoaverdegarden": f"{base}/obras/lagoaverdegarden.php",
        "sevilha": f"{base}/obras/sevilha.php",
    }
    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    logger.info(f"Total de links Engeplan: {len(links)}")
    return links


def _coletar_links_midas(config, logger):
    """Coleta links da Midas de todas as paginas de status."""
    links = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(config["padrao_link"], href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Midas: {len(links)}")
    return links


def _coletar_links_engefortes(config, logger):
    """Coleta links da Engefortes (Google Sites)."""
    links = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(config["padrao_link"], href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    # Slugs conhecidos (fallback do scraping da homepage)
    slugs_conhecidos = {
        "mezzano": f"{base}/empreendimentos/mezzano",
        "napoles": f"{base}/empreendimentos/napoles",
        "vita": f"{base}/empreendimentos/vita",
        "versatti": f"{base}/empreendimentos/versatti",
        "terrace": f"{base}/empreendimentos/terrace",
        "pace-residencial": f"{base}/empreendimentos/pace-residencial",
        "jardim-dos-ipês": f"{base}/empreendimentos/jardim-dos-ip%C3%AAs",
        "spazio-harmonia": f"{base}/empreendimentos/spazio-harmonia",
    }
    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    logger.info(f"Total de links Engefortes: {len(links)}")
    return links


# ============================================================
# FUNCAO DE EXTRACAO DE COORDENADAS
# ============================================================

def extrair_coordenadas(html):
    """Extrai coordenadas de Google Maps embeds ou JS no HTML."""
    if not html:
        return None, None

    # 1. Padrao iframe Google Maps com coordenadas no src
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

    # 6. Padrao embed pb com coords
    match = re.search(r'!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)', html)
    if match:
        # Note: in Google pb format, 2d is longitude and 3d is latitude
        return match.group(2), match.group(1)

    return None, None


# ============================================================
# FUNCAO DE EXTRACAO DE CIDADE DA URL DA CATAGUA
# ============================================================

def _extrair_cidade_da_url_catagua(url):
    """Extrai cidade da URL pattern /imoveis/{cidade}/{slug}."""
    match = re.search(r'/imoveis/([\w-]+)/', url)
    if match:
        cidade_slug = match.group(1)
        mapa = {
            "piracicaba": "Piracicaba",
            "limeira": "Limeira",
            "americana": "Americana",
            "sao-carlos": "São Carlos",
            "conchal": "Conchal",
            "rio-das-pedras": "Rio das Pedras",
            "sta-barbara-doeste": "Santa Bárbara d'Oeste",
            "pirassununga": "Pirassununga",
            "sumare": "Sumaré",
            "mogi-guacu": "Mogi Guaçu",
            "nova-odessa": "Nova Odessa",
        }
        return mapa.get(cidade_slug, cidade_slug.replace("-", " ").title())
    return None


def _extrair_fase_da_url_midas(url):
    """Extrai fase da URL da Midas /empreendimentos/{status}/{slug}."""
    if "/lancamentos/" in url:
        return "Lançamento"
    elif "/construcao/" in url:
        return "Em Construção"
    elif "/concluidos/" in url:
        return "Pronto"
    return None


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper F6: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "catagua":
        links = _coletar_links_catagua(config, logger)
    elif empresa_key == "grp":
        links = _coletar_links_grp(config, logger)
    elif empresa_key == "engeplan":
        links = _coletar_links_engeplan(config, logger)
    elif empresa_key == "midas":
        links = _coletar_links_midas(config, logger)
    elif empresa_key == "engefortes":
        links = _coletar_links_engefortes(config, logger)
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

        html = fetch_html(url, logger)
        if not html:
            erros += 1
            continue

        try:
            dados = extrair_dados_empreendimento(html, url, config, logger)

            if not dados.get("nome") or dados["nome"].strip() == "":
                logger.warning(f"  Nome nao encontrado, pulando")
                erros += 1
                continue

            nome = dados["nome"]

            # === Enriquecimento especifico por empresa ===

            # Extrair coordenadas do HTML
            lat, lon = extrair_coordenadas(html)
            if lat and lon:
                dados["latitude"] = lat
                dados["longitude"] = lon

            # --- Coevo: Taubate/SP, limpeza de nome ---
            if empresa_key == "coevo":
                # Limpar nome: remover " - Coevo" e sufixos
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Coevo|Construtora|Incorporadora)\s*.*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Extrair cidade/estado da pagina
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidades_coevo = [
                        ("Taubaté", "SP"), ("São José dos Campos", "SP"),
                        ("Jacareí", "SP"), ("Pindamonhangaba", "SP"),
                        ("Caçapava", "SP"), ("Tremembé", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_coevo:
                        if re.search(re.escape(cidade_nome), text_tmp, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break

            # --- Bracon: SP, alto padrao (studios/1-2 dorms em bairros nobres) ---
            if empresa_key == "bracon":
                # Limpar nome: remover prefixos "Bracon " se redundante
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Bracon|Incorporadora)\s*.*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Bracon e alto padrao em SP (studios 17m2 por R$300k+)
                dados["prog_mcmv"] = 0

            # --- Midas: Blumenau/SC, fase da URL ---
            if empresa_key == "midas":
                # Limpar nome: remover " | Midas" e variantes do <title>
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*\|\s*Midas\s*(?:[-–—]\s*Construtora.*)?$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                fase_url = _extrair_fase_da_url_midas(url)
                if fase_url and not dados.get("fase"):
                    dados["fase"] = fase_url
                # Detectar fase do conteudo da pagina (progresso de obra)
                if not dados.get("fase"):
                    soup_midas = BeautifulSoup(html, "html.parser")
                    text_midas = soup_midas.get_text(separator="\n", strip=True).lower()
                    if "100%" in text_midas and ("conclu" in text_midas or "entregue" in text_midas):
                        dados["fase"] = "Pronto"
                # Cidade da pagina
                if not dados.get("cidade"):
                    soup_midas = BeautifulSoup(html, "html.parser")
                    text_midas = soup_midas.get_text(separator="\n", strip=True)
                    if re.search(r'Blumenau', text_midas, re.I):
                        dados["cidade"] = "Blumenau"
                        dados["estado"] = "SC"

            # --- Catagua: cidade da URL (sempre sobrescreve - URL e confiavel), limpeza de nome ---
            if empresa_key == "catagua":
                cidade_url = _extrair_cidade_da_url_catagua(url)
                if cidade_url:
                    dados["cidade"] = cidade_url
                    dados["estado"] = "SP"
                # Limpar nome: Catagua usa titles genericos como "Apartamentos em Limeira - Condominio Barcelona"
                if dados.get("nome"):
                    nome_cat = dados["nome"]
                    # Remover "Cataguá Construtora" e variantes
                    nome_cat = re.sub(r'\s*[-–—]?\s*Catagu[áa]\s*Construtora\s*$', '', nome_cat, flags=re.I).strip()
                    # Padroes: "Apartamentos em X - NomeReal" ou "Casas em X - NomeReal"
                    match_nome = re.match(
                        r'(?:Apartamentos?|Casas?|Condom[ií]nio|Home Club|Empreendimento)\s+'
                        r'(?:novos?\s+)?(?:em\s+)?(?:[\w\s\']+?)\s*[-–—]\s*(.+)',
                        nome_cat, re.I
                    )
                    if match_nome:
                        nome_real = match_nome.group(1).strip()
                        if nome_real and len(nome_real) > 2:
                            nome_cat = nome_real
                    # "Home Club em Limeira - Alvoratta" -> "Alvoratta"
                    # "Empreendimento Terras de Santa Maria" -> "Terras de Santa Maria"
                    nome_cat = re.sub(r'^Empreendimento\s+', '', nome_cat, flags=re.I).strip()
                    # Remover sufixos: " : o apartamento ideal para voce!"
                    nome_cat = re.sub(r'\s*:\s*o\s+apartamento\s+ideal.*$', '', nome_cat, flags=re.I).strip()
                    # Se ficou generico (apenas "Apartamentos em X" sem parte real)
                    if re.match(r'^(?:Apartamentos?|Casas?|Condom[ií]nio)\s+(?:novos?\s+)?(?:em\s+)?(?:Bairro\s+Planejado\s+)?(?:em\s+)?\w+$', nome_cat, re.I):
                        # Usar o slug como nome
                        nome_cat = slug.replace("-", " ").replace("empreendimento ", "").title()
                    dados["nome"] = nome_cat
                # Limpar nome
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Cataguá|Catagua|Construtora)\s*.*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- Engefortes: Canoas/RS, MCMV ---
            if empresa_key == "engefortes":
                if not dados.get("cidade"):
                    soup_eng = BeautifulSoup(html, "html.parser")
                    text_eng = soup_eng.get_text(separator="\n", strip=True)
                    cidades_engefortes = [
                        ("Canoas", "RS"), ("Esteio", "RS"),
                        ("Sapucaia do Sul", "RS"), ("Novo Hamburgo", "RS"),
                        ("Estância Velha", "RS"),
                    ]
                    for cidade_nome, estado_val in cidades_engefortes:
                        if re.search(re.escape(cidade_nome), text_eng, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break

            # --- GRP: Maringa/PR ---
            if empresa_key == "grp":
                if not dados.get("cidade"):
                    dados["cidade"] = "Maringá"
                    dados["estado"] = "PR"
                # Limpar nome
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:GRP|Construtora)\s*.*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # GRP tem empreendimentos de alto padrao (Axis, etc)
                # Verificar se preco > 600k
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0

            # --- Engeplan: SP, site PHP antigo ---
            if empresa_key == "engeplan":
                if not dados.get("cidade"):
                    soup_ep = BeautifulSoup(html, "html.parser")
                    text_ep = soup_ep.get_text(separator="\n", strip=True)
                    # Tentar extrair bairro/cidade do texto
                    cidades_ep = [
                        ("São Paulo", "SP"), ("Mandaqui", "SP"),
                        ("Osasco", "SP"), ("Guarulhos", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_ep:
                        if re.search(re.escape(cidade_nome), text_ep, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                # Limpar nome: remover "ENGEPLAN-", telefone, sufixos
                if dados.get("nome"):
                    nome_limpo = dados["nome"]
                    # Remover "ENGEPLAN-" e variantes
                    nome_limpo = re.sub(r'\s*ENGEPLAN\s*[-–—]?\s*', '', nome_limpo, flags=re.I).strip()
                    # Remover telefone "(11) 2236-2528" etc
                    nome_limpo = re.sub(r'\s*\(\d+\)\s*[\d\s-]+\s*$', '', nome_limpo).strip()
                    nome_limpo = re.sub(r'^\s*\(\d+\)\s*[\d\s-]+\s*$', '', nome_limpo).strip()
                    # Remover "- Construtora" etc
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Engeplan|Construtora)\s*.*$', '', nome_limpo, flags=re.I).strip()
                    # Se nome ficou vazio ou e so telefone, usar slug
                    if not nome_limpo or len(nome_limpo) < 3 or re.match(r'^[\d\s()-]+$', nome_limpo):
                        # Mapear slugs para nomes reais
                        nomes_engeplan = {
                            "sevilha": "Studio Sevilha",
                            "lagoaverdegarden": "Lagoa Verde Residence",
                            "ultramarino": "Edifício Ultramarino",
                        }
                        nome_limpo = nomes_engeplan.get(slug, slug.replace("-", " ").title())
                    dados["nome"] = nome_limpo

            # --- Baliza: RS, MCMV ---
            if empresa_key == "baliza":
                if not dados.get("cidade"):
                    soup_bal = BeautifulSoup(html, "html.parser")
                    text_bal = soup_bal.get_text(separator="\n", strip=True)
                    cidades_baliza = [
                        ("Novo Hamburgo", "RS"), ("São Leopoldo", "RS"),
                        ("Sapucaia do Sul", "RS"), ("Gravataí", "RS"),
                        ("Canoas", "RS"), ("Esteio", "RS"),
                        ("Porto Alegre", "RS"),
                    ]
                    for cidade_nome, estado_val in cidades_baliza:
                        if re.search(re.escape(cidade_nome), text_bal, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default (exceto Bracon que e alto padrao)
            if empresa_key != "bracon":
                if "prog_mcmv" not in dados or dados.get("prog_mcmv") == 0:
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
    parser = argparse.ArgumentParser(description="Scraper Batch F6 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: coevo, bracon, todas)")
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
        print("\nEmpresas configuradas (Batch F6):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTAS:")
        print(f"  - Anton (antonincorporacoes.com.br): site JS-only, coleta pode falhar")
        print(f"  - PRM (prmempreendimentos.com.br): site retorna 403, coleta pode falhar")
        print(f"\nUso: python scrapers/generico_novas_empresas_f6.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f6.py --empresa todas")
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
            resultados[empresa_key] = {"novos": 0, "atualizados": 0, "erros": -1}

    if len(resultados) > 1:
        print("\n" + "=" * 60)
        print("  RESUMO GERAL — Batch F6")
        print("=" * 60)
        total_novos = 0
        total_erros = 0
        for key, r in resultados.items():
            if r:
                status = "OK" if r["erros"] >= 0 else "FALHA"
                print(f"  {EMPRESAS[key]['nome_banco']:20s} +{r['novos']} novos, {r['erros']} erros [{status}]")
                total_novos += r["novos"]
                total_erros += max(r["erros"], 0)
        print(f"  {'TOTAL':20s} +{total_novos} novos, {total_erros} erros")
        print("=" * 60)


if __name__ == "__main__":
    main()

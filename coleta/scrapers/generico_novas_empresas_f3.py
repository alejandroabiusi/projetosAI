"""
Scraper Batch F3 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F3.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    hexagonal, vitaurbana, access, donum, village, nvr, colinas, maxplural, marka, gadens

Uso:
    python scrapers/generico_novas_empresas_f3.py --empresa hexagonal
    python scrapers/generico_novas_empresas_f3.py --empresa todas
    python scrapers/generico_novas_empresas_f3.py --listar
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
    logger = logging.getLogger(f"scraper.f3.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f3_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F3
# ============================================================
EMPRESAS = {
    "hexagonal": {
        "nome_banco": "Hexagonal",
        "base_url": "https://www.hexagonal.com.br",
        "nome_from_title": True,
        "estado_default": "PE",
        "urls_listagem": [
            "https://www.hexagonal.com.br/empreendimentos/",
            "https://www.hexagonal.com.br/",
        ],
        "padrao_link": r"hexagonal\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vitaurbana": {
        "nome_banco": "Vita Urbana",
        "base_url": "https://vitaurbana.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://vitaurbana.com.br/nossos-empreendimentos/",
            "https://vitaurbana.com.br/",
        ],
        "padrao_link": r"vitaurbana\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|lojas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?|pav\.?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "access": {
        "nome_banco": "Access",
        "base_url": "https://accessacasaesua.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "urls_listagem": [
            "https://accessacasaesua.com.br/imoveis-a-venda/",
            "https://accessacasaesua.com.br/",
        ],
        "padrao_link": r"accessacasaesua\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .titulo_descricao", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*vagas?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "donum": {
        "nome_banco": "Donum",
        "base_url": "https://construtoradonum.com.br",
        "nome_from_title": True,
        "estado_default": "MG",
        # Donum: links coletados via pagina /imoveis e homepage
        "urls_listagem": [
            "https://construtoradonum.com.br/imoveis",
            "https://construtoradonum.com.br/",
        ],
        "padrao_link": r"construtoradonum\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "village": {
        "nome_banco": "Village",
        "base_url": "https://villageconstrucoes.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [
            "https://villageconstrucoes.com.br/empreendimentos",
            "https://villageconstrucoes.com.br/",
        ],
        "padrao_link": r"villageconstrucoes\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*vagas?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "nvr": {
        "nome_banco": "NVR",
        "base_url": "https://www.nvrempreendimentos.com.br",
        # NAO usar nome_from_title: title generico "Valor Real | Tradição em Construir Sonhos"
        "estado_default": "PR",
        "urls_listagem": [
            "https://www.nvrempreendimentos.com.br/empreendimentos",
            "https://www.nvrempreendimentos.com.br/",
        ],
        # NVR usa /empreendimento/{status}/{slug}
        "padrao_link": r"nvrempreendimentos\.com\.br/empreendimento/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*vagas?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "colinas": {
        "nome_banco": "Colinas",
        "base_url": "https://colinasengenharia.com.br",
        "nome_from_title": True,
        "estado_default": "PB",
        "cidade_default": "Campina Grande",
        "urls_listagem": [
            "https://colinasengenharia.com.br/empreendimentos/",
            "https://colinasengenharia.com.br/",
        ],
        "padrao_link": r"colinasengenharia\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "maxplural": {
        "nome_banco": "MaxPlural",
        "base_url": "https://www.maxplural.com.br",
        "nome_from_title": True,
        "estado_default": "PE",
        "urls_listagem": [
            "https://www.maxplural.com.br/site/empreendimentos",
            "https://www.maxplural.com.br/",
        ],
        # MaxPlural usa /site/empreendimento/{slug}
        "padrao_link": r"maxplural\.com\.br/site/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Prof[ªa]?\.?)[^,\n]+(?:,\s*[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "marka": {
        "nome_banco": "Marka Prime",
        "base_url": "https://markaprime.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "urls_listagem": [
            "https://markaprime.com.br/empreendimentos/",
            "https://markaprime.com.br/empreendimentos/page/2/",
            "https://markaprime.com.br/",
        ],
        "padrao_link": r"markaprime\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Dep\.)[^,\n]+(?:,\s*[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*vagas?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "gadens": {
        "nome_banco": "Gadens",
        "base_url": "https://www.gadens.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "cidade_default": "Curitiba",
        "urls_listagem": [
            "https://www.gadens.com.br/empreendimentos/",
            "https://www.gadens.com.br/",
        ],
        "padrao_link": r"gadens\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[-aA]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_donum(config, logger):
    """Coleta links da Donum via pagina /imoveis e homepage."""
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
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                # Filtrar slugs genericos como "breve-lancamento"
                if slug in ("breve-lancamento", "imoveis", "empreendimento"):
                    continue
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    # Empreendimentos conhecidos do site (fallback se JS nao renderiza)
    slugs_conhecidos = [
        "residencial-atenas", "residencial-dadiva", "residencial-fiorano",
        "residencial-mirante-da-mata", "mirante-do-sol", "residencial-montaltino",
        "park-sion", "residencial-riviera", "residencial-santorini", "residencial-viena",
    ]
    for slug in slugs_conhecidos:
        if slug not in links:
            links[slug] = f"{base}/empreendimento/{slug}"

    logger.info(f"Total de links Donum: {len(links)}")
    return links


def _coletar_links_nvr(config, logger):
    """Coleta links da NVR varrendo a pagina de empreendimentos por categoria."""
    links = {}
    base = config["base_url"]

    # Categorias de status da NVR
    categorias = [
        "lancamentos-e-futuros-lancamentos",
        "obras-em-andamento",
        "obras-finalizadas",
    ]

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

    logger.info(f"Total de links NVR: {len(links)}")
    return links


def _coletar_links_maxplural(config, logger):
    """Coleta links da MaxPlural via pagina /site/empreendimentos."""
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
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links MaxPlural: {len(links)}")
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
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper F3: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "donum":
        links = _coletar_links_donum(config, logger)
    elif empresa_key == "nvr":
        links = _coletar_links_nvr(config, logger)
    elif empresa_key == "maxplural":
        links = _coletar_links_maxplural(config, logger)
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

            # --- Hexagonal: cidade/estado/bairro do h4 (ex: "Bairro XXX Cidade/UF") ---
            if empresa_key == "hexagonal":
                soup_hex = BeautifulSoup(html, "html.parser")
                # Cidades conhecidas da Hexagonal (PE)
                cidades_hex = ["Caruaru", "Igarassu", "Recife", "Jaboatão dos Guararapes",
                               "Olinda", "Paulista", "Petrolina", "Garanhuns"]
                # Procurar h4 com padrao "Bairro XXX Cidade/UF" ou "Cidade/UF"
                for h4 in soup_hex.find_all("h4"):
                    txt = h4.get_text(strip=True)
                    # Procurar cidade conhecida + /UF
                    for cidade_hex in cidades_hex:
                        pattern = re.escape(cidade_hex) + r'/([A-Z]{2})$'
                        match_c = re.search(pattern, txt)
                        if match_c:
                            dados["cidade"] = cidade_hex
                            dados["estado"] = match_c.group(1)
                            # Bairro: tudo entre "Bairro " e a cidade
                            match_b = re.search(r'Bairro\s+(.+?)\s+' + re.escape(cidade_hex), txt, re.I)
                            if match_b:
                                dados["bairro"] = match_b.group(1).strip()
                            break
                    if dados.get("cidade"):
                        break
                # Fallback: padrao generico Cidade/UF
                if not dados.get("cidade"):
                    for h4 in soup_hex.find_all("h4"):
                        txt = h4.get_text(strip=True)
                        match_cidade = re.search(r'([\w\sçãõáéíóúâê]+)/([A-Z]{2})$', txt)
                        if match_cidade:
                            cidade_cand = match_cidade.group(1).strip()
                            # Se tem "Bairro", pegar apenas a ultima palavra como cidade
                            if "bairro" in cidade_cand.lower():
                                parts = cidade_cand.split()
                                dados["cidade"] = parts[-1] if parts else cidade_cand
                            else:
                                dados["cidade"] = cidade_cand
                            dados["estado"] = match_cidade.group(2)
                            break
                # Limpar nome: remover " | Casas e Apartamentos" etc
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*\|.*$', '', dados["nome"]).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- Vita Urbana: todas em SP capital, bairro do nome ---
            if empresa_key == "vitaurbana":
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                # Limpar nome: remover " – Vitaurbana" ou " - Vitaurbana"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Vitaurbana\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Extrair bairro do nome "Nurban BAIRRO"
                if dados.get("nome"):
                    match_bairro = re.search(r'Nurban\s+(.+)', dados["nome"], re.I)
                    if match_bairro:
                        dados["bairro"] = match_bairro.group(1).strip()
                # Extrair endereco se presente
                soup_vita = BeautifulSoup(html, "html.parser")
                for h2 in soup_vita.find_all(["h2", "h3"]):
                    txt = h2.get_text(strip=True)
                    if re.search(r'(?:Av\.|Rua|R\.)\s+', txt):
                        dados["endereco"] = txt
                        break

            # --- Access: limpar nome + cidade e bairro da pagina ---
            if empresa_key == "access":
                # Limpar nome: remover sufixo com detalhes "| 2 dorms..." etc
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*\|.*$', '', dados["nome"]).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                soup_acc = BeautifulSoup(html, "html.parser")
                # Access usa classes .cidade-zona e .bairro
                cidade_el = soup_acc.select_one(".cidade-zona, .cidade")
                if cidade_el:
                    txt = cidade_el.get_text(strip=True)
                    # Pode ser "Sorocaba - SP"
                    match_cid = re.search(r'([\w\s]+)\s*[-–]\s*([A-Z]{2})', txt)
                    if match_cid:
                        dados["cidade"] = match_cid.group(1).strip()
                        dados["estado"] = match_cid.group(2)
                    else:
                        dados["cidade"] = txt
                bairro_el = soup_acc.select_one(".bairro")
                if bairro_el:
                    dados["bairro"] = bairro_el.get_text(strip=True)
                # Fase de .categoria-empreendimento
                fase_el = soup_acc.select_one(".categoria-empreendimento")
                if fase_el:
                    fase_txt = fase_el.get_text(strip=True).lower()
                    if "obra" in fase_txt:
                        dados["fase"] = "Em Construção"
                    elif "pronto" in fase_txt:
                        dados["fase"] = "Pronto"
                    elif "lança" in fase_txt or "lanca" in fase_txt:
                        dados["fase"] = "Lançamento"
                    elif "última" in fase_txt or "ultima" in fase_txt:
                        dados["fase"] = "Pronto"

            # --- Donum: cidade/estado do texto ---
            if empresa_key == "donum":
                soup_don = BeautifulSoup(html, "html.parser")
                text_don = soup_don.get_text(separator="\n", strip=True)
                # Donum atua em BH, Betim, Nova Lima, Contagem, Araraquara, etc.
                cidades_donum = [
                    ("Belo Horizonte", "MG"), ("Betim", "MG"), ("Nova Lima", "MG"),
                    ("Contagem", "MG"), ("Araraquara", "SP"), ("Araras", "SP"),
                    ("Mogi Guaçu", "SP"), ("Valinhos", "SP"), ("Ribeirão das Neves", "MG"),
                    ("Santa Luzia", "MG"), ("Sabará", "MG"), ("Ibirité", "MG"),
                ]
                for cidade_nome, estado_val in cidades_donum:
                    if re.search(re.escape(cidade_nome), text_don, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    cidade_estado = extrair_cidade_estado(text_don)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                # Fase: "100% vendido"
                if re.search(r'100%\s*vendido', text_don, re.I):
                    dados["fase"] = "100% Vendido"
                elif re.search(r'pronto\s*para\s*morar', text_don, re.I):
                    dados["fase"] = "Pronto"

            # --- Village: limpar nome + cidade do texto ---
            if empresa_key == "village":
                # Limpar nome: remover " – Village Construções" ou " - Village Construções"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—•\u2022]\s*Village\s+Constru[çc][õo]es\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                soup_vil = BeautifulSoup(html, "html.parser")
                text_vil = soup_vil.get_text(separator="\n", strip=True)
                cidades_village = [
                    ("Cascavel", "PR"), ("Toledo", "PR"), ("Foz do Iguaçu", "PR"),
                    ("Marechal Cândido Rondon", "PR"), ("Palotina", "PR"),
                ]
                for cidade_nome, estado_val in cidades_village:
                    if re.search(re.escape(cidade_nome), text_vil, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    cidade_estado = extrair_cidade_estado(text_vil)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                # Fase: "100% VENDIDO" ou "EM CONSTRUÇÃO" badges
                if re.search(r'100%\s*vendido', text_vil, re.I):
                    dados["fase"] = "100% Vendido"

            # --- NVR: paginas JS-only, slug como nome ---
            if empresa_key == "nvr":
                soup_nvr = BeautifulSoup(html, "html.parser")
                # Site NVR e renderizado via JS: HTML estatico nao tem conteudo real
                # Usar slug da URL como nome (sempre confiavel)
                nome_nvr = slug.replace("-", " ").title()
                if nome_nvr and len(nome_nvr) > 2:
                    dados["nome"] = nome_nvr
                text_nvr = soup_nvr.get_text(separator="\n", strip=True)
                # Cidade da pagina
                cidades_nvr = [
                    ("Campo Largo", "PR"), ("Araucária", "PR"), ("Curitiba", "PR"),
                    ("Pinhais", "PR"), ("Colombo", "PR"), ("São José dos Pinhais", "PR"),
                    ("Fazenda Rio Grande", "PR"), ("Almirante Tamandaré", "PR"),
                ]
                for cidade_nome, estado_val in cidades_nvr:
                    if re.search(re.escape(cidade_nome), text_nvr, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    cidade_estado = extrair_cidade_estado(text_nvr)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                # Fase da URL
                if "obras-finalizadas" in url:
                    dados["fase"] = "Pronto"
                elif "obras-em-andamento" in url:
                    dados["fase"] = "Em Construção"
                elif "lancamentos" in url:
                    dados["fase"] = "Lançamento"

            # --- Colinas: todas em Campina Grande/PB ---
            if empresa_key == "colinas":
                dados["cidade"] = "Campina Grande"
                dados["estado"] = "PB"
                # Detectar "Esgotado" como 100% Vendido
                soup_col = BeautifulSoup(html, "html.parser")
                text_col = soup_col.get_text(separator="\n", strip=True)
                if re.search(r'esgotado', text_col, re.I):
                    dados["fase"] = "100% Vendido"
                elif re.search(r'conclu[ií]do', text_col, re.I):
                    dados["fase"] = "Pronto"

            # --- MaxPlural: cidade/estado do texto ---
            if empresa_key == "maxplural":
                soup_max = BeautifulSoup(html, "html.parser")
                text_max = soup_max.get_text(separator="\n", strip=True)
                # MaxPlural atua em Recife/PE e Maragogi/AL
                cidades_max = [
                    ("Recife", "PE"), ("Maragogi", "AL"), ("Jaboatão dos Guararapes", "PE"),
                    ("Olinda", "PE"), ("Paulista", "PE"), ("Boa Viagem", "PE"),
                ]
                for cidade_nome, estado_val in cidades_max:
                    if re.search(re.escape(cidade_nome), text_max, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    cidade_estado = extrair_cidade_estado(text_max)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                # Extrair bairro do endereco
                match_bairro = re.search(r'(\w[\w\s]+?)\s*,\s*Recife', text_max)
                if match_bairro and not dados.get("bairro"):
                    dados["bairro"] = match_bairro.group(1).strip()

            # --- Marka Prime: cidade/estado do texto ---
            if empresa_key == "marka":
                soup_mk = BeautifulSoup(html, "html.parser")
                text_mk = soup_mk.get_text(separator="\n", strip=True)
                # Marka atua em SP capital e Guarulhos
                # Priorizar endereco: buscar "BAIRRO - Cidade – SP" ou "Cidade – SP"
                # Primeiro buscar no endereco extraido
                endereco_mk = dados.get("endereco", "")
                if re.search(r'Guarulhos', endereco_mk, re.I):
                    dados["cidade"] = "Guarulhos"
                    dados["estado"] = "SP"
                elif re.search(r'São Paulo', endereco_mk, re.I):
                    dados["cidade"] = "São Paulo"
                    dados["estado"] = "SP"
                else:
                    # Buscar padrao "BAIRRO - Cidade – SP" no texto
                    match_endereco_cidade = re.search(
                        r'[-–]\s*(Guarulhos|São Paulo)\s*[-–]\s*SP', text_mk, re.I
                    )
                    if match_endereco_cidade:
                        dados["cidade"] = match_endereco_cidade.group(1).strip()
                        dados["estado"] = "SP"
                    else:
                        dados["cidade"] = "São Paulo"
                        dados["estado"] = "SP"
                # Bairro do nome "Marka BAIRRO"
                if dados.get("nome"):
                    match_bairro = re.search(r'Marka\s+(.+)', dados["nome"], re.I)
                    if match_bairro:
                        bairro_cand = match_bairro.group(1).strip()
                        if bairro_cand.lower() not in ("prime", "unik"):
                            dados["bairro"] = bairro_cand
                # Limpar nome: remover "| Marka Prime"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*\|\s*Marka\s*Prime\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase: detectar do texto
                title_tag = soup_mk.find("title")
                title_text = title_tag.string.strip() if title_tag and title_tag.string else ""
                # Paginas individuais podem ter badges de fase
                if re.search(r'\bem\s*obras?\b', text_mk, re.I):
                    if not dados.get("fase"):
                        dados["fase"] = "Em Construção"
                if re.search(r'\bentregue\b', text_mk, re.I):
                    dados["fase"] = "Pronto"
                if re.search(r'\bbreve\s*lança', text_mk, re.I):
                    dados["fase"] = "Breve Lançamento"

            # --- Gadens: todas em Curitiba/PR ---
            if empresa_key == "gadens":
                dados["cidade"] = "Curitiba"
                dados["estado"] = "PR"
                # Extrair bairro do endereco "Rua XXX, NNN - BAIRRO"
                if dados.get("endereco"):
                    match_bairro = re.search(r'-\s*([\w\s]+?)$', dados["endereco"])
                    if match_bairro:
                        dados["bairro"] = match_bairro.group(1).strip()
                # Fase do texto
                soup_gad = BeautifulSoup(html, "html.parser")
                text_gad = soup_gad.get_text(separator="\n", strip=True)
                if re.search(r'\bentregue\b', text_gad, re.I):
                    dados["fase"] = "Pronto"
                elif re.search(r'0%.*conclu[ií]d', text_gad, re.I):
                    dados["fase"] = "Lançamento"
                # Gadens nao e MCMV (empreendimentos de alto padrao em Curitiba)
                dados["prog_mcmv"] = 0

            # Limpar nome generico
            nome = dados.get("nome", "")
            nome_lower = nome.lower()
            nomes_genericos = ["incorporadora", "construtora", "home", "início",
                               "empreendimentos", "imoveis", "imóveis", "página",
                               "engenharia", "nossos"]
            if any(gen in nome_lower for gen in nomes_genericos) and len(nome) < 40:
                soup_tmp = BeautifulSoup(html, "html.parser")
                for tag in ["h2", "h3"]:
                    heading = soup_tmp.find(tag)
                    if heading:
                        txt = heading.get_text(strip=True)
                        if txt and 2 < len(txt) < 60 and not any(g in txt.lower() for g in nomes_genericos):
                            dados["nome"] = txt
                            break
                if any(gen in dados.get("nome", "").lower() for gen in nomes_genericos):
                    dados["nome"] = slug.replace("-", " ").title()

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default (exceto Gadens que ja foi setada)
            if "prog_mcmv" not in dados or (dados.get("prog_mcmv") is None):
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1
            elif dados.get("prog_mcmv") == 0 and empresa_key != "gadens":
                # Manter 0 apenas se ja setado explicitamente (Gadens)
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
    parser = argparse.ArgumentParser(description="Scraper Batch F3 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: hexagonal, gadens, todas)")
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
        print("\nEmpresas configuradas (Batch F3):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nUso: python scrapers/generico_novas_empresas_f3.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f3.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F3")
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

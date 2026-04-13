"""
Scraper Batch F10 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F10.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    ecovila, verbena, citta, coase, cmo, rve, camposgouveia
    (L7 descartada: l7construtora.com nao possui pagina de empreendimentos)
    (ADM descartada: admconstrutora.com.br DNS nao resolve)
    (A&C Lima descartada: aeclima.com.br DNS nao resolve)

Uso:
    python scrapers/generico_novas_empresas_f10.py --empresa ecovila
    python scrapers/generico_novas_empresas_f10.py --empresa todas
    python scrapers/generico_novas_empresas_f10.py --listar
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
    logger = logging.getLogger(f"scraper.f10.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f10_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F10
# ============================================================
EMPRESAS = {
    "ecovila": {
        "nome_banco": "Eco Vila",
        "base_url": "https://www.ecovilaincorporadora.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.ecovilaincorporadora.com.br/",
            "https://www.ecovilaincorporadora.com.br/empreendimentos/",
        ],
        "padrao_link": r"ecovilaincorporadora\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "verbena": {
        "nome_banco": "Verbena Invest",
        "base_url": "https://verbenainc.com",
        # Verbena nao tem paginas individuais — scraping da pagina de lancamentos
        "nome_from_title": False,
        "urls_listagem": [],  # Coleta customizada via _coletar_links_verbena
        "padrao_link": r"verbenainc\.com/lancamentos/#?([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "citta": {
        "nome_banco": "Città",
        "base_url": "https://cittaempreendimentos.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://cittaempreendimentos.com.br/",
            "https://cittaempreendimentos.com.br/empreendimentos/",
        ],
        # Citta usa /{nome}/ como padrao (sem /empreendimentos/ no path)
        "padrao_link": r"cittaempreendimentos\.com\.br/(kaleo|huios|emunah|bayit|yafah|zayit)(?:-cadastro)?/?",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|flats?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bflats?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "coase": {
        "nome_banco": "COASE",
        "base_url": "https://coaseconstrutora.com.br",
        "estado_default": "RS",
        "cidade_default": "Santa Maria",
        "nome_from_title": True,
        "urls_listagem": [
            "https://coaseconstrutora.com.br/",
            "https://coaseconstrutora.com.br/empreendimentos",
            "https://coaseconstrutora.com.br/empreendimentos?categoria=lancamentos",
            "https://coaseconstrutora.com.br/empreendimentos?categoria=em-obras",
            "https://coaseconstrutora.com.br/empreendimentos?categoria=prontos-para-morar",
        ],
        "padrao_link": r"coaseconstrutora\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3.empreendimento-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "cmo": {
        "nome_banco": "CMO",
        "base_url": "https://cmoconstrutora.com.br",
        "estado_default": "GO",
        "cidade_default": "Goiânia",
        # nome_from_title=False: CMO usa titulo generico "CMO Construtora - Goiania GO" em todas as paginas
        "nome_from_title": False,
        "urls_listagem": [
            "https://cmoconstrutora.com.br/",
            "https://cmoconstrutora.com.br/empreendimentos",
            "https://cmoconstrutora.com.br/empreendimentos?status=Em%20obras",
            "https://cmoconstrutora.com.br/empreendimentos?status=Lan%C3%A7amento",
            "https://cmoconstrutora.com.br/empreendimentos?status=Pronto%20para%20morar",
        ],
        # CMO usa /empreendimentos/{slug} e tambem /{slug} direto
        "padrao_link": r"cmoconstrutora\.com\.br/(?:empreendimentos/)?([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|QUADRA)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?|Q)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[-–]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "rve": {
        "nome_banco": "RVE",
        "base_url": "https://rve.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://rve.com.br/",
            "https://rve.com.br/empreendimentos",
            "https://rve.com.br/empreendimentos?page=2",
        ],
        "padrao_link": r"rve\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "camposgouveia": {
        "nome_banco": "Campos Gouveia",
        "base_url": "https://www.camposgouveia.com.br",
        "estado_default": "PE",
        "cidade_default": "Recife",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.camposgouveia.com.br/",
            "https://www.camposgouveia.com.br/empreendimentos/",
            "https://www.camposgouveia.com.br/empreendimentos/lancamento/",
            "https://www.camposgouveia.com.br/empreendimentos/em-construcao/",
        ],
        # Campos Gouveia usa /project/{slug}/ (nao /empreendimentos/)
        "padrao_link": r"camposgouveia\.com\.br/(?:project|empreendimentos)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .project-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_verbena(config, logger):
    """Coleta empreendimentos da Verbena da pagina de lancamentos.
    Verbena nao tem paginas individuais — todos os dados estao em /lancamentos/ e /empreendimentos-page/.
    Retornamos links ficticios para cada empreendimento detectado.
    """
    links = {}
    urls = [
        "https://verbenainc.com/lancamentos/",
        "https://verbenainc.com/empreendimentos-page/",
    ]

    for url in urls:
        logger.info(f"Coletando links de: {url}")
        html = fetch_html(url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        # Procurar headings com nomes de empreendimentos
        for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
            text = tag.get_text(strip=True)
            if not text or len(text) < 3 or len(text) > 80:
                continue
            # Filtrar headings de navegacao/genéricos
            lower = text.lower()
            if any(skip in lower for skip in [
                "verbena", "menu", "contato", "sobre", "parceiro", "fale",
                "nosso", "conheça", "empreendimento", "lançamento", "saiba",
                "investir", "footer", "header", "copyright", "direito",
                "parceria", "newsletter", "redes", "social",
            ]):
                continue
            # Verificar se parece nome de empreendimento
            slug = re.sub(r'[^a-z0-9]+', '-', lower).strip('-')
            if slug and slug not in links:
                # Usar URL da pagina como fonte
                links[slug] = f"https://verbenainc.com/lancamentos/#{slug}"

    logger.info(f"Total de links Verbena: {len(links)}")
    return links


def _coletar_links_citta(config, logger):
    """Coleta links da Citta. Como os slugs sao nomes proprios no root,
    precisamos coletar da pagina de empreendimentos e da home."""
    links = {}
    # Nomes conhecidos de empreendimentos Citta
    known_slugs = ["kaleo", "huios", "emunah", "bayit", "yafah", "zayit"]

    for url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url}")
        html = fetch_html(url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip().rstrip("/")
            if href.startswith("/"):
                href = config["base_url"] + href

            for slug in known_slugs:
                if slug in href.lower() and slug not in links:
                    # Normalizar URL (remover -cadastro se existir)
                    clean_url = f"{config['base_url']}/{slug}/"
                    links[slug] = clean_url
                    break

        time.sleep(DELAY)

    # Garantir que todos os conhecidos estejam na lista
    for slug in known_slugs:
        if slug not in links:
            links[slug] = f"{config['base_url']}/{slug}/"

    logger.info(f"Total de links Citta: {len(links)}")
    return links


def _coletar_links_camposgouveia(config, logger):
    """Coleta links da Campos Gouveia. Usa /project/{slug}/ como padrao."""
    links = {}

    for url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url}")
        html = fetch_html(url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = config["base_url"] + href
            elif not href.startswith("http"):
                continue

            match = re.search(r'camposgouveia\.com\.br/project/([\w-]+)', href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/") + "/"
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Campos Gouveia: {len(links)}")
    return links


def _coletar_links_cmo(config, logger):
    """Coleta links da CMO. Usa /empreendimentos/{slug} e /{slug} direto."""
    links = {}
    base = config["base_url"]

    # Slugs a ignorar (paginas genericas, nao empreendimentos)
    slugs_ignorar = {
        "empreendimentos", "lancamento", "em-obras", "pronto-para-morar", "entregue",
        "sobre", "contato", "fale-conosco", "politica-de-privacidade", "trabalhe-conosco",
        "blog", "noticias", "projeto", "favicon.ico", "wp-content", "wp-admin",
        "institucional", "area-do-corretor", "financiamento", "cmo-mais",
        "stand-de-vendas", "portfolio", "imprensa", "sustentabilidade",
    }

    for url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url}")
        html = fetch_html(url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            if "cmoconstrutora.com.br" not in href:
                continue

            # Padrao /empreendimentos/{slug}
            match = re.search(r'cmoconstrutora\.com\.br/empreendimentos/([\w-]+)', href)
            if match:
                slug = match.group(1)
                if slug not in slugs_ignorar:
                    url_empreendimento = f"{base}/empreendimentos/{slug}"
                    if slug not in links:
                        links[slug] = url_empreendimento
                continue

            # Padrao /{slug} direto (das paginas filtradas)
            match = re.search(r'cmoconstrutora\.com\.br/([\w-]+)/?$', href)
            if match:
                slug = match.group(1)
                if slug not in slugs_ignorar and len(slug) > 3:
                    # Verificar se e URL de empreendimento (nao pagina generica)
                    # Usar /empreendimentos/{slug} como URL canonica
                    url_empreendimento = f"{base}/empreendimentos/{slug}"
                    if slug not in links:
                        links[slug] = url_empreendimento

        time.sleep(DELAY)

    logger.info(f"Total de links CMO: {len(links)}")
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

    # 5b. Padrao parseFloat: centerLat = parseFloat('-XX.XXX'); centerLng = parseFloat('-XX.XXX')
    match = re.search(r'(?:center)?[Ll]at\s*=\s*(?:parseFloat\(\s*["\']?\s*)?(-?\d+\.\d{4,})', html)
    if match:
        lat_val = match.group(1)
        match_lng = re.search(r'(?:center)?[Ll](?:ng|on)\s*=\s*(?:parseFloat\(\s*["\']?\s*)?(-?\d+\.\d{4,})', html)
        if match_lng:
            return lat_val, match_lng.group(1)

    # 6. Padrao embed pb com coords
    match = re.search(r'!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)', html)
    if match:
        # Note: in Google pb format, 2d is longitude and 3d is latitude
        return match.group(2), match.group(1)

    return None, None


# ============================================================
# FUNCAO DE EXTRACAO DE CIDADE/ESTADO DA PAGINA
# ============================================================

def _extrair_cidade_estado_pagina(html, config):
    """Tenta extrair cidade/estado do HTML da pagina."""
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # Tentar funcao generica
    cidade_estado = extrair_cidade_estado(text)
    if cidade_estado:
        return cidade_estado

    # Fallback: defaults da config
    cidade = config.get("cidade_default")
    estado = config.get("estado_default")
    if cidade and estado:
        return cidade, estado

    return None, None


# ============================================================
# PROCESSAMENTO ESPECIAL: VERBENA (pagina unica com multiplos empreendimentos)
# ============================================================

def _processar_verbena(config, logger, atualizar=False, limite=None, sem_imagens=False):
    """Processa Verbena de forma especial.
    Verbena (verbenainc.com) nao tem paginas individuais de empreendimentos.
    Dados estao em /lancamentos/ e /empreendimentos-page/ como secoes em pagina unica.
    Usamos lista conhecida de empreendimentos e scraping direcionado.
    """
    nome_banco = config["nome_banco"]
    novos = 0
    atualizados = 0
    erros = 0

    # Empreendimentos conhecidos da Verbena (extraidos manualmente do site)
    empreendimentos_verbena = [
        {
            "nome": "Infinity Residence",
            "cidade": "Porto Seguro",
            "estado": "BA",
            "fase": "Lançamento",
            "url_fonte": "https://verbenainc.com/lancamentos/",
        },
        {
            "nome": "Único Verbena",
            "cidade": "Porto Seguro",
            "estado": "BA",
            "fase": "Em Obras",
            "url_fonte": "https://verbenainc.com/lancamentos/",
        },
        {
            "nome": "Alessio",
            "cidade": "Porto Belo",
            "estado": "SC",
            "fase": "Lançamento",
            "url_fonte": "https://verbenainc.com/lancamentos/",
        },
        {
            "nome": "Atlantic Tower",
            "cidade": "Porto Belo",
            "estado": "SC",
            "fase": "Em Obras",
            "url_fonte": "https://verbenainc.com/lancamentos/",
        },
    ]

    # Tentar enriquecer com dados do site
    html_lancamentos = fetch_html("https://verbenainc.com/lancamentos/", logger)
    html_empreendimentos = fetch_html("https://verbenainc.com/empreendimentos-page/", logger)
    time.sleep(DELAY)

    total = len(empreendimentos_verbena)
    if limite:
        empreendimentos_verbena = empreendimentos_verbena[:limite]

    for i, dados in enumerate(empreendimentos_verbena, 1):
        nome = dados["nome"]
        dados["empresa"] = nome_banco
        logger.info(f"[{i}/{total}] Processando: {nome}")

        # Tentar extrair dados adicionais do HTML
        for html in [html_lancamentos, html_empreendimentos]:
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            # Procurar secao com o nome do empreendimento
            for tag in soup.find_all(string=re.compile(re.escape(nome), re.I)):
                parent = tag.find_parent(["section", "div"])
                if not parent:
                    continue
                parent_text = parent.get_text(separator=" ", strip=True)

                # Extrair dormitorios
                if not dados.get("dormitorios_descricao"):
                    dorm_match = re.search(r'\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)', parent_text)
                    if dorm_match:
                        dados["dormitorios_descricao"] = dorm_match.group(0)

                # Extrair metragem
                if not dados.get("metragens_descricao"):
                    met_match = re.search(r'\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]', parent_text)
                    if met_match:
                        dados["metragens_descricao"] = met_match.group(0)

                # Extrair total unidades
                if not dados.get("total_unidades"):
                    uni_match = re.search(r'(\d+)\s*(?:unidades?|apartamentos?)', parent_text, re.I)
                    if uni_match:
                        dados["total_unidades"] = int(uni_match.group(1))

                # Coordenadas
                if not dados.get("latitude"):
                    lat, lon = extrair_coordenadas(str(parent))
                    if lat and lon:
                        dados["latitude"] = lat
                        dados["longitude"] = lon

                break  # Usar primeiro match

        # MCMV
        preco = dados.get("preco_a_partir")
        if preco and preco > 600000:
            dados["prog_mcmv"] = 0
        else:
            dados["prog_mcmv"] = 1

        dados["data_coleta"] = datetime.now().isoformat()

        existe = empreendimento_existe(nome_banco, nome)
        if existe and atualizar:
            atualizar_empreendimento(nome_banco, nome, dados)
            atualizados += 1
            logger.info(f"  Atualizado: {nome}")
        elif not existe:
            inserir_empreendimento(dados)
            novos += 1
            logger.info(f"  Inserido: {nome} | {dados.get('cidade', 'N/A')} | {dados.get('fase', 'N/A')}")
        else:
            logger.info(f"  Ja existe: {nome}")

    return {"novos": novos, "atualizados": atualizados, "erros": erros}


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper F10: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Verbena: processamento especial (pagina unica)
    if empresa_key == "verbena":
        result = _processar_verbena(config, logger, atualizar, limite, sem_imagens)
        logger.info("=" * 60)
        logger.info(f"RELATORIO - {nome_banco}")
        logger.info(f"  Novos inseridos: {result['novos']}")
        logger.info(f"  Atualizados: {result['atualizados']}")
        logger.info(f"  Erros: {result['erros']}")
        logger.info(f"  Total no banco: {contar_empreendimentos(nome_banco)}")
        logger.info("=" * 60)
        return result

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "citta":
        links = _coletar_links_citta(config, logger)
    elif empresa_key == "camposgouveia":
        links = _coletar_links_camposgouveia(config, logger)
    elif empresa_key == "cmo":
        links = _coletar_links_cmo(config, logger)
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

            # Eco Vila: cidade normalmente no endereco ou titulo (Hortolandia, Campinas, Sumare)
            if empresa_key == "ecovila":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    # Procurar padrao "Cidade - SP" ou "Cidade/SP"
                    cidade_match = re.search(
                        r'(Hortol[aâ]ndia|Campinas|Sumar[eé]|Valinhos|Paul[ií]nia|Indaiatuba|Americana|Jundiai)\s*[-/,]\s*(SP)',
                        text_tmp, re.I
                    )
                    if cidade_match:
                        dados["cidade"] = cidade_match.group(1).strip()
                        dados["estado"] = "SP"
                    else:
                        # Tentar extrair do endereco
                        end = dados.get("endereco", "")
                        cidade_end = re.search(
                            r'(Hortol[aâ]ndia|Campinas|Sumar[eé]|Valinhos|Paul[ií]nia|Indaiatuba|Americana|Jundiai)',
                            end, re.I
                        )
                        if cidade_end:
                            dados["cidade"] = cidade_end.group(1).strip()
                # Limpar nome: remover prefixo "Eco Vila " se redundante com nome_banco
                if dados.get("nome"):
                    nome_clean = dados["nome"].strip()
                    # Se o titulo e muito generico, melhorar
                    if nome_clean.lower() in ["eco vila", "eco vila incorporadora", "empreendimentos"]:
                        # Tentar pegar do slug
                        dados["nome"] = slug.replace("-", " ").title()

            # Citta: limpar nome (titulo inclui " - Citta Empreendimentos") + extrair cidade
            if empresa_key == "citta":
                if dados.get("nome"):
                    # Remover sufixo " - Citta Empreendimentos" ou similares
                    nome_limpo = re.sub(r'\s*[-–—|]\s*Citt[aà]\s+Empreendimentos\s*$', '', dados["nome"], flags=re.I).strip()
                    # Remover sufixo "Cadastro" de nomes como "Zayit Cadastro"
                    nome_limpo = re.sub(r'\s+Cadastro\s*$', '', nome_limpo, flags=re.I).strip()
                    if nome_limpo:
                        dados["nome"] = nome_limpo

            if empresa_key == "citta":
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                if not dados.get("cidade"):
                    # Citta atua em PB e SC
                    cidade_match = re.search(
                        r'(Cabedelo|Jo[aã]o\s+Pessoa|Campina\s+Grande|Florian[oó]polis|S[aã]o\s+Jos[eé]|Itaja[ií]|Balne[aá]rio\s+Cambori[uú]|Intermares)',
                        text_tmp, re.I
                    )
                    if cidade_match:
                        cidade = cidade_match.group(1).strip()
                        # Intermares e bairro de Cabedelo
                        if cidade.lower() == "intermares":
                            dados["cidade"] = "Cabedelo"
                        else:
                            dados["cidade"] = cidade
                # Estado: sempre definir baseado na cidade (default PB)
                cidade_citta = (dados.get("cidade") or "").lower()
                if any(sc in cidade_citta for sc in ["florianópolis", "florianopolis", "são josé", "sao jose", "itajaí", "itajai", "balneário", "balneario"]):
                    dados["estado"] = "SC"
                elif cidade_citta in ["cabedelo", "joão pessoa", "joao pessoa", "campina grande", ""] or re.search(r'\bPB\b|Para[ií]ba', text_tmp):
                    dados["estado"] = "PB"
                elif re.search(r'\bSC\b|Santa\s+Catarina', text_tmp):
                    dados["estado"] = "SC"
                else:
                    dados["estado"] = "PB"  # Default para Citta

            # COASE: todos os empreendimentos sao em Santa Maria/RS
            if empresa_key == "coase":
                # Corrigir cidades extraidas incorretamente (ex: "Bairro Camobi de Santa Maria")
                cidade = dados.get("cidade", "")
                if not cidade or "camobi" in cidade.lower() or "santa maria" in cidade.lower() or "bairro" in cidade.lower():
                    dados["cidade"] = "Santa Maria"
                dados["estado"] = "RS"

            # CMO: cidade default Goiania/GO
            if empresa_key == "cmo":
                if not dados.get("cidade"):
                    dados["cidade"] = "Goiânia"
                if not dados.get("estado"):
                    dados["estado"] = "GO"
                # CMO: nao e MCMV (medio/alto padrao em Goiania — precos a partir de 400k+)
                # Verificar pelo preco, mas manter como MCMV se preco baixo
                preco = dados.get("preco_a_partir")
                if preco and preco > 350000:
                    dados["prog_mcmv"] = 0

            # RVE: cidade frequentemente no texto
            if empresa_key == "rve":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_match = re.search(
                        r'(S[aã]o\s+Jos[eé]\s+do\s+Rio\s+Preto|S[aã]o\s+Paulo|Sorocaba|Jundia[ií]|Campinas|Itapevi|Osasco|Barueri|Carapicu[ií]ba|Cotia|Guarulhos|S[aã]o\s+Bernardo)',
                        text_tmp, re.I
                    )
                    if cidade_match:
                        dados["cidade"] = cidade_match.group(1).strip()
                    # Correcao de acentos
                    if dados.get("cidade"):
                        cidade = dados["cidade"]
                        if "sao jose do rio preto" in cidade.lower():
                            dados["cidade"] = "São José do Rio Preto"
                        elif "sao paulo" in cidade.lower():
                            dados["cidade"] = "São Paulo"

            # Campos Gouveia: default Recife/PE
            if empresa_key == "camposgouveia":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_match = re.search(
                        r'(Recife|Jaboat[aã]o|Olinda|Paulista|Cabo\s+de\s+Santo\s+Agostinho)',
                        text_tmp, re.I
                    )
                    if cidade_match:
                        dados["cidade"] = cidade_match.group(1).strip()
                    else:
                        dados["cidade"] = "Recife"
                if not dados.get("estado"):
                    dados["estado"] = "PE"
                # Limpar nomes: remover prefixo "Edf. " se presente no titulo
                # (manter se o titulo real e "Edf. Praca dos Jasmins")
                # Na verdade, manter como esta — o nome vem do <title> ou <h1>

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default
            if "prog_mcmv" not in dados or dados.get("prog_mcmv") is None:
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
    parser = argparse.ArgumentParser(description="Scraper Batch F10 - 7 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: ecovila, cmo, todas)")
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
        print("\nEmpresas configuradas (Batch F10):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTAS:")
        print(f"  L7 (l7construtora.com) descartada — site sem pagina de empreendimentos")
        print(f"  ADM (admconstrutora.com.br) descartada — DNS nao resolve")
        print(f"  A&C Lima (aeclima.com.br) descartada — DNS nao resolve")
        print(f"\nUso: python scrapers/generico_novas_empresas_f10.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f10.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F10")
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

"""
Scraper Batch F11 — 10 Novas Incorporadoras
=============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F11.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    visconde, fama, coral, girollar, herc, urben, yees, moinho, swa
    (ARD descartada: dominio ardempreendimentos.com.br estacionado/parqueado)

Uso:
    python scrapers/generico_novas_empresas_f11.py --empresa visconde
    python scrapers/generico_novas_empresas_f11.py --empresa todas
    python scrapers/generico_novas_empresas_f11.py --listar
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
    logger = logging.getLogger(f"scraper.f11.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f11_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F11
# ============================================================
EMPRESAS = {
    "visconde": {
        "nome_banco": "Visconde",
        "base_url": "https://www.viscondeconstrutora.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.viscondeconstrutora.com.br/imoveis/",
        ],
        # Pattern: /imoveis/sp/{cidade}/apartamentos/{slug}/
        "padrao_link": r"viscondeconstrutora\.com\.br/imoveis/[^/]+/[^/]+/[^/]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.entry-title, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "fama": {
        "nome_banco": "FAMA",
        "base_url": "https://famaempreendimentos.com.br",
        "estado_default": "TO",
        "cidade_default": "Palmas",
        "nome_from_title": True,
        "urls_listagem": [
            "https://famaempreendimentos.com.br/",
        ],
        "padrao_link": r"famaempreendimentos\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Quadra)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "coral": {
        "nome_banco": "Coral",
        "base_url": "https://coralconstrutora.com.br",
        "estado_default": "SC",
        "cidade_default": "Florianópolis",
        "nome_from_title": True,
        "urls_listagem": [
            "https://coralconstrutora.com.br/",
            "https://coralconstrutora.com.br/empreendimentos/",
        ],
        # Pattern: /slug-direto/ (nao tem /empreendimento/)
        "padrao_link": r"coralconstrutora\.com\.br/((?:recanto|mirante|jardins|one|souto)[\w-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2.entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Servidão)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "girollar": {
        "nome_banco": "Girollar",
        "base_url": "https://girollar.com.br",
        "estado_default": "SC",
        "cidade_default": "Blumenau",
        "nome_from_title": True,
        "urls_listagem": [
            "https://girollar.com.br/",
            "https://girollar.com.br/empreendimentos/",
        ],
        "padrao_link": r"girollar\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "herc": {
        "nome_banco": "Herc",
        "base_url": "http://construtoraherc.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        # Herc usa dominios externos por empreendimento — coleta customizada
        "urls_listagem": [],  # Coleta customizada via _coletar_links_herc
        "padrao_link": r"construtoraherc\.com\.br",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "urben": {
        "nome_banco": "Urben",
        "base_url": "https://www.urben.com.br",
        "estado_default": "SP",
        "cidade_default": "Ribeirão Preto",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.urben.com.br/empreendimentos/",
            "https://www.urben.com.br/",
        ],
        "padrao_link": r"urben\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "yees": {
        "nome_banco": "Yees",
        "base_url": "https://yeesinc.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://yeesinc.com.br/",
            "https://yeesinc.com.br/empreendimentos/",
        ],
        "padrao_link": r"yeesinc\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "moinho": {
        "nome_banco": "Moinho",
        "base_url": "https://construtoramoinho.com.br",
        "estado_default": "SC",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construtoramoinho.com.br/",
            "https://construtoramoinho.com.br/empreendimentos/",
        ],
        "padrao_link": r"construtoramoinho\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2.entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:(?:a|e)\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "swa": {
        "nome_banco": "SWA",
        "base_url": "https://swarealty.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://swarealty.com.br/",
            "https://swarealty.com.br/imoveis/",
        ],
        "padrao_link": r"swarealty\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2.entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_herc(config, logger):
    """
    Coleta links da Herc Construtora.
    Herc usa dominios externos por empreendimento (habitatsantacecilia.com.br, etc.)
    e tambem paginas internas (?page_id=333).
    Coletamos os links da homepage e processamos cada um individualmente.
    """
    links = {}
    base = config["base_url"]

    logger.info(f"Coletando links da Herc: {base}")
    html = fetch_html(base, logger)
    if not html:
        # Tentar com https
        html = fetch_html(base.replace("http://", "https://"), logger)
    if not html:
        logger.error("Nao foi possivel acessar o site da Herc")
        return links

    soup = BeautifulSoup(html, "html.parser")

    # Mapear empreendimentos conhecidos com seus dominios externos
    # Extrair todos os links que parecem ser de empreendimentos
    empreendimentos_externos = {}

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = a_tag.get_text(strip=True)

        # Ignorar links internos genericos
        if not href or href == "#" or href.startswith("tel:") or href.startswith("mailto:"):
            continue

        # Links para dominios externos de empreendimentos
        # Pattern: https://nomeempreendimento.com.br/
        if re.match(r'https?://[\w]+(?:[\w-]*)\.com\.br/?$', href):
            if "construtoraherc" not in href and "hostinger" not in href and "instagram" not in href and "facebook" not in href and "youtube" not in href and "linkedin" not in href and "whatsapp" not in href and "wa.me" not in href:
                # Extrair slug do dominio
                match_dom = re.match(r'https?://([\w-]+)\.com\.br', href)
                if match_dom:
                    slug = match_dom.group(1)
                    empreendimentos_externos[slug] = href.rstrip("/") + "/"
                    logger.info(f"  Encontrado empreendimento externo: {slug} -> {href}")

        # Links internos page_id (empreendimentos futuros)
        if "page_id=" in href and "construtoraherc" in href:
            page_match = re.search(r'page_id=(\d+)', href)
            if page_match:
                slug = f"herc-pagina-{page_match.group(1)}"
                if slug not in empreendimentos_externos:
                    empreendimentos_externos[slug] = href
                    logger.info(f"  Encontrado empreendimento interno: {slug} -> {href}")

    links = empreendimentos_externos
    logger.info(f"Total links Herc: {len(links)}")
    return links


def _coletar_links_visconde(config, logger):
    """Coleta links da Visconde da pagina /imoveis/ que tem URL hierarquica."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]

    for list_url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {list_url}")
        html = fetch_html(list_url, logger)
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
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/") + "/"
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total links Visconde: {len(links)}")
    return links


def _coletar_links_coral(config, logger):
    """Coleta links da Coral que usa slugs diretos (sem /empreendimento/)."""
    links = {}
    base = config["base_url"]

    for list_url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {list_url}")
        html = fetch_html(list_url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            # Filtrar links de empreendimentos (nao paginas genericas)
            if "coralconstrutora.com.br/" not in href:
                continue

            # Extrair path
            path_match = re.search(r'coralconstrutora\.com\.br/([^/?#]+)/?$', href)
            if path_match:
                slug = path_match.group(1)
                # Filtrar paginas genericas
                genericas = [
                    "empreendimentos", "sobre", "contato", "blog", "noticias",
                    "trabalhe-conosco", "area-do-cliente", "politica-de-privacidade",
                    "wp-content", "wp-admin", "feed", "comments", "page",
                ]
                if slug.lower() not in genericas and not slug.startswith("wp-"):
                    url_limpa = href.split("?")[0].split("#")[0].rstrip("/") + "/"
                    if slug not in links:
                        links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total links Coral: {len(links)}")
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

    # 3. Padrao ll=lat,lon (Waze e Google Maps)
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

    # 7. Padrao Waze navigate com coords
    match = re.search(r'waze\.com/ul\?ll=(-?\d+\.\d+)%2C(-?\d+\.\d+)', html)
    if match:
        return match.group(1), match.group(2)

    # 8. Padrao coords em data-attributes ou JSON
    match = re.search(r'"latitude"\s*:\s*"?(-?\d+\.\d{4,})"?\s*,\s*"longitude"\s*:\s*"?(-?\d+\.\d{4,})"?', html)
    if match:
        return match.group(1), match.group(2)

    return None, None


# ============================================================
# FUNCOES DE EXTRACAO DE CIDADE/ESTADO DA URL (Visconde)
# ============================================================

def _extrair_cidade_da_url_visconde(url):
    """Extrai cidade da URL pattern /imoveis/sp/{cidade}/.../{slug}/."""
    match = re.search(r'/imoveis/([^/]+)/([^/]+)/', url)
    if match:
        estado_slug = match.group(1).upper()
        cidade_slug = match.group(2)
        cidade = cidade_slug.replace("-", " ").title()
        # Correcoes comuns
        correcoes = {
            "Sao Paulo": "São Paulo",
            "Sao Jose": "São José",
            "Jacarei": "Jacareí",
            "Mogi Mirim": "Mogi Mirim",
            "Campos Do Jordao": "Campos do Jordão",
        }
        cidade = correcoes.get(cidade, cidade)
        return cidade, estado_slug if estado_slug != "SEM-ESTADO" else "SP"
    return None, None


def _extrair_cidade_yees(html, url):
    """Extrai cidade/estado do texto da pagina da Yees (formato 'Cidade - UF')."""
    if not html:
        return None, None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    # Padrao "Sorocaba - SP" ou "Campinas - SP"
    match = re.search(r'([\w\s]+?)\s*[-–]\s*(SP|RJ|MG|PR|SC|RS|BA|CE|PE|GO|DF|TO|PA|AM|MA|PI|RN|PB|SE|AL|ES|MT|MS|RO|AC|AP|RR)\b', text)
    if match:
        cidade = match.group(1).strip()
        estado = match.group(2).strip()
        # Filtrar falsos positivos (palavras comuns antes de " - SP")
        if len(cidade) > 2 and len(cidade) < 40 and not any(g in cidade.lower() for g in ["home", "menu", "click", "saiba", "veja"]):
            return cidade, estado
    return None, None


def _extrair_cidade_moinho(html, url):
    """Extrai cidade/estado do texto da pagina da Moinho (formato 'Cidade . SC')."""
    if not html:
        return None, None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    # Padrao "Biguacu . SC" ou "Biguaçu • SC"
    match = re.search(r'([\w\sçãõáéíóúâêôÇÃÕÁÉÍÓÚÂÊÔ]+?)\s*[•·.]\s*(SC|PR|RS|SP)\b', text)
    if match:
        cidade = match.group(1).strip()
        estado = match.group(2).strip()
        if len(cidade) > 2 and len(cidade) < 40:
            return cidade, estado
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
    logger.info(f"Iniciando scraper F11: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "herc":
        links = _coletar_links_herc(config, logger)
    elif empresa_key == "visconde":
        links = _coletar_links_visconde(config, logger)
    elif empresa_key == "coral":
        links = _coletar_links_coral(config, logger)
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
            # Para Herc (dominios externos), usar config simplificado
            if empresa_key == "herc":
                dados = _extrair_dados_herc(html, url, config, logger)
            else:
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

            # Visconde: cidade da URL
            if empresa_key == "visconde":
                cidade_url, estado_url = _extrair_cidade_da_url_visconde(url)
                if cidade_url and not dados.get("cidade"):
                    dados["cidade"] = cidade_url
                if estado_url and not dados.get("estado"):
                    dados["estado"] = estado_url
                # Limpar nome: remover " – Visconde Construtora" do title
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Visconde\s*Construtora.*$', '', dados["nome"]).strip()

            # FAMA: limpar nome (remover sufixo " - Fama Empreendimentos")
            if empresa_key == "fama":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Fama\s*Empreendimentos.*$', '', dados["nome"]).strip()

            # Coral: limpar nome e extrair bairro/cidade do titulo
            if empresa_key == "coral":
                if dados.get("nome"):
                    # Titulo formato: "Nome - BAIRRO - Cidade/SC"
                    dados["nome"] = re.sub(r'\s*[-–]\s*Coral\s*Construtora.*$', '', dados["nome"]).strip()
                # Extrair cidade do title tag
                soup_tmp = BeautifulSoup(html, "html.parser")
                title_tag = soup_tmp.find("title")
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Pattern: "Nome - BAIRRO - Cidade/SC"
                    match_loc = re.search(r'-\s*([^-]+)/\s*(SC|PR|RS)\s*$', title_text)
                    if match_loc and not dados.get("cidade"):
                        dados["cidade"] = match_loc.group(1).strip().title()
                        dados["estado"] = match_loc.group(2).strip()

            # Girollar: limpar nome
            if empresa_key == "girollar":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Girollar.*$', '', dados["nome"]).strip()

            # Herc: ja tratado em _extrair_dados_herc
            if empresa_key == "herc":
                pass

            # Urben: limpar nome e extrair cidade da pagina
            if empresa_key == "urben":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Urben.*$', '', dados["nome"]).strip()
                # Extrair cidade da pagina
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]

            # Yees: extrair cidade/estado do texto
            if empresa_key == "yees":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Yees\s*(?:Construtora|Inc).*$', '', dados["nome"]).strip()
                cidade, estado = _extrair_cidade_yees(html, url)
                if cidade and not dados.get("cidade"):
                    dados["cidade"] = cidade
                if estado and not dados.get("estado"):
                    dados["estado"] = estado

            # Moinho: extrair cidade/estado do texto
            if empresa_key == "moinho":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*Moinho\s*Construtora.*$', '', dados["nome"]).strip()
                cidade, estado = _extrair_cidade_moinho(html, url)
                if cidade and not dados.get("cidade"):
                    dados["cidade"] = cidade
                if estado and not dados.get("estado"):
                    dados["estado"] = estado

            # SWA: limpar nome
            if empresa_key == "swa":
                if dados.get("nome"):
                    dados["nome"] = re.sub(r'\s*[-–]\s*SWA\s*Realty.*$', '', dados["nome"]).strip()

            # Aplicar defaults de cidade/estado se nao extraidos da pagina
            if not dados.get("cidade") and config.get("cidade_default"):
                dados["cidade"] = config["cidade_default"]
            if not dados.get("estado") and config.get("estado_default"):
                dados["estado"] = config["estado_default"]

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # Filtrar nomes genericos (titulo do site, nao nome do empreendimento)
            nome_lower = nome.lower()
            genericos_exatos = ["construtora", "incorporadora", "empreendimentos", "home", "página inicial", "pagina inicial", "novos empreendimentos"]
            if any(g == nome_lower for g in genericos_exatos):
                logger.warning(f"  Nome generico detectado: '{nome}', pulando")
                erros += 1
                continue

            # MCMV por default
            if "prog_mcmv" not in dados or dados.get("prog_mcmv") == 0:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # Urben Tellus: alto padrao (121-242m2), nao MCMV
            if empresa_key == "urben" and dados.get("area_max_m2") and dados["area_max_m2"] > 150:
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
# EXTRACAO ESPECIAL PARA HERC (dominios externos)
# ============================================================

def _extrair_dados_herc(html, url, config, logger):
    """
    Extrai dados de paginas de empreendimentos Herc.
    Cada empreendimento vive em um dominio proprio (ex: habitatsantacecilia.com.br).
    """
    soup = BeautifulSoup(html, "html.parser")
    texto_completo = soup.get_text(separator="\n", strip=True)

    dados = {
        "empresa": config["nome_banco"],
        "url_fonte": url,
    }

    # Nome do title tag
    title_tag = soup.find("title")
    if title_tag:
        nome = title_tag.get_text(strip=True)
        # Limpar sufixos comuns
        nome = re.sub(r'\s*[-–|]\s*(?:Herc|Construtora|Home|home).*$', '', nome).strip()
        nome = re.sub(r'^\s*(?:Home|home)\s*[-–|]\s*', '', nome).strip()
        # Limpar extras apos ponto ou pipe (ex: "Safira Santana . Apartamento de...")
        nome = re.sub(r'\s*[.|]\s*Apartamento.*$', '', nome).strip()
        # Limpar # e similares
        nome = re.sub(r'\s*#\s*$', '', nome).strip()
        # Se ficou como dominio (ex: topaziocupece.com.br), transformar
        if ".com" in nome:
            nome = ""
        if nome and len(nome) > 2:
            dados["nome"] = nome

    # Se nao achou no title, tentar h1/h2
    if not dados.get("nome"):
        for tag in ["h1", "h2"]:
            el = soup.find(tag)
            if el:
                text = el.get_text(strip=True)
                if text and len(text) > 2 and len(text) < 60:
                    # Filtrar headings genericos/slogans
                    text_lower = text.lower()
                    if not any(g in text_lower for g in [
                        "melhor opção", "comodidade", "home", "menu",
                        "moderno", "completo", "acessível", "acessivel",
                        "seu novo lar", "lançamento", "lancamento",
                        "breve", "obras", "entregue",
                    ]):
                        dados["nome"] = text
                        break

    # Se ainda nao achou, usar dominio como nome limpo
    if not dados.get("nome"):
        match_dom = re.match(r'https?://([\w-]+)\.com\.br', url)
        if match_dom:
            raw = match_dom.group(1)
            # Converter camelCase ou concatenado para nome legivel
            # Ex: "habitatsantacecilia" -> "Habitat Santa Cecilia"
            nomes_dominios = {
                "habitatsantacecilia": "Habitat Santa Cecília",
                "agataresidencial": "Ágata Residencial",
                "topaziocupece": "Topázio Cupecê",
                "safirasantana": "Safira Santana",
                "quartzoimirim": "Quartzo Imirim",
            }
            dados["nome"] = nomes_dominios.get(raw, raw.replace("-", " ").title())

    # Cidade/Estado
    dados["cidade"] = config.get("cidade_default", "São Paulo")
    dados["estado"] = config.get("estado_default", "SP")

    # Bairro: tentar extrair do texto
    # Padrao comum Herc: "Bairro - São Paulo"
    bairro_match = re.search(r'(?:Zona\s+(?:Norte|Sul|Leste|Oeste))\s*[-–]\s*São\s*Paulo', texto_completo)
    if bairro_match:
        dados["bairro"] = bairro_match.group(0).split("-")[0].strip().split("–")[0].strip()

    # Endereco
    end_match = re.search(r'(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?[^-\n]*', texto_completo)
    if end_match:
        dados["endereco"] = end_match.group(0).strip()

    # Fase
    fase = detectar_fase(texto_completo, soup)
    if fase:
        dados["fase"] = fase

    # Preco
    preco = extrair_preco(texto_completo)
    if preco:
        dados["preco_a_partir"] = preco

    # Dormitorios
    dorm_match = re.search(r'(\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?))|\bstudios?\b', texto_completo, re.IGNORECASE)
    if dorm_match:
        dados["dormitorios_descricao"] = dorm_match.group(0).strip()

    # Metragens
    metragens = re.findall(r'(\d+(?:[.,]\d+)?)\s*m[²2]', texto_completo)
    if metragens:
        metragens_num = []
        for m in metragens:
            try:
                val = float(m.replace(",", "."))
                if 15 <= val <= 500:  # Filtrar metragens absurdas
                    metragens_num.append(val)
            except ValueError:
                pass
        if metragens_num:
            dados["area_min_m2"] = min(metragens_num)
            dados["area_max_m2"] = max(metragens_num)
            dados["metragens_descricao"] = " | ".join(f"{v}m²" for v in sorted(set(metragens_num)))

    # Total unidades
    units_match = re.search(r'(\d+)\s*(?:unidades?|apartamentos?|aptos?)', texto_completo, re.IGNORECASE)
    if units_match:
        dados["total_unidades"] = int(units_match.group(1))

    # Itens lazer
    itens = extrair_itens_lazer(soup, texto_completo)
    if itens:
        dados["itens_lazer"] = " | ".join(itens)
        atributos = detectar_atributos_binarios(texto_completo)
        dados.update(atributos)

    return dados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper Batch F11 - 9 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: visconde, fama, todas)")
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
        print("\nEmpresas configuradas (Batch F11):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTA: ARD (ardempreendimentos.com.br) descartada — dominio estacionado/parqueado")
        print(f"\nUso: python scrapers/generico_novas_empresas_f11.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f11.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F11")
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

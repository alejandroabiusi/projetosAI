"""
Scraper Batch F7 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F7.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    construplan, geratriz, pafil, vlp3, dc4, credlar, ffb, makeit, usinadeobras, brn

Notas:
    - Construplan: empreendimentos em 2 dominios (cpconstruplan.com.br + cpresidencial.com.br)
      Projetos com dominios externos proprios sao ignorados (ex: cerqueira2164.com.br)
    - BRN (brn.com.br): site retorna 403 via WebFetch mas funciona com requests
    - Credlar (credlarconstrutora.com.br): site timeout persistente, coleta falha
    - Usina de Obras: site JS-only (React), coleta via sitemap + tentativa de fetch
    - Make.it: site JS-only (SPA), nomes extraidos do slug (paginas nao renderizam)

Uso:
    python scrapers/generico_novas_empresas_f7.py --empresa geratriz
    python scrapers/generico_novas_empresas_f7.py --empresa todas
    python scrapers/generico_novas_empresas_f7.py --listar
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
from xml.etree import ElementTree

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
    logger = logging.getLogger(f"scraper.f7.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f7_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F7
# ============================================================
EMPRESAS = {
    "construplan": {
        "nome_banco": "Construplan",
        "base_url": "https://www.cpconstruplan.com.br",
        "nome_from_title": True,
        # Construplan tem empreendimentos em 2 dominios:
        # cpconstruplan.com.br/empreendimento/{slug} e cpresidencial.com.br/empreendimentos/{slug}
        # Coleta customizada via _coletar_links_construplan
        "urls_listagem": [
            "https://www.cpconstruplan.com.br/empreendimentos/",
        ],
        "padrao_link": r"(?:cpconstruplan\.com\.br/empreendimento|cpresidencial\.com\.br/empreendimentos)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "geratriz": {
        "nome_banco": "Geratriz",
        "base_url": "https://geratriz.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://geratriz.com.br/",
            "https://geratriz.com.br/imoveis/",
        ],
        "padrao_link": r"geratriz\.com\.br/((?:bosque|reserva|parque|residencial|jardim)[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Alameda)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "pafil": {
        "nome_banco": "Pafil",
        "base_url": "https://www.pafil.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.pafil.com.br/empreendimentos/",
        ],
        "padrao_link": r"pafil\.com\.br/([\w][\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vlp3": {
        "nome_banco": "VLP3",
        "base_url": "https://www.vlp3.eng.br",
        "estado_default": "PR",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.vlp3.eng.br/",
            "https://www.vlp3.eng.br/empreendimentos/",
        ],
        "padrao_link": r"vlp3\.eng\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "dc4": {
        "nome_banco": "DC4",
        "base_url": "https://dc4.com.br",
        "estado_default": "RJ",
        "nome_from_title": True,
        # DC4 organiza por status: /imoveis/lancamento/, /imoveis/em-construcao/, etc
        "urls_listagem": [
            "https://dc4.com.br/",
            "https://dc4.com.br/imoveis/lancamento/",
            "https://dc4.com.br/imoveis/em-construcao/",
            "https://dc4.com.br/imoveis/breves-lancamentos/",
            "https://dc4.com.br/imoveis/prontos/",
        ],
        "padrao_link": r"dc4\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bgarden\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "credlar": {
        "nome_banco": "Credlar",
        "base_url": "https://credlarconstrutora.com.br",
        "estado_default": "SP",
        "cidade_default": "Praia Grande",
        "nome_from_title": True,
        # Site com timeout persistente — coleta provavelmente falhara
        "urls_listagem": [
            "https://credlarconstrutora.com.br/empreendimentos/",
            "https://credlarconstrutora.com.br/",
        ],
        "padrao_link": r"credlarconstrutora\.com\.br/empreendimentos?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "ffb": {
        "nome_banco": "FFB",
        "base_url": "https://ffb.com.br",
        "estado_default": "SE",
        "cidade_default": "Aracaju",
        "nome_from_title": True,
        "urls_listagem": [
            "https://ffb.com.br/",
            "https://ffb.com.br/empreendimentos/",
        ],
        "padrao_link": r"ffb\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "makeit": {
        "nome_banco": "Make.it",
        "base_url": "https://makeitconstrutora.com.br",
        "estado_default": "PR",
        "nome_from_title": True,
        # Make.it usa URLs /imoveis/{slug} para paginas individuais
        "urls_listagem": [
            "https://makeitconstrutora.com.br/",
            "https://makeitconstrutora.com.br/imovel/todos",
        ],
        "padrao_link": r"makeitconstrutora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "usinadeobras": {
        "nome_banco": "Usina de Obras",
        "base_url": "https://www.usinadeobras.com.br",
        "estado_default": "PE",
        "nome_from_title": True,
        # Site JS-only (React), pages nao renderizam via requests.
        # Coleta via sitemap + tentativa de fetch.
        "sitemap_url": "https://www.usinadeobras.com.br/sitemap.xml",
        "urls_listagem": [],  # Coleta customizada via sitemap
        "padrao_link": r"usinadeobras\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
        # Slugs de projetos residenciais do sitemap (filtrados manualmente para excluir obras publicas)
        "slugs_residenciais": [
            "engenho-planalto", "engenho-paratibe",
            "residencial-suassuna-quadra-1", "residencial-suassuna-quadra-2",
            "residencial-suassuna-quadra-3", "residencial-suassuna-quadra-6",
            "residencial-aritana", "residencial-nossa-prata",
            "cone-nova", "paulista-prime-residence",
            "capibaribe-prime-residence", "mira",
            "varzea-prime-residence", "augusto-lucena",
        ],
    },

    "brn": {
        "nome_banco": "BRN",
        "base_url": "https://brn.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        # Site retorna 403 — coleta falhara
        "urls_listagem": [
            "https://brn.com.br/",
            "https://brn.com.br/empreendimentos/",
            "https://www.brn.com.br/empreendimentos/",
        ],
        "padrao_link": r"brn\.com\.br/empreendimentos?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_construplan(config, logger):
    """Coleta links da Construplan de 2 dominios."""
    links = {}
    base_cp = "https://www.cpconstruplan.com.br"
    base_res = "https://cpresidencial.com.br"

    # 1. Pagina de listagem do site principal
    for list_url in config["urls_listagem"]:
        logger.info(f"Coletando links de: {list_url}")
        html = fetch_html(list_url, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            # Links internos cpconstruplan.com.br/empreendimento/{slug}
            match = re.search(r'cpconstruplan\.com\.br/empreendimento/([\w-]+)', href)
            if match:
                slug = match.group(1)
                url_limpa = f"{base_cp}/empreendimento/{slug}/"
                links[slug] = url_limpa
                continue

            # Links cpresidencial.com.br/empreendimentos/{slug}
            match = re.search(r'cpresidencial\.com\.br/empreendimentos/([\w-]+)', href)
            if match:
                slug = match.group(1)
                url_limpa = f"{base_res}/empreendimentos/{slug}/"
                links[slug] = url_limpa
                continue

        time.sleep(DELAY)

    logger.info(f"Total de links Construplan: {len(links)}")
    return links


def _coletar_links_pafil(config, logger):
    """Coleta links da Pafil filtrando paginas que nao sao empreendimentos."""
    links = {}
    base = config["base_url"]

    # Paginas que NAO sao empreendimentos
    paginas_excluir = {
        "empreendimentos", "sobre", "contato", "categorias",
        "fale-conosco", "trabalhe-conosco", "politica-de-privacidade",
        "portfolio", "blog", "noticias", "a-pafil", "institucional",
        "aplicativo-meu-pafil", "mentes-saudaveis", "wp-content",
        "wp-admin", "wp-login", "feed", "xmlrpc",
    }

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

            if "pafil.com.br" not in href:
                continue

            # Extrair slug: pafil.com.br/{slug}/
            match = re.search(r'pafil\.com\.br/([\w][\w-]+?)/?(?:\?.*)?$', href)
            if match:
                slug = match.group(1)
                # Filtrar paginas institucionais
                if slug.lower() in paginas_excluir:
                    continue
                # Filtrar categorias
                if "/categorias/" in href:
                    continue
                url_limpa = f"{base}/{slug}/"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Pafil: {len(links)}")
    return links


def _coletar_links_dc4(config, logger):
    """Coleta links da DC4 de multiplas paginas de status."""
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
                url_limpa = f"{base}/imovel/{slug}/"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links DC4: {len(links)}")
    return links


def _coletar_links_usinadeobras(config, logger):
    """Coleta links da Usina de Obras via sitemap (site JS-only)."""
    links = {}
    base = config["base_url"]

    # Usar lista fixa de slugs residenciais extraidos do sitemap
    slugs = config.get("slugs_residenciais", [])
    for slug in slugs:
        url = f"{base}/empreendimento/{slug}"
        links[slug] = url

    logger.info(f"Total de links Usina de Obras (do sitemap): {len(links)}")
    logger.warning("NOTA: Site JS-only, paginas individuais podem nao renderizar via requests")
    return links


def _coletar_links_makeit(config, logger):
    """Coleta links da Make.it das paginas de listagem."""
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
                # Filtrar slugs que sao paginas de filtro, nao empreendimentos
                if slug in ("todos", "minha-casa-minha-vida", "sem-minha-casa-minha-vida"):
                    continue
                url_limpa = f"{base}/imoveis/{slug}"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Make.it: {len(links)}")
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
# FUNCOES AUXILIARES DE EXTRACAO POR EMPRESA
# ============================================================

def _extrair_fase_da_url_dc4(url):
    """Infere fase a partir da URL de listagem DC4 (quando veio de qual pagina)."""
    # Sera usada apenas como fallback se a pagina individual nao tiver info
    return None


def _limpar_nome_empresa(nome, empresa_key):
    """Limpa nome removendo sufixos de empresa."""
    if not nome:
        return nome

    # Mapa de padroes por empresa
    padroes = {
        "construplan": r'\s*[-–—|]\s*(?:Construplan|CP\s*(?:Residencial|Construtora)|Construtora)\s*.*$',
        "geratriz": r'\s*[-–—|]\s*(?:Geratriz|Construtora)\s*.*$',
        "pafil": r'\s*[-–—|]\s*(?:Pafil|Construtora|Incorporadora)\s*.*$',
        "vlp3": r'\s*[-–—|]\s*(?:VLP3|Engenharia)\s*.*$',
        "dc4": r'\s*[-–—|]\s*(?:DC4|Construtora)\s*.*$',
        "credlar": r'\s*[-–—|]\s*(?:Credlar|Construtora)\s*.*$',
        "ffb": r'\s*[-–—|]\s*(?:FFB|Constru[çc][õo]es)\s*.*$',
        "makeit": r'\s*[-–—|]\s*(?:Make\.?it|Construtora|Grupo\s*Gadens)\s*.*$',
        "usinadeobras": r'\s*[-–—|]\s*(?:Usina\s*de\s*Obras|Construtora)\s*.*$',
        "brn": r'\s*[-–—|]\s*(?:BRN(?:PAR)?|Construtora|Incorporadora)\s*.*$',
    }

    padrao = padroes.get(empresa_key)
    if padrao:
        nome_limpo = re.sub(padrao, '', nome, flags=re.I).strip()
        if nome_limpo and len(nome_limpo) > 2:
            return nome_limpo

    return nome


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper F7: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "construplan":
        links = _coletar_links_construplan(config, logger)
    elif empresa_key == "pafil":
        links = _coletar_links_pafil(config, logger)
    elif empresa_key == "dc4":
        links = _coletar_links_dc4(config, logger)
    elif empresa_key == "usinadeobras":
        links = _coletar_links_usinadeobras(config, logger)
    elif empresa_key == "makeit":
        links = _coletar_links_makeit(config, logger)
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

            # === Limpeza generica de nome ===
            dados["nome"] = _limpar_nome_empresa(dados["nome"], empresa_key)

            nome = dados["nome"]

            # === Enriquecimento especifico por empresa ===

            # Extrair coordenadas do HTML
            lat, lon = extrair_coordenadas(html)
            if lat and lon:
                dados["latitude"] = lat
                dados["longitude"] = lon

            # --- Construplan: cidade da pagina, nomes com "CP Residencial" ---
            if empresa_key == "construplan":
                # Limpar prefixos adicionais
                if dados.get("nome"):
                    nome_cp = dados["nome"]
                    nome_cp = re.sub(r'^(?:Empreendimento|Residencial)\s+', '', nome_cp, flags=re.I).strip()
                    if nome_cp and len(nome_cp) > 2:
                        dados["nome"] = nome_cp
                # Extrair cidade da pagina
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidades_cp = [
                        ("Ribeirão Preto", "SP"), ("São Paulo", "SP"),
                        ("Cotia", "SP"), ("Jundiaí", "SP"),
                        ("Barueri", "SP"), ("Campinas", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_cp:
                        if re.search(re.escape(cidade_nome), text_tmp, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                # Precos altos -> nao MCMV (Construplan tem alto padrao tambem)
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0

            # --- Geratriz: Sorocaba/SP ---
            if empresa_key == "geratriz":
                if not dados.get("cidade"):
                    soup_ger = BeautifulSoup(html, "html.parser")
                    text_ger = soup_ger.get_text(separator="\n", strip=True)
                    cidades_ger = [
                        ("Sorocaba", "SP"), ("Votorantim", "SP"),
                        ("Itu", "SP"), ("Salto de Pirapora", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_ger:
                        if re.search(re.escape(cidade_nome), text_ger, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    # Fallback: Sorocaba (sede da Geratriz)
                    if not dados.get("cidade"):
                        dados["cidade"] = "Sorocaba"
                        dados["estado"] = "SP"

            # --- Pafil: Interior SP (Ribeirao Preto area) ---
            if empresa_key == "pafil":
                if not dados.get("cidade"):
                    soup_paf = BeautifulSoup(html, "html.parser")
                    text_paf = soup_paf.get_text(separator="\n", strip=True)
                    cidades_pafil = [
                        ("Ribeirão Preto", "SP"), ("Jardinópolis", "SP"),
                        ("Brodowski", "SP"), ("Bonfim Paulista", "SP"),
                        ("Cravinhos", "SP"), ("Sertãozinho", "SP"),
                        ("Franca", "SP"), ("Barretos", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_pafil:
                        if re.search(re.escape(cidade_nome), text_paf, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                # Pafil tem misto: residencial MCMV e comercial alto padrao
                # Comercial nao e MCMV
                nome_lower = (dados.get("nome") or "").lower()
                if any(t in nome_lower for t in ["comercial", "empresarial", "office", "corporate"]):
                    dados["prog_mcmv"] = 0

            # --- VLP3: Maringa/PR ---
            if empresa_key == "vlp3":
                if not dados.get("cidade"):
                    soup_vlp = BeautifulSoup(html, "html.parser")
                    text_vlp = soup_vlp.get_text(separator="\n", strip=True)
                    cidades_vlp = [
                        ("Maringá", "PR"), ("Londrina", "PR"),
                        ("Sarandi", "PR"), ("Paiçandu", "PR"),
                    ]
                    for cidade_nome, estado_val in cidades_vlp:
                        if re.search(re.escape(cidade_nome), text_vlp, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        dados["cidade"] = "Maringá"
                        dados["estado"] = "PR"

            # --- DC4: RJ, cidade da pagina ---
            if empresa_key == "dc4":
                if not dados.get("cidade"):
                    soup_dc4 = BeautifulSoup(html, "html.parser")
                    text_dc4 = soup_dc4.get_text(separator="\n", strip=True)
                    cidades_dc4 = [
                        ("Campos dos Goytacazes", "RJ"), ("Campo Grande", "RJ"),
                        ("Rio de Janeiro", "RJ"), ("Niterói", "RJ"),
                        ("Nova Iguaçu", "RJ"), ("Duque de Caxias", "RJ"),
                        ("São Gonçalo", "RJ"), ("Belford Roxo", "RJ"),
                    ]
                    for cidade_nome, estado_val in cidades_dc4:
                        if re.search(re.escape(cidade_nome), text_dc4, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        dados["cidade"] = "Rio de Janeiro"
                        dados["estado"] = "RJ"
                # Campo Grande e bairro do RJ, nao cidade
                if dados.get("cidade") == "Campo Grande":
                    dados["cidade"] = "Rio de Janeiro"
                    dados["bairro"] = "Campo Grande"

            # --- FFB: Aracaju/SE ---
            if empresa_key == "ffb":
                if not dados.get("cidade"):
                    soup_ffb = BeautifulSoup(html, "html.parser")
                    text_ffb = soup_ffb.get_text(separator="\n", strip=True)
                    if re.search(r'Aracaju', text_ffb, re.I):
                        dados["cidade"] = "Aracaju"
                        dados["estado"] = "SE"
                    elif re.search(r'Sergipe', text_ffb, re.I):
                        dados["estado"] = "SE"
                # FFB faz empreendimentos residenciais de medio padrao
                # Dumont Design parece alto padrao
                nome_lower = (dados.get("nome") or "").lower()
                if "design" in nome_lower or "dumont" in nome_lower:
                    dados["prog_mcmv"] = 0

            # --- Make.it: Curitiba/PR, site JS-only ---
            if empresa_key == "makeit":
                # Site JS-only: todas as paginas retornam o mesmo <title> generico
                # Usar slug formatado como nome do empreendimento
                nome_generico = (dados.get("nome") or "").lower()
                if "garantia de qualidade" in nome_generico or "gadecons" in nome_generico or not dados.get("nome") or len(dados.get("nome", "")) < 3:
                    # Mapear slugs para nomes reais (capitalize corretamente)
                    nomes_makeit = {
                        "essence": "Essence",
                        "lume": "Lume",
                        "arbo": "Arbo",
                        "uniq-570": "Uniq 570",
                        "make-it-bravo": "Bravo",
                        "tuo": "Tuo!",
                        "la-vita": "La Vita!",
                        "figo": "Figo!",
                        "azzurra": "Azzurra!",
                        "evviva": "Evviva",
                        "allegra": "Allegra",
                        "felicita": "Felicita",
                    }
                    dados["nome"] = nomes_makeit.get(slug, slug.replace("-", " ").title())
                if not dados.get("cidade"):
                    soup_mk = BeautifulSoup(html, "html.parser")
                    text_mk = soup_mk.get_text(separator="\n", strip=True)
                    cidades_mk = [
                        ("Curitiba", "PR"), ("Pinhais", "PR"),
                        ("São José dos Pinhais", "PR"),
                        ("Colombo", "PR"), ("Araucária", "PR"),
                    ]
                    for cidade_nome, estado_val in cidades_mk:
                        if re.search(re.escape(cidade_nome), text_mk, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        dados["cidade"] = "Curitiba"
                        dados["estado"] = "PR"

            # --- Usina de Obras: PE, site JS-only ---
            if empresa_key == "usinadeobras":
                # Site JS-only: provavelmente so teremos o <title>
                if not dados.get("cidade"):
                    # Tentar inferir de nomes conhecidos
                    nome_lower = (dados.get("nome") or "").lower()
                    if "paulista" in nome_lower:
                        dados["cidade"] = "Paulista"
                        dados["estado"] = "PE"
                    elif "capibaribe" in nome_lower:
                        dados["cidade"] = "Recife"
                        dados["estado"] = "PE"
                    elif "camaragibe" in nome_lower:
                        dados["cidade"] = "Camaragibe"
                        dados["estado"] = "PE"
                    elif "várzea" in nome_lower or "varzea" in nome_lower:
                        dados["cidade"] = "Recife"
                        dados["estado"] = "PE"
                        dados["bairro"] = "Várzea"
                    else:
                        dados["estado"] = "PE"

            # --- BRN: Interior SP, MCMV ---
            if empresa_key == "brn":
                if not dados.get("cidade"):
                    soup_brn = BeautifulSoup(html, "html.parser")
                    text_brn = soup_brn.get_text(separator="\n", strip=True)
                    cidades_brn = [
                        ("Ribeirão Preto", "SP"), ("Araraquara", "SP"),
                        ("São Carlos", "SP"), ("Franca", "SP"),
                        ("Bauru", "SP"), ("Marília", "SP"),
                        ("Presidente Prudente", "SP"), ("São José do Rio Preto", "SP"),
                        ("Piracicaba", "SP"), ("Limeira", "SP"),
                        ("Campinas", "SP"), ("Sorocaba", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_brn:
                        if re.search(re.escape(cidade_nome), text_brn, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default
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
    parser = argparse.ArgumentParser(description="Scraper Batch F7 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: geratriz, pafil, todas)")
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
        print("\nEmpresas configuradas (Batch F7):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTAS:")
        print(f"  - BRN (brn.com.br): site retorna 403, coleta pode falhar")
        print(f"  - Credlar (credlarconstrutora.com.br): site com timeout, coleta pode falhar")
        print(f"  - Usina de Obras (usinadeobras.com.br): site JS-only, coleta limitada")
        print(f"\nUso: python scrapers/generico_novas_empresas_f7.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f7.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F7")
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

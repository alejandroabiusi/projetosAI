"""
Scraper Batch F12 — 12 Novas Incorporadoras (ultimas do grupo facil)
=====================================================================
Scraper generico para 12 novas incorporadoras identificadas no Batch F12.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    flamac, gremp3, tear, esdras, vital, maclucer, mondeo, pramorar,
    dimelo, realmarka, zuma, ceg

    Flamac: JS-only (site nao renderiza sem JS)
    Mondeo: 403 Forbidden
    Pramorar: JS-only (SPA, conteudo nao acessivel via requests)

Uso:
    python scrapers/generico_novas_empresas_f12.py --empresa gremp3
    python scrapers/generico_novas_empresas_f12.py --empresa todas
    python scrapers/generico_novas_empresas_f12.py --listar
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
    logger = logging.getLogger(f"scraper.f12.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f12_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F12
# ============================================================
EMPRESAS = {
    "gremp3": {
        "nome_banco": "Gremp3",
        "base_url": "https://gremp3.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": False,  # title eh generico ("Gremp3 | Projeto e Construcao")
        "urls_listagem": [
            "https://gremp3.com.br/empreendimentos",
        ],
        "padrao_link": r"gremp3\.com\.br/detalhes/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h3, h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "tear": {
        "nome_banco": "Tear",
        "base_url": "https://www.tearincorporadora.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.tearincorporadora.com.br/nossos-projetos/",
        ],
        "padrao_link": r"tearincorporadora\.com\.br/projeto/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "esdras": {
        "nome_banco": "Esdras",
        "base_url": "https://esdrasconstrutora.com.br",
        "estado_default": "SP",
        "cidade_default": "São José dos Campos",
        "nome_from_title": True,
        "urls_listagem": [
            "https://esdrasconstrutora.com.br/empreendimentos/",
            "https://esdrasconstrutora.com.br/lancamentos/",
            "https://esdrasconstrutora.com.br/pre-lancamentos/",
        ],
        "padrao_link": r"esdrasconstrutora\.com\.br/(?:pre-lancamentos/)?([\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vital": {
        "nome_banco": "Vital",
        "base_url": "https://construtoravital.com",
        "estado_default": "MG",
        "cidade_default": "Poços de Caldas",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construtoravital.com/imoveis/",
        ],
        "padrao_link": r"construtoravital\.com/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "maclucer": {
        "nome_banco": "Mac Lucer",
        "base_url": "https://maclucer.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://maclucer.com.br/",
            "https://maclucer.com.br/empreendimentos/",
            "https://maclucer.com.br/empreendimentos/lancamentos/",
            "https://maclucer.com.br/empreendimentos/obras-iniciadas/",
            "https://maclucer.com.br/empreendimentos/prontos-para-morar/",
        ],
        "padrao_link": r"maclucer\.com\.br/empreendimentos/([\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "dimelo": {
        "nome_banco": "Dimelo",
        "base_url": "https://dimeloconstrutora.com.br",
        "estado_default": "PE",
        "nome_from_title": True,
        "urls_listagem": [
            "https://dimeloconstrutora.com.br/todos-os-imoveis/",
            "https://dimeloconstrutora.com.br/destaque/lancamento/",
            "https://dimeloconstrutora.com.br/destaque/em-obras/",
            "https://dimeloconstrutora.com.br/destaque/entregue/",
        ],
        "padrao_link": r"dimeloconstrutora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|flats?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bflats?\b|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "realmarka": {
        "nome_banco": "Realmarka",
        "base_url": "https://realmarka.com.br",
        "estado_default": "PR",
        "nome_from_title": True,
        "urls_listagem": [
            "https://realmarka.com.br/buscar-empreendimentos/",
            "https://realmarka.com.br/buscar-empreendimentos/?status=lancamento",
            "https://realmarka.com.br/buscar-empreendimentos/?status=em-construcao",
            "https://realmarka.com.br/buscar-empreendimentos/?status=entregue",
        ],
        "padrao_link": r"realmarka\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Travessa)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "zuma": {
        "nome_banco": "Zuma",
        "base_url": "https://zuma.eng.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://zuma.eng.br/empreendimentos/",
            "https://zuma.eng.br/status/breves-lancamentos/",
            "https://zuma.eng.br/status/lancamentos/",
            "https://zuma.eng.br/status/em-construcao/",
            "https://zuma.eng.br/status/obras-realizadas/",
        ],
        "padrao_link": r"zuma\.eng\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "ceg": {
        "nome_banco": "CEG",
        "base_url": "https://cegmais.com.br",
        "estado_default": "SC",
        "cidade_default": "Itajaí",
        "nome_from_title": True,
        "urls_listagem": [],  # JS-rendered, coleta via URLs hardcoded
        "padrao_link": r"cegmais\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^–\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}

# Mac Lucer: links hardcoded pois listagem eh JS-only
MACLUCER_URLS = {
    "manawa": "https://maclucer.com.br/empreendimentos/manawa/",
    "vinea": "https://maclucer.com.br/empreendimentos/vinea/",
    "soleggiato": "https://maclucer.com.br/empreendimentos/soleggiato/",
    "maita-residencial": "https://maclucer.com.br/empreendimentos/maita-residencial/",
    "singolare": "https://maclucer.com.br/empreendimentos/singolare/",
    "avela-vila-residencial": "https://maclucer.com.br/empreendimentos/avela-vila-residencial/",
    "evo-residence": "https://maclucer.com.br/empreendimentos/evo-residence/",
    "nexus-residence": "https://maclucer.com.br/empreendimentos/nexus-residence/",
    "vallis-residencial": "https://maclucer.com.br/empreendimentos/vallis-residencial/",
    "residencial-bellacqua": "https://maclucer.com.br/empreendimentos/residencial-bellacqua/",
    "altissimi-residencial": "https://maclucer.com.br/empreendimentos/altissimi-residencial/",
    "vista-bella-cajamar": "https://maclucer.com.br/empreendimentos/vista-bella-cajamar/",
    "residencial-mutton": "https://maclucer.com.br/empreendimentos/residencial-mutton/",
    "residencial-aurora-gardens": "https://maclucer.com.br/empreendimentos/residencial-aurora-gardens/",
    "pecan-town-country": "https://maclucer.com.br/empreendimentos/pecan-town-country/",
    "residencial-olivio-boa": "https://maclucer.com.br/empreendimentos/residencial-olivio-boa/",
}

# CEG: links hardcoded pois listagem eh JS-only
CEG_URLS = {
    "upper": "https://cegmais.com.br/empreendimentos/upper/",
    "hub-45": "https://cegmais.com.br/empreendimentos/hub-45/",
    "sense": "https://cegmais.com.br/empreendimentos/sense/",
    "atmos-sky": "https://cegmais.com.br/empreendimentos/atmos-sky/",
    "reserva-242": "https://cegmais.com.br/empreendimentos/reserva-242/",
    "rua-tijucas": "https://cegmais.com.br/empreendimentos/rua-tijucas/",
    "atmos-blue": "https://cegmais.com.br/empreendimentos/atmos-blue/",
    "atmos-time": "https://cegmais.com.br/empreendimentos/atmos-time/",
    "atmos-beach": "https://cegmais.com.br/empreendimentos/atmos-beach/",
    "atmos-home": "https://cegmais.com.br/empreendimentos/atmos-home/",
    "euro-park": "https://cegmais.com.br/empreendimentos/euro-park/",
}

# Empresas que falharam na verificacao inicial
EMPRESAS_FALHAS = {
    "flamac": "JS-only (site WIX/JS, conteudo nao renderiza sem navegador)",
    "mondeo": "403 Forbidden (servidor bloqueia requests)",
    "pramorar": "JS-only (SPA, conteudo nao acessivel via requests)",
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_gremp3(config, logger):
    """Coleta links da Gremp3 que usa URLs relativas sem barra inicial (detalhes/slug)."""
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
            # Gremp3 usa hrefs relativos como "detalhes/tucuruvi"
            if href.startswith("detalhes/"):
                slug = href.replace("detalhes/", "").strip("/")
                if slug and slug not in links:
                    full_url = f"{base}/{href.rstrip('/')}"
                    links[slug] = full_url
            elif "gremp3.com.br/detalhes/" in href:
                match = re.search(r"gremp3\.com\.br/detalhes/([\w-]+)", href)
                if match:
                    slug = match.group(1)
                    url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                    if slug not in links:
                        links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Gremp3: {len(links)}")
    return links


def _coletar_links_esdras(config, logger):
    """Coleta links da Esdras que tem URLs heterogeneas (raiz e /pre-lancamentos/)."""
    links = {}
    base = config["base_url"]

    # Paginas do site que NAO sao empreendimentos
    paginas_site = {
        "empreendimentos", "lancamentos", "pre-lancamentos",
        "contato", "sobre", "blog", "politica-de-privacidade",
        "trabalhe-conosco", "a-esdras", "quem-somos", "fale-conosco",
        "entre-em-contato", "acesse-o-sac", "sac", "wp-login",
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

            # Filtrar URLs irrelevantes
            skip_patterns = [
                "#", "javascript:", "mailto:", "tel:",
                "/wp-content", "/wp-admin", "/page/", "/feed/",
                "/contato", "/sobre", "/blog", "/categoria",
            ]
            if any(p in href.lower() for p in skip_patterns):
                continue

            # Deve ser do dominio esdras
            if "esdrasconstrutora.com.br" not in href:
                continue

            # Extrair slug
            url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
            # /pre-lancamentos/slug ou /slug (direto na raiz)
            match_pre = re.search(r"esdrasconstrutora\.com\.br/pre-lancamentos/([\w-]+)$", url_limpa)
            match_raiz = re.search(r"esdrasconstrutora\.com\.br/([\w-]+)$", url_limpa)

            slug = None
            if match_pre:
                slug = match_pre.group(1)
            elif match_raiz:
                candidate = match_raiz.group(1)
                if candidate.lower() not in paginas_site:
                    slug = candidate

            if slug and slug not in links and slug.lower() not in paginas_site:
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Esdras: {len(links)}")
    return links


def _coletar_links_maclucer(config, logger):
    """Coleta links da Mac Lucer filtrando sub-categorias."""
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

            if "maclucer.com.br/empreendimentos/" not in href:
                continue

            url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
            match = re.search(r"maclucer\.com\.br/empreendimentos/([\w-]+)$", url_limpa)
            if match:
                slug = match.group(1)
                # Filtrar categorias de status (nao sao empreendimentos)
                if slug.lower() in ["lancamentos", "obras-iniciadas", "prontos-para-morar", "entregues"]:
                    continue
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Mac Lucer: {len(links)}")
    return links


def _coletar_links_realmarka(config, logger):
    """Coleta links da Realmarka de multiplas paginas de status."""
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

            if "realmarka.com.br/empreendimento/" not in href:
                continue

            url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
            match = re.search(r"realmarka\.com\.br/empreendimento/([\w-]+)$", url_limpa)
            if match:
                slug = match.group(1)
                if slug not in links:
                    links[slug] = url_limpa

        # Checar se ha paginacao
        for page_num in range(2, 5):
            sep = "&" if "?" in list_url else "?"
            page_url = f"{list_url}{sep}pagina={page_num}"
            html_page = fetch_html(page_url, logger)
            if not html_page:
                break
            soup_page = BeautifulSoup(html_page, "html.parser")
            found_new = False
            for a in soup_page.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("/"):
                    href = base + href
                if "realmarka.com.br/empreendimento/" not in href:
                    continue
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                match = re.search(r"realmarka\.com\.br/empreendimento/([\w-]+)$", url_limpa)
                if match:
                    slug = match.group(1)
                    if slug not in links:
                        links[slug] = url_limpa
                        found_new = True
            if not found_new:
                break
            time.sleep(DELAY)

        time.sleep(DELAY)

    logger.info(f"Total de links Realmarka: {len(links)}")
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
    logger.info(f"Iniciando scraper F12: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "gremp3":
        links = _coletar_links_gremp3(config, logger)
    elif empresa_key == "esdras":
        links = _coletar_links_esdras(config, logger)
    elif empresa_key == "maclucer":
        links = dict(MACLUCER_URLS)
        logger.info(f"Usando URLs hardcoded para Mac Lucer: {len(links)} empreendimentos")
    elif empresa_key == "realmarka":
        links = _coletar_links_realmarka(config, logger)
    elif empresa_key == "ceg":
        links = dict(CEG_URLS)
        logger.info(f"Usando URLs hardcoded para CEG: {len(links)} empreendimentos")
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

            # Gremp3: nome vem do h3 (title eh generico); limpar e extrair cidade
            if empresa_key == "gremp3":
                # Se o nome extraido eh generico, buscar h3 diretamente
                nome_lower = (dados.get("nome") or "").lower()
                if any(g in nome_lower for g in ["projeto e construção", "projeto e construcao", "gremp3", "para morar"]):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    h3 = soup_tmp.find("h3")
                    if h3:
                        nome_h3 = h3.get_text(strip=True)
                        if nome_h3 and len(nome_h3) > 1 and len(nome_h3) < 80:
                            dados["nome"] = nome_h3

            # Gremp3: extrair cidade/estado do endereco na pagina
            if empresa_key == "gremp3":
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                # Padroes como "SÃO PAULO - SP" ou "ZONA NORTE - SÃO PAULO - SP"
                match_cidade = re.search(r'(?:ZONA\s+\w+\s*-\s*)?([A-ZÀ-Ú\s]+)\s*-\s*([A-Z]{2})\b', text_full)
                if match_cidade:
                    cidade_raw = match_cidade.group(1).strip().title()
                    estado_raw = match_cidade.group(2).strip().upper()
                    if len(cidade_raw) > 2 and estado_raw in [
                        "SP", "RJ", "MG", "PR", "SC", "RS", "BA", "CE", "PE", "GO", "DF",
                        "ES", "PA", "MA", "MT", "MS", "PB", "RN", "AL", "SE", "PI",
                    ]:
                        dados["cidade"] = cidade_raw.replace("Sao Paulo", "São Paulo")
                        dados["estado"] = estado_raw

            # Tear: nome pode vir com sufixo do site ou subtitulo
            if empresa_key == "tear":
                if dados.get("nome"):
                    # Limpar sufixos comuns do titulo
                    nome_limpo = re.sub(r'\s*[\|–-]\s*(Tear|Incorporadora|Bem-estar).*$', '', dados["nome"], flags=re.IGNORECASE).strip()
                    # Limpar subtitulo se concatenado (ex: "Maison | Para poucos...")
                    nome_limpo = re.sub(r'\s*[\|]\s*.*$', '', nome_limpo).strip()
                    if nome_limpo and len(nome_limpo) > 1:
                        dados["nome"] = nome_limpo

            # Esdras: validar nome (pode capturar paginas internas)
            if empresa_key == "esdras":
                nome_lower = (dados.get("nome") or "").lower()
                # Nomes invalidos que indicam paginas do site, nao empreendimentos
                nomes_invalidos = [
                    "quem somos", "entre em contato", "acesse o sac",
                    "contato", "fale conosco", "trabalhe conosco",
                    "política de privacidade", "politica de privacidade",
                    "a esdras", "esdras construtora",
                ]
                if any(inv in nome_lower for inv in nomes_invalidos):
                    logger.warning(f"  Nome invalido (pagina do site): {dados['nome']}, pulando")
                    erros += 1
                    continue

            # Esdras: extrair cidade do texto da pagina
            if empresa_key == "esdras" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Vital: limpar sufixo do titulo e extrair cidade do texto
            if empresa_key == "vital" and dados.get("nome"):
                # Remover sufixo generico do <title> da Construtora Vital
                nome_limpo = re.sub(
                    r'\s*[-–|]\s*(?:Sustentabilidade|Construtora\s*Vital|construtoravital).*$',
                    '', dados["nome"], flags=re.IGNORECASE
                ).strip()
                if nome_limpo and len(nome_limpo) > 1:
                    dados["nome"] = nome_limpo

            if empresa_key == "vital" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Mac Lucer: limpar sufixo do titulo e extrair cidade
            if empresa_key == "maclucer" and dados.get("nome"):
                nome_limpo = re.sub(
                    r'\s*[-–|]\s*(?:Mac\s*Lucer|MacLucer|Empreendimentos).*$',
                    '', dados["nome"], flags=re.IGNORECASE
                ).strip()
                if nome_limpo and len(nome_limpo) > 1:
                    dados["nome"] = nome_limpo

            if empresa_key == "maclucer" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Dimelo: limpar sufixo do titulo e extrair cidade
            if empresa_key == "dimelo" and dados.get("nome"):
                nome_limpo = re.sub(
                    r'\s*[-–|]\s*(?:Apresenta[çc][ãa]o\s*de\s*Projetos|Dimelo|Construtora).*$',
                    '', dados["nome"], flags=re.IGNORECASE
                ).strip()
                # Tambem limpar caractere especial unicode dash
                nome_limpo = re.sub(r'\s*[–—]\s*.*Projetos.*$', '', nome_limpo, flags=re.IGNORECASE).strip()
                if nome_limpo and len(nome_limpo) > 1:
                    dados["nome"] = nome_limpo

            # Dimelo: extrair cidade do texto (atua em PE, praias)
            if empresa_key == "dimelo" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]
                # Dimelo atua em Porto de Galinhas/Tamandare (Ipojuca/Tamandare - PE)
                if not dados.get("cidade"):
                    if "porto de galinhas" in text_full.lower():
                        dados["cidade"] = "Ipojuca"
                        dados["estado"] = "PE"
                    elif "tamandaré" in text_full.lower() or "tamandare" in text_full.lower():
                        dados["cidade"] = "Tamandaré"
                        dados["estado"] = "PE"

            # Realmarka: extrair cidade do endereco na pagina (atua em Curitiba/regiao)
            if empresa_key == "realmarka" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]

            # Zuma: extrair cidade do texto (Campinas e regiao)
            if empresa_key == "zuma" and not dados.get("cidade"):
                soup_tmp = BeautifulSoup(html, "html.parser")
                text_full = soup_tmp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_full)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]
                # Fallback: checar texto da pagina para cidades conhecidas
                if not dados.get("cidade"):
                    text_lower = text_full.lower()
                    if "campinas" in text_lower:
                        dados["cidade"] = "Campinas"
                        dados["estado"] = "SP"
                    elif "piracicaba" in text_lower:
                        dados["cidade"] = "Piracicaba"
                        dados["estado"] = "SP"
                    elif "sumaré" in text_lower or "sumare" in text_lower:
                        dados["cidade"] = "Sumaré"
                        dados["estado"] = "SP"

            # CEG: extrair cidade do texto (atua em Itajai/SC) e limpar nome
            if empresa_key == "ceg":
                if dados.get("nome"):
                    nome_limpo = re.sub(
                        r'\s*[-–|]\s*(?:CEG|Incorporadora|cegmais).*$',
                        '', dados["nome"], flags=re.IGNORECASE
                    ).strip()
                    if nome_limpo and len(nome_limpo) > 1:
                        dados["nome"] = nome_limpo
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_full = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_full)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default
            if "prog_mcmv" not in dados or dados.get("prog_mcmv") == 0:
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # CEG nao e MCMV (medio/alto padrao em Itajai)
            if empresa_key == "ceg":
                dados["prog_mcmv"] = 0

            # Realmarka nao e MCMV (medio/alto padrao em Curitiba)
            if empresa_key == "realmarka":
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
    parser = argparse.ArgumentParser(description="Scraper Batch F12 - 12 novas incorporadoras (ultimas faceis)")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: gremp3, zuma, todas)")
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
        print("\nEmpresas configuradas (Batch F12):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nEmpresas com falha (nao disponiveis):")
        for key, motivo in EMPRESAS_FALHAS.items():
            print(f"  {key:15s} -> {motivo}")
        print(f"\nUso: python scrapers/generico_novas_empresas_f12.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f12.py --empresa todas")
        return

    if not args.empresa:
        parser.print_help()
        return

    if args.empresa.lower() == "todas":
        empresas = list(EMPRESAS.keys())
    else:
        empresa_key = args.empresa.lower()
        if empresa_key in EMPRESAS_FALHAS:
            print(f"Empresa '{empresa_key}' nao disponivel: {EMPRESAS_FALHAS[empresa_key]}")
            return
        if empresa_key not in EMPRESAS:
            print(f"Empresa '{empresa_key}' nao encontrada. Disponiveis:")
            for key in EMPRESAS:
                print(f"  {key}")
            print(f"\nNao disponiveis:")
            for key, motivo in EMPRESAS_FALHAS.items():
                print(f"  {key:15s} -> {motivo}")
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
        print("  RESUMO GERAL — Batch F12")
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

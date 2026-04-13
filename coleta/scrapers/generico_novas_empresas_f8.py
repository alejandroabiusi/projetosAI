"""
Scraper Batch F8 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F8.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    azure, prodomo, minerva, vvc, perfecta, imangai, florenca
    (Iplano descartada: imobiliaria, nao incorporadora)
    (ATR descartada: site Next.js JS-only, sem dados SSR)
    (New Home descartada: dominio newhomeconstrutora.com.br nao resolve DNS)

Uso:
    python scrapers/generico_novas_empresas_f8.py --empresa azure
    python scrapers/generico_novas_empresas_f8.py --empresa todas
    python scrapers/generico_novas_empresas_f8.py --listar
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
    logger = logging.getLogger(f"scraper.f8.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f8_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F8
# ============================================================
EMPRESAS = {
    "azure": {
        "nome_banco": "Azure",
        "base_url": "https://azureincorporadora.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://azureincorporadora.com.br/",
            "https://azureincorporadora.com.br/empreendimentos",
        ],
        "padrao_link": r"azureincorporadora\.com\.br/([\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Alameda)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "prodomo": {
        "nome_banco": "Pro Domo",
        "base_url": "https://prodomoconstrutora.com.br",
        "estado_default": "MG",
        "cidade_default": "Belo Horizonte",
        "nome_from_title": True,
        "urls_listagem": [
            "https://prodomoconstrutora.com.br/empreendimentos/",
            "https://prodomoconstrutora.com.br/",
        ],
        "padrao_link": r"prodomoconstrutora\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*residenciais?|unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*(?:vagas?\s*(?:de\s*)?(?:auto|garagem|estacionamento))"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "minerva": {
        "nome_banco": "Minerva",
        "base_url": "https://construtoraminerva.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construtoraminerva.com.br/",
            "https://construtoraminerva.com.br/empreendimentos/",
        ],
        "padrao_link": r"construtoraminerva\.com\.br/empreendimentos/(?!page)([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vvc": {
        "nome_banco": "VVC",
        "base_url": "https://vvcconstrutora.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        # VVC organiza por status: /lancamentos/, /em-obras/, /realizados/
        "urls_listagem": [
            "https://vvcconstrutora.com.br/",
            "https://vvcconstrutora.com.br/lancamentos",
            "https://vvcconstrutora.com.br/em-obras",
            "https://vvcconstrutora.com.br/realizados",
        ],
        "padrao_link": r"vvcconstrutora\.com\.br/(?:lancamentos|em-obras|realizados)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "perfecta": {
        "nome_banco": "Perfecta",
        "base_url": "https://perfectaeng.com.br",
        "estado_default": "PE",
        "nome_from_title": True,
        "urls_listagem": [
            "https://perfectaeng.com.br/",
        ],
        "padrao_link": r"perfectaeng\.com\.br/([\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Praia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bduplex\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "imangai": {
        "nome_banco": "Imangai",
        "base_url": "https://imangai.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        # Imangai usa URLs detail_{slug}.php
        # Coleta customizada via _coletar_links_imangai
        "urls_listagem": [
            "https://imangai.com.br/",
        ],
        "padrao_link": r"imangai\.com\.br/detail_([\w]+)\.php",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*(?:totais?)?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "florenca": {
        "nome_banco": "Florença",
        "base_url": "https://www.florencaincorporadora.com.br",
        "nome_from_title": True,
        # Florenca tem apenas 1 empreendimento ativo no site
        "urls_listagem": [
            "https://www.florencaincorporadora.com.br/",
        ],
        "padrao_link": r"florencaincorporadora\.com\.br/([\w-]+)/?$",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Monsenhor)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_azure(config, logger):
    """Coleta links da Azure filtrando paginas institucionais."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]

    # Paginas que NAO sao empreendimentos
    paginas_excluir = {
        "empreendimentos", "sobre", "contato", "a-azure", "institucional",
        "blog", "noticias", "politica-de-privacidade", "fale-conosco",
        "trabalhe-conosco", "wp-content", "wp-admin", "wp-login",
        "feed", "xmlrpc", "home", "privacy-policy", "invista",
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

            if "azureincorporadora.com.br" not in href:
                continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                if slug.lower() in paginas_excluir:
                    continue
                # Ignorar links com multiplos segmentos (paginas internas)
                if "/" in slug:
                    continue
                url_limpa = f"{base}/{slug}"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Azure: {len(links)}")
    return links


def _coletar_links_vvc(config, logger):
    """Coleta links da VVC.

    VVC nao tem links individuais para cada empreendimento nas paginas de listagem.
    A maioria dos empreendimentos e exibida inline nas paginas de categoria.
    Apenas Brisas do Japi tem pagina propria (/brisas-do-japi).
    Empreendimentos realizados nao tem paginas individuais.
    """
    links = {}
    base = config["base_url"]

    # Brisas do Japi e o unico com pagina propria
    links["brisas-do-japi"] = f"{base}/brisas-do-japi"

    logger.info(f"Total de links VVC: {len(links)}")
    logger.info("NOTA: VVC nao tem paginas individuais para empreendimentos realizados")
    return links


def _coletar_links_perfecta(config, logger):
    """Coleta links da Perfecta filtrando paginas institucionais."""
    links = {}
    base = config["base_url"]

    paginas_excluir = {
        "sobre", "contato", "a-perfecta", "institucional",
        "blog", "noticias", "politica-de-privacidade", "fale-conosco",
        "trabalhe-conosco", "nossos-empreendimentos", "empreendimentos",
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

            if "perfectaeng.com.br" not in href:
                continue

            match = re.search(r'perfectaeng\.com\.br/([\w-]+)/?$', href)
            if match:
                slug = match.group(1)
                if slug.lower() in paginas_excluir:
                    continue
                url_limpa = f"{base}/{slug}/"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Perfecta: {len(links)}")
    return links


def _coletar_links_imangai(config, logger):
    """Coleta links da Imangai (paginas detail_{slug}.php)."""
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

            # Links relativos: detail_marau.php
            match = re.search(r'detail_([\w]+)\.php', href)
            if match:
                slug = match.group(1)
                url_limpa = f"{base}/detail_{slug}.php"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Imangai: {len(links)}")
    return links


def _coletar_links_florenca(config, logger):
    """Coleta links da Florenca filtrando paginas institucionais."""
    links = {}
    base = config["base_url"]

    paginas_excluir = {
        "sobre", "contato", "institucional", "blog", "noticias",
        "politica-de-privacidade", "fale-conosco", "trabalhe-conosco",
        "empreendimentos", "privacy-policy",
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

            if "florencaincorporadora.com.br" not in href:
                continue

            match = re.search(r'florencaincorporadora\.com\.br/([\w-]+)/?$', href)
            if match:
                slug = match.group(1)
                if slug.lower() in paginas_excluir:
                    continue
                url_limpa = f"{base}/{slug}"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Florenca: {len(links)}")
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

def _limpar_nome_empresa(nome, empresa_key):
    """Limpa nome removendo sufixos de empresa."""
    if not nome:
        return nome

    # Mapa de padroes por empresa
    padroes = {
        "azure": r'\s*[-–—|]\s*(?:Azure\s*Inc\.?|Azure\s*Incorporadora|HOME|EMPREENDIMENTOS)\s*.*$',
        "prodomo": r'\s*[-–—|]\s*(?:Pro\s*Domo|Construtora)\s*.*$',
        "minerva": r'\s*[-–—|]\s*(?:Construtora\s*Minerva|Minerva|Sonhos\s*existem)\s*.*$',
        "vvc": r'\s*[-–—|]\s*(?:VVC|Construtora)\s*.*$',
        "perfecta": r'\s*[-–—|]\s*(?:Perfecta|Engenharia)\s*.*$',
        "imangai": r'\s*[-–—|]\s*(?:Imangai|Empreendimentos)\s*.*$',
        "florenca": r'\s*[-–—|]\s*(?:Floren[çc]a|Incorporadora)\s*.*$',
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
    logger.info(f"Iniciando scraper F8: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "azure":
        links = _coletar_links_azure(config, logger)
    elif empresa_key == "vvc":
        links = _coletar_links_vvc(config, logger)
    elif empresa_key == "perfecta":
        links = _coletar_links_perfecta(config, logger)
    elif empresa_key == "imangai":
        links = _coletar_links_imangai(config, logger)
    elif empresa_key == "florenca":
        links = _coletar_links_florenca(config, logger)
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

            # --- Azure: SP medio/alto padrao ---
            if empresa_key == "azure":
                # Azure e medio/alto padrao em SP — nao MCMV
                dados["prog_mcmv"] = 0
                # Azure atua exclusivamente em SP capital
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                # Tentar extrair bairro da pagina
                soup_az = BeautifulSoup(html, "html.parser")
                text_az = soup_az.get_text(separator="\n", strip=True)
                bairros_sp = [
                    "Perdizes", "Vila Mariana", "Vila Clementino", "Paulista",
                    "Consolação", "Pinheiros", "Brooklin", "Moema",
                    "Itaim Bibi", "Jardins", "Higienópolis", "Liberdade",
                    "Bela Vista", "Butantã", "Mooca",
                ]
                for bairro in bairros_sp:
                    if re.search(re.escape(bairro), text_az, re.I):
                        dados["bairro"] = bairro
                        break

            # --- Pro Domo: BH/MG, MCMV ---
            if empresa_key == "prodomo":
                if not dados.get("cidade"):
                    soup_pd = BeautifulSoup(html, "html.parser")
                    text_pd = soup_pd.get_text(separator="\n", strip=True)
                    cidades_pd = [
                        ("Belo Horizonte", "MG"), ("Contagem", "MG"),
                        ("Betim", "MG"), ("Santa Luzia", "MG"),
                        ("Sabará", "MG"), ("Ribeirão das Neves", "MG"),
                        ("Ibirité", "MG"), ("Nova Lima", "MG"),
                        ("Lagoa Santa", "MG"), ("Sete Lagoas", "MG"),
                    ]
                    for cidade_nome, estado_val in cidades_pd:
                        if re.search(re.escape(cidade_nome), text_pd, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        dados["cidade"] = "Belo Horizonte"
                        dados["estado"] = "MG"

            # --- Minerva: SP, MCMV ---
            if empresa_key == "minerva":
                # Minerva atua em SP capital (zona leste) e litoral de SP
                soup_mv = BeautifulSoup(html, "html.parser")
                text_mv = soup_mv.get_text(separator="\n", strip=True)
                # Verificar se e litoral (Caraguatatuba, Praia Grande, etc)
                cidades_litoral = [
                    ("Caraguatatuba", "SP"), ("Praia Grande", "SP"),
                    ("Santos", "SP"), ("São Vicente", "SP"),
                    ("Guarujá", "SP"), ("Mongaguá", "SP"),
                    ("Ubatuba", "SP"),
                ]
                # Detectar litoral tambem pelo nome do empreendimento
                nome_mv = (dados.get("nome") or "").lower()
                if "perequ" in nome_mv or "pescador" in nome_mv:
                    # Perequê-Açu e em Ubatuba, Pescadores pode ser litoral
                    for cidade_nome, estado_val in cidades_litoral:
                        if re.search(re.escape(cidade_nome), text_mv, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            cidade_litoral = True
                            break
                cidade_litoral = False
                for cidade_nome, estado_val in cidades_litoral:
                    if re.search(re.escape(cidade_nome), text_mv, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        cidade_litoral = True
                        break
                if not cidade_litoral:
                    # Bairros como Itaquera, Guaianases, Analia Franco sao SP capital
                    dados["cidade"] = "São Paulo"
                    dados["estado"] = "SP"
                    # Tentar extrair bairro
                    bairros_minerva = [
                        "Itaquera", "Guaianases", "Vila Carmosina",
                        "Anália Franco", "José Bonifácio", "São Mateus",
                        "Cidade Tiradentes", "Ermelino Matarazzo",
                    ]
                    for bairro in bairros_minerva:
                        if re.search(re.escape(bairro), text_mv, re.I):
                            dados["bairro"] = bairro
                            break

            # --- VVC: Jundiai/SP ---
            if empresa_key == "vvc":
                # VVC atua em Jundiai e regiao — extrair cidade do conteudo
                soup_vvc = BeautifulSoup(html, "html.parser")
                text_vvc = soup_vvc.get_text(separator="\n", strip=True)
                cidade_encontrada = False
                cidades_vvc = [
                    ("Jundiaí", "SP"), ("Várzea Paulista", "SP"),
                    ("Itupeva", "SP"), ("Itatiba", "SP"),
                    ("Campo Limpo Paulista", "SP"), ("Louveira", "SP"),
                ]
                for cidade_nome, estado_val in cidades_vvc:
                    if re.search(re.escape(cidade_nome), text_vvc, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        cidade_encontrada = True
                        break
                if not cidade_encontrada:
                    dados["cidade"] = "Jundiaí"
                    dados["estado"] = "SP"
                # Empreendimentos comerciais nao sao MCMV
                nome_lower = (dados.get("nome") or "").lower()
                if any(t in nome_lower for t in ["comercial", "empresarial", "office", "mila motos"]):
                    dados["prog_mcmv"] = 0

            # --- Perfecta: Litoral PE, alto padrao ---
            if empresa_key == "perfecta":
                # Perfecta e alto padrao litoral — nao MCMV
                dados["prog_mcmv"] = 0
                if not dados.get("cidade"):
                    soup_pf = BeautifulSoup(html, "html.parser")
                    text_pf = soup_pf.get_text(separator="\n", strip=True)
                    # Perfecta atua no litoral de PE
                    locais_perfecta = [
                        ("Recife", "PE"), ("Porto de Galinhas", "PE"),
                        ("Muro Alto", "PE"), ("Carneiros", "PE"),
                        ("Ipojuca", "PE"), ("Tamandaré", "PE"),
                        ("Boa Viagem", "PE"),
                    ]
                    for local_nome, estado_val in locais_perfecta:
                        if re.search(re.escape(local_nome), text_pf, re.I):
                            if local_nome in ("Porto de Galinhas", "Muro Alto"):
                                dados["cidade"] = "Ipojuca"
                            elif local_nome == "Carneiros":
                                dados["cidade"] = "Tamandaré"
                            elif local_nome == "Boa Viagem":
                                dados["cidade"] = "Recife"
                            else:
                                dados["cidade"] = local_nome
                            dados["estado"] = estado_val
                            break

            # --- Imangai: SP, MCMV ---
            if empresa_key == "imangai":
                if not dados.get("cidade"):
                    soup_im = BeautifulSoup(html, "html.parser")
                    text_im = soup_im.get_text(separator="\n", strip=True)
                    # Imangai atua em SP capital (zona leste)
                    cidades_imangai = [
                        ("São Paulo", "SP"), ("Guarulhos", "SP"),
                        ("Itaquera", "SP"), ("Mauá", "SP"),
                    ]
                    for cidade_nome, estado_val in cidades_imangai:
                        if re.search(re.escape(cidade_nome), text_im, re.I):
                            # Itaquera e bairro de SP, nao cidade
                            if cidade_nome == "Itaquera":
                                dados["cidade"] = "São Paulo"
                                dados["bairro"] = "Itaquera"
                            else:
                                dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        dados["cidade"] = "São Paulo"
                        dados["estado"] = "SP"
                # Limpar nomes com formato do title: "Residencial X - 1 e 2 quartos em Y | Imangai"
                if dados.get("nome"):
                    nome_im = dados["nome"]
                    nome_im = re.sub(r'\s*[-–—|]\s*\d+\s*(?:e\s*\d+\s*)?quartos?.*$', '', nome_im, flags=re.I).strip()
                    nome_im = re.sub(r'\s*[-–—|]\s*Imangai.*$', '', nome_im, flags=re.I).strip()
                    # Remover sufixos como " - 3 quartos" que ficam no nome
                    nome_im = re.sub(r'\s*-\s*\d+\s*quartos?\b.*$', '', nome_im, flags=re.I).strip()
                    if nome_im and len(nome_im) > 2:
                        dados["nome"] = nome_im

            # --- Florenca: poucos empreendimentos ---
            if empresa_key == "florenca":
                if not dados.get("cidade"):
                    soup_fl = BeautifulSoup(html, "html.parser")
                    text_fl = soup_fl.get_text(separator="\n", strip=True)
                    cidades_florenca = [
                        ("Águas de Lindóia", "SP"), ("Lindóia", "SP"),
                        ("Serra Negra", "SP"), ("Porto Alegre", "RS"),
                        ("Canoas", "RS"), ("Cachoeirinha", "RS"),
                        ("Gravataí", "RS"), ("Novo Hamburgo", "RS"),
                    ]
                    for cidade_nome, estado_val in cidades_florenca:
                        if re.search(re.escape(cidade_nome), text_fl, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    # Inferir cidade do nome do empreendimento
                    if not dados.get("cidade"):
                        nome_fl = (dados.get("nome") or "").lower()
                        if "lindoia" in nome_fl or "lind" in nome_fl:
                            dados["cidade"] = "Águas de Lindóia"
                            dados["estado"] = "SP"

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default (exceto Azure e Perfecta ja marcados acima)
            if "prog_mcmv" not in dados or dados.get("prog_mcmv") == 0:
                # Nao sobrescrever se ja definido como 0 por empresa
                if empresa_key not in ("azure", "perfecta"):
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
    parser = argparse.ArgumentParser(description="Scraper Batch F8 - 7 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: azure, prodomo, todas)")
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
        print("\nEmpresas configuradas (Batch F8):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTAS:")
        print(f"  - Iplano (iplano.com.br): descartada — imobiliaria, nao incorporadora")
        print(f"  - ATR (atrincorporadora.com.br): descartada — site Next.js JS-only, sem dados SSR")
        print(f"  - New Home (newhomeconstrutora.com.br): descartada — dominio nao resolve DNS")
        print(f"\nUso: python scrapers/generico_novas_empresas_f8.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f8.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F8")
        print("=" * 60)
        total_novos = 0
        total_erros = 0
        for key, r in resultados.items():
            if r:
                status = "OK" if r["erros"] >= 0 else "FALHA"
                print(f"  {EMPRESAS[key]['nome_banco']:20s} +{r['novos']} novos, {r['erros']} erros [{status}]")
                total_novos += r["novos"]
                total_erros += max(r["erros"], 0)
        print(f"\n  TOTAL: +{total_novos} novos, {total_erros} erros")
        print("=" * 60)


if __name__ == "__main__":
    main()

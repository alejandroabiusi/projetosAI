"""
Scraper Batch F9 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F9.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    abiatar, diamond, reitzfeld, dmc, lumiar, habras, pejota
    (Porto5 descartada: porto5.com.br retorna 403 Forbidden)
    (Ola Casa Nova descartada: digaola.com.br retorna 403 Forbidden)
    (CRWanderley descartada: wanderleyconstrucoes.com.br e Vue.js SPA, sem dados SSR)

Uso:
    python scrapers/generico_novas_empresas_f9.py --empresa abiatar
    python scrapers/generico_novas_empresas_f9.py --empresa todas
    python scrapers/generico_novas_empresas_f9.py --listar
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
    logger = logging.getLogger(f"scraper.f9.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f9_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F9
# ============================================================
EMPRESAS = {
    "abiatar": {
        "nome_banco": "Abiatar",
        "base_url": "https://abiatar.com",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://abiatar.com/empreendimentos/",
        ],
        "padrao_link": r"abiatar\.com/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "diamond": {
        "nome_banco": "Diamond",
        "base_url": "https://construtoradiamond.com.br",
        "estado_default": "RS",
        "cidade_default": "Lajeado",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construtoradiamond.com.br/empreendimentos",
        ],
        "padrao_link": r"construtoradiamond\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|lojas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bduplex\b|\bcobertura\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "reitzfeld": {
        "nome_banco": "Reitzfeld",
        "base_url": "https://reitzfeld.com.br",
        "estado_default": "SP",
        "nome_from_title": False,  # Title e slogan generico "Valores que constroem historias"
        # Reitzfeld: links na homepage (destaques) e em portfolio.html
        # URLs individuais sao /{slug}.html
        "urls_listagem": [
            "https://reitzfeld.com.br/",
            "https://reitzfeld.com.br/portfolio.html",
        ],
        "padrao_link": r"reitzfeld\.com\.br/([\w-]+)\.html",
        "excluir_links": ["index", "portfolio", "a-empresa", "fale-conosco", "imoveis",
                          "trabalhe-conosco", "politica-privacidade", "politica-de-privacidade",
                          "reitzfeld", "contato", "obras_em_andamento", "sobre", "obras"],
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .titulo-empreendimento", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Al\.|Alameda)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "dmc": {
        "nome_banco": "DMC",
        "base_url": "https://dmcconstrutora.com",
        "estado_default": "MG",
        "cidade_default": "Patos de Minas",
        "nome_from_title": True,
        "urls_listagem": [
            "https://dmcconstrutora.com/Empreendimentos",
        ],
        "padrao_link": r"dmcconstrutora\.com/Empreendimentos-Interna/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3.titulo", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*(?:vagas?\s*(?:de\s*)?(?:garagem|estacionamento))"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lumiar": {
        "nome_banco": "Lumiar",
        "base_url": "https://construtoralumiar.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        # Lumiar usa paginas .html diretamente
        "urls_listagem": [
            "https://construtoralumiar.com.br/",
            "https://construtoralumiar.com.br/imoveis.html",
        ],
        "padrao_link": r"construtoralumiar\.com\.br/([\w-]+)\.html",
        "excluir_links": ["index", "imoveis", "a-empresa", "fale-conosco",
                          "trabalhe-conosco", "financiamento", "mcmv", "fgts",
                          "simulador", "politica-de-privacidade", "simulacao"],
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .titulo-imovel", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "habras": {
        "nome_banco": "Habras",
        "base_url": "https://habrasconstrutora.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        # Habras: Elementor/WP site. Homepage mistura empreendimentos com blog/utilitarios.
        # Usamos coleta customizada que filtra agressivamente.
        "urls_listagem": [
            "https://habrasconstrutora.com.br/",
        ],
        "padrao_link": r"habrasconstrutora\.com\.br/([\w-]+)/?$",
        # Lista ampla de paginas que NAO sao empreendimentos
        "excluir_links": ["wp-content", "wp-admin", "wp-login", "wp-json",
                          "feed", "xmlrpc", "wp-includes", "category",
                          "politica-de-privacidade", "contato", "sobre",
                          "trabalhe-conosco", "area-do-cliente", "quem-somos",
                          "imoveis", "portfolio", "blog", "fale-conosco",
                          "unidades-his", "comments", "home", "inicio",
                          # Blog posts / utilitarios
                          "minha-casa-minha-vida-ferraz", "apartamento-planta-ferraz",
                          "subsidio-habitacional", "amortizacao-financiamento",
                          "consorcio-e-financiamento", "juros-de-obra",
                          "aprovado-no-financiamento",
                          "financiamento-imobiliario-antes-comprar",
                          "como-usar-fgts-imovel", "renda-financiamento",
                          "o-que-e-itbi", "documentos-financiamento",
                          "entrada-minima-apartamento",
                          "investir-apartamento-na-planta",
                          "indice-nacional-de-custo-de-construcao-o-que-e",
                          "financiamento-imobiliario-para-autonomos"],
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "pejota": {
        "nome_banco": "Pejota",
        "base_url": "https://pejotaempreendimentos.com.br",
        "estado_default": "BA",
        "cidade_default": "Salvador",
        "nome_from_title": True,
        "urls_listagem": [
            "https://pejotaempreendimentos.com.br/",
            "https://pejotaempreendimentos.com.br/empreendimentos-pejota/",
        ],
        "padrao_link": r"pejotaempreendimentos\.com\.br/empreendimentos/([\w-]+)",
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
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_reitzfeld(config, logger):
    """Coleta links da Reitzfeld da homepage e portfolio.html."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]
    excluir = config.get("excluir_links", [])

    for url_listagem in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_listagem}")
        html = fetch_html(url_listagem, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                if not href.startswith("#") and href.endswith(".html"):
                    href = base + "/" + href
                else:
                    continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                if slug in excluir:
                    continue
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Reitzfeld: {len(links)}")
    return links


def _coletar_links_lumiar(config, logger):
    """Coleta links da Lumiar de imoveis.html e homepage."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]
    excluir = config.get("excluir_links", [])

    for url_listagem in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_listagem}")
        html = fetch_html(url_listagem, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                if not href.startswith("#") and href.endswith(".html"):
                    href = base + "/" + href
                else:
                    continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                if slug in excluir:
                    continue
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Lumiar: {len(links)}")
    return links


def _coletar_links_habras(config, logger):
    """Coleta links da Habras da homepage (Elementor carousel).

    Filtra agressivamente para excluir blog posts e paginas utilitarias.
    Blog posts tipicamente tem slugs com palavras como 'financiamento', 'como-', 'o-que-', etc.
    """
    links = {}
    base = config["base_url"]
    excluir = set(config.get("excluir_links", []))

    # Padroes de slug que indicam blog post / pagina utilitaria
    blog_patterns = [
        "financiamento", "subsidio", "amortiza", "consorcio", "juros",
        "aprovado", "fgts", "renda", "itbi", "documentos", "entrada",
        "investir", "indice", "como-", "o-que-", "tudo-sobre",
        "minha-casa-minha-vida", "apartamento-planta", "comprar-",
        "vantagens-", "dicas-", "entenda-", "saiba-", "veja-",
    ]

    def _is_blog_slug(slug):
        """Detecta se slug parece ser de blog post."""
        slug_lower = slug.lower()
        for pattern in blog_patterns:
            if pattern in slug_lower:
                return True
        # Blog slugs geralmente sao longos (mais de 4 palavras)
        parts = slug.split("-")
        if len(parts) > 5:
            return True
        return False

    for url_listagem in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_listagem}")
        html = fetch_html(url_listagem, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Buscar links que apontam para paginas internas de empreendimentos
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            # Filtrar links internos do site
            if "habrasconstrutora.com.br" not in href:
                continue

            # Extrair slug
            match = re.search(r'habrasconstrutora\.com\.br/([\w-]+)/?$', href)
            if match:
                slug = match.group(1)
                if slug in excluir or slug in ("", "home", "inicio"):
                    continue
                if slug.startswith("wp-") or slug.startswith("tag-") or slug.startswith("category-"):
                    continue
                if _is_blog_slug(slug):
                    logger.debug(f"  Excluindo blog slug: {slug}")
                    continue
                url_limpa = f"{base}/{slug}/"
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links Habras: {len(links)}")
    return links


def _coletar_links_dmc(config, logger):
    """Coleta links da DMC da pagina /Empreendimentos."""
    links = {}
    base = config["base_url"]

    for url_listagem in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_listagem}")
        html = fetch_html(url_listagem, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            match = re.search(r'dmcconstrutora\.com/Empreendimentos-Interna/([\w-]+)', href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links DMC: {len(links)}")
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
    logger.info(f"Iniciando scraper F9: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "reitzfeld":
        links = _coletar_links_reitzfeld(config, logger)
    elif empresa_key == "lumiar":
        links = _coletar_links_lumiar(config, logger)
    elif empresa_key == "habras":
        links = _coletar_links_habras(config, logger)
    elif empresa_key == "dmc":
        links = _coletar_links_dmc(config, logger)
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

            # DMC: limpar prefixo EMPREENDIMENTO do nome
            if empresa_key == "dmc":
                nome_dmc = dados.get("nome", "")
                # Remover prefixo "EMPREENDIMENTO " se presente
                if nome_dmc.upper().startswith("EMPREENDIMENTO "):
                    nome_dmc = nome_dmc[len("EMPREENDIMENTO "):].strip()
                # Capitalizar corretamente (de CAPS para Title Case)
                if nome_dmc == nome_dmc.upper() and len(nome_dmc) > 3:
                    nome_dmc = nome_dmc.title()
                dados["nome"] = nome_dmc
                # Cidade e estado defaults: Patos de Minas, MG
                if not dados.get("cidade"):
                    # Tentar extrair da pagina
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]

            # Reitzfeld: nome do slug (titulo e slogan generico), extrair cidade
            if empresa_key == "reitzfeld":
                # Nome: h5 ou derivar do slug
                soup_tmp = BeautifulSoup(html, "html.parser")
                # Reitzfeld usa h5 para titulos
                h5 = soup_tmp.find("h5")
                nome_h5 = h5.get_text(strip=True) if h5 else ""
                # Se o nome extraido e generico ou slogan, usar slug
                nome_atual = (dados.get("nome") or "").lower()
                nomes_gen = ["valores que constroem", "reitzfeld", "novos ares", ""]
                if any(gen in nome_atual for gen in nomes_gen) or not dados.get("nome"):
                    # Mapear slug para nome correto
                    mapa_nomes_reitzfeld = {
                        "breeze-bosque-da-saude": "Breeze Bosque da Saude",
                        "bras120": "Bras 120",
                        "zenit": "Zenit Agua Verde",
                    }
                    nome_do_slug = mapa_nomes_reitzfeld.get(slug, slug.replace("-", " ").title())
                    dados["nome"] = nome_do_slug

            # Reitzfeld: extrair cidade do titulo ou texto da pagina
            if empresa_key == "reitzfeld":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                    # Tentar extrair cidade do titulo
                    title_tag = soup_tmp.find("title")
                    if title_tag and not dados.get("cidade"):
                        title_text = title_tag.get_text(strip=True)
                        # Padroes como "Projeto - Bairro - Cidade"
                        for cidade_candidata in ["São Paulo", "Curitiba"]:
                            if cidade_candidata.lower() in title_text.lower():
                                dados["cidade"] = cidade_candidata
                                break

            # Lumiar: limpar nome e extrair cidade
            if empresa_key == "lumiar":
                nome_lumiar = dados.get("nome", "")
                # Remover sufixo "::.."|"::.." e variantes
                nome_lumiar = re.sub(r'\s*::?\s*\.{0,3}\s*$', '', nome_lumiar).strip()
                # Remover "LUMIAR Construtora" e similares se nome e generico
                if any(x in nome_lumiar.lower() for x in ["minha casa", "simule", "financiamento"]):
                    # Pagina utilitaria, nao empreendimento - pular
                    logger.warning(f"  Pagina utilitaria Lumiar: {nome_lumiar}, pulando")
                    erros += 1
                    continue
                dados["nome"] = nome_lumiar
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                    # Mapear bairros/cidades conhecidos do slug
                    slug_lower = slug.lower()
                    mapa_cidades_lumiar = {
                        "suzano": "Suzano",
                        "mogi-das-cruzes": "Mogi das Cruzes",
                        "itaquaquecetuba": "Itaquaquecetuba",
                        "jarinu": "Jarinu",
                    }
                    for chave, cidade_nome in mapa_cidades_lumiar.items():
                        if chave in slug_lower and not dados.get("cidade"):
                            dados["cidade"] = cidade_nome
                            break
                    if not dados.get("cidade"):
                        # Default Zona Leste SP
                        dados["cidade"] = "São Paulo"

            # Habras: extrair cidade do titulo da pagina
            if empresa_key == "habras":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    title_tag = soup_tmp.find("title")
                    if title_tag:
                        title_text = title_tag.get_text(strip=True)
                        # Padroes como "Natur Atibaia - Habras" ou "Morada Lorena"
                        cidades_habras = {
                            "lorena": "Lorena",
                            "atibaia": "Atibaia",
                            "carapicuíba": "Carapicuíba",
                            "carapicuiba": "Carapicuíba",
                            "suzano": "Suzano",
                        }
                        for chave, cidade_nome in cidades_habras.items():
                            if chave in title_text.lower() or chave in slug.lower():
                                dados["cidade"] = cidade_nome
                                break
                    # Tentar da pagina
                    if not dados.get("cidade"):
                        text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                        cidade_estado = extrair_cidade_estado(text_tmp)
                        if cidade_estado:
                            dados["cidade"] = cidade_estado[0]
                            dados["estado"] = cidade_estado[1]

            # Abiatar: extrair cidade do texto da pagina
            if empresa_key == "abiatar":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                    else:
                        # Abiatar atua em SP e Tabao da Serra
                        cidades_abiatar = {
                            "taboao": "Taboão da Serra",
                            "taboas": "Taboão da Serra",
                        }
                        for chave, cidade_nome in cidades_abiatar.items():
                            if chave in text_tmp.lower():
                                dados["cidade"] = cidade_nome
                                break

            # Pejota: extrair cidade/estado do endereco
            if empresa_key == "pejota":
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                    # Buscar "Salvador - BA" no texto
                    match_salvador = re.search(r'Salvador\s*[-–]\s*BA', text_tmp)
                    if match_salvador and not dados.get("cidade"):
                        dados["cidade"] = "Salvador"
                        dados["estado"] = "BA"

            # Diamond: alto padrao, nao MCMV
            if empresa_key == "diamond":
                dados["prog_mcmv"] = 0
                if not dados.get("cidade"):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    text_tmp = soup_tmp.get_text(separator="\n", strip=True)
                    cidade_estado = extrair_cidade_estado(text_tmp)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # Limpar nomes genericos
            nome_lower = (dados.get("nome") or "").lower()
            nomes_genericos = ["construtora", "incorporadora", "home", "inicio",
                               "empreendimentos", "página", "pagina"]
            if any(gen == nome_lower.strip() for gen in nomes_genericos):
                # Tentar h2 como fallback
                soup_tmp = BeautifulSoup(html, "html.parser")
                h2 = soup_tmp.find("h2")
                if h2:
                    nome_h2 = h2.get_text(strip=True)
                    if nome_h2 and len(nome_h2) > 2 and len(nome_h2) < 80:
                        dados["nome"] = nome_h2
                        nome = nome_h2

            # MCMV por default (exceto diamond que ja foi marcado acima)
            if empresa_key != "diamond" and ("prog_mcmv" not in dados or dados.get("prog_mcmv") == 0):
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1

            # Reitzfeld: nao e MCMV (medio/alto padrao em SP)
            if empresa_key == "reitzfeld":
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
    parser = argparse.ArgumentParser(description="Scraper Batch F9 - 7 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: abiatar, diamond, todas)")
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
        print("\nEmpresas configuradas (Batch F9):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTA: Porto5 (porto5.com.br) descartada — 403 Forbidden")
        print(f"NOTA: Ola Casa Nova (digaola.com.br) descartada — 403 Forbidden")
        print(f"NOTA: CRWanderley (wanderleyconstrucoes.com.br) descartada — Vue.js SPA, sem dados SSR")
        print(f"\nUso: python scrapers/generico_novas_empresas_f9.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f9.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F9")
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

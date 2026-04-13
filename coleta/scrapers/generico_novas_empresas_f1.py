"""
Scraper Batch F1 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F1.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    lyx, holos, pride, longitude, maskan, realiza, adn, estacao1, prestes
    (Bicalho descartada: site WIX JS-only, impossivel scraping via requests)

Uso:
    python scrapers/generico_novas_empresas_f1.py --empresa lyx
    python scrapers/generico_novas_empresas_f1.py --empresa todas
    python scrapers/generico_novas_empresas_f1.py --listar
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
    logger = logging.getLogger(f"scraper.f1.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f1_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F1
# ============================================================
EMPRESAS = {
    "lyx": {
        "nome_banco": "Lyx",
        "base_url": "https://www.lyx.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.lyx.com.br/empreendimentos/",
        ],
        "padrao_link": r"lyx\.com\.br/empreendimento/([\w-]+)",
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

    "holos": {
        "nome_banco": "Holos",
        "base_url": "https://holosconstrutora.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://holosconstrutora.com.br/produtos/",
            "https://holosconstrutora.com.br/produtos/page/2/",
        ],
        "padrao_link": r"holosconstrutora\.com\.br/produtos/((?:mood|zoom)[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "pride": {
        "nome_banco": "Pride",
        "base_url": "https://www.construtorapride.com.br",
        "estado_default": "PR",
        "nome_from_title": True,
        # Pride usa WP REST API para listar slugs + Next.js para paginas individuais
        "sitemap_url": "https://construtorapride.com.br/sitemap-0.xml",
        "wp_api_url": "https://wp.construtorapride.com.br/wp-json/wp/v2/empreendimentos?per_page=100",
        "urls_listagem": [
            "https://www.construtorapride.com.br/empreendimentos",
        ],
        "padrao_link": r"construtorapride\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b|\bcoliving\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "longitude": {
        "nome_banco": "Longitude",
        "base_url": "https://www.longitude.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.longitude.com.br/imoveis",
        ],
        "padrao_link": r"longitude\.com\.br/imoveis/[\w-]+/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b|\bmultidorm\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "maskan": {
        "nome_banco": "Maskan",
        "base_url": "https://maskan.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "urls_listagem": [
            "https://maskan.com.br/",
            "https://maskan.com.br/produtos/",
        ],
        "padrao_link": r"maskan\.com\.br/produtos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "realiza": {
        "nome_banco": "Realiza",
        "base_url": "https://realizaconstrutora.com.br",
        # Realiza usa Next.js com __NEXT_DATA__: coleta links via JSON paginado
        # URL individual: /imovel/{slug}
        "nome_from_title": True,
        "urls_listagem": [],  # Coleta customizada via _coletar_links_realiza
        "padrao_link": r"realizaconstrutora\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "adn": {
        "nome_banco": "ADN",
        "base_url": "https://adnconstrutora.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://adnconstrutora.com.br/",
            "https://adnconstrutora.com.br/produtos/",
        ],
        "padrao_link": r"adnconstrutora\.com\.br/produtos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia|Via)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*residenciais?|unidades?|apartamentos?|aptos?|UHs?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"[Tt]erreo\s*\+\s*(\d+)|(\d+)\s*(?:andares?|pavimentos?\s*tipos?)"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*(?:vagas?\s*(?:de\s*)?(?:auto|garagem|estacionamento|moto))"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[o��]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "estacao1": {
        "nome_banco": "Estação 1",
        "base_url": "https://estacao1construtora.com.br",
        "estado_default": "BA",
        "cidade_default": "Feira de Santana",
        "nome_from_title": True,
        "urls_listagem": [
            "https://estacao1construtora.com.br/imoveis/",
        ],
        "padrao_link": r"estacao1construtora\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "prestes": {
        "nome_banco": "Prestes",
        "base_url": "https://www.prestes.com",
        "estado_default": "PR",
        # Prestes tem <title> generico — nome vem do h1/h2 da pagina
        "urls_listagem": [
            "https://www.prestes.com/",
            "https://www.prestes.com/empreendimentos/ponta-grossa",
            "https://www.prestes.com/empreendimentos/guarapuava",
            "https://www.prestes.com/empreendimentos/curitiba",
            "https://www.prestes.com/empreendimentos/apucarana",
            "https://www.prestes.com/empreendimentos/londrina",
        ],
        "padrao_link": r"prestes\.com/empreendimentos/[\w-]+/([\w.-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# FUNCOES ESPECIAIS DE COLETA DE LINKS
# ============================================================

def _coletar_links_pride(config, logger):
    """Coleta links da Pride via WP REST API + sitemap + listagem HTML."""
    links = {}

    # 1. WP REST API (fonte principal: 42 empreendimentos)
    wp_api_url = config.get("wp_api_url")
    if wp_api_url:
        logger.info(f"Coletando links via WP REST API: {wp_api_url}")
        try:
            resp = requests.get(wp_api_url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    slug = item.get("slug", "")
                    title = item.get("title", {}).get("rendered", "")
                    # Filtrar "black week" e similares (nao sao empreendimentos)
                    if slug and "black-week" not in slug:
                        url = f"https://www.construtorapride.com.br/empreendimentos/{slug}"
                        links[slug] = url
                logger.info(f"WP API: {len(links)} empreendimentos encontrados")
        except Exception as e:
            logger.error(f"Erro na WP API: {e}")

    # 2. Complementar com sitemap e listagem HTML (via funcao padrao)
    links_html = coletar_links_empreendimentos(config, logger)
    for slug, url in links_html.items():
        if slug not in links and "black-week" not in slug and "cidades" not in slug:
            links[slug] = url

    return links


def _coletar_links_realiza(config, logger):
    """Coleta links da Realiza via __NEXT_DATA__ paginado."""
    links = {}
    base = "https://realizaconstrutora.com.br"

    for page in range(1, 15):
        url = f"{base}/imoveis?page={page}"
        logger.info(f"Coletando links de: {url}")
        html = fetch_html(url, logger)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            break

        try:
            data = json.loads(script.string)
            imoveis = data.get("props", {}).get("pageProps", {}).get("imoveisPage", {}).get("imoveis", [])
            if not imoveis:
                break

            for im in imoveis:
                slug = im.get("slug", "")
                nome = im.get("nome", "")
                if slug and slug not in links:
                    full_url = f"{base}/imovel/{slug}"
                    links[slug] = full_url
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Erro ao parsear __NEXT_DATA__: {e}")
            break

        time.sleep(DELAY)

    logger.info(f"Total de links Realiza: {len(links)}")
    return links


def _coletar_links_longitude(config, logger):
    """Coleta links da Longitude da pagina /imoveis (SSR com cards)."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]

    url = f"{base}/imoveis"
    logger.info(f"Coletando links de: {url}")
    html = fetch_html(url, logger)
    if not html:
        return links

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
            links[slug] = url_limpa

    logger.info(f"Total de links Longitude: {len(links)}")
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
# FUNCAO DE EXTRACAO DE CIDADE DA PAGINA DA PRESTES
# ============================================================

def _extrair_cidade_da_url_prestes(url):
    """Extrai cidade da URL pattern /empreendimentos/{cidade}/{slug}."""
    match = re.search(r'/empreendimentos/([\w-]+)/', url)
    if match:
        cidade_slug = match.group(1)
        # Mapear slug para nome correto
        mapa = {
            "ponta-grossa": "Ponta Grossa",
            "guarapuava": "Guarapuava",
            "curitiba": "Curitiba",
            "apucarana": "Apucarana",
            "londrina": "Londrina",
        }
        return mapa.get(cidade_slug, cidade_slug.replace("-", " ").title())
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
    logger.info(f"Iniciando scraper F1: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "pride":
        links = _coletar_links_pride(config, logger)
    elif empresa_key == "realiza":
        links = _coletar_links_realiza(config, logger)
    elif empresa_key == "longitude":
        links = _coletar_links_longitude(config, logger)
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

            # Prestes: cidade da URL + limpeza de nome
            if empresa_key == "prestes":
                cidade_url = _extrair_cidade_da_url_prestes(url)
                if cidade_url and not dados.get("cidade"):
                    dados["cidade"] = cidade_url
                # Limpar prefixo "Empreendimento " do nome
                if dados.get("nome") and dados["nome"].lower().startswith("empreendimento "):
                    dados["nome"] = dados["nome"][len("empreendimento "):].strip()
                # Se o nome ficou generico (ex: titulo do site), usar h2 ou slug
                nome_lower = (dados.get("nome") or "").lower()
                if any(gen in nome_lower for gen in ["incorporação", "incorporacao", "construtora", "prestes"]):
                    soup_tmp = BeautifulSoup(html, "html.parser")
                    h2 = soup_tmp.find("h2")
                    if h2:
                        nome_h2 = h2.get_text(strip=True)
                        if nome_h2 and len(nome_h2) > 2 and len(nome_h2) < 60:
                            dados["nome"] = nome_h2
                    if not dados.get("nome") or any(gen in dados["nome"].lower() for gen in ["incorporação", "incorporacao", "construtora"]):
                        dados["nome"] = slug.replace("-", " ").replace(".", " ").title()

            # Longitude: cidade da URL pattern /imoveis/{cidade}/...
            if empresa_key == "longitude":
                match_cidade = re.search(r'/imoveis/([\w-]+)/', url)
                if match_cidade and not dados.get("cidade"):
                    cidade_slug = match_cidade.group(1)
                    dados["cidade"] = cidade_slug.replace("-", " ").title()
                    # Correcoes comuns
                    if dados["cidade"] == "Sao Paulo":
                        dados["cidade"] = "São Paulo"
                    elif dados["cidade"] == "Sao Jose Do Rio Preto":
                        dados["cidade"] = "São José do Rio Preto"

            # ADN: cidade frequentemente no texto do card da pagina
            if empresa_key == "adn" and not dados.get("cidade"):
                # Tentar extrair da pagina
                soup_adn = BeautifulSoup(html, "html.parser")
                text_adn = soup_adn.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_adn)
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

            # Pride: nao e MCMV (medio/alto padrao em Curitiba)
            if empresa_key == "pride":
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
    parser = argparse.ArgumentParser(description="Scraper Batch F1 - 9 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: lyx, holos, todas)")
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
        print("\nEmpresas configuradas (Batch F1):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTA: Bicalho (bicalhoempreendimentos.com.br) descartada — site WIX JS-only")
        print(f"\nUso: python scrapers/generico_novas_empresas_f1.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f1.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F1")
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

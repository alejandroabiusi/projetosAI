"""
Scraper Batch F5 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F5.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    objeto, acpo, livus, maislar, engefic, livon, solum, yonder, jcr, lbx

Notas:
    - Solum: site Wix JS-only, coleta via sitemap (paginas individuais renderizadas)
    - Yonder: site Wix JS-only, coleta falha (anotado)
    - ACPO: Next.js, /empreendimentos retorna 404; coleta via homepage + fallback slugs

Uso:
    python scrapers/generico_novas_empresas_f5.py --empresa livus
    python scrapers/generico_novas_empresas_f5.py --empresa todas
    python scrapers/generico_novas_empresas_f5.py --listar
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
    logger = logging.getLogger(f"scraper.f5.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f5_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F5
# ============================================================
EMPRESAS = {
    "objeto": {
        "nome_banco": "Objeto",
        "base_url": "https://objeto.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "Cotia",
        "urls_listagem": [
            "https://objeto.com.br/empreendimentos",
            "https://objeto.com.br/",
            "https://reservalageado.com.br/imoveis-a-venda",
            "https://reservalageado.com.br/",
        ],
        # Objeto usa paginas em reservalageado.com.br e objeto.com.br
        "padrao_link": r"(?:objeto\.com\.br|reservalageado\.com\.br)/(?:empreendimentos?|imoveis?)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "acpo": {
        "nome_banco": "ACPO",
        "base_url": "https://acpo.com.br",
        "nome_from_title": True,
        "estado_default": "RS",
        "cidade_default": "Pelotas",
        "urls_listagem": [
            "https://acpo.com.br/",
            "https://acpo.com.br/empreendimentos",
            "https://acpo.com.br/oportunidades",
        ],
        "padrao_link": r"acpo\.com\.br/(?:empreendimentos?|oportunidades)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "livus": {
        "nome_banco": "Livus",
        "base_url": "https://www.livusinc.com.br",
        "nome_from_title": False,  # Title generico "Empreendimento Livus" — nome vem do h2
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://www.livusinc.com.br/",
        ],
        # Livus usa URLs na raiz: /livus-oratorio, /livus-cupece
        "padrao_link": r"livusinc\.com\.br/(livus-[\w-]+|lotes-[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "maislar": {
        "nome_banco": "Mais Lar",
        "base_url": "https://maislar.com",
        "nome_from_title": True,
        "urls_listagem": [
            "https://maislar.com/imoveis/",
            "https://maislar.com/",
        ],
        "padrao_link": r"maislar\.com/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|blocos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "engefic": {
        "nome_banco": "Engefic",
        "base_url": "https://engefic.com.br",
        "nome_from_title": True,
        "estado_default": "RJ",
        "urls_listagem": [
            "https://engefic.com.br/empreendimentos/",
            "https://engefic.com.br/",
        ],
        "padrao_link": r"engefic\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "livon": {
        "nome_banco": "Livon",
        "base_url": "https://www.livonincorporadora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://www.livonincorporadora.com.br/empreendimentos/",
            "https://www.livonincorporadora.com.br/",
        ],
        "padrao_link": r"livonincorporadora\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*(?:habitacionais?)?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "solum": {
        "nome_banco": "Solum",
        "base_url": "https://www.solumconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "RS",
        "cidade_default": "Pelotas",
        # Solum e Wix JS-only; coleta via sitemap
        "sitemap_url": "https://www.solumconstrutora.com.br/pages-sitemap.xml",
        "urls_listagem": [],  # Coleta customizada via _coletar_links_solum
        "padrao_link": r"solumconstrutora\.com\.br/([\w%-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "yonder": {
        "nome_banco": "Yonder",
        "base_url": "https://www.yonderincorporadora.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        # Yonder e Wix JS-only; tentativa via listagem + sitemap
        "urls_listagem": [
            "https://www.yonderincorporadora.com.br/",
        ],
        "padrao_link": r"yonderincorporadora\.com\.br/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "jcr": {
        "nome_banco": "JCR Lidera",
        "base_url": "https://jcrincorporadora.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "Sorocaba",
        # JCR usa WP com paginas na raiz: /grand-ville/, /zafira-pre-lancamento/
        "sitemap_url": "https://jcrincorporadora.com.br/page-sitemap1.xml",
        "urls_listagem": [
            "https://jcrincorporadora.com.br/",
            "https://jcrincorporadora.com.br/home/",
        ],
        "padrao_link": r"jcrincorporadora\.com\.br/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "lbx": {
        "nome_banco": "LBX",
        "base_url": "https://www.lbx.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [
            "https://www.lbx.com.br/empreendimentos",
            "https://www.lbx.com.br/",
        ],
        "padrao_link": r"lbx\.com\.br/empreendimento/([\w-]+-\d+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
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

def _coletar_links_objeto(config, logger):
    """Coleta links da Objeto — empreendimentos em objeto.com.br e reservalageado.com.br."""
    links = {}

    # 1. Coletar da pagina de empreendimentos e homepage
    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Links relativos
            if href.startswith("/"):
                if "reservalageado" in url_list:
                    href = "https://reservalageado.com.br" + href
                else:
                    href = config["base_url"] + href
            elif not href.startswith("http"):
                continue

            match = re.search(config["padrao_link"], href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                if slug not in links:
                    links[slug] = url_limpa

        time.sleep(DELAY)

    # Empreendimentos conhecidos (fallback)
    slugs_conhecidos = {
        "residencial-dos-sabias": "https://objeto.com.br/empreendimentos",
        "residencial-dos-tucanos": "https://objeto.com.br/empreendimentos",
        "residencial-das-maritacas": "https://objeto.com.br/empreendimentos",
        "residencial-dos-portinari": "https://objeto.com.br/empreendimentos",
        "residencial-beija-flor": "https://objeto.com.br/empreendimentos",
    }
    # Objeto nao tem paginas individuais — empreendimentos estao na pagina principal
    # Vamos usar a pagina de empreendimentos como fonte para todos
    if not links:
        for slug, url in slugs_conhecidos.items():
            links[slug] = url

    logger.info(f"Total de links Objeto: {len(links)}")
    return links


def _coletar_links_acpo(config, logger):
    """Coleta links da ACPO — site Next.js, /empreendimentos pode ser JS-only."""
    links = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Tentar __NEXT_DATA__
        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            try:
                data = json.loads(script.string)
                # Tentar extrair empreendimentos do JSON
                props = data.get("props", {}).get("pageProps", {})
                emps = props.get("empreendimentos", props.get("properties", []))
                if isinstance(emps, list):
                    for emp in emps:
                        slug = emp.get("slug", emp.get("id", ""))
                        if slug:
                            links[str(slug)] = f"{base}/empreendimentos/{slug}"
            except (json.JSONDecodeError, KeyError):
                pass

        # Tambem buscar links normais
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

    # Empreendimentos conhecidos (do depoimentos da homepage)
    slugs_conhecidos = {
        "moov-residencial": f"{base}/empreendimentos/moov-residencial",
        "connect-duque": f"{base}/empreendimentos/connect-duque",
    }
    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    logger.info(f"Total de links ACPO: {len(links)}")
    return links


def _coletar_links_livus(config, logger):
    """Coleta links da Livus com fases baseadas nas secoes da homepage."""
    links = {}
    fases_da_listagem = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Mapear secoes de fase
        current_fase = None
        for el in soup.find_all(["h2", "h3", "h4", "a", "div"]):
            if el.name in ("h2", "h3", "h4"):
                txt = el.get_text(strip=True).lower()
                if "lançamento" in txt and "futuro" not in txt and "breve" not in txt:
                    current_fase = "Lançamento"
                elif "em obra" in txt or "construção" in txt:
                    current_fase = "Em Construção"
                elif "futuro" in txt or "breve" in txt:
                    current_fase = "Breve Lançamento"
                elif "concluído" in txt or "entregue" in txt or "pronto" in txt:
                    current_fase = "Pronto"
            elif el.name == "a" and el.get("href"):
                href = el["href"].strip()
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
                    if current_fase and slug not in fases_da_listagem:
                        fases_da_listagem[slug] = current_fase

        time.sleep(DELAY)

    # Slugs conhecidos (fallback)
    slugs_conhecidos = [
        "livus-oratorio", "livus-cupece", "livus-jaragua",
        "livus-mooca", "livus-vila-sonia", "livus-santa-catarina", "livus-sao-mateus",
        "livus-vila-guarani", "livus-itaquera", "livus-patriarca-ii",
        "livus-itaim-paulista", "livus-patriarca", "livus-vilaema",
        "livus-parque-ecologico", "livus-barena", "lotes-vilas-do-rio",
    ]
    for slug in slugs_conhecidos:
        if slug not in links:
            links[slug] = f"{base}/{slug}"

    config["_fases_listagem"] = fases_da_listagem
    logger.info(f"Total de links Livus: {len(links)}")
    return links


def _coletar_links_solum(config, logger):
    """Coleta links da Solum via sitemap (site Wix JS-only)."""
    links = {}
    sitemap_url = config.get("sitemap_url")

    if sitemap_url:
        logger.info(f"Coletando links via sitemap: {sitemap_url}")
        try:
            resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for loc in soup.find_all("loc"):
                    url = loc.get_text(strip=True)
                    if "solumconstrutora.com.br/" in url:
                        path = url.rstrip("/").split("solumconstrutora.com.br/")[-1]
                        # Filtrar paginas nao-empreendimento
                        paginas_excluir = {
                            "", "empreendimentos", "sac", "contact", "blog",
                            "trabalheconosco", "sobre-nos", "home2", "teste",
                            "book-online", "minhacasaminhavida", "portadeentrada",
                        }
                        # Excluir paginas de oportunidade/landing/copia
                        if (path and path not in paginas_excluir
                                and not path.startswith("oportunidade-")
                                and not path.startswith("cópia-")
                                and not path.startswith("lp-")
                                and not path.startswith("lançamento")):
                            slug = path.replace("%", "pct").replace("â", "a").replace("ã", "a")
                            links[slug] = url
        except Exception as e:
            logger.error(f"Erro ao acessar sitemap: {e}")

    logger.info(f"Total de links Solum: {len(links)}")
    return links


def _coletar_links_jcr(config, logger):
    """Coleta links da JCR via sitemap WP + listagem HTML."""
    links = {}
    base = config["base_url"]

    # 1. Sitemap WP (paginas individuais)
    sitemap_url = config.get("sitemap_url")
    if sitemap_url:
        logger.info(f"Coletando links via sitemap: {sitemap_url}")
        try:
            resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # Paginas que sao empreendimentos (filtrar genéricas)
                paginas_excluir = {
                    "trabalhe-conosco-novo", "contato-novo", "evolucao-das-obras",
                    "", "home", "a-jcr", "blog", "contato", "trabalhe-conosco",
                    "politica-da-qualidade", "politica-de-privacidade",
                    "seja-um-fornecedor", "sample-page",
                    "meeting-inscricao-confirmada",
                }
                for loc in soup.find_all("loc"):
                    url = loc.get_text(strip=True)
                    if "jcrincorporadora.com.br/" in url:
                        path = url.rstrip("/").split("jcrincorporadora.com.br/")[-1]
                        if (path and path not in paginas_excluir
                                and not path.startswith("pagina-de-obrigado")
                                and not path.startswith("meeting-")
                                and not path.startswith("promocao-")):
                            slug = path
                            links[slug] = url.rstrip("/") + "/"
        except Exception as e:
            logger.error(f"Erro ao acessar sitemap JCR: {e}")

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

            if "jcrincorporadora.com.br/" in href:
                path = href.rstrip("/").split("jcrincorporadora.com.br/")[-1]
                paginas_excluir_html = {
                    "", "#", "home", "a-jcr", "blog", "contato",
                    "trabalhe-conosco", "evolucao-das-obras",
                    "politica-da-qualidade", "politica-de-privacidade",
                }
                if (path and path not in paginas_excluir_html
                        and "#" not in path
                        and not path.startswith("wp-")
                        and not path.startswith("pagina-de-obrigado")):
                    slug = path.rstrip("/")
                    if slug not in links:
                        links[slug] = href.rstrip("/") + "/"

        time.sleep(DELAY)

    logger.info(f"Total de links JCR: {len(links)}")
    return links


def _coletar_links_lbx(config, logger):
    """Coleta links da LBX da pagina /empreendimentos."""
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

    logger.info(f"Total de links LBX: {len(links)}")
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
    logger.info(f"Iniciando scraper F5: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "objeto":
        links = _coletar_links_objeto(config, logger)
    elif empresa_key == "acpo":
        links = _coletar_links_acpo(config, logger)
    elif empresa_key == "livus":
        links = _coletar_links_livus(config, logger)
    elif empresa_key == "solum":
        links = _coletar_links_solum(config, logger)
    elif empresa_key == "jcr":
        links = _coletar_links_jcr(config, logger)
    elif empresa_key == "lbx":
        links = _coletar_links_lbx(config, logger)
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

        # Objeto: todos os empreendimentos estao na mesma pagina
        if empresa_key == "objeto" and url == "https://objeto.com.br/empreendimentos":
            # Para Objeto, o nome vem do slug pois nao ha paginas individuais
            nome_formatado = slug.replace("-", " ").title()
            # Converter para nomes reais
            nomes_objeto = {
                "residencial-dos-sabias": "Condomínio Residencial dos Sabiás",
                "residencial-dos-tucanos": "Condomínio Residencial dos Tucanos",
                "residencial-das-maritacas": "Condomínio Residencial das Maritacas",
                "residencial-dos-portinari": "Condomínio Residencial dos Portinari",
                "residencial-beija-flor": "Residencial Beija-Flor",
            }
            nome_final = nomes_objeto.get(slug, nome_formatado)

            if empreendimento_existe(nome_banco, nome_final) and not atualizar:
                logger.info(f"  Ja existe: {nome_final}")
                continue

            dados = {
                "empresa": nome_banco,
                "nome": nome_final,
                "url_fonte": "https://objeto.com.br/empreendimentos",
                "cidade": "Cotia",
                "estado": "SP",
                "bairro": "Reserva Lageado",
                "prog_mcmv": 1,
                "data_coleta": datetime.now().isoformat(),
            }
            # Fetch a pagina geral para extrair dados
            html_obj = fetch_html(url, logger)
            if html_obj:
                lat, lon = extrair_coordenadas(html_obj)
                if lat and lon:
                    dados["latitude"] = lat
                    dados["longitude"] = lon

            if empreendimento_existe(nome_banco, nome_final) and atualizar:
                atualizar_empreendimento(nome_banco, nome_final, dados)
                atualizados += 1
                logger.info(f"  Atualizado: {nome_final}")
            elif not empreendimento_existe(nome_banco, nome_final):
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {nome_final} | Cotia | MCMV")
            time.sleep(DELAY)
            continue

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

            # --- ACPO: Pelotas/RS ---
            if empresa_key == "acpo":
                soup_acpo = BeautifulSoup(html, "html.parser")
                text_acpo = soup_acpo.get_text(separator="\n", strip=True)
                cidades_acpo = [
                    ("Pelotas", "RS"), ("Rio Grande", "RS"),
                    ("Canguçu", "RS"), ("São Lourenço do Sul", "RS"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_acpo:
                    if re.search(re.escape(cidade_nome), text_acpo, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    dados["cidade"] = "Pelotas"
                    dados["estado"] = "RS"
                # Limpar nome: remover " - ACPO"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*ACPO\s*(?:Empreendimentos?)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- Livus: SP capital, MCMV ---
            if empresa_key == "livus":
                dados["estado"] = "SP"
                # Nome: title generico "Empreendimento Livus" — extrair do h2 com "Livus" no texto
                soup_liv_nome = BeautifulSoup(html, "html.parser")
                nomes_ruins_livus = ["empreendimento livus", "empreendimento", "livus inc",
                                     "livus incorporadora", "viva o novo", "esse site",
                                     "cookies", "política", "politica", "aceitar"]
                nome_encontrado = False
                for tag in ["h2", "h3", "h1"]:
                    for heading in soup_liv_nome.find_all(tag):
                        txt = heading.get_text(strip=True)
                        if not txt or len(txt) < 4 or len(txt) > 60:
                            continue
                        txt_lower = txt.lower()
                        if any(r in txt_lower for r in nomes_ruins_livus):
                            continue
                        if "livus" in txt_lower:
                            dados["nome"] = txt
                            nome_encontrado = True
                            break
                    if nome_encontrado:
                        break
                # Se nao encontrou nome com "Livus", usar slug formatado
                if not nome_encontrado:
                    # slug como "livus-cupece" -> "Livus Cupecê"
                    nome_from_slug = slug.replace("-", " ").title()
                    # Acentuar bairros conhecidos
                    acentos = {
                        "Cupece": "Cupecê", "Oratorio": "Oratório", "Jaragua": "Jaraguá",
                        "Sao Mateus": "São Mateus", "Vila Sonia": "Vila Sônia",
                        "Parque Ecologico": "Parque Ecológico",
                    }
                    for plain, accent in acentos.items():
                        nome_from_slug = nome_from_slug.replace(plain, accent)
                    dados["nome"] = nome_from_slug
                # Limpar nome: remover " - Livus"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Livus\s*(?:Inc\.?|Incorporadora)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Cidade do texto ou bairro do slug
                soup_liv = BeautifulSoup(html, "html.parser")
                # Remover nav/footer antes de buscar cidade
                for tag in soup_liv.find_all(["nav", "footer", "header"]):
                    tag.decompose()
                text_liv = soup_liv.get_text(separator="\n", strip=True)
                # Extrair bairro do nome/slug
                bairros_sp_livus = {
                    "oratorio": ("Vila Prudente", "São Paulo"),
                    "cupece": ("Cupecê", "São Paulo"),
                    "jaragua": ("Jaraguá", "São Paulo"),
                    "mooca": ("Mooca", "São Paulo"),
                    "vila-sonia": ("Vila Sônia", "São Paulo"),
                    "santa-catarina": ("Santa Catarina", "São Paulo"),
                    "sao-mateus": ("São Mateus", "São Paulo"),
                    "vila-guarani": ("Vila Guarani", "São Paulo"),
                    "itaquera": ("Itaquera", "São Paulo"),
                    "patriarca": ("Patriarca", "São Paulo"),
                    "itaim-paulista": ("Itaim Paulista", "São Paulo"),
                    "vilaema": ("Vila Ema", "São Paulo"),
                    "parque-ecologico": ("Parque Ecológico", "São Paulo"),
                    "barena": ("Barena", "São Paulo"),
                }
                for bairro_slug, (bairro_nome, cidade) in bairros_sp_livus.items():
                    if bairro_slug in slug.lower():
                        dados["bairro"] = bairro_nome
                        dados["cidade"] = cidade
                        break
                if not dados.get("cidade"):
                    # Tentar extrair do texto
                    cidade_estado = extrair_cidade_estado(text_liv)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]
                    else:
                        dados["cidade"] = "São Paulo"
                # Fase da listagem
                fases_list = config.get("_fases_listagem", {})
                if slug in fases_list:
                    dados["fase"] = fases_list[slug]
                # Fase do texto
                if not dados.get("fase"):
                    text_liv_full = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True).lower()
                    if "breve lançamento" in text_liv_full:
                        dados["fase"] = "Breve Lançamento"
                    elif "concluído" in text_liv_full or "entregue" in text_liv_full:
                        dados["fase"] = "Pronto"
                    elif "100% vendido" in text_liv_full or "esgotado" in text_liv_full:
                        dados["fase"] = "100% Vendido"

            # --- Mais Lar: BH/MG e cidades de MG/MT ---
            if empresa_key == "maislar":
                soup_ml = BeautifulSoup(html, "html.parser")
                text_ml = soup_ml.get_text(separator="\n", strip=True)
                # Extrair cidade do texto ou endereco
                cidades_maislar = [
                    ("Belo Horizonte", "MG"), ("Contagem", "MG"),
                    ("Betim", "MG"), ("Ribeirão das Neves", "MG"),
                    ("Santa Luzia", "MG"), ("Vespasiano", "MG"),
                    ("Cuiabá", "MT"), ("Várzea Grande", "MT"),
                    ("Sete Lagoas", "MG"), ("Lagoa Santa", "MG"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_maislar:
                    if re.search(re.escape(cidade_nome), text_ml, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    # Tentar padrao "Cidade/UF"
                    match_cid = re.search(r'([\w\s]+?)\s*/\s*([A-Z]{2})', text_ml)
                    if match_cid:
                        dados["cidade"] = match_cid.group(1).strip()
                        dados["estado"] = match_cid.group(2)
                    else:
                        dados["cidade"] = "Belo Horizonte"
                        dados["estado"] = "MG"
                # Limpar nome: remover " - Mais Lar"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Mais\s*Lar|MaisLar).*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do texto
                text_ml_lower = text_ml.lower()
                if "lançamento" in text_ml_lower and "breve" not in text_ml_lower:
                    dados["fase"] = "Lançamento"
                elif "breve lançamento" in text_ml_lower:
                    dados["fase"] = "Breve Lançamento"
                elif "em construção" in text_ml_lower or "em obras" in text_ml_lower:
                    dados["fase"] = "Em Construção"
                elif "entregue" in text_ml_lower or "pronto" in text_ml_lower:
                    dados["fase"] = "Pronto"

            # --- Engefic: RJ, bairros do RJ ---
            if empresa_key == "engefic":
                soup_eng = BeautifulSoup(html, "html.parser")
                text_eng = soup_eng.get_text(separator="\n", strip=True)
                # Cidades RJ
                cidades_engefic = [
                    ("Rio de Janeiro", "RJ"), ("Nova Iguaçu", "RJ"),
                    ("Duque de Caxias", "RJ"), ("São João de Meriti", "RJ"),
                    ("Belford Roxo", "RJ"), ("Nilópolis", "RJ"),
                    ("Mesquita", "RJ"), ("Campo Grande", "RJ"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_engefic:
                    if re.search(re.escape(cidade_nome), text_eng, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    dados["cidade"] = "Rio de Janeiro"
                    dados["estado"] = "RJ"
                # Limpar nome: remover " - Engefic"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Engefic\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do texto
                text_eng_lower = text_eng.lower()
                if "lançamento" in text_eng_lower:
                    dados["fase"] = "Lançamento"
                elif "em construção" in text_eng_lower or "em obras" in text_eng_lower:
                    dados["fase"] = "Em Construção"
                elif "entregue" in text_eng_lower or "pronto" in text_eng_lower:
                    dados["fase"] = "Pronto"

            # --- Livon: Campinas/SP e cidades do interior de SP ---
            if empresa_key == "livon":
                soup_livon = BeautifulSoup(html, "html.parser")
                text_livon = soup_livon.get_text(separator="\n", strip=True)
                cidades_livon = [
                    ("Campinas", "SP"), ("Valinhos", "SP"), ("Vinhedo", "SP"),
                    ("Jundiaí", "SP"), ("Indaiatuba", "SP"), ("Hortolândia", "SP"),
                    ("Sumaré", "SP"), ("Paulínia", "SP"), ("Americana", "SP"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_livon:
                    if re.search(re.escape(cidade_nome), text_livon, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    dados["cidade"] = "Campinas"
                    dados["estado"] = "SP"
                # Limpar nome: remover " - Livon"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Livon\s*(?:Incorporadora)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do texto
                text_livon_lower = text_livon.lower()
                if "breve lançamento" in text_livon_lower or "pré-lançamento" in text_livon_lower:
                    dados["fase"] = "Breve Lançamento"
                elif "lançamento" in text_livon_lower:
                    dados["fase"] = "Lançamento"
                elif "em construção" in text_livon_lower or "em obras" in text_livon_lower:
                    dados["fase"] = "Em Construção"
                elif "pronto para morar" in text_livon_lower:
                    dados["fase"] = "Pronto"

            # --- Solum: Pelotas/RS, site Wix ---
            if empresa_key == "solum":
                dados["estado"] = "RS"
                soup_sol = BeautifulSoup(html, "html.parser")
                text_sol = soup_sol.get_text(separator="\n", strip=True)
                cidades_solum = [
                    ("Pelotas", "RS"), ("Rio Grande", "RS"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_solum:
                    if re.search(re.escape(cidade_nome), text_sol, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    dados["cidade"] = "Pelotas"
                    dados["estado"] = "RS"
                # Nome do slug se nao foi extraido (Wix JS-only)
                if not dados.get("nome") or dados["nome"].strip() == "":
                    dados["nome"] = slug.replace("residencial", "Residencial ").replace("solar", "Solar ").replace("pct", "%").title().strip()
                # Limpar nome generico
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Solum\s*(?:Construtora)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- Yonder: SP zona leste ---
            if empresa_key == "yonder":
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                # Limpar nome: remover " - Yonder"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Yonder\s*(?:Incorporadora)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- JCR Lidera: Sorocaba/SP ---
            if empresa_key == "jcr":
                dados["cidade"] = "Sorocaba"
                dados["estado"] = "SP"
                # Limpar nome: remover " - JCR Incorporadora"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*JCR\s*(?:Incorporadora|Incorp)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Remover sufixo "pre-lancamento" do slug para nomes duplicados
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Pré\s*Lançamento\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do slug e texto
                soup_jcr = BeautifulSoup(html, "html.parser")
                text_jcr = soup_jcr.get_text(separator="\n", strip=True).lower()
                if "pre-lancamento" in slug or "pré-lançamento" in text_jcr or "pré lançamento" in text_jcr:
                    dados["fase"] = "Breve Lançamento"
                elif "lançamento" in text_jcr:
                    dados["fase"] = "Lançamento"
                elif "em construção" in text_jcr or "em obras" in text_jcr:
                    dados["fase"] = "Em Construção"
                elif "entregue" in text_jcr or "pronto" in text_jcr:
                    dados["fase"] = "Pronto"

            # --- LBX: Maringá/PR ---
            if empresa_key == "lbx":
                soup_lbx = BeautifulSoup(html, "html.parser")
                text_lbx = soup_lbx.get_text(separator="\n", strip=True)
                cidades_lbx = [
                    ("Maringá", "PR"), ("Paranavaí", "PR"), ("Londrina", "PR"),
                    ("Umuarama", "PR"), ("Campo Mourão", "PR"),
                    ("Cianorte", "PR"), ("Sarandi", "PR"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_lbx:
                    if re.search(re.escape(cidade_nome), text_lbx, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                if not found_city:
                    dados["cidade"] = "Maringá"
                    dados["estado"] = "PR"
                # Limpar nome: remover " - LBX" e sufixo numerico do slug
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*LBX\s*(?:Construtora)?\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do texto
                text_lbx_lower = text_lbx.lower()
                if "pré-lançamento" in text_lbx_lower or "pré lançamento" in text_lbx_lower:
                    dados["fase"] = "Breve Lançamento"
                elif "lançamento" in text_lbx_lower:
                    dados["fase"] = "Lançamento"
                elif "em construção" in text_lbx_lower or "em obras" in text_lbx_lower:
                    dados["fase"] = "Em Construção"
                elif "entregue" in text_lbx_lower or "pronto" in text_lbx_lower:
                    dados["fase"] = "Pronto"

            # Limpar nome generico
            nome = dados.get("nome", "")
            nome_lower = nome.lower()
            nomes_genericos = ["incorporadora", "construtora", "home", "início",
                               "empreendimentos", "imoveis", "imóveis", "página",
                               "engenharia", "nossos", "apartamentos"]
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
                    dados["nome"] = slug.replace("-", " ").replace("_", " ").title()

            # Atualizar variavel nome apos limpezas
            nome = dados["nome"]

            # MCMV por default
            if "prog_mcmv" not in dados or (dados.get("prog_mcmv") is None):
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
    parser = argparse.ArgumentParser(description="Scraper Batch F5 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: livus, lbx, todas)")
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
        print("\nEmpresas configuradas (Batch F5):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTAS:")
        print(f"  - Solum: site Wix, coleta via sitemap (pode falhar parcialmente)")
        print(f"  - Yonder: site Wix JS-only, coleta provavelmente falhara")
        print(f"\nUso: python scrapers/generico_novas_empresas_f5.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f5.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F5")
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

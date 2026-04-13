"""
Scraper Batch F2 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F2.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    brio, faj, embraplan, rottas, gpci, mp, integra, vca (falha), telesil (falha), pinheiro (falha)

Notas:
    - VCA (vcaconstrutora.com.br): certificado SSL expirado, impossivel scraping
    - Telesil (telesil.com.br): retorna 403 Forbidden
    - Pinheiro (pinheiroincorp.com.br): retorna 403 Forbidden

Uso:
    python scrapers/generico_novas_empresas_f2.py --empresa brio
    python scrapers/generico_novas_empresas_f2.py --empresa todas
    python scrapers/generico_novas_empresas_f2.py --listar
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
    logger = logging.getLogger(f"scraper.f2.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f2_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F2
# ============================================================
EMPRESAS = {
    "brio": {
        "nome_banco": "Brio",
        "base_url": "https://brioincorporadora.com.br",
        "nome_from_title": True,
        # Brio organiza por regiao/cidade — coleta via paginas regionais
        "urls_listagem": [],  # Coleta customizada via _coletar_links_brio
        "padrao_link": r"brioincorporadora\.com\.br/empreendimentos/[\w-]+/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "faj": {
        "nome_banco": "FAJ",
        "base_url": "https://fajrealiza.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://fajempreendimentos.com/nossos-empreendimentos/",
            "https://fajrealiza.com.br/",
        ],
        # Links apontam para fajrealiza.com.br/empreendimentos/{slug}/
        "padrao_link": r"fajrealiza\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "embraplan": {
        "nome_banco": "Embraplan",
        "base_url": "https://embraplan.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "urls_listagem": [
            "https://embraplan.com.br/empreendimentos/",
            "https://embraplan.com.br/apartamentos/",
            "https://embraplan.com.br/",
        ],
        "padrao_link": r"embraplan\.com\.br/apartamento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "rottas": {
        "nome_banco": "Rottas",
        "base_url": "https://rottasconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [
            "https://rottasconstrutora.com.br/rottas/",
        ],
        # Rottas usa dois padroes: /rottas/imoveis/{slug} e /rottas/meo/{slug}
        "padrao_link": r"rottasconstrutora\.com\.br/rottas/(?:imoveis|meo)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_vagas": {"method": "regex", "pattern": r"(\d+)\s*vagas?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "gpci": {
        "nome_banco": "GPCI",
        "base_url": "https://gpci.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "urls_listagem": [
            "https://gpci.com.br/imoveis/",
            "https://gpci.com.br/",
        ],
        "padrao_link": r"gpci\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|lotes?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "mp": {
        "nome_banco": "MP",
        "base_url": "https://mpincorporadora.com.br",
        "nome_from_title": True,
        "estado_default": "RJ",
        "urls_listagem": [
            "https://mpincorporadora.com.br/",
            "https://mpincorporadora.com.br/categoria/empreendimentos/",
        ],
        # MP usa URLs diretas: /nome-empreendimento/ e /lp/nome/
        "padrao_link": r"mpincorporadora\.com\.br/((?:jardim|water|monumental|exclusive|encantos)[\w-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|blocos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*(?:torres?|blocos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "integra": {
        "nome_banco": "Integra Urbano",
        "base_url": "https://integraurbano.com.br",
        # NAO usar nome_from_title: title = "X – Integra Urbano® | Desde 1999"
        # O split por "|" antes de "–" produz "Desde 1999" (bug do parser generico)
        "estado_default": "SP",
        # Integra usa query params: ?produtos={slug}
        "urls_listagem": [
            "https://integraurbano.com.br/?destaque=todos",
            "https://integraurbano.com.br/",
        ],
        "padrao_link": r"integraurbano\.com\.br/\?produtos=([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
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

def _coletar_links_brio(config, logger):
    """Coleta links da Brio varrendo paginas regionais de listagem."""
    links = {}
    base = config["base_url"]
    padrao = config["padrao_link"]

    # Paginas regionais conhecidas (homepage lista as regioes)
    paginas_regionais = [
        f"{base}/empreendimentos/ribeirao-preto/ribeirao-preto/",
        f"{base}/empreendimentos/ribeirao-preto/sertaozinho/",
        f"{base}/empreendimentos/ribeirao-preto/serrana/",
        f"{base}/empreendimentos/sorocaba/sorocaba/",
        f"{base}/empreendimentos/sorocaba/votorantim/",
        f"{base}/empreendimentos/sao-paulo/sao-paulo/",
    ]

    for url_regional in paginas_regionais:
        logger.info(f"Coletando links de: {url_regional}")
        html = fetch_html(url_regional, logger)
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

    logger.info(f"Total de links Brio: {len(links)}")
    return links


def _coletar_links_mp(config, logger):
    """Coleta links da MP Incorporadora via pagina de categorias."""
    links = {}
    base = config["base_url"]

    # Empreendimentos conhecidos pelo nome (do site)
    # Mapeados manualmente a partir da pagina /categoria/empreendimentos/
    empreendimentos_mp = [
        "encantos-do-fonseca",
        "jardim-central_3",
        "water-park-pendotiba-resort",
        "monumentalokara",
        "jardim-imperial-ii",
        "exclusive-noronha",
        "jardim-pendotiba-3",
        "jardim-pendotiba-2",
        "jardim-central2",
        "jardim-bougainville",
        "jardim-imperial",
        "jardim-pendotiba",
        "jardim-central",
        "jardim-alcantara-2",
    ]

    # Primeiro tentar coletar da pagina de categorias
    url_cat = f"{base}/categoria/empreendimentos/"
    logger.info(f"Coletando links de: {url_cat}")
    html = fetch_html(url_cat, logger)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            if "mpincorporadora.com.br/" not in href:
                continue

            # Extrair slug do link
            slug_candidate = href.replace(base + "/", "").strip("/").split("/")[0]

            # Apenas aceitar se o slug corresponde a um empreendimento conhecido
            # ou se contem "jardim", "water", "monumental", "exclusive", "encantos"
            nomes_empreendimento = ["jardim", "water", "monumental", "exclusive", "encantos",
                                    "residencial", "parque", "portal", "village"]
            if slug_candidate and any(n in slug_candidate.lower() for n in nomes_empreendimento):
                if slug_candidate not in links:
                    url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                    links[slug_candidate] = url_limpa

    # Garantir que todos os empreendimentos conhecidos estejam na lista
    for slug in empreendimentos_mp:
        if slug not in links:
            links[slug] = f"{base}/{slug}"

    logger.info(f"Total de links MP: {len(links)}")
    return links


def _coletar_links_integra(config, logger):
    """Coleta links da Integra Urbano (usa query params ?produtos=slug)."""
    links = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        # Procurar por links com ?produtos=
        matches = re.findall(r'href=["\']([^"\']*\?produtos=([\w-]+))["\']', html)
        for full_url, slug in matches:
            if slug not in links:
                if full_url.startswith("/") or full_url.startswith("?"):
                    full_url = f"{base}/?produtos={slug}"
                links[slug] = full_url

        # Tambem procurar no texto por padroes de slug
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            match = re.search(r'\?produtos=([\w-]+)', href)
            if match:
                slug = match.group(1)
                if slug not in links:
                    if not href.startswith("http"):
                        href = f"{base}/?produtos={slug}"
                    links[slug] = href

        time.sleep(DELAY)

    logger.info(f"Total de links Integra Urbano: {len(links)}")
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
# FUNCAO AUXILIAR: EXTRAIR CIDADE DA URL DA BRIO
# ============================================================

def _extrair_cidade_brio(url):
    """Extrai cidade da URL pattern /empreendimentos/{regiao}/{cidade}/{slug}."""
    match = re.search(r'/empreendimentos/([\w-]+)/([\w-]+)/', url)
    if match:
        cidade_slug = match.group(2)
        mapa = {
            "ribeirao-preto": "Ribeirão Preto",
            "sertaozinho": "Sertãozinho",
            "serrana": "Serrana",
            "sorocaba": "Sorocaba",
            "votorantim": "Votorantim",
            "sao-paulo": "São Paulo",
        }
        return mapa.get(cidade_slug, cidade_slug.replace("-", " ").title())
    return None


def _extrair_estado_brio(url):
    """Infere estado a partir da regiao na URL da Brio."""
    if "/ribeirao-preto/" in url or "/sorocaba/" in url or "/sao-paulo/" in url:
        return "SP"
    return "SP"  # Brio atua em SP


# ============================================================
# PROCESSAR EMPRESA
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper F2: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "brio":
        links = _coletar_links_brio(config, logger)
    elif empresa_key == "mp":
        links = _coletar_links_mp(config, logger)
    elif empresa_key == "integra":
        links = _coletar_links_integra(config, logger)
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

            # Brio: cidade e estado da URL + limpeza de nome
            if empresa_key == "brio":
                cidade_url = _extrair_cidade_brio(url)
                if cidade_url:
                    # Sempre usar cidade da URL (mais confiavel que texto da pagina)
                    dados["cidade"] = cidade_url
                estado_url = _extrair_estado_brio(url)
                if estado_url:
                    dados["estado"] = estado_url
                # Limpar nome: remover "\n em \n CIDADE" do title
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*\n\s*em\s*\n\s*.*$', '', dados["nome"], flags=re.S).strip()
                    # Tambem remover " em CIDADE" inline
                    nome_limpo = re.sub(r'\s+em\s+(?:Ribeirão Preto|Sertãozinho|Serrana|Sorocaba|Votorantim|São Paulo)\s*$', '', nome_limpo, flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # FAJ: extrair cidade do texto da pagina
            if empresa_key == "faj":
                soup_faj = BeautifulSoup(html, "html.parser")
                text_faj = soup_faj.get_text(separator="\n", strip=True)
                # FAJ atua em S.J. Rio Preto e Catanduva
                cidade_estado = extrair_cidade_estado(text_faj)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]
                elif not dados.get("cidade"):
                    # Buscar por padroes comuns
                    if re.search(r"Rio Preto|SJRP|São José do Rio Preto", text_faj, re.I):
                        dados["cidade"] = "São José do Rio Preto"
                        dados["estado"] = "SP"
                    elif re.search(r"Catanduva", text_faj, re.I):
                        dados["cidade"] = "Catanduva"
                        dados["estado"] = "SP"

            # Embraplan: cidade do texto (Piracicaba, Osasco, etc.)
            if empresa_key == "embraplan":
                soup_emb = BeautifulSoup(html, "html.parser")
                text_emb = soup_emb.get_text(separator="\n", strip=True)
                # Embraplan atua em Piracicaba, Osasco, Praia Grande, Guarulhos
                cidades_embraplan = [
                    ("Piracicaba", "SP"), ("Osasco", "SP"), ("Praia Grande", "SP"),
                    ("Guarulhos", "SP"), ("Barueri", "SP"), ("Campinas", "SP"),
                    ("São Paulo", "SP"), ("Carapicuíba", "SP"),
                ]
                cidade_encontrada = False
                for cidade_nome, estado_val in cidades_embraplan:
                    if re.search(re.escape(cidade_nome), text_emb, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        cidade_encontrada = True
                        break
                if not cidade_encontrada:
                    cidade_estado = extrair_cidade_estado(text_emb)
                    if cidade_estado:
                        dados["cidade"] = cidade_estado[0]
                        dados["estado"] = cidade_estado[1]

            # Rottas: limpeza de nome + cidade
            if empresa_key == "rottas":
                # Limpar nome: remover " - Apartamentos/Casas/Condomínio Clube em CIDADE"
                if dados.get("nome"):
                    nome_limpo = re.sub(
                        r'\s*[-–—]\s*(?:Apartamentos?|Casas?|Condom[ií]nio\s+Clube?|Loteamento)\s+em\s+.*$',
                        '', dados["nome"], flags=re.I
                    ).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

                # Extrair cidade do nome original (antes da limpeza) ou do texto
                soup_rot = BeautifulSoup(html, "html.parser")
                title_rot = soup_rot.find("title")
                title_text = title_rot.string.strip() if title_rot and title_rot.string else ""
                # Titulo format: "NOME - Tipo em CIDADE | Rottas"
                match_cidade = re.search(r'(?:Apartamentos?|Casas?|Condom[ií]nio)\s+(?:Clube\s+)?em\s+([\w\s]+?)(?:\s*[|–—-]\s*|\s*$)', title_text, re.I)
                if match_cidade:
                    cidade_titulo = match_cidade.group(1).strip()
                    dados["cidade"] = cidade_titulo
                    # Inferir estado
                    cidades_pr = ["Curitiba", "Ponta Grossa", "Londrina", "Maringá"]
                    cidades_sc = ["Joinville", "Jaraguá do Sul", "Florianópolis"]
                    if any(c.lower() in cidade_titulo.lower() for c in cidades_pr):
                        dados["estado"] = "PR"
                    elif any(c.lower() in cidade_titulo.lower() for c in cidades_sc):
                        dados["estado"] = "SC"

                if not dados.get("cidade"):
                    text_rot = soup_rot.get_text(separator="\n", strip=True)
                    # Buscar cidades especificas no texto
                    cidades_rottas = [
                        ("Curitiba", "PR"), ("Ponta Grossa", "PR"), ("Londrina", "PR"),
                        ("Maringá", "PR"), ("Joinville", "SC"), ("Jaraguá do Sul", "SC"),
                    ]
                    for cidade_nome, estado_val in cidades_rottas:
                        if re.search(re.escape(cidade_nome), text_rot, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            break
                    if not dados.get("cidade"):
                        cidade_estado = extrair_cidade_estado(text_rot)
                        if cidade_estado:
                            dados["cidade"] = cidade_estado[0]
                            dados["estado"] = cidade_estado[1]

            # GPCI: extrair cidade do texto
            if empresa_key == "gpci":
                soup_gp = BeautifulSoup(html, "html.parser")
                text_gp = soup_gp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_gp)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]
                elif not dados.get("cidade"):
                    # GPCI atua em Indaiatuba, Salto, S.J. Rio Preto, Pres. Prudente
                    if re.search(r"Indaiatuba", text_gp, re.I):
                        dados["cidade"] = "Indaiatuba"
                    elif re.search(r"Salto", text_gp, re.I):
                        dados["cidade"] = "Salto"
                    elif re.search(r"Rio Preto", text_gp, re.I):
                        dados["cidade"] = "São José do Rio Preto"
                    elif re.search(r"Presidente Prudente", text_gp, re.I):
                        dados["cidade"] = "Presidente Prudente"

            # MP: atua em Niteroi e Sao Goncalo/RJ
            if empresa_key == "mp":
                soup_mp = BeautifulSoup(html, "html.parser")
                text_mp = soup_mp.get_text(separator="\n", strip=True)
                cidade_estado = extrair_cidade_estado(text_mp)
                if cidade_estado:
                    dados["cidade"] = cidade_estado[0]
                    dados["estado"] = cidade_estado[1]
                elif not dados.get("cidade"):
                    if re.search(r"São Gonçalo|Sao Goncalo", text_mp, re.I):
                        dados["cidade"] = "São Gonçalo"
                        dados["estado"] = "RJ"
                    elif re.search(r"Niterói|Niteroi", text_mp, re.I):
                        dados["cidade"] = "Niterói"
                        dados["estado"] = "RJ"

            # Integra: nome do title (antes de " – ")
            if empresa_key == "integra":
                soup_int_nome = BeautifulSoup(html, "html.parser")
                title_tag = soup_int_nome.find("title")
                if title_tag and title_tag.string:
                    title_text = title_tag.string.strip()
                    # Title format: "In Perdizes – Integra Urbano® | Desde 1999"
                    for sep in [" – ", " - ", " — "]:
                        if sep in title_text:
                            nome_titulo = title_text.split(sep)[0].strip()
                            if nome_titulo and len(nome_titulo) > 2 and len(nome_titulo) < 60:
                                dados["nome"] = nome_titulo
                            break

            # Integra: extrair cidade real (nao apenas bairro)
            if empresa_key == "integra":
                soup_int = BeautifulSoup(html, "html.parser")
                text_int = soup_int.get_text(separator="\n", strip=True)
                # Mapear bairros que sao de SP vs outras cidades
                bairros_sp = ["Perdizes", "Vila Sônia", "Vila Sonia", "Jardim Monte Kemel",
                              "Freguesia do Ó", "Freguesia do O", "Butantã", "Butanta",
                              "Vila Mascote", "Ipiranga", "Mooca", "Lapa", "Pinheiros",
                              "Chácara do Jockey", "Chacara do Jockey"]
                cidade_encontrada = None
                # Primeiro checar cidades especificas
                if re.search(r"Guarulhos", text_int, re.I):
                    cidade_encontrada = "Guarulhos"
                elif re.search(r"Suzano", text_int, re.I):
                    cidade_encontrada = "Suzano"
                elif re.search(r"Vila Augusta", text_int, re.I):
                    # Vila Augusta e em Guarulhos
                    cidade_encontrada = "Guarulhos"
                else:
                    cidade_encontrada = "São Paulo"
                dados["cidade"] = cidade_encontrada
                dados["estado"] = "SP"
                # Guardar bairro se detectado
                if dados.get("cidade") == "São Paulo":
                    # O campo cidade pode ter sido preenchido com bairro pelo parser generico
                    # Mover para bairro e corrigir cidade
                    pass  # bairro ja deve ter sido extraido pelo parser CSS

            # Limpar nome generico
            nome = dados.get("nome", "")
            nome_lower = nome.lower()
            # Remover nomes que sao claramente titulo do site
            nomes_genericos = ["incorporadora", "construtora", "home", "início",
                               "empreendimentos", "imoveis", "imóveis", "página"]
            if any(gen in nome_lower for gen in nomes_genericos) and len(nome) < 40:
                # Tentar h2 ou outro heading
                soup_tmp = BeautifulSoup(html, "html.parser")
                for tag in ["h2", "h3"]:
                    heading = soup_tmp.find(tag)
                    if heading:
                        txt = heading.get_text(strip=True)
                        if txt and 2 < len(txt) < 60 and not any(g in txt.lower() for g in nomes_genericos):
                            dados["nome"] = txt
                            break
                # Se ainda generico, usar slug
                if any(gen in dados.get("nome", "").lower() for gen in nomes_genericos):
                    dados["nome"] = slug.replace("-", " ").title()

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
    parser = argparse.ArgumentParser(description="Scraper Batch F2 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: brio, gpci, todas)")
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
        print("\nEmpresas configuradas (Batch F2):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nNOTA: VCA (vcaconstrutora.com.br) descartada — certificado SSL expirado")
        print(f"NOTA: Telesil (telesil.com.br) descartada — retorna 403 Forbidden")
        print(f"NOTA: Pinheiro (pinheiroincorp.com.br) descartada — retorna 403 Forbidden")
        print(f"\nUso: python scrapers/generico_novas_empresas_f2.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f2.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F2")
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

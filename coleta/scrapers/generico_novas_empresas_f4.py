"""
Scraper Batch F4 — 10 Novas Incorporadoras
============================================
Scraper generico para 10 novas incorporadoras identificadas no Batch F4.
Reutiliza a logica core do generico_empreendimentos.py sem modifica-lo.

Empresas:
    mf7, ideale, louly, vitale, blendi, vl, riobranco, neourb, m91, ctv

Uso:
    python scrapers/generico_novas_empresas_f4.py --empresa mf7
    python scrapers/generico_novas_empresas_f4.py --empresa todas
    python scrapers/generico_novas_empresas_f4.py --listar
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
    logger = logging.getLogger(f"scraper.f4.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"f4_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# CONFIGURACAO DAS EMPRESAS — Batch F4
# ============================================================
EMPRESAS = {
    "mf7": {
        "nome_banco": "MF7",
        "base_url": "https://www.mf7.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://www.mf7.com.br/empreendimentos/",
        ],
        "padrao_link": r"mf7\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*aut[oô]nomas?|unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "ideale": {
        "nome_banco": "Ideale",
        "base_url": "https://idealeempreendimentos.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://idealeempreendimentos.com.br/",
        ],
        # Ideale usa URLs na raiz: /myid-tucuruvi-signature/ etc.
        "padrao_link": r"idealeempreendimentos\.com\.br/((?:myid|vit|vizoo)[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "louly": {
        "nome_banco": "Louly Caixe",
        "base_url": "https://loulyinc.com.br",
        "nome_from_title": False,  # Title generico "Apartamentos Louly Inc..."
        "estado_default": "GO",
        "cidade_default": "Goiânia",
        "urls_listagem": [
            "https://loulyinc.com.br/empreendimentos",
            "https://www.loulyinc.com.br/empreendimentos",
        ],
        "padrao_link": r"loulyinc\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:a\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[-aA]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vitale": {
        "nome_banco": "Vitale",
        "base_url": "https://construtoravitale.com.br",
        "nome_from_title": True,
        "estado_default": "RJ",
        "cidade_default": "Rio de Janeiro",
        "urls_listagem": [
            "https://construtoravitale.com.br/empreendimentos/",
            "https://construtoravitale.com.br/",
        ],
        "padrao_link": r"construtoravitale\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "blendi": {
        "nome_banco": "Blendi",
        "base_url": "https://www.blendi.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [
            "https://www.blendi.com.br/empreendimentos/",
        ],
        "padrao_link": r"blendi\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vl": {
        "nome_banco": "VL",
        "base_url": "https://www.vlconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "PE",
        "urls_listagem": [
            "https://www.vlconstrutora.com.br/imoveis/",
            "https://www.vlconstrutora.com.br/",
        ],
        "padrao_link": r"vlconstrutora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?[\d.]+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "riobranco": {
        "nome_banco": "Rio Branco",
        "base_url": "https://construtorariobranco.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "Salto",
        "urls_listagem": [
            "https://construtorariobranco.com.br/imoveis",
            "https://construtorariobranco.com.br/",
        ],
        # Dois padroes de URL (imoveis-a-venda tem URLs com espacos no Wix)
        "padrao_link": r"construtorariobranco\.com\.br/(?:imoveis|imoveis-a-venda)/([\w%+\s-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "neourb": {
        "nome_banco": "NeoUrb",
        "base_url": "https://neourb.com.br",
        "nome_from_title": True,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "urls_listagem": [
            "https://neourb.com.br/empreendimentos/",
            "https://neourb.com.br/",
        ],
        "padrao_link": r"neourb\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*(?:HIS|R2V)?|unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(?:[Tt]érreo\s*\+\s*)?(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "m91": {
        "nome_banco": "M91",
        "base_url": "https://m91.eng.br",
        "nome_from_title": True,
        "estado_default": "RS",
        "urls_listagem": [
            "https://m91.eng.br/",
            "https://m91.eng.br/empreendimentos",
        ],
        # M91 usa subpaginas como /caxias/belluno.html ou links externos
        "padrao_link": r"m91\.eng\.br/([\w/.-]+\.html)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "ctv": {
        "nome_banco": "CTV",
        "base_url": "https://www.ctvconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "RJ",
        "cidade_default": "Rio de Janeiro",
        "urls_listagem": [
            "https://www.ctvconstrutora.com.br/empreendimentos/",
            "https://www.ctvconstrutora.com.br/",
        ],
        # CTV usa URLs na raiz: /ctv-beat/, /sal/, /go-quintino/
        "padrao_link": r"ctvconstrutora\.com\.br/((?:ctv|sal|go|star|adele|agora)[\w-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2, h3", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?[^-\n]*"},
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

def _coletar_links_mf7(config, logger):
    """Coleta links da MF7 com status/fase dos badges da listagem."""
    links = {}
    fases_da_listagem = {}

    for url_list in config["urls_listagem"]:
        # Tentar com e sem filtros
        urls_tentar = [
            url_list,
            url_list + "?tipo=lancamento",
            url_list + "?tipo=breve-lancamento",
            url_list + "?tipo=em-obras",
            url_list + "?tipo=pronto-para-morar",
        ]
        for url in urls_tentar:
            logger.info(f"Coletando links de: {url}")
            html = fetch_html(url, logger)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            # Detectar fase da URL do filtro
            fase_filtro = None
            if "tipo=lancamento" in url:
                fase_filtro = "Lançamento"
            elif "tipo=breve-lancamento" in url:
                fase_filtro = "Breve Lançamento"
            elif "tipo=em-obras" in url:
                fase_filtro = "Em Construção"
            elif "tipo=pronto-para-morar" in url:
                fase_filtro = "Pronto"

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                match = re.search(config["padrao_link"], href)
                if match:
                    slug = match.group(1)
                    if slug in ("empreendimentos", "empreendimento"):
                        continue
                    url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                    if not url_limpa.startswith("http"):
                        url_limpa = config["base_url"] + "/" + url_limpa.lstrip("/")
                    if slug not in links:
                        links[slug] = url_limpa
                    if fase_filtro and slug not in fases_da_listagem:
                        fases_da_listagem[slug] = fase_filtro

            time.sleep(DELAY)

    logger.info(f"Total de links MF7: {len(links)}")
    # Guardar fases no config para uso posterior
    config["_fases_listagem"] = fases_da_listagem
    return links


def _coletar_links_ideale(config, logger):
    """Coleta links da Ideale da homepage (nao tem pagina /empreendimentos)."""
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

    # Slugs conhecidos (fallback)
    slugs_conhecidos = [
        "myid-vista-guilhermina-esperanca", "myid-tucuruvi-signature",
        "myid-adolfo-pinheiro", "myid-parada-inglesa-2-3",
        "myid-parada-inglesa-2", "myid-parada-inglesa",
        "vit-jundiai", "vizoo-bras",
    ]
    for slug in slugs_conhecidos:
        if slug not in links:
            links[slug] = f"{base}/{slug}/"

    logger.info(f"Total de links Ideale: {len(links)}")
    return links


def _coletar_links_ctv(config, logger):
    """Coleta links da CTV com fases baseadas nas secoes da pagina."""
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
        for el in soup.find_all(["h2", "h3", "a"]):
            if el.name in ("h2", "h3"):
                txt = el.get_text(strip=True).lower()
                if "lançamento" in txt or "lancamento" in txt:
                    current_fase = "Lançamento"
                elif "em obra" in txt:
                    current_fase = "Em Construção"
                elif "realizado" in txt or "entregue" in txt:
                    current_fase = "Pronto"
            elif el.name == "a" and el.get("href"):
                href = el["href"].strip()
                if href.startswith("/"):
                    href = base + href
                elif not href.startswith("http"):
                    continue

                if "ctvconstrutora.com.br/" in href:
                    # Extrair slug da URL (excluir paginas genéricas)
                    path = href.rstrip("/").split("ctvconstrutora.com.br/")[-1]
                    paginas_excluir = {
                        "empreendimentos", "", "#", "contato", "sobre", "a-ctv",
                        "trabalhe-conosco", "quem-somos", "saiu-na-midia",
                        "lancamentos", "obras", "realizados",
                        "politica-de-privacidade", "minha-casa-minha-vida",
                    }
                    if path and path not in paginas_excluir:
                        slug = path.rstrip("/")
                        # Excluir evolucao-de-obras pages e paginas com /
                        if ("/" not in slug and not slug.startswith("#")
                                and not slug.startswith("evolucao")):
                            url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                            if slug not in links:
                                links[slug] = url_limpa
                            if current_fase and slug not in fases_da_listagem:
                                fases_da_listagem[slug] = current_fase

        time.sleep(DELAY)

    # Slugs conhecidos das secoes "Em Obras" e "Lancamentos"
    slugs_emobras = {
        "ctv-sunny-residencial": "Em Construção",
        "ctv-beat": "Em Construção",
        "sal": "Em Construção",
        "ctvvitoria": "Em Construção",
        "ctvnobel": "Em Construção",
        "ctvmob": "Em Construção",
        "go-quintino": "Em Construção",
    }
    slugs_lancamento = {
        "ctv-agora": "Lançamento",
    }
    slugs_pronto = {
        "star-residencial": "Pronto",
        "adele-residence": "Pronto",
    }
    for slug_map in (slugs_emobras, slugs_lancamento, slugs_pronto):
        for slug, fase in slug_map.items():
            if slug not in links:
                links[slug] = f"{base}/{slug}/"
            # Hardcoded fases always override (HTML parsing can be unreliable)
            fases_da_listagem[slug] = fase

    config["_fases_listagem"] = fases_da_listagem
    logger.info(f"Total de links CTV: {len(links)}")
    return links


def _coletar_links_m91(config, logger):
    """Coleta links da M91 — poucos empreendimentos, links mistos."""
    links = {}
    base = config["base_url"]

    for url_list in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_list}")
        html = fetch_html(url_list, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        paginas_excluir_m91 = {"politica-de-privacidade", "contato", "sobre", "index"}
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Links relativos internos (.html)
            if href.endswith(".html") and not href.startswith("http"):
                slug_raw = href.replace("/", "_").replace(".html", "")
                if slug_raw not in paginas_excluir_m91 and "politica" not in slug_raw:
                    full_url = base + "/" + href.lstrip("/")
                    if slug_raw not in links:
                        links[slug_raw] = full_url
            # Links internos com padrao
            elif "m91.eng.br" in href:
                match = re.search(config["padrao_link"], href)
                if match:
                    slug_raw = match.group(1).replace("/", "_").replace(".html", "")
                    if slug_raw not in paginas_excluir_m91 and "politica" not in slug_raw:
                        url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                        if slug_raw not in links:
                            links[slug_raw] = url_limpa

        time.sleep(DELAY)

    # Empreendimentos conhecidos (fallback — site tem poucos links)
    slugs_conhecidos = {
        "caxias_belluno": f"{base}/caxias/belluno.html",
    }
    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    logger.info(f"Total de links M91: {len(links)}")
    return links


def _coletar_links_riobranco(config, logger):
    """Coleta links da Rio Branco — site Wix com URLs mistas."""
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

            if "construtorariobranco.com.br/" in href:
                match = re.search(config["padrao_link"], href)
                if match:
                    slug = match.group(1)
                    url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                    if slug not in links:
                        links[slug] = url_limpa

        # Tambem buscar em dados JSON/slidesData
        for script in soup.find_all("script"):
            text = script.string or ""
            # Buscar URLs em slidesData ou similar
            for m in re.finditer(r'/imoveis(?:-a-venda)?/([\w%+\s-]+)', text):
                slug = m.group(1).strip()
                if slug and slug not in links:
                    path = m.group(0)
                    links[slug] = base + path

        time.sleep(DELAY)

    # Empreendimentos conhecidos (Wix site, links dificeis de parsear)
    slugs_conhecidos = {
        "estacao-real": f"{base}/imoveis/estacao-real",
        "morada-dos-eucaliptos": f"{base}/imoveis-a-venda/morada%20dos%20eucaliptos",
        "altos-do-avecuia": f"{base}/imoveis-a-venda/altos%20do%20avecuia",
        "residencial-capital": f"{base}/imoveis-a-venda/residencial%20capital",
        "vista-parque-residencial": f"{base}/imoveis-a-venda/vista%20parque%20residencial",
    }
    for slug, url in slugs_conhecidos.items():
        if slug not in links:
            links[slug] = url

    # Limpar links com espacos — URL-encode
    links_limpos = {}
    for slug, url in links.items():
        slug_limpo = slug.replace(" ", "-").replace("%20", "-").strip()
        if slug_limpo and slug_limpo not in links_limpos:
            # URL-encode espacos no URL
            url_limpo = url.replace(" ", "%20")
            links_limpos[slug_limpo] = url_limpo
    links = links_limpos

    logger.info(f"Total de links Rio Branco: {len(links)}")
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
    logger.info(f"Iniciando scraper F4: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links (com metodos customizados por empresa)
    if empresa_key == "mf7":
        links = _coletar_links_mf7(config, logger)
    elif empresa_key == "ideale":
        links = _coletar_links_ideale(config, logger)
    elif empresa_key == "ctv":
        links = _coletar_links_ctv(config, logger)
    elif empresa_key == "m91":
        links = _coletar_links_m91(config, logger)
    elif empresa_key == "riobranco":
        links = _coletar_links_riobranco(config, logger)
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

            # --- MF7: SP capital, alto padrão (NÃO MCMV) ---
            if empresa_key == "mf7":
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                dados["prog_mcmv"] = 0  # Alto padrão
                # Limpar nome: remover " - MF7"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*MF7\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase da listagem (prioridade sobre deteccao automatica)
                fases_list = config.get("_fases_listagem", {})
                if slug in fases_list:
                    dados["fase"] = fases_list[slug]
                # Detectar fase do texto
                if not dados.get("fase"):
                    soup_mf7 = BeautifulSoup(html, "html.parser")
                    text_mf7 = soup_mf7.get_text(separator="\n", strip=True).lower()
                    if "futuro lançamento" in text_mf7 or "breve lançamento" in text_mf7:
                        dados["fase"] = "Breve Lançamento"
                    elif "pronto para morar" in text_mf7:
                        dados["fase"] = "Pronto"

            # --- Ideale: SP, studios, limpar nome ---
            if empresa_key == "ideale":
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                # Limpar nome: remover " - Ideale Empreendimentos"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Ideale\s+Empreendimentos\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Cidade de Jundiai para "vit-jundiai"
                if "jundiai" in slug.lower() or "jundiaí" in (dados.get("nome") or "").lower():
                    dados["cidade"] = "Jundiaí"
                # Fase: "Canteiro de obras" = Em Construção
                soup_id = BeautifulSoup(html, "html.parser")
                text_id = soup_id.get_text(separator="\n", strip=True).lower()
                if "canteiro de obras" in text_id:
                    dados["fase"] = "Em Construção"
                elif "breve lançamento" in text_id:
                    dados["fase"] = "Breve Lançamento"
                elif "100% vendido" in text_id or "esgotado" in text_id:
                    dados["fase"] = "100% Vendido"

            # --- Louly Caixe: Goiânia/GO, cidade/bairro da pagina ---
            if empresa_key == "louly":
                soup_lo = BeautifulSoup(html, "html.parser")
                text_lo = soup_lo.get_text(separator="\n", strip=True)
                # Cidades do Louly
                cidades_louly = [
                    ("Goiânia", "GO"), ("Aparecida de Goiânia", "GO"),
                    ("Anápolis", "GO"), ("Senador Canedo", "GO"),
                    ("Trindade", "GO"),
                ]
                for cidade_nome, estado_val in cidades_louly:
                    if re.search(re.escape(cidade_nome), text_lo, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    # Tentar padrao "Bairro, Cidade-GO"
                    match_cid = re.search(r'([\w\s]+?)\s*[-–]\s*GO', text_lo)
                    if match_cid:
                        dados["cidade"] = match_cid.group(1).strip()
                        dados["estado"] = "GO"
                    else:
                        dados["cidade"] = "Goiânia"
                        dados["estado"] = "GO"
                # Bairro do texto (Parque Amazônia, Serrinha, etc.)
                match_bairro = re.search(r'(?:Bairro|Setor|Parque|Jardim)\s+([\w\s]+?)(?:\s*[-–,]|\n)', text_lo)
                if match_bairro and not dados.get("bairro"):
                    dados["bairro"] = match_bairro.group(0).strip().rstrip(",-–")
                # Fase
                if re.search(r'100%\s*vendido', text_lo, re.I):
                    dados["fase"] = "100% Vendido"
                elif re.search(r'entregue', text_lo, re.I):
                    dados["fase"] = "Pronto"
                # Nome do h1/h2 (title e generico)
                for tag in ["h1", "h2"]:
                    heading = soup_lo.find(tag)
                    if heading:
                        txt = heading.get_text(strip=True)
                        if txt and len(txt) > 3 and "louly" not in txt.lower() and "apartamento" not in txt.lower():
                            dados["nome"] = txt
                            break

            # --- Vitale: RJ, bairro do texto ---
            if empresa_key == "vitale":
                dados["cidade"] = "Rio de Janeiro"
                dados["estado"] = "RJ"
                # Limpar nome: remover " - Construtora Vitale"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Construtora\s+Vitale\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Extrair bairro de bairros conhecidos do RJ
                soup_vit = BeautifulSoup(html, "html.parser")
                text_vit = soup_vit.get_text(separator="\n", strip=True)
                bairros_rj = [
                    "Recreio dos Bandeirantes", "Pechincha", "Irajá", "Vargem Grande",
                    "Encantado", "Campinho", "Méier", "Madureira", "Vila da Penha",
                    "Barra da Tijuca", "Jacarepaguá", "Taquara", "Ilha do Governador",
                    "Campo Grande", "Santa Cruz", "Bangu",
                ]
                for bairro in bairros_rj:
                    if re.search(re.escape(bairro), text_vit, re.I):
                        dados["bairro"] = bairro
                        break
                # Fase
                if re.search(r'100%\s*vendido', text_vit, re.I):
                    dados["fase"] = "100% Vendido"
                elif re.search(r'entregue', text_vit, re.I):
                    dados["fase"] = "Pronto"

            # --- Blendi: Londrina/Apucarana/Maringá/Curitiba PR, MCMV ---
            if empresa_key == "blendi":
                soup_bl = BeautifulSoup(html, "html.parser")
                # Remover nav e footer antes de buscar cidade (HQ Curitiba contamina)
                for tag in soup_bl.find_all(["nav", "footer", "header"]):
                    tag.decompose()
                text_bl_body = soup_bl.get_text(separator="\n", strip=True)
                text_bl = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
                # Buscar cidade no corpo da pagina (sem footer) primeiro
                cidades_blendi = [
                    ("Londrina", "PR"), ("Apucarana", "PR"), ("Maringá", "PR"),
                    ("Cambé", "PR"), ("Rolândia", "PR"), ("Arapongas", "PR"),
                ]
                found_city = False
                for cidade_nome, estado_val in cidades_blendi:
                    if re.search(re.escape(cidade_nome), text_bl_body, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_city = True
                        break
                # Se nao achou cidade especifica no corpo, pode ser Curitiba
                if not found_city:
                    # Verificar se "Curitiba" aparece no corpo (nao so footer)
                    if re.search(r'(?:em|de)\s+Curitiba', text_bl_body, re.I):
                        dados["cidade"] = "Curitiba"
                        dados["estado"] = "PR"
                    else:
                        dados["cidade"] = "Londrina"
                        dados["estado"] = "PR"
                # Limpar nome: remover " - Blendi"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Blendi\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase: "Entregue" = Pronto (prioridade sobre deteccao automatica)
                if re.search(r'entregue', text_bl, re.I):
                    dados["fase"] = "Pronto"
                elif re.search(r'100%\s*vendido', text_bl, re.I):
                    dados["fase"] = "100% Vendido"
                # Se todas as fases de obra estao a 0%, é Lançamento
                elif re.search(r'Infraestrutura.*?0%.*?Funda[çc][ãa]o.*?0%', text_bl, re.DOTALL):
                    dados["fase"] = "Lançamento"
                # Endereco: "Avenida Sete de Setembro, 4979" etc.
                match_end = re.search(r'((?:Av\.|Avenida|Rua|R\.)\s+[^,\n]+(?:,\s*\d+)?)', text_bl)
                if match_end and not dados.get("endereco"):
                    dados["endereco"] = match_end.group(1).strip()

            # --- VL: Recife/PE, MCMV, coords do JS ---
            if empresa_key == "vl":
                soup_vl = BeautifulSoup(html, "html.parser")
                # Buscar cidade no endereco primeiro (mais preciso que texto geral)
                endereco_vl = dados.get("endereco", "")
                text_vl = soup_vl.get_text(separator="\n", strip=True)
                # Cidades especificas ANTES de Recife (Recife aparece no footer)
                cidades_vl = [
                    ("São Lourenço da Mata", "PE"), ("Caruaru", "PE"),
                    ("Paulista", "PE"), ("Olinda", "PE"),
                    ("Jaboatão dos Guararapes", "PE"),
                    ("Cabo de Santo Agostinho", "PE"),
                    ("Camaragibe", "PE"), ("Abreu e Lima", "PE"),
                    ("Recife", "PE"),
                ]
                # Priorizar endereco
                found_vl = False
                for cidade_nome, estado_val in cidades_vl:
                    if re.search(re.escape(cidade_nome), endereco_vl, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        found_vl = True
                        break
                if not found_vl:
                    # Buscar no texto, mas excluir footer
                    for tag_rm in soup_vl.find_all(["footer", "nav"]):
                        tag_rm.decompose()
                    text_vl_body = soup_vl.get_text(separator="\n", strip=True)
                    for cidade_nome, estado_val in cidades_vl:
                        if re.search(re.escape(cidade_nome), text_vl_body, re.I):
                            dados["cidade"] = cidade_nome
                            dados["estado"] = estado_val
                            found_vl = True
                            break
                if not found_vl:
                    dados["cidade"] = "Recife"
                    dados["estado"] = "PE"
                # Limpar nome: remover " - Apartamentos à venda é com a VL Construtora"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Apartamentos.*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase: % de construcao
                match_pct = re.search(r'(\d+[.,]\d+)%\s*(?:de\s*)?(?:conclus|execu|constru)', text_vl, re.I)
                if match_pct:
                    pct = float(match_pct.group(1).replace(",", "."))
                    if pct >= 99:
                        dados["fase"] = "Pronto"
                    elif pct > 0:
                        dados["fase"] = "Em Construção"
                        dados["evolucao_obra_pct"] = int(pct)
                elif re.search(r'lançamento', text_vl, re.I):
                    dados["fase"] = "Lançamento"

            # --- Rio Branco: Salto/SP, MCMV ---
            if empresa_key == "riobranco":
                soup_rb = BeautifulSoup(html, "html.parser")
                text_rb = soup_rb.get_text(separator="\n", strip=True)
                # Tentar extrair cidade do title ou texto
                cidades_rb = [
                    ("Salto", "SP"), ("Itu", "SP"), ("Indaiatuba", "SP"),
                    ("Sorocaba", "SP"),
                ]
                for cidade_nome, estado_val in cidades_rb:
                    if re.search(re.escape(cidade_nome), text_rb, re.I):
                        dados["cidade"] = cidade_nome
                        dados["estado"] = estado_val
                        break
                if not dados.get("cidade"):
                    dados["cidade"] = "Salto"
                    dados["estado"] = "SP"
                # Limpar nome do title
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*(?:Salto|Itu|Construtora Rio Branco).*$', '',
                                        dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo

            # --- NeoUrb: SP capital ---
            if empresa_key == "neourb":
                dados["cidade"] = "São Paulo"
                dados["estado"] = "SP"
                # Limpar nome: remover " – Neourb Incorporadora"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*Neourb\s+Incorporadora\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase do texto
                soup_neo = BeautifulSoup(html, "html.parser")
                text_neo = soup_neo.get_text(separator="\n", strip=True).lower()
                if "breve lançamento" in text_neo:
                    dados["fase"] = "Breve Lançamento"
                elif "100% vendido" in text_neo or "esgotado" in text_neo:
                    dados["fase"] = "100% Vendido"
                # Bairro do nome/slug
                bairros_sp = {
                    "mooca": "Mooca", "barra-funda": "Barra Funda",
                    "perdizes": "Perdizes", "vila-clementino": "Vila Clementino",
                    "vila-sonia": "Vila Sônia", "chacara-santo-antonio": "Chácara Santo Antônio",
                }
                for bairro_slug, bairro_nome in bairros_sp.items():
                    if bairro_slug in slug.lower():
                        dados["bairro"] = bairro_nome
                        break

            # --- M91: Pelotas/RS + Caxias do Sul/RS ---
            if empresa_key == "m91":
                soup_m91 = BeautifulSoup(html, "html.parser")
                text_m91 = soup_m91.get_text(separator="\n", strip=True)
                if re.search(r'Caxias do Sul', text_m91, re.I) or "caxias" in slug.lower():
                    dados["cidade"] = "Caxias do Sul"
                    dados["estado"] = "RS"
                else:
                    dados["cidade"] = "Pelotas"
                    dados["estado"] = "RS"
                # Nome: pegar do h1/h2 se disponivel
                nomes_ruins_m91 = ["m91", "cadastre", "aproveite", "principais",
                                   "benefício", "beneficio", "lançamento", "lancamento",
                                   "engenharia", "construtora", "visite", "decorado",
                                   "apartamento", "contato", "sinimbu"]
                for tag in ["h1", "h2", "h3"]:
                    heading = soup_m91.find(tag)
                    if heading:
                        txt = heading.get_text(strip=True)
                        if txt and len(txt) > 3 and not any(r in txt.lower() for r in nomes_ruins_m91):
                            dados["nome"] = txt
                            break
                # Se nome ainda generico, usar slug formatado
                nome_lower = dados.get("nome", "").lower()
                if not dados.get("nome") or any(r in nome_lower for r in nomes_ruins_m91):
                    # Para slug como "caxias_belluno" -> "Belluno"
                    parts = slug.split("_")
                    nome_from_slug = parts[-1].replace("-", " ").title() if len(parts) > 1 else slug.replace("_", " ").replace("-", " ").title()
                    dados["nome"] = nome_from_slug
                # Fase
                if re.search(r'lançamento', text_m91, re.I):
                    dados["fase"] = "Lançamento"
                elif re.search(r'em\s*constru[çc]', text_m91, re.I):
                    dados["fase"] = "Em Construção"
                elif re.search(r'100%\s*vendido', text_m91, re.I):
                    dados["fase"] = "100% Vendido"

            # --- CTV: RJ, bairro do texto ---
            if empresa_key == "ctv":
                dados["cidade"] = "Rio de Janeiro"
                dados["estado"] = "RJ"
                # Limpar nome: remover " – CTV Construtora"
                if dados.get("nome"):
                    nome_limpo = re.sub(r'\s*[-–—]\s*CTV\s+Construtora\s*$', '', dados["nome"], flags=re.I).strip()
                    if nome_limpo and len(nome_limpo) > 2:
                        dados["nome"] = nome_limpo
                # Fase da listagem (prioridade sobre deteccao automatica)
                fases_list = config.get("_fases_listagem", {})
                if slug in fases_list:
                    dados["fase"] = fases_list[slug]
                # Bairros RJ
                soup_ctv = BeautifulSoup(html, "html.parser")
                text_ctv = soup_ctv.get_text(separator="\n", strip=True)
                bairros_rj_ctv = [
                    "Madureira", "Engenho de Dentro", "Centro", "Campinho",
                    "Irajá", "Cascadura", "Quintino", "Vila da Penha",
                    "Ilha do Governador", "Maria da Graça", "Olaria",
                    "Méier", "Jardim Sulacap", "Praça Mauá",
                ]
                for bairro in bairros_rj_ctv:
                    if re.search(re.escape(bairro), text_ctv, re.I):
                        dados["bairro"] = bairro
                        break

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

            # MCMV por default (exceto MF7 que ja foi setada como nao-MCMV)
            if "prog_mcmv" not in dados or (dados.get("prog_mcmv") is None):
                preco = dados.get("preco_a_partir")
                if preco and preco > 600000:
                    dados["prog_mcmv"] = 0
                else:
                    dados["prog_mcmv"] = 1
            elif dados.get("prog_mcmv") == 0 and empresa_key != "mf7":
                # Manter 0 apenas se ja setado explicitamente (MF7)
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
    parser = argparse.ArgumentParser(description="Scraper Batch F4 - 10 novas incorporadoras")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: mf7, ctv, todas)")
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
        print("\nEmpresas configuradas (Batch F4):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nUso: python scrapers/generico_novas_empresas_f4.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_novas_empresas_f4.py --empresa todas")
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
        print("  RESUMO GERAL — Batch F4")
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

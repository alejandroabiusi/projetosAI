"""
Scraper Generico para Incorporadoras Server-Side
=================================================
Funciona via requests (sem Selenium) para sites que renderizam HTML
no servidor. Cada empresa e configurada via dicionario com URLs,
padroes de link e seletores CSS para extracao de dados.

Uso:
    python scrapers/generico_empreendimentos.py --empresa magik_jc
    python scrapers/generico_empreendimentos.py --empresa kazzas --atualizar
    python scrapers/generico_empreendimentos.py --empresa todas
    python scrapers/generico_empreendimentos.py --empresa magik_jc --limite 5
    python scrapers/generico_empreendimentos.py --listar

Empresas configuradas:
    magik_jc, kazzas, vibra, pacaembu, mundo_apto, conx, benx, metrocasa,
    novvo, novolar, emccamp, sugoi, arbore, ampla, mlar, eph, unica,
    riformato, cavazani, smart, bm7, construlike, fyp, vl, jotanunes,
    piacentini, aclf, setai_gp, vinx, bp8, open, stanza,
    rev3, eme, sousa_araujo, econ
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


# ============================================================
# HEADERS
# ============================================================
HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

DELAY = 2  # segundos entre requisicoes


# ============================================================
# CONFIGURACAO DAS EMPRESAS
# ============================================================
EMPRESAS = {
    "magik_jc": {
        "nome_banco": "Magik JC",
        "base_url": "https://magikjc.com.br",
        "urls_listagem": [
            "https://magikjc.com.br/empreendimentos",
        ],
        "padrao_link": r"magikjc\.com\.br/empreendimento/([^/]+)",
        "endereco_sede": r"Av\.?\s*Ang[eé]lica\s*,?\s*1996",
        "parsers": {
            "nome": {"method": "css", "selector": "h1.title-product, h2.main-title, .info-product h3", "attr": "text"},
            "bairro": {"method": "css", "selector": "#locale h3, .location .neighborhood, .info-product .location", "attr": "text"},
            "endereco": {"method": "css", "selector": "#locale", "attr": "text"},
            "endereco_fallback": {"method": "regex", "pattern": r"(?:Rua|Av\.|Avenida|R\.|Al\.|Alameda)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?\s*residenciais?|UHs?|apartamentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?dorms?\.?(?:\s*(?:com|c/)\s*(?:suíte|suite))?|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "kazzas": {
        "nome_banco": "Kazzas",
        "base_url": "https://kazzas.com.br",
        "urls_listagem": [
            "https://kazzas.com.br/",
            "https://kazzas.com.br/imoveis",
        ],
        "padrao_link": r"kazzas\.com\.br/imoveis/([^/]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, .pdp-title h1, .enterprise-name", "attr": "text"},
            "bairro": {"method": "css", "selector": ".pdp-location, .enterprise-location", "attr": "text"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?quarto[s]?|\d+\s*dorms?\.?|\bstudio[s]?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:\s*(?:e|a)\s*\d+)?\s*m[²2]"},
        },
    },

    "vibra": {
        "nome_banco": "Vibra Residencial",
        "base_url": "https://vibraresidencial.com.br",
        "urls_listagem": [
            "https://vibraresidencial.com.br",
        ],
        "padrao_link": r"vibraresidencial\.com\.br/produtos/([^/]+)",
        "parsers": {
            "nome": {"method": "regex", "pattern": r"SOBRE\s+O\s+(.+?)(?:\n|$)"},
            "nome_fallback": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"Terreno:\s*(.+?)(?:\n|Loja:)"},
            "numero_torres": {"method": "regex", "pattern": r"N[ÚU]MERO DE TORRES:\s*(\d+)"},
            "total_unidades": {"method": "regex", "pattern": r"TOTAL DE UNIDADES:\s*(\d+)"},
            "numero_andares": {"method": "regex", "pattern": r"ANDARES:\s*(\d+)"},
            "numero_vagas": {"method": "regex", "pattern": r"N[ÚU]MERO DE VAGAS:\s*(\d+)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"N[ÚU]MERO DE QUARTOS:\s*(.+?)(?:\n|$)"},
            "metragens_descricao": {"method": "regex", "pattern": r"Planta de\s*(\d+m[²2])"},
        },
    },

    "pacaembu": {
        "nome_banco": "Pacaembu",
        "base_url": "https://pacaembu.com",
        "urls_listagem": [
            "https://pacaembu.com/imoveis-residenciais",
            "https://pacaembu.com/imoveis-residenciais?status=cadastro",
            "https://pacaembu.com/imoveis-residenciais?status=lancamento",
            "https://pacaembu.com/imoveis-residenciais?status=em-obras",
            "https://pacaembu.com/imoveis-residenciais?status=concluida",
            "https://pacaembu.com/imoveis-residenciais?page=2",
            "https://pacaembu.com/imoveis-residenciais?page=3",
            "https://pacaembu.com/imoveis-residenciais?page=4",
            "https://pacaembu.com/imoveis-residenciais?page=5",
        ],
        "padrao_link": r"pacaembu\.com/imoveis-residenciais/([a-z][\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.title", "attr": "text"},
            "cidade": {"method": "regex", "pattern": r"(?:Cidade|cidade):\s*(.+?)(?:\n|$)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*dorms?\.?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "mundo_apto": {
        "nome_banco": "Mundo Apto",
        "base_url": "https://mundoapto.com.br",
        "urls_listagem": [
            "https://mundoapto.com.br/",
            "https://mundoapto.com.br/imoveis/",
        ],
        "padrao_link": r"mundoapto\.com\.br/(mundo-apto-[^/]+|mapp-[^/]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.et_pb_module_header", "attr": "text"},
            "bairro": {"method": "regex", "pattern": r"(?:Zona\s+(?:Leste|Oeste|Norte|Sul|Central))"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"(?:studio|Studio)[s]?,?\s*(?:\d+\s*(?:e\s*\d+\s*)?dorms?\.?)|\d+\s*(?:e\s*\d+\s*)?dorms?\.?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "conx": {
        "nome_banco": "Conx",
        "base_url": "https://conx.com.br",
        "urls_listagem": [
            "http://conx.com.br/produtos/destaque/todos/",
            "http://conx.com.br/produtos/categoria/futuro-lancamento/",
            "http://conx.com.br/produtos/categoria/breve-lancamento/",
            "http://conx.com.br/produtos/categoria/lancamento/",
            "http://conx.com.br/produtos/categoria/em-obras/",
            "http://conx.com.br/produtos/categoria/pronto-para-morar/",
        ],
        "padrao_link": r"conx\.com\.br/produto/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.title, .topico h2", "attr": "text"},
            "bairro": {"method": "regex", "pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|suítes?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:\s*(?:a|e)\s*\d+)?\s*m[²2]"},
        },
    },

    "benx": {
        "nome_banco": "Benx",
        "base_url": "https://www.benx.com.br",
        "urls_listagem": [
            "https://www.benx.com.br/empreendimentos",
        ],
        "padrao_link": r"benx\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "bairro": {"method": "css", "selector": ".endereco", "attr": "text"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|suítes?|quartos?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:\s*(?:a|e)\s*\d+)?\s*m[²2]"},
        },
    },

    "metrocasa": {
        "nome_banco": "Metrocasa",
        "base_url": "https://www.metrocasa.com.br",
        "urls_listagem": [
            "https://www.metrocasa.com.br/",
            "https://www.metrocasa.com.br/imoveis",
            "https://www.metrocasa.com.br/zona/central",
            "https://www.metrocasa.com.br/zona/leste",
            "https://www.metrocasa.com.br/zona/norte",
            "https://www.metrocasa.com.br/zona/oeste",
            "https://www.metrocasa.com.br/zona/sul",
        ],
        "padrao_link": r"metrocasa\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    # ============================================================
    # NOVAS INCORPORADORAS (2026-03)
    # ============================================================

    "novvo": {
        "nome_banco": "Novvo",
        "base_url": "https://meunovvo.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://meunovvo.com.br/",
        ],
        "padrao_link": r"meunovvo\.com\.br/(novvo-marajoara|santamarina|novvoanaliafranco|vilaprudente|novvo-barra-funda)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:Av\.|Avenida|Rua|R\.|Estrada|Estr\.)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*andares?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "novolar": {
        "nome_banco": "Novolar",
        "base_url": "https://www.novolar.com.br",
        "fase_selector": "div.selos",
        "urls_listagem": [
            "https://www.novolar.com.br/",
            "https://www.novolar.com.br/imoveis/",
        ],
        "padrao_link": r"novolar\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*(?:n[ºo°]?\s*)?\d+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?)"},
        },
    },

    "emccamp": {
        "nome_banco": "Emccamp",
        "base_url": "https://emccamp.com.br",
        "urls_listagem": [
            "https://emccamp.com.br/",
            "https://emccamp.com.br/imoveis/",
        ],
        "padrao_link": r"emccamp\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "numero_andares": {"method": "regex", "pattern": r"(\d+)\s*(?:andares?|pavimentos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*[Aa]\s*\d+(?:[.,]\d+)?\s*m[²2]|\d+(?:[.,]\d+)?\s*m[²2]"},
        },
    },

    "sugoi": {
        "nome_banco": "SUGOI",
        "nome_from_title": True,
        "base_url": "https://sugoisa.com.br",
        "urls_listagem": [
            "https://sugoisa.com.br/todos-imoveis/",
        ],
        "padrao_link": r"sugoisa\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:Av\.|Avenida|Rua|R\.|Estr\.|Estrada)[^,\n]+(?:,\s*[\d.]+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[-aA]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "arbore": {
        "nome_banco": "Árbore",
        "base_url": "https://arboreengenharia.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://arboreengenharia.com.br/",
            "https://arboreengenharia.com.br/empreendimentos/",
        ],
        "padrao_link": r"arboreengenharia\.com\.br/empreendimentos/([\w_-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:Rua|R\.|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"Bairro\s+([\w\s]+?)(?:\s*[-–]|\n)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "ampla": {
        "nome_banco": "Ampla",
        "nome_from_title": True,
        "base_url": "https://www.amplaincorporadora.com.br",
        "urls_listagem": [
            "https://www.amplaincorporadora.com.br/",
        ],
        "padrao_link": r"amplaincorporadora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "mlar": {
        "nome_banco": "M.Lar",
        "base_url": "https://www.mlarempreendimentos.com.br",
        "nome_from_title": True,
        "estado_default": "CE",
        "urls_listagem": [
            "https://www.mlarempreendimentos.com.br/",
        ],
        "padrao_link": r"mlarempreendimentos\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida)[^,\n]+(?:,\s*\d+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|suítes?|dormit[oó]rios?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "eph": {
        "nome_banco": "EPH",
        "nome_from_title": True,
        "base_url": "https://ephincorporadora.com.br",
        "urls_listagem": [
            "https://ephincorporadora.com.br/",
        ],
        "padrao_link": r"ephincorporadora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title, .entry-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:Estr\.|Estrada|R\.|Rua|Av\.|Avenida)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "unica": {
        "nome_banco": "Ún1ca",
        "base_url": "https://un1ca.com.br",
        "estado_default": "CE",
        "urls_listagem": [
            "https://un1ca.com.br/",
        ],
        "padrao_link": r"un1ca\.com\.br/(?:empreendimento/)?(?!blog|contato|sobre|politica)(unica[\w-]+|un1ca[\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida)[^,\n]+(?:,\s*\d+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    # ============================================================
    # NOVAS INCORPORADORAS (2026-03 batch 2)
    # ============================================================

    "riformato": {
        "nome_banco": "Riformato",
        "base_url": "https://riformato.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://riformato.com.br/fase/lancamento/",
            "https://riformato.com.br/fase/breve-lancamentos/",
            "https://riformato.com.br/fase/construcao/",
            "https://riformato.com.br/fase/prontos/",
            "https://riformato.com.br/fase/entregues/",
        ],
        "padrao_link": r"riformato\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"method": "regex", "pattern": r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*São Paulo"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "cavazani": {
        "nome_banco": "Cavazani",
        "base_url": "https://cavazani.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://cavazani.com.br/empreendimentos/",
        ],
        "padrao_link": r"cavazani\.com\.br/((?:urbano|rubi|turmalinas|parque|solar|sinfonia)[\w-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "smart": {
        "nome_banco": "Smart Construtora",
        "base_url": "https://smartincorporadora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://smartincorporadora.com.br/",
            "https://smartincorporadora.com.br/imoveis/",
        ],
        "padrao_link": r"smartincorporadora\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "bm7": {
        "nome_banco": "BM7",
        "base_url": "https://bm7construtora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://bm7construtora.com.br/",
            "https://bm7construtora.com.br/todos-os-empreendimentos/",
        ],
        "padrao_link": r"bm7(?:construtora|empreendimentos)\.com\.br/(?:property|empreendimento)/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "construlike": {
        "nome_banco": "Construlike",
        "base_url": "https://construlike.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construlike.com.br/",
            "https://construlike.com.br/empreendimentos",
        ],
        "padrao_link": r"construlike\.com\.br/empreendimentos?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "numero_torres": {"method": "regex", "pattern": r"(\d+)\s*torres?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "fyp": {
        "nome_banco": "FYP Engenharia",
        "base_url": "https://fyp.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://fyp.com.br/",
            "https://fyp.com.br/imoveis/",
        ],
        "padrao_link": r"fyp\.com\.br/((?:aov|flexmed|maxy|masb|masa|cerejeiras|alamedas|california|boa-vista|jardim|varandas|residencial)[\w-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vl": {
        "nome_banco": "VL Construtora",
        "base_url": "https://www.vlconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "PE",
        "urls_listagem": [
            "https://www.vlconstrutora.com.br/",
            "https://www.vlconstrutora.com.br/encontre-seu-apartamento/",
        ],
        "padrao_link": r"vlconstrutora\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "jotanunes": {
        "nome_banco": "Jotanunes",
        "base_url": "https://jotanunes.com",
        "nome_from_title": True,
        "estado_default": "SE",
        "urls_listagem": [
            "https://jotanunes.com/",
            "https://jotanunes.com/empreendimentos/",
        ],
        "padrao_link": r"jotanunes\.com/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "piacentini": {
        "nome_banco": "Piacentini",
        "base_url": "https://piacentiniconstrutora.com.br",
        "nome_from_title": True,
        "estado_default": "PR",
        "urls_listagem": [
            "https://piacentiniconstrutora.com.br/",
            "https://piacentiniconstrutora.com.br/empreendimentos/",
        ],
        "padrao_link": r"piacentiniconstrutora\.com\.br/empreendimentos?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "aclf": {
        "nome_banco": "ACLF",
        "base_url": "https://aclf.com.br",
        "nome_from_title": True,
        "estado_default": "PE",
        "urls_listagem": [
            "https://aclf.com.br/",
            "https://aclf.com.br/imovel-estado/lancamento/",
            "https://aclf.com.br/imovel-estado/em-construcao/",
            "https://aclf.com.br/imovel-estado/portfolio/",
        ],
        "padrao_link": r"aclf\.com\.br/imovel/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "setai_gp": {
        "nome_banco": "Setai/Grupo GP",
        "base_url": "https://setaigrupogp.com.br",
        "nome_from_title": True,
        "estado_default": "PB",
        "cidade_default": "João Pessoa",
        "urls_listagem": [
            "https://setaigrupogp.com.br/",
            "https://setaigrupogp.com.br/empreendimentos-setai/",
        ],
        "padrao_link": r"setaigrupogp\.com\.br/empreendimentos?(?:-setai)?/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|suítes?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vinx": {
        "nome_banco": "Vinx",
        "base_url": "https://vinx.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://vinx.com.br/empreendimentos",
            "https://vinx.com.br/empreendimentos?f=t_6",
            "https://vinx.com.br/empreendimentos?f=t_8",
        ],
        "padrao_link": r"vinx\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "bp8": {
        "nome_banco": "BP8",
        "base_url": "https://bp8construtora.com.br",
        "nome_from_title": True,
        "urls_listagem": [
            "https://bp8construtora.com.br/empreendimentos/",
        ],
        "padrao_link": r"bp8construtora\.com\.br/((?:pin|pop|via|villa|ceo|reserva|ramada)[\w_-]*)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    # ============================================================
    # NOVAS INCORPORADORAS (2026-03 batch 3)
    # ============================================================

    "open": {
        "nome_banco": "Construtora Open",
        "base_url": "https://construtoraopen.com.br",
        "estado_default": "RS",
        "nome_from_title": True,
        "urls_listagem": [
            "https://construtoraopen.com.br/imoveis/",
        ],
        "padrao_link": r"construtoraopen\.com\.br/empreendimento/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1.elementor-heading-title", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+(?:\s*,\s*\d+)*(?:\s*e\s*\d+)?\s*dormit[oó]rios?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "stanza": {
        "nome_banco": "Stanza",
        "base_url": "https://stanza.com.br",
        "estado_default": "SE",
        "fase_selector": "ul.list-categories li",
        "urls_listagem": [
            "https://stanza.com.br/empreendimentos/",
        ],
        "padrao_link": r"stanza\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1.nome-empreendimento, h2.nome-empreendimento", "attr": "text"},
            "bairro": {"method": "css", "selector": "address.localizacao", "attr": "text"},
            "cidade": {"method": "css", "selector": "p.cidade", "attr": "text"},
            "endereco": {"method": "css", "selector": ".localizacao-sessao p.mb-4, .ficha-tecnica .item span.f16", "attr": "text"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+(?:\s*ou\s*\d+)?\s*quartos?(?:\s*(?:com|c/)\s*(?:su[ií]te|varanda))?"},
            "metragens_descricao": {"method": "regex", "pattern": r"[\d.,]+\s*m[²2]"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?)"},
        },
    },

    "rev3": {
        "nome_banco": "Rev3",
        "base_url": "https://rev3.com.br",
        "estado_default": "SP",
        "fase_selector": "div.empreendimento-capa-titulo span a[href*='estagio_da_obra']",
        "urls_listagem": [
            "https://rev3.com.br/empreendimentos/",
        ],
        "padrao_link": r"rev3\.com\.br/empreendimento/([\w%-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "div.empreendimento-capa-titulo h1, h1", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*[\d.]+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?)(?:\s*(?:com|c/)\s*(?:su[ií]te|varanda))?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "eme": {
        "nome_banco": "HM Engenharia",
        "base_url": "https://eme.maishm.com.br",
        "estado_default": "SP",
        "nome_from_title": True,
        "urls_listagem": [
            "https://eme.maishm.com.br/imoveis",
            "https://eme.maishm.com.br/imoveis?status=lancamento",
            "https://eme.maishm.com.br/imoveis?status=construcao",
            "https://eme.maishm.com.br/imoveis?status=pronto",
        ],
        "padrao_link": r"eme\.maishm\.com\.br/imoveis/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h4.text-title-6", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Rod\.|Rodovia)[^,\n]+(?:,\s*\d+)?[^/\n]*"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"[Aa]ptos?\.?\s*(?:com|de)?\s*\d+(?:\s*(?:e|ou)\s*\d+)?\s*dorms?\.?|\d+\s*dormit[oó]rios?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*m[²2]"},
        },
    },

    "sousa_araujo": {
        "nome_banco": "Sousa Araujo",
        "base_url": "https://sousaaraujo.com.br",
        "estado_default": "SP",
        "fase_selector": "span.badge.align-self-start",
        "urls_listagem": [
            "https://sousaaraujo.com.br/empreendimentos",
            "https://sousaaraujo.com.br/empreendimentos?status=lancamento",
            "https://sousaaraujo.com.br/empreendimentos?status=em-construcao",
            "https://sousaaraujo.com.br/empreendimentos?status=breve-lancamento",
        ],
        "padrao_link": r"sousaaraujo\.com\.br/empreendimentos/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+(?:\s*e\s*\d+)?\s*dormit[oó]rios?(?:\s*(?:com|c/)\s*(?:su[ií]te|varanda))?"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*m[²2]"},
        },
    },

    "econ": {
        "nome_banco": "Econ Construtora",
        "base_url": "https://econconstrutora.com.br",
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "nome_from_title": True,
        "sitemap_url": "https://econconstrutora.com.br/sitemap.xml",
        "urls_listagem": [],
        "padrao_link": r"econconstrutora\.com\.br/imovel/[\w-]+/([\w-]+)",
        "parsers": {
            "nome": {"method": "css", "selector": "h1, h2", "attr": "text"},
            "endereco": {"method": "regex", "pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Al\.|Alameda)[^,\n]+(?:,\s*\d+)?[^-\n]*"},
            "dormitorios_descricao": {"method": "regex", "pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)"},
            "metragens_descricao": {"method": "regex", "pattern": r"\d+(?:[.,]\d+)?\s*m[²2]"},
            "total_unidades": {"method": "regex", "pattern": r"(\d+)\s*(?:unidades?|apartamentos?)"},
        },
    },
}


# ============================================================
# LOGGER
# ============================================================
def setup_logger(empresa_key):
    logger = logging.getLogger(f"scraper.generico.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"generico_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# FUNCOES DE SCRAPING
# ============================================================

def fetch_html(url, logger):
    """Busca HTML de uma URL via requests."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.text
        else:
            logger.warning(f"Status {resp.status_code} para {url}")
            return None
    except Exception as e:
        logger.error(f"Erro ao acessar {url}: {e}")
        return None


def coletar_links_empreendimentos(config, logger):
    """Coleta todos os links de empreendimentos das paginas de listagem ou sitemap."""
    padrao = config["padrao_link"]
    base_url = config["base_url"]
    links = {}  # slug -> url_completa

    # Se tem sitemap, buscar links de la
    sitemap_url = config.get("sitemap_url")
    if sitemap_url:
        logger.info(f"Coletando links via sitemap: {sitemap_url}")
        html = fetch_html(sitemap_url, logger)
        if html:
            for match in re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", html):
                url_found = match.strip()
                m = re.search(padrao, url_found)
                if m:
                    slug = m.group(1)
                    url_limpa = url_found.split("?")[0].split("#")[0].rstrip("/")
                    links[slug] = url_limpa

    for url_listagem in config["urls_listagem"]:
        logger.info(f"Coletando links de: {url_listagem}")
        html = fetch_html(url_listagem, logger)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Resolver URLs relativas
            if href.startswith("/"):
                href = base_url + href
            elif not href.startswith("http"):
                continue

            match = re.search(padrao, href)
            if match:
                slug = match.group(1)
                url_limpa = href.split("?")[0].split("#")[0].rstrip("/")
                links[slug] = url_limpa

        time.sleep(DELAY)

    logger.info(f"Total de links encontrados: {len(links)}")
    return links


def extrair_por_parser(texto, parser_config):
    """Extrai dado usando a configuracao do parser."""
    method = parser_config.get("method")

    if method == "regex":
        pattern = parser_config["pattern"]
        matches = re.findall(pattern, texto, re.IGNORECASE)
        if matches:
            # Deduplicar mantendo ordem de aparicao
            vistos = set()
            unicos = []
            for m in matches:
                normalizado = str(m).strip().lower()
                if normalizado and normalizado not in vistos:
                    vistos.add(normalizado)
                    unicos.append(str(m).strip())
            if len(unicos) > 1:
                return " | ".join(unicos)
            return unicos[0] if unicos else None
    return None


def extrair_por_css(soup, parser_config):
    """Extrai dado usando seletor CSS."""
    selector = parser_config.get("selector", "")
    attr = parser_config.get("attr", "text")

    # Tenta cada seletor (separados por virgula)
    for sel in selector.split(","):
        sel = sel.strip()
        elem = soup.select_one(sel)
        if elem:
            if attr == "text":
                texto = elem.get_text(strip=True)
                if texto:
                    return texto
            else:
                return elem.get(attr, "")
    return None


def extrair_dados_empreendimento(html, url, config, logger):
    """Extrai dados de uma pagina de empreendimento."""
    soup = BeautifulSoup(html, "html.parser")
    texto_completo = soup.get_text(separator="\n", strip=True)

    dados = {
        "empresa": config["nome_banco"],
        "url_fonte": url,
        "estado": config.get("estado_default", "SP"),
        "cidade": None,  # Será extraído do HTML/endereço
    }

    # Extrair slug da URL
    match = re.search(config["padrao_link"], url)
    if match:
        dados["slug"] = match.group(1)

    # Processar parsers
    for campo, parser_config in config.get("parsers", {}).items():
        if campo.endswith("_fallback"):
            continue  # Processado depois se o principal falhar

        method = parser_config.get("method")
        valor = None

        if method == "css":
            valor = extrair_por_css(soup, parser_config)
        elif method == "regex":
            valor = extrair_por_parser(texto_completo, parser_config)

        if valor:
            # Campos numericos
            if campo in ("numero_torres", "total_unidades", "numero_andares"):
                try:
                    dados[campo] = int(re.search(r"\d+", valor).group())
                except (ValueError, AttributeError):
                    dados[campo + "_raw"] = valor
            elif campo == "numero_vagas":
                dados[campo] = valor
            else:
                dados[campo] = valor

    # Filtrar endereco da sede (se configurado)
    sede_pattern = config.get("endereco_sede")
    if sede_pattern and dados.get("endereco"):
        endereco_limpo = re.sub(sede_pattern, "", dados["endereco"], flags=re.IGNORECASE).strip(" |-,\n")
        if not endereco_limpo or len(endereco_limpo) < 5:
            # Tentar fallback
            fallback_end = config.get("parsers", {}).get("endereco_fallback")
            if fallback_end:
                valor_fb = extrair_por_parser(texto_completo, fallback_end)
                if valor_fb:
                    valor_fb = re.sub(sede_pattern, "", valor_fb, flags=re.IGNORECASE).strip(" |-,\n")
                    if valor_fb and len(valor_fb) >= 5:
                        dados["endereco"] = valor_fb
                    else:
                        dados["endereco"] = None
                else:
                    dados["endereco"] = None
            else:
                dados["endereco"] = None
        else:
            dados["endereco"] = endereco_limpo

    # Nomes invalidos comuns (capturados de botoes, status, etc)
    _NOMES_INVALIDOS = {
        "fale com a gente!", "not acceptable!", "em obras", "lançamento",
        "lancamento", "pronto para morar", "entregue", "breve lançamento",
        "breve lancamento", "100% vendido", "futuro lançamento",
    }
    _PREFIXOS_INVALIDOS = ("apartamento ", "imóvel ", "imovel ", "comprar ", "venda de ")

    def _nome_valido(n):
        if not n or len(n.strip()) < 3:
            return False
        nl = n.strip().lower()
        if nl in _NOMES_INVALIDOS:
            return False
        if any(nl.startswith(p) for p in _PREFIXOS_INVALIDOS):
            return False
        if len(nl) > 80:
            return False
        return True

    # Se config pede nome do <title>, usar direto
    if config.get("nome_from_title") or not _nome_valido(dados.get("nome")):
        title_tag = soup.find("title")
        if title_tag and title_tag.string:
            title_text = title_tag.string.strip()
            # Separar pelo ULTIMO separador (preserva nomes compostos como "X | Cond. 04")
            nome_empresa = config.get("nome_banco", "").lower()
            for sep in [" - ", " | ", " – ", " — "]:
                if sep in title_text:
                    partes = title_text.split(sep)
                    # Remove partes que contenham o nome da empresa
                    _fases_title = ["breve lançamento", "breve lancamento", "futuro lançamento",
                                     "futuro lancamento", "lançamento", "lancamento",
                                     "em obra", "em obras", "em construção", "em construcao",
                                     "pronto para morar", "pronto para entregar",
                                     "imóvel pronto", "imovel pronto", "100% vendido"]
                    partes_validas = [p.strip() for p in partes if nome_empresa not in p.strip().lower()
                                      and "incorporadora" not in p.strip().lower()
                                      and "construtora" not in p.strip().lower()
                                      and "engenharia" not in p.strip().lower()
                                      and not any(f in p.strip().lower() for f in _fases_title)]
                    if partes_validas:
                        title_text = (sep).join(partes_validas).strip()
                    else:
                        title_text = partes[0].strip()
                    break
            if _nome_valido(title_text):
                dados["nome"] = title_text

    # Fallback via parser nome_fallback
    if not _nome_valido(dados.get("nome")):
        fallback = config.get("parsers", {}).get("nome_fallback")
        if fallback:
            valor = extrair_por_css(soup, fallback) if fallback.get("method") == "css" else extrair_por_parser(texto_completo, fallback)
            if _nome_valido(valor):
                dados["nome"] = valor

    # Ultimo recurso: slug formatado
    if not _nome_valido(dados.get("nome")):
        slug = dados.get("slug", "")
        dados["nome"] = slug.replace("-", " ").replace("_", " ").title()

    # Extrair fase/status — primeiro via seletor específico da config, depois genérico
    fase = None
    fase_sel = config.get("fase_selector") if config else None
    if fase_sel and soup:
        el = soup.select_one(fase_sel)
        if el:
            t = el.get_text(strip=True).lower()
            # Match direto no texto curto do selo (inclui "pronto" isolado)
            fase_map = {
                "breve lançamento": "Breve Lançamento", "breve lancamento": "Breve Lançamento",
                "lançamento": "Lançamento", "lancamento": "Lançamento",
                "em obra": "Em Construção", "em construção": "Em Construção", "em construcao": "Em Construção",
                "obras iniciadas": "Em Construção", "obras em finalização": "Em Construção",
                "100% vendido": "100% Vendido",
                "pronto para morar": "Pronto", "pronto": "Pronto", "entregue": "Pronto",
            }
            for kw, val in fase_map.items():
                if kw in t:
                    fase = val
                    break
    if not fase:
        fase = detectar_fase(texto_completo, soup)
    if fase:
        dados["fase"] = fase

    # Extrair preco
    preco = extrair_preco(texto_completo)
    if preco:
        dados["preco_a_partir"] = preco

    # Extrair metragens numericas
    metragens = dados.get("metragens_descricao", "")
    if metragens:
        # Captura intervalos "X a Y m2" e valores isolados, com virgula ou ponto decimal
        nums = []
        # Primeiro captura intervalos completos: "24 a 41 m²"
        for match in re.finditer(r"(\d+(?:[.,]\d+)?)\s*[Aa]\s*(\d+(?:[.,]\d+)?)\s*m[²2²]?", metragens):
            for g in [match.group(1), match.group(2)]:
                v = float(g.replace(",", "."))
                if 15.0 <= v <= 150.0:
                    nums.append(v)
        # Depois captura valores isolados
        for match in re.finditer(r"(\d+(?:[.,]\d+)?)\s*m[²2²]", metragens):
            v = float(match.group(1).replace(",", "."))
            if 15.0 <= v <= 150.0:
                nums.append(v)
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)

    # Extrair itens de lazer do texto completo
    itens_lazer = extrair_itens_lazer(soup, texto_completo)
    if itens_lazer:
        dados["itens_lazer"] = " | ".join(itens_lazer)

    # Detectar atributos binarios
    binarios = detectar_atributos_binarios(texto_completo)
    dados.update(binarios)

    # Extrair cidade e estado do texto (padrao "Cidade/UF" ou "Cidade - UF")
    if not dados.get("cidade"):
        cidade_estado = extrair_cidade_estado(texto_completo)
        if cidade_estado:
            dados["cidade"] = cidade_estado[0]
            dados["estado"] = cidade_estado[1]

    # Extrair bairro e cidade do texto se nao encontrado via parser
    if not dados.get("bairro"):
        bairro = extrair_bairro(texto_completo, url)
        if bairro:
            dados["bairro"] = bairro

    return dados


def detectar_fase(texto, soup=None):
    """Detecta fase do empreendimento a partir do HTML e texto.

    Estratégia:
    1. Procura em elementos HTML de status (span.status, badges, etc.)
    2. Procura no texto principal (<main>/<article>)
    3. Fallback: texto completo com restrições

    Ordem de prioridade:
    breve lançamento > lançamento > em construção/obras > 100% vendido > pronto
    """
    FASES = [
        (["breve lançamento", "breve lancamento", "futuros lançamentos", "futuros lancamentos", "futuro lançamento", "futuro lancamento"], "Breve Lançamento"),
        (["lançamento", "lancamento"], "Lançamento"),
        (["em obra", "em construção", "em construcao",
          "obras em andamento", "obra em andamento", "obras iniciadas", "obra iniciada",
          "obras em finalização", "obras em finalizacao", "obra em finalização", "obra em finalizacao"], "Em Construção"),
        (["100% vendido"], "100% Vendido"),
        (["pronto para morar", "pronto para entregar", "imóvel pronto", "imovel pronto", "pronto", "entregue", "entregues"], "Pronto"),
    ]

    def match_fase(t):
        t = t.lower()
        for keywords, fase in FASES:
            for kw in keywords:
                if kw in t:
                    return fase
        return None

    # 0. H2 subtitle (Smart e similares): h2 do nome tem sibling com fase
    #    Ex: h2="Smart Vila Augusta" → sibling="Breve Lançamento em Guarulhos | 389 unidades"
    #    Também: h2="Status da obra" indica Em Construção
    if soup:
        for h2 in soup.find_all('h2'):
            h2_text = h2.get_text(strip=True).lower()
            # h2 "Status da obra" = empreendimento em construção
            if 'status da obra' in h2_text:
                return "Em Construção"
            # Pular h2 de seções genéricas
            if any(skip in h2_text for skip in ['conheça outros', 'outros imóveis', 'outros imoveis', 'relacionados']):
                continue
            # Checar subtitle (sibling do h2)
            nxt = h2.find_next_sibling()
            if nxt:
                sub = nxt.get_text(strip=True)
                if sub:
                    fase = match_fase(sub)
                    if fase:
                        return fase

    # 1a. Taxonomia WordPress via classes CSS (ex: progresso-da-obra-em-construcao)
    if soup:
        # Procurar elemento com classe de taxonomia WP de progresso/estagio
        tax_el = soup.find(class_=re.compile(r'progresso[-_]', re.I))
        if not tax_el:
            tax_el = soup.find(class_=re.compile(r'estagio[-_](?!obra)', re.I))
        if tax_el:
            classes_str = " ".join(tax_el.get("class", []))
            fase = match_fase(classes_str.replace("-", " "))
            if fase:
                return fase

    # 1b. Procurar em elementos HTML de status (badges, tags)
    #     Ignora elementos dentro de cards de listagem (outros empreendimentos)
    if soup:
        for sel in ['span.status', '.status', '.badge', '.tag', '.fase', '[class*="status"]', '[class*="fase"]', '[class*="estagio"]']:
            for el in soup.select(sel):
                # Pular se está dentro de card/listagem de outros empreendimentos
                if el.find_parent(class_=re.compile(r'card|listing|grid|carousel|slider|swiper|related|outros', re.I)):
                    continue
                fase = match_fase(el.get_text())
                if fase:
                    return fase

        # Links de taxonomia de status (ex: Magik JC /estagio_obra/pronto/)
        # Ignora links dentro de nav/header/menu (que listam todas as fases)
        for el in soup.select('a[href*="estagio_obra"], a[href*="estagio-obra"], a[href*="status-obra"]'):
            if el.find_parent(['nav', 'header', 'ul', 'ol']):
                continue
            fase = match_fase(el.get_text())
            if fase:
                return fase

    # 2. Extrair do <title> (padrão "Nome | Status" usado por Conx e outros)
    if soup:
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            if '|' in title_text:
                # Pega a parte depois do último "|" (geralmente o status)
                title_part = title_text.rsplit('|', 1)[-1].strip()
                fase = match_fase(title_part)
                if fase:
                    return fase

    # 3. Texto do conteúdo principal (<main>/<article>)
    if soup:
        main_soup = soup.find('main') or soup.find('article') or soup.find(id='content') or soup.find(class_='content')
        if main_soup:
            fase = match_fase(main_soup.get_text(separator="\n", strip=True))
            if fase:
                return fase

    # 4. Fallback: texto com prioridade invertida
    #    "lançamento" é genérico demais (aparece em menus, links, outros imóveis)
    #    Priorizar fases específicas antes de cair em "lançamento"
    texto_lower = texto.lower()
    texto_500 = texto_lower[:500]

    # 4a. Fases específicas no texto completo (alta confiança)
    for keywords, fase in FASES:
        if fase in ("Breve Lançamento", "Lançamento"):
            continue
        for kw in keywords:
            if kw in ("pronto",):  # "pronto" sozinho é genérico
                continue
            if kw in texto_lower:
                return fase

    # 4b. "breve lançamento" nos primeiros 500 chars (evita pegar de menus)
    for kw in FASES[0][0]:  # keywords de Breve Lançamento
        if kw in texto_500:
            return "Breve Lançamento"

    # 4c. "lançamento" nos primeiros 500 chars (baixa confiança)
    for kw in FASES[1][0]:  # keywords de Lançamento
        if kw in texto_500:
            return "Lançamento"

    return None


def extrair_preco(texto):
    """Extrai preco a partir do texto."""
    # Padroes: R$ 205.000, R$205000, a partir de R$ 205.000
    matches = re.findall(r"R\$\s*([\d.,]+)", texto)
    if matches:
        for m in matches:
            try:
                valor = float(m.replace(".", "").replace(",", "."))
                if valor > 50000:  # Filtra valores muito baixos (provavelmente entrada/parcela)
                    return valor
            except ValueError:
                continue
    return None


def extrair_itens_lazer(soup, texto):
    """Extrai lista de itens de lazer."""
    itens = set()

    # Lista de termos de lazer para buscar
    termos_lazer = [
        "piscina", "churrasqueira", "fitness", "academia", "playground",
        "brinquedoteca", "salão de festas", "salao de festas", "pet care",
        "pet place", "coworking", "bicicletário", "bicicletario", "quadra",
        "delivery", "horta", "lavanderia", "redário", "redario", "rooftop",
        "sauna", "spa", "sport bar", "cinema", "cine", "mini mercado",
        "market", "espaço beleza", "espaco beleza", "gourmet", "salão de jogos",
        "salao de jogos", "luau", "deck", "solarium", "solário",
        "portaria", "hall social", "espaço luau", "mini quadra",
        "beach tennis", "bike station", "cobertura", "sala funcional",
    ]

    texto_lower = texto.lower()
    for termo in termos_lazer:
        if termo in texto_lower:
            # Capitalizar para padronizar
            itens.add(termo.title())

    return sorted(itens)


def extrair_cidade_estado(texto, max_chars=3000):
    """Extrai cidade e estado do texto em padroes como 'São Paulo/SP', 'Fortaleza - CE'.
    Limita busca aos primeiros max_chars caracteres para evitar capturar dados do footer/sidebar."""
    UFS_VALIDAS = {"AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"}
    texto_topo = texto[:max_chars]
    # Padrao: "Cidade/UF" ou "Cidade - UF" ou "Cidade – UF"
    for pattern in [
        r"([A-ZÀ-Ú][a-zà-ú]+(?:[ \t]+(?:de|do|da|dos|das|e|[A-ZÀ-Ú][a-zà-ú]+))*)[ \t]*/[ \t]*([A-Z]{2})",
        r"([A-ZÀ-Ú][a-zà-ú]+(?:[ \t]+(?:de|do|da|dos|das|e|[A-ZÀ-Ú][a-zà-ú]+))*)[ \t]*[-–][ \t]*([A-Z]{2})\b",
    ]:
        matches = re.findall(pattern, texto_topo)
        for cidade, uf in matches:
            cidade = cidade.strip()
            uf = uf.strip()
            if uf in UFS_VALIDAS and len(cidade) >= 3:
                return (cidade, uf)
    return None


def extrair_bairro(texto, url):
    """Tenta extrair bairro do texto."""
    # Padrao: "Bairro - Cidade / UF"
    match = re.search(r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*São Paulo\s*/\s*SP", texto)
    if match:
        return match.group(1).strip()
    # Padrao: "Bairro, São Paulo"
    match = re.search(r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*,\s*São Paulo", texto)
    if match:
        return match.group(1).strip()
    return None


def download_imagens(html, url, empresa_key, slug, logger):
    """Baixa imagens do empreendimento (fachadas, plantas, lazer)."""
    soup = BeautifulSoup(html, "html.parser")
    img_dir = os.path.join(DOWNLOADS_DIR, empresa_key, "imagens", slug)
    os.makedirs(img_dir, exist_ok=True)

    # Coletar URLs de imagens
    img_urls = set()
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if any(skip in src.lower() for skip in ["logo", "icon", "avatar", "pixel", "svg", "data:", "base64"]):
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            base = re.match(r"https?://[^/]+", url)
            if base:
                src = base.group() + src
        elif not src.startswith("http"):
            continue

        # Filtrar por tamanho minimo provavel (URLs com resolucao)
        if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            img_urls.add(src)

    # Tambem buscar em data-src, data-lazy, etc.
    for attr in ["data-src", "data-lazy-src", "data-bg", "data-image"]:
        for elem in soup.find_all(True, attrs={attr: True}):
            src = elem[attr]
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                base = re.match(r"https?://[^/]+", url)
                if base:
                    src = base.group() + src
            if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                img_urls.add(src)

    if not img_urls:
        return 0

    baixadas = 0
    for i, img_url in enumerate(sorted(img_urls)):
        try:
            # Gerar nome do arquivo
            ext = ".jpg"
            for e in [".png", ".webp", ".jpeg"]:
                if e in img_url.lower():
                    ext = e
                    break

            nome_arquivo = f"{slug}_{i+1:03d}{ext}"
            caminho = os.path.join(img_dir, nome_arquivo)

            if os.path.exists(caminho):
                continue

            resp = requests.get(img_url, headers=HEADERS, timeout=15, stream=True)
            if resp.status_code == 200 and len(resp.content) > 5000:  # > 5KB
                with open(caminho, "wb") as f:
                    f.write(resp.content)
                baixadas += 1
        except Exception:
            pass

    if baixadas > 0:
        logger.info(f"  {baixadas} imagens baixadas para {slug}")
    return baixadas


# ============================================================
# MAIN
# ============================================================

def processar_empresa(empresa_key, atualizar=False, limite=None, sem_imagens=False):
    """Processa uma empresa completa."""
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros de {nome_banco}")
    logger.info("=" * 60)

    # Fase 1: Coletar links
    links = coletar_links_empreendimentos(config, logger)
    if not links:
        logger.warning("Nenhum link de empreendimento encontrado!")
        return

    logger.info(f"Links coletados: {len(links)}")

    # Filtrar ja processados (a menos que --atualizar)
    if not atualizar:
        links_novos = {}
        for slug, url in links.items():
            nome_teste = slug.replace("-", " ").title()
            if not empreendimento_existe(nome_banco, nome_teste):
                links_novos[slug] = url
            # Testar tambem sem title case
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

            # Verificar se existe
            existe = empreendimento_existe(nome_banco, nome)

            if existe and atualizar:
                atualizar_empreendimento(nome_banco, nome, dados)
                atualizados += 1
                logger.info(f"  Atualizado: {nome}")
            elif not existe:
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {nome} | {dados.get('fase', 'N/A')} | {dados.get('dormitorios_descricao', 'N/A')} | {dados.get('metragens_descricao', 'N/A')}")
            else:
                logger.info(f"  Ja existe: {nome}")

            # Download de imagens
            if not sem_imagens:
                download_imagens(html, url, empresa_key, slug, logger)

        except Exception as e:
            logger.error(f"  Erro: {e}")
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


def main():
    parser = argparse.ArgumentParser(description="Scraper generico de empreendimentos")
    parser.add_argument("--empresa", type=str, required=False,
                       help="Chave da empresa (ex: magik_jc, kazzas, todas)")
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
        print("\nEmpresas configuradas:")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} -> {cfg['nome_banco']}")
        print(f"\nUse: python scrapers/generico_empreendimentos.py --empresa <chave>")
        print(f"  ou: python scrapers/generico_empreendimentos.py --empresa todas")
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
        resultado = processar_empresa(
            empresa_key,
            atualizar=args.atualizar,
            limite=args.limite,
            sem_imagens=args.sem_imagens,
        )
        resultados[empresa_key] = resultado

    if len(resultados) > 1:
        print("\n" + "=" * 60)
        print("  RESUMO GERAL")
        print("=" * 60)
        total_novos = 0
        total_erros = 0
        for key, r in resultados.items():
            if r:
                print(f"  {EMPRESAS[key]['nome_banco']:20s} +{r['novos']} novos, {r['erros']} erros")
                total_novos += r["novos"]
                total_erros += r["erros"]
        print(f"  {'TOTAL':20s} +{total_novos} novos, {total_erros} erros")
        print("=" * 60)


if __name__ == "__main__":
    main()

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
    magik_jc, kazzas, vibra, pacaembu, mundo_apto, conx, benx, metrocasa
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
    """Coleta todos os links de empreendimentos das paginas de listagem."""
    padrao = config["padrao_link"]
    base_url = config["base_url"]
    links = {}  # slug -> url_completa

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
        "estado": "SP",  # Default, sera ajustado se encontrado
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

    # Fallbacks
    if not dados.get("nome"):
        fallback = config.get("parsers", {}).get("nome_fallback")
        if fallback:
            valor = extrair_por_css(soup, fallback) if fallback.get("method") == "css" else extrair_por_parser(texto_completo, fallback)
            if valor:
                dados["nome"] = valor

    # Se ainda nao tem nome, usar o slug formatado
    if not dados.get("nome"):
        slug = dados.get("slug", "")
        dados["nome"] = slug.replace("-", " ").title()

    # Extrair fase/status a partir do texto
    fase = detectar_fase(texto_completo)
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

    # Extrair bairro e cidade do texto se nao encontrado via parser
    if not dados.get("bairro"):
        bairro = extrair_bairro(texto_completo, url)
        if bairro:
            dados["bairro"] = bairro

    return dados


def detectar_fase(texto):
    """Detecta fase do empreendimento a partir do texto."""
    texto_lower = texto.lower()
    if "100% vendido" in texto_lower or "vendido" in texto_lower[:500]:
        return "100% Vendido"
    if "pronto para morar" in texto_lower or "pronto para entregar" in texto_lower or "imóvel pronto" in texto_lower or "imovel pronto" in texto_lower:
        return "Pronto"
    if "entregue" in texto_lower[:500] or "entregues" in texto_lower[:500]:
        return "Pronto"
    if "em obra" in texto_lower or "em construção" in texto_lower or "em construcao" in texto_lower or "obras em andamento" in texto_lower:
        return "Em Construção"
    if "breve lançamento" in texto_lower or "breve lancamento" in texto_lower:
        return "Breve Lançamento"
    if "futuro lançamento" in texto_lower or "futuro lancamento" in texto_lower:
        return "Futuro Lançamento"
    if "lançamento" in texto_lower[:500] or "lancamento" in texto_lower[:500]:
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

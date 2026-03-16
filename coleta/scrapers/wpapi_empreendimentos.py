"""
Scraper para sites WordPress com API REST exposta
===================================================
Coleta empreendimentos via /wp-json/wp/v2/{post_type} e depois
enriquece visitando cada pagina individual para extrair dados
detalhados (endereco, metragem, dormitorios, fase, coordenadas).

Sites configurados:
    acl         — ACL Incorporadora (SP Capital)
    vic         — VIC Engenharia (MG/SP/RJ/DF/BA)
    vasco       — Vasco Construtora (RS)
    carrilho    — Carrilho Construtora (PE)

Uso:
    python scrapers/wpapi_empreendimentos.py --empresa acl
    python scrapers/wpapi_empreendimentos.py --empresa todas
    python scrapers/wpapi_empreendimentos.py --listar
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

from config.settings import REQUESTS, LOGS_DIR
from data.database import (
    inserir_empreendimento,
    empreendimento_existe,
    atualizar_empreendimento,
    detectar_atributos_binarios,
    contar_empreendimentos,
)

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "application/json, text/html",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

DELAY = 2

# ============================================================
# CONFIGURACAO DAS EMPRESAS COM WP API
# ============================================================
EMPRESAS = {
    "acl": {
        "nome_banco": "ACL Incorporadora",
        "base_url": "https://aclinc.com.br",
        "api_endpoint": "https://aclinc.com.br/wp-json/wp/v2/empreendimento",
        "post_type": "empreendimento",
        "per_page": 50,
        "estado_default": "SP",
        "cidade_default": "São Paulo",
        "status_selectors": [
            '.status', '.badge', '[class*="status"]',
            'span.elementor-button-text',
        ],
        "parsers": {
            "endereco": {"pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Al\.|Alameda)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"pattern": r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*São Paulo\s*/\s*SP"},
            "total_unidades": {"pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vic": {
        "nome_banco": "VIC Engenharia",
        "base_url": "https://vicengenharia.com.br",
        "api_endpoint": "https://vicengenharia.com.br/wp-json/wp/v2/empreendimentos",
        "post_type": "empreendimentos",
        "per_page": 50,
        "estado_default": "MG",
        "extrair_localizacao_classes": True,  # cidade/estado via class_list
        "endereco_sede": "Av. Álvares Cabral, 1777",  # ignorar este endereco (sede)
        "status_selectors": [
            '.status', '.badge', '[class*="status"]', '[class*="fase"]',
        ],
        "parsers": {
            "total_unidades": {"pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?)"},
            "dormitorios_descricao": {"pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "vasco": {
        "nome_banco": "Vasco Construtora",
        "base_url": "https://construtoravasco.com.br",
        "api_endpoint": "https://construtoravasco.com.br/wp-json/wp/v2/imovel",
        "post_type": "imovel",
        "per_page": 50,
        "estado_default": "RS",
        "taxonomies": {
            "cidade": "https://construtoravasco.com.br/wp-json/wp/v2/cidade",
            "estagio": "https://construtoravasco.com.br/wp-json/wp/v2/estagio",
        },
        "status_selectors": [
            '.status', '.badge', '[class*="status"]', '[class*="estagio"]',
            'div.banner__tag', 'span.tag',
        ],
        "parsers": {
            "endereco": {"pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"pattern": r"(?:Bairro|bairro):\s*(.+?)(?:\n|$)"},
            "total_unidades": {"pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|casas?)"},
            "dormitorios_descricao": {"pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)"},
            "metragens_descricao": {"pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },

    "carrilho": {
        "nome_banco": "Carrilho",
        "base_url": "https://carrilho.com.br",
        "api_endpoint": "https://carrilho.com.br/wp-json/wp/v2/empreendimento",
        "post_type": "empreendimento",
        "per_page": 50,
        "estado_default": "PE",
        "cidade_default": "Recife",
        "status_selectors": [
            '.status', '.badge', '[class*="status"]', '[class*="fase"]',
        ],
        "parsers": {
            "endereco": {"pattern": r"(?:R\.|Rua|Av\.|Avenida|Estr\.|Estrada|Pç\.|Praça)[^,\n]+(?:,\s*\d+)?"},
            "bairro": {"pattern": r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+(?:de|do|da|dos|das|[A-ZÀ-Ú])[a-zà-ú]*)*)\s*[-–]\s*Recife"},
            "total_unidades": {"pattern": r"(\d+)\s*(?:unidades?|apartamentos?|aptos?|studios?)"},
            "dormitorios_descricao": {"pattern": r"\d+\s*(?:e\s*\d+\s*)?(?:dorms?\.?|quartos?|dormit[oó]rios?)|\bstudios?\b"},
            "metragens_descricao": {"pattern": r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?)?\s*m[²2]"},
        },
    },
}


# ============================================================
# LOGGER
# ============================================================
def setup_logger(empresa_key):
    logger = logging.getLogger(f"scraper.wpapi.{empresa_key}")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, f"wpapi_{empresa_key}.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ============================================================
# FUNCOES AUXILIARES (importadas do generico)
# ============================================================

def extrair_preco(texto):
    matches = re.findall(r"R\$\s*([\d.,]+)", texto)
    for m in matches:
        try:
            valor = float(m.replace(".", "").replace(",", "."))
            if valor > 50000:
                return valor
        except ValueError:
            continue
    return None


def extrair_coordenadas_gmaps(html):
    """Extrai coordenadas de Google Maps embed."""
    # Padrao !2d{lng}!3d{lat}
    match = re.search(r'!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)', html)
    if match:
        lng = float(match.group(1))
        lat = float(match.group(2))
        if -60 < lat < 10 and -80 < lng < -30:  # Brasil
            return lat, lng
    # Padrao q=lat,lng
    match = re.search(r'maps[^"]*[?&]q=(-?\d+\.\d+),(-?\d+\.\d+)', html)
    if match:
        lat = float(match.group(1))
        lng = float(match.group(2))
        if -60 < lat < 10 and -80 < lng < -30:
            return lat, lng
    return None, None


def extrair_cidade_estado(texto, max_chars=3000):
    UFS = {"AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"}
    texto_topo = texto[:max_chars]
    for pattern in [
        r"([A-ZÀ-Ú][a-zà-ú]+(?:[ \t]+(?:de|do|da|dos|das|e|[A-ZÀ-Ú][a-zà-ú]+))*)[ \t]*/[ \t]*([A-Z]{2})",
        r"([A-ZÀ-Ú][a-zà-ú]+(?:[ \t]+(?:de|do|da|dos|das|e|[A-ZÀ-Ú][a-zà-ú]+))*)[ \t]*[-–][ \t]*([A-Z]{2})\b",
    ]:
        matches = re.findall(pattern, texto_topo)
        for cidade, uf in matches:
            cidade = cidade.strip()
            if uf in UFS and len(cidade) >= 3:
                return (cidade, uf)
    return None


def detectar_fase(texto, soup=None):
    """Detecta fase do empreendimento."""
    FASES = [
        (["breve lançamento", "breve lancamento", "futuro lançamento", "futuro lancamento"], "Breve Lançamento"),
        (["lançamento", "lancamento"], "Lançamento"),
        (["em obra", "em construção", "em construcao", "obras em andamento", "obra em andamento"], "Em Construção"),
        (["100% vendido"], "100% Vendido"),
        (["pronto para morar", "pronto para entregar", "imóvel pronto", "imovel pronto", "entregue", "entregues"], "Pronto"),
    ]

    def match_fase(t):
        t = t.lower()
        for keywords, fase in FASES:
            for kw in keywords:
                if kw in t:
                    return fase
        return None

    if soup:
        for sel in ['span.status', '.status', '.badge', '.fase', '[class*="status"]', '[class*="fase"]', '[class*="estagio"]']:
            for el in soup.select(sel):
                fase = match_fase(el.get_text())
                if fase:
                    return fase

    if soup:
        main_soup = soup.find('main') or soup.find('article') or soup.find(id='content') or soup.find(class_='content')
        if main_soup:
            fase = match_fase(main_soup.get_text(separator="\n", strip=True))
            if fase:
                return fase

    texto_lower = texto.lower()
    for keywords, fase in FASES:
        for kw in keywords:
            if kw in texto_lower[:500]:
                return fase
    for keywords, fase in FASES:
        if fase in ("Breve Lançamento", "Lançamento"):
            continue
        for kw in keywords:
            if kw in ("vendido", "entregue", "entregues"):
                continue
            if kw in texto_lower:
                return fase
    return None


def extrair_itens_lazer(texto):
    termos = [
        "piscina", "churrasqueira", "fitness", "academia", "playground",
        "brinquedoteca", "salão de festas", "salao de festas", "pet care",
        "pet place", "coworking", "bicicletário", "bicicletario", "quadra",
        "delivery", "horta", "lavanderia", "rooftop", "sauna", "spa",
        "gourmet", "salão de jogos", "salao de jogos",
    ]
    itens = set()
    texto_lower = texto.lower()
    for t in termos:
        if t in texto_lower:
            itens.add(t.title())
    return sorted(itens)


# ============================================================
# SCRAPING VIA WP API
# ============================================================

def fetch_api(url, logger, params=None):
    """Fetch JSON from WP REST API."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"API status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro API {url}: {e}")
        return None


def fetch_html(url, logger):
    try:
        resp = requests.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"Status {resp.status_code} para {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao acessar {url}: {e}")
        return None


def fetch_taxonomies(config, logger):
    """Pre-fetch taxonomy terms (cidade, estagio) for mapping IDs to names."""
    tax_map = {}
    for tax_name, tax_url in config.get("taxonomies", {}).items():
        terms = {}
        page = 1
        while True:
            data = fetch_api(tax_url, logger, params={"per_page": 100, "page": page})
            if not data:
                break
            for item in data:
                terms[item["id"]] = item["name"]
            if len(data) < 100:
                break
            page += 1
            time.sleep(0.5)
        tax_map[tax_name] = terms
        logger.info(f"  Taxonomia '{tax_name}': {len(terms)} termos")
    return tax_map


UFS_SLUG = {
    "ac": "AC", "al": "AL", "am": "AM", "ap": "AP", "ba": "BA", "ce": "CE",
    "df": "DF", "es": "ES", "go": "GO", "ma": "MA", "mg": "MG", "ms": "MS",
    "mt": "MT", "pa": "PA", "pb": "PB", "pe": "PE", "pi": "PI", "pr": "PR",
    "rj": "RJ", "rn": "RN", "ro": "RO", "rr": "RR", "rs": "RS", "sc": "SC",
    "se": "SE", "sp": "SP", "to": "TO",
}


def extrair_localizacao_de_classes(class_list):
    """
    Extrai cidade e estado das classes CSS do post WP.
    Classes tipo: localizacao-sp, localizacao-contagem, localizacao-jundiai
    """
    estado = None
    cidades = []
    for cls in class_list:
        if not cls.startswith("localizacao-"):
            continue
        valor = cls.replace("localizacao-", "")
        if valor in UFS_SLUG:
            estado = UFS_SLUG[valor]
        else:
            # Converter slug para nome de cidade
            cidade = valor.replace("-", " ").title()
            # Correcoes de nomes compostos
            cidade = (cidade
                      .replace(" De ", " de ")
                      .replace(" Do ", " do ")
                      .replace(" Da ", " da ")
                      .replace(" Dos ", " dos ")
                      .replace(" Das ", " das ")
                      .replace(" E ", " e "))
            cidades.append(cidade)
    # Retorna a primeira cidade encontrada (a mais especifica)
    cidade = cidades[0] if cidades else None
    return cidade, estado


def coletar_posts_api(config, logger):
    """Coleta todos os posts via WP REST API com paginacao."""
    posts = []
    page = 1
    per_page = config.get("per_page", 50)
    api_url = config["api_endpoint"]

    while True:
        logger.info(f"  API pagina {page}...")
        data = fetch_api(api_url, logger, params={"per_page": per_page, "page": page})
        if not data:
            break
        posts.extend(data)
        if len(data) < per_page:
            break
        page += 1
        time.sleep(1)

    logger.info(f"  Total posts da API: {len(posts)}")
    return posts


def extrair_dados_pagina(html, url, config, logger):
    """Extrai dados detalhados de uma pagina de empreendimento."""
    soup = BeautifulSoup(html, "html.parser")
    texto = soup.get_text(separator="\n", strip=True)
    dados = {}

    # Status/fase - site-specific selectors first
    for sel in config.get("status_selectors", []):
        for el in soup.select(sel):
            t = el.get_text(strip=True).lower()
            if any(kw in t for kw in ["lançamento", "lancamento", "construção", "construcao",
                                       "obra", "pronto", "vendido", "entregue"]):
                fase = detectar_fase(t)
                if fase:
                    dados["fase"] = fase
                    break
        if "fase" in dados:
            break

    if "fase" not in dados:
        fase = detectar_fase(texto, soup)
        if fase:
            dados["fase"] = fase

    # Parsers regex
    for campo, cfg in config.get("parsers", {}).items():
        pattern = cfg["pattern"]
        matches = re.findall(pattern, texto, re.IGNORECASE)
        if matches:
            vistos = set()
            unicos = []
            for m in matches:
                n = str(m).strip().lower()
                if n and n not in vistos:
                    vistos.add(n)
                    unicos.append(str(m).strip())
            valor = " | ".join(unicos) if len(unicos) > 1 else unicos[0]

            if campo in ("total_unidades",):
                try:
                    dados[campo] = int(re.search(r"\d+", valor).group())
                except (ValueError, AttributeError):
                    pass
            else:
                dados[campo] = valor

    # Coordenadas do Google Maps embed
    lat, lng = extrair_coordenadas_gmaps(html)
    if lat and lng:
        dados["latitude"] = lat
        dados["longitude"] = lng

    # Preco
    preco = extrair_preco(texto)
    if preco:
        dados["preco_a_partir"] = preco

    # Cidade/estado do texto
    cidade_estado = extrair_cidade_estado(texto)
    if cidade_estado:
        dados["cidade"] = cidade_estado[0]
        dados["estado"] = cidade_estado[1]

    # Metragens numericas
    metragens = dados.get("metragens_descricao", "")
    if metragens:
        nums = []
        for match in re.finditer(r"(\d+(?:[.,]\d+)?)\s*[Aa]\s*(\d+(?:[.,]\d+)?)\s*m[²2]", metragens):
            for g in [match.group(1), match.group(2)]:
                v = float(g.replace(",", "."))
                if 15.0 <= v <= 500.0:
                    nums.append(v)
        for match in re.finditer(r"(\d+(?:[.,]\d+)?)\s*m[²2]", metragens):
            v = float(match.group(1).replace(",", "."))
            if 15.0 <= v <= 500.0:
                nums.append(v)
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)

    # Itens de lazer
    itens = extrair_itens_lazer(texto)
    if itens:
        dados["itens_lazer"] = " | ".join(itens)

    # Atributos binarios
    dados.update(detectar_atributos_binarios(texto))

    return dados


def processar_empresa(empresa_key, atualizar=False):
    config = EMPRESAS[empresa_key]
    logger = setup_logger(empresa_key)
    nome_banco = config["nome_banco"]

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper WP API: {nome_banco}")
    logger.info(f"Banco atual: {contar_empreendimentos(nome_banco)} registros")
    logger.info("=" * 60)

    # Fetch taxonomy mappings
    tax_map = fetch_taxonomies(config, logger)

    # Fetch all posts via API
    posts = coletar_posts_api(config, logger)
    if not posts:
        logger.warning("Nenhum post encontrado via API!")
        return

    novos = 0
    atualizados = 0
    erros = 0

    for i, post in enumerate(posts):
        # Extract basic data from API response
        title = post.get("title", {}).get("rendered", "").strip()
        title = re.sub(r"<[^>]+>", "", title)  # Remove HTML tags
        slug = post.get("slug", "")
        link = post.get("link", "")

        if not title or not link:
            continue

        logger.info(f"[{i+1}/{len(posts)}] {title}")

        # Check if already exists
        if not atualizar and empreendimento_existe(nome_banco, title):
            logger.info(f"  Ja existe, pulando")
            continue

        # Build base data
        dados = {
            "empresa": nome_banco,
            "nome": title,
            "slug": slug,
            "url_fonte": link,
            "estado": config.get("estado_default", "SP"),
            "data_coleta": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Extrair localizacao das classes CSS (VIC e similares)
        if config.get("extrair_localizacao_classes"):
            class_list = post.get("class_list", [])
            cidade_cls, estado_cls = extrair_localizacao_de_classes(class_list)
            if cidade_cls:
                dados["cidade"] = cidade_cls
            if estado_cls:
                dados["estado"] = estado_cls

        # Map taxonomy terms
        if "cidade" in tax_map:
            cidade_ids = post.get("cidade", [])
            if cidade_ids:
                cidade_nome = tax_map["cidade"].get(cidade_ids[0])
                if cidade_nome:
                    dados["cidade"] = cidade_nome

        if "estagio" in tax_map:
            estagio_ids = post.get("estagio", [])
            if estagio_ids:
                estagio_nome = tax_map["estagio"].get(estagio_ids[0])
                if estagio_nome:
                    fase = detectar_fase(estagio_nome)
                    if fase:
                        dados["fase"] = fase

        # Default cidade
        if not dados.get("cidade") and config.get("cidade_default"):
            dados["cidade"] = config["cidade_default"]

        # Fetch individual page for detailed data
        time.sleep(DELAY)
        html = fetch_html(link, logger)
        if html:
            page_data = extrair_dados_pagina(html, link, config, logger)

            # Filtrar endereco da sede (se configurado)
            endereco_sede = config.get("endereco_sede")
            if endereco_sede and page_data.get("endereco"):
                if endereco_sede.lower() in page_data["endereco"].lower():
                    logger.info(f"  Ignorando endereco da sede: {page_data['endereco']}")
                    page_data.pop("endereco", None)
                    # Coordenadas vindas dessa pagina provavelmente sao da sede tambem
                    page_data.pop("latitude", None)
                    page_data.pop("longitude", None)

            # Merge page_data into dados (page_data wins for non-None values)
            for k, v in page_data.items():
                if v is not None:
                    dados[k] = v

        # Insert or update
        try:
            if atualizar and empreendimento_existe(nome_banco, title):
                atualizar_empreendimento(nome_banco, title, dados)
                atualizados += 1
                logger.info(f"  Atualizado: {title}")
            else:
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {title}")
        except Exception as e:
            erros += 1
            logger.error(f"  Erro ao salvar {title}: {e}")

    logger.info("=" * 60)
    logger.info(f"Concluido {nome_banco}: {novos} novos, {atualizados} atualizados, {erros} erros")
    logger.info(f"Total no banco: {contar_empreendimentos(nome_banco)}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Scraper WP API para incorporadoras")
    parser.add_argument("--empresa", type=str, help="Chave da empresa ou 'todas'")
    parser.add_argument("--atualizar", action="store_true", help="Atualiza registros existentes")
    parser.add_argument("--listar", action="store_true", help="Lista empresas configuradas")
    args = parser.parse_args()

    if args.listar:
        print("\nEmpresas configuradas (WP API):")
        for key, cfg in EMPRESAS.items():
            print(f"  {key:15s} — {cfg['nome_banco']}")
        return

    if not args.empresa:
        parser.print_help()
        return

    if args.empresa == "todas":
        for key in EMPRESAS:
            processar_empresa(key, atualizar=args.atualizar)
    elif args.empresa in EMPRESAS:
        processar_empresa(args.empresa, atualizar=args.atualizar)
    else:
        print(f"Empresa '{args.empresa}' nao encontrada. Use --listar para ver opcoes.")


if __name__ == "__main__":
    main()

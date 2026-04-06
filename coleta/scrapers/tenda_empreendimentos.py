"""
Scraper para Tenda Construtora
================================
Extrai dados da API JSON embutida no HTML de cada página por estado.
A página renderiza `var productsFinal = JSON.parse('[...]')` com todos
os empreendimentos do estado, incluindo lat/lon, preço e detalhes.

Uso:
    python scrapers/tenda_empreendimentos.py
    python scrapers/tenda_empreendimentos.py --limite 5
    python scrapers/tenda_empreendimentos.py --atualizar
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

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

DELAY = 2
NOME_BANCO = "Tenda"
BASE_URL = "https://www.tenda.com"

ESTADOS = ["sp", "rj", "ba", "pe", "go", "rs", "pr", "ce", "mg", "pb"]

UF_MAP = {
    "sp": "SP", "rj": "RJ", "ba": "BA", "pe": "PE", "go": "GO",
    "rs": "RS", "pr": "PR", "ce": "CE", "mg": "MG", "pb": "PB",
}

ESTAGIO_OBRA_MAP = {
    1: "Breve Lançamento",
    2: "Lançamento",
    3: "Em Construção",
    4: "Em Construção",
    5: "Pronto",
    6: "Lançamento",
}


def setup_logger():
    logger = logging.getLogger("scraper.tenda")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "tenda_empreendimentos.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


def coletar_empreendimentos_por_estado(logger):
    """Coleta empreendimentos via API JSON com parâmetro _estado."""
    API_URL = f"{BASE_URL}/Empreendimento/FiltroSuperiorAvancado"
    API_HEADERS = {
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    todos = []

    for uf_slug in ESTADOS:
        uf = UF_MAP.get(uf_slug, uf_slug.upper())
        logger.info(f"Coletando {uf} via API...")

        try:
            resp = requests.get(API_URL, headers=API_HEADERS,
                              params={"_estado": uf}, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"  API status {resp.status_code} para {uf}")
                continue

            data = resp.json()
            records = data.get("records", [])
            total = data.get("total", len(records))
            logger.info(f"  {uf}: {len(records)} empreendimentos (total declarado: {total})")

            for p in records:
                p["_uf_slug"] = uf_slug
            todos.extend(records)

        except Exception as e:
            logger.error(f"  Erro em {uf}: {e}")

        time.sleep(DELAY)

    # Deduplicar por Id
    vistos = set()
    unicos = []
    for p in todos:
        pid = p.get("Id")
        if pid and pid not in vistos:
            vistos.add(pid)
            unicos.append(p)

    logger.info(f"Total coletados: {len(unicos)} (de {len(todos)} antes de dedup)")
    return unicos


def converter_produto_para_dados(product, logger):
    """Converte um registro da API Tenda para o formato do banco."""
    dados = {
        "empresa": NOME_BANCO,
        "nome": product.get("Nome", ""),
        "slug": product.get("Slug", ""),
        "url_fonte": f"{BASE_URL}/apartamentos-a-venda/{product.get('Estado', 'sp').lower()}/{product.get('Cidade', '').lower().replace(' ', '-')}/{product.get('Slug', '')}",
        "cidade": product.get("Cidade", ""),
        "estado": product.get("Estado", ""),
        "bairro": product.get("Bairro", ""),
        "endereco": product.get("Logradouro", ""),
    }

    # Número do endereço
    if product.get("Numero") and product["Numero"] != "s/n":
        dados["endereco"] = f"{dados['endereco']}, {product['Numero']}"

    # Coordenadas
    lat = product.get("Latitude")
    lng = product.get("Longitude")
    if lat and lng:
        try:
            lat_f = float(str(lat).replace(",", "."))
            lng_f = float(str(lng).replace(",", "."))
            if -60 < lat_f < 10 and -80 < lng_f < -30:
                dados["latitude"] = lat_f
                dados["longitude"] = lng_f
        except (ValueError, TypeError):
            pass

    # Preço
    valor = product.get("ValorImovel", "")
    if valor:
        try:
            dados["preco_a_partir"] = float(str(valor).replace(".", "").replace(",", "."))
        except (ValueError, TypeError):
            pass

    # Renda
    renda = product.get("RendaFamiliar", "")
    if renda:
        try:
            dados["renda_minima"] = float(str(renda).replace(".", "").replace(",", "."))
        except (ValueError, TypeError):
            pass

    # Quartos
    quartos = product.get("Quartos", "")
    if quartos:
        dados["dormitorios_descricao"] = f"{quartos} quartos"

    # Fase/Status
    estagio = product.get("EstagioObra")
    if estagio:
        dados["fase"] = ESTAGIO_OBRA_MAP.get(estagio, "Lançamento")

    status = product.get("StatusEmpreendimento")
    if status:
        status_lower = str(status).lower()
        if "pronto" in status_lower:
            dados["fase"] = "Pronto"
        elif "obra" in status_lower or "construção" in status_lower:
            dados["fase"] = "Em Construção"
        elif "lançamento" in status_lower or "lancamento" in status_lower:
            dados["fase"] = "Lançamento"
        elif "breve" in status_lower:
            dados["fase"] = "Breve Lançamento"

    # Facilidades/Lazer
    facilidades = product.get("Facilidades", [])
    if facilidades and isinstance(facilidades, list):
        itens = [f.get("Nome", "") for f in facilidades if f.get("Nome")]
        if itens:
            dados["itens_lazer"] = " | ".join(itens)

    # Atributos binários do texto completo
    texto_completo = " ".join([
        dados.get("nome", ""),
        dados.get("dormitorios_descricao", ""),
        dados.get("itens_lazer", ""),
        product.get("Descricao", "") or "",
    ])
    binarios = detectar_atributos_binarios(texto_completo)
    dados.update(binarios)

    # MCMV (Tenda é focada em MCMV)
    dados["prog_mcmv"] = 1

    return dados


def download_imagens_api(product, slug, logger):
    """Baixa imagens do empreendimento via dados da API."""
    img_dir = os.path.join(DOWNLOADS_DIR, "tenda", "imagens", slug)
    os.makedirs(img_dir, exist_ok=True)

    img_urls = set()

    # Fotos fachadas
    for foto in product.get("FotosFachadas", []) or []:
        url = foto.get("Foto", "")
        if url:
            img_urls.add(("fachada", url))

    # Fotos decorados
    for foto in product.get("FotosDecorados", []) or []:
        url = foto.get("Foto", "")
        if url:
            img_urls.add(("decorado", url))

    # Foto principal
    for key in ["Foto", "Foto_mobile", "Logotipo"]:
        url = product.get(key, "")
        if url and url.startswith("http"):
            img_urls.add(("geral", url))

    count = 0
    for tipo, img_url in list(img_urls)[:30]:
        try:
            fname = f"{slug}_{tipo}_{re.sub(r'[^w.-]', '_', img_url.split('/')[-1].split('?')[0])}"[:100]
            if not fname or len(fname) < 5:
                continue
            fpath = os.path.join(img_dir, fname)
            if os.path.exists(fpath):
                continue
            resp = requests.get(img_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 5000:
                with open(fpath, "wb") as f:
                    f.write(resp.content)
                count += 1
        except Exception:
            continue

    if count:
        logger.info(f"  {count} imagens baixadas para {slug}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limite", type=int, default=0, help="Limite de empreendimentos")
    parser.add_argument("--atualizar", action="store_true", help="Atualizar existentes")
    args = parser.parse_args()

    logger = setup_logger()
    total_antes = contar_empreendimentos(NOME_BANCO)

    logger.info("=" * 60)
    logger.info(f"Iniciando scraper: {NOME_BANCO}")
    logger.info(f"Banco atual: {total_antes} registros de {NOME_BANCO}")
    logger.info("=" * 60)

    # Coletar todos os empreendimentos via API JSON
    products = coletar_empreendimentos_por_estado(logger)
    logger.info(f"Total coletados da API: {len(products)}")

    inseridos = 0
    atualizados = 0
    erros = 0
    processados = 0

    for i, product in enumerate(products, 1):
        if args.limite and processados >= args.limite:
            break

        nome = product.get("Nome", "")
        if not nome or len(nome) < 3:
            continue

        try:
            dados = converter_produto_para_dados(product, logger)

            if empreendimento_existe(NOME_BANCO, dados["nome"]):
                if args.atualizar:
                    atualizar_empreendimento(dados)
                    atualizados += 1
                continue

            inserir_empreendimento(dados)
            inseridos += 1
            processados += 1

            fase_str = dados.get("fase", "N/A")
            cidade_str = dados.get("cidade", "N/A")
            preco_str = f"R${dados.get('preco_a_partir', 0):,.0f}" if dados.get("preco_a_partir") else "N/A"
            logger.info(f"  [{i}/{len(products)}] {dados['nome']} | {cidade_str}/{dados.get('estado','')} | {fase_str} | {preco_str}")

            download_imagens_api(product, dados.get("slug", ""), logger)

        except Exception as e:
            logger.error(f"  Erro em {nome}: {e}")
            erros += 1

    total_depois = contar_empreendimentos(NOME_BANCO)
    logger.info("=" * 60)
    logger.info(f"RELATORIO - {NOME_BANCO}")
    logger.info(f"  Total da API: {len(products)}")
    logger.info(f"  Novos inseridos: {inseridos}")
    logger.info(f"  Atualizados: {atualizados}")
    logger.info(f"  Erros: {erros}")
    logger.info(f"  Total no banco: {total_depois}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

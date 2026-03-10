"""
Scraper MRV - Detalhes via API GraphQL
=======================================
Complementa os registros ja existentes no banco com dados detalhados:
metragens, total de unidades, endereco, CEP, coordenadas, itens de lazer,
registro de incorporacao e URLs de imagens.

Endpoint:
  https://mrv.com.br/graphql/execute.json/mrv/properties-details
  ;path=/content/dam/mrv/content-fragments/detalhe-empreendimento/{estado}/{tipo}/{cidade}/{slug}/{slug}

Uso:
    python mrv_detalhes.py
    python mrv_detalhes.py --limite 10
    python mrv_detalhes.py --testar
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import unicodedata
import requests
from html import unescape
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import REQUESTS, LOGS_DIR
from data.database import (
    atualizar_empreendimento,
    detectar_atributos_binarios,
    garantir_coluna,
    get_connection,
)

# ============================================================
# CONFIGURACAO
# ============================================================

EMPRESA_NOME = "MRV"
DELAY = 1.5  # segundos entre requisicoes

API_DETALHE = (
    "https://mrv.com.br/graphql/execute.json/mrv/properties-details"
    ";path=/content/dam/mrv/content-fragments/detalhe-empreendimento"
    "/{estado}/{tipo}/{cidade}/{slug}/{slug}"
)

# Tipos tentados em ordem de probabilidade
TIPOS = ["apartamentos", "casas", "lotes"]

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": "https://mrv.com.br/",
}

# ============================================================
# LOGGING
# ============================================================

os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "mrv_detalhes.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# HELPERS
# ============================================================

def slugify(texto):
    """Converte texto com acentos para slug: 'São Paulo' -> 'sao-paulo'
    Trata tambem o encoding nao-padrao da API MRV: ~u00e9 -> é
    """
    if not texto:
        return ""
    # Trata encoding nao-padrao da API MRV (~u00e3 -> ã, ~u00ed -> í, etc.)
    texto = re.sub(r"~u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), texto)
    # Decodifica HTML entities (&atilde; -> ã)
    texto = unescape(texto)
    # Normaliza unicode (decompoe acentos)
    texto = unicodedata.normalize("NFD", texto)
    # Remove diacriticos (acentos)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    # Minusculas
    texto = texto.lower()
    # Substitui espacos e caracteres especiais por hifen
    texto = re.sub(r"[^a-z0-9]+", "-", texto)
    # Remove hifens nas bordas
    texto = texto.strip("-")
    return texto


def limpar(texto):
    """Decodifica HTML entities e normaliza espacos."""
    if not texto:
        return None
    texto = unescape(str(texto))
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto if texto else None


def extrair_metragens(tipologias):
    """
    Extrai metragens da lista de tipologias.
    Retorna (metragens_descricao, area_min, area_max).
    Ex: tipologias = [{"titulo": "2 Quartos", "areaTotal": ["43.93"]}]
    """
    if not tipologias:
        return None, None, None

    areas = []
    descricoes = []

    for tip in tipologias:
        titulo = limpar(tip.get("titulo", ""))
        area_list = tip.get("areaTotal") or []
        for a in area_list:
            try:
                valor = float(str(a).replace(",", "."))
                areas.append(valor)
                if titulo:
                    descricoes.append(f"{titulo}: {valor}m²")
                else:
                    descricoes.append(f"{valor}m²")
            except (ValueError, TypeError):
                continue

    if not areas:
        return None, None, None

    metragens_str = " | ".join(descricoes) if descricoes else None
    return metragens_str, min(areas), max(areas)


def extrair_lazer(diferenciais):
    """
    Extrai itens de lazer e diferenciais da lista diferenciaisNew.
    Retorna string com itens separados por |.
    """
    if not diferenciais:
        return None

    itens = []
    for d in diferenciais:
        titulo = limpar(d.get("titulo", ""))
        if titulo:
            itens.append(titulo)

    return " | ".join(itens) if itens else None


def extrair_imagem_principal(imagens):
    """Retorna URL da imagem de vitrine com destaque=true, ou primeira disponivel."""
    if not imagens:
        return None

    for bloco in imagens:
        if not isinstance(bloco, dict):
            continue
        for key, lista in bloco.items():
            if not isinstance(lista, list):
                continue
            for img in lista:
                if isinstance(img, dict) and img.get("destaque") == "true":
                    return img.get("urlImagem")

    # Fallback: primeira imagem de qualquer bloco
    for bloco in imagens:
        if not isinstance(bloco, dict):
            continue
        for key, lista in bloco.items():
            if not isinstance(lista, list):
                continue
            for img in lista:
                if isinstance(img, dict) and img.get("urlImagem"):
                    return img.get("urlImagem")

    return None


# ============================================================
# BUSCA NA API
# ============================================================

def buscar_detalhe(estado_slug, cidade_slug, slug):
    """
    Tenta buscar detalhe do empreendimento tentando cada tipo (apartamentos, casas, lotes).
    Retorna (item, tipo_encontrado) ou (None, None) se nao encontrar.
    """
    for tipo in TIPOS:
        url = API_DETALHE.format(
            estado=estado_slug,
            tipo=tipo,
            cidade=cidade_slug,
            slug=slug,
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            dados = resp.json()
            items = dados.get("data", {}).get("empreendimentosList", {}).get("items", [])
            if items:
                return items[0], tipo
        except requests.RequestException as e:
            logger.warning(f"Erro ao buscar {slug} ({tipo}): {e}")
            continue
        except (json.JSONDecodeError, KeyError):
            continue

    return None, None


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_detalhe(item, nome_empreendimento):
    """
    Extrai campos de detalhe do item da API e retorna dict para atualizar_empreendimento.
    """
    # Endereco e localizacao
    endereco = limpar(item.get("endereco"))
    cep = limpar(item.get("cep"))
    latitude = item.get("latitude")
    longitude = item.get("longitude")

    try:
        latitude = float(latitude) if latitude else None
        longitude = float(longitude) if longitude else None
    except (ValueError, TypeError):
        latitude = longitude = None

    # Unidades e area
    total_unidades = item.get("totalUnidades")
    try:
        total_unidades = int(total_unidades) if total_unidades else None
    except (ValueError, TypeError):
        total_unidades = None

    total_garagem = limpar(item.get("totalGaragem"))
    area_terreno = item.get("areaTotalEmpreendimento")
    try:
        area_terreno = float(str(area_terreno).replace(",", ".")) if area_terreno else None
    except (ValueError, TypeError):
        area_terreno = None

    # Metragens via tipologias
    tipologias = item.get("tipologias") or []
    metragens_descricao, area_min, area_max = extrair_metragens(tipologias)

    # Lazer
    diferenciais = item.get("diferenciaisNew") or []
    itens_lazer = extrair_lazer(diferenciais)

    # Registro de incorporacao
    ri = limpar(item.get("ri"))

    # URL corrigida
    cidade_raw = limpar(item.get("cidade", ""))
    estado_raw = slugify(limpar(item.get("estado", "")) or "")
    cidade_slug = slugify(cidade_raw or "")
    tipo_imovel = (item.get("tipoImovel") or "").lower()
    slug_item = item.get("_path", "").strip("/").split("/")[-1] if item.get("_path") else None

    url_fonte = None
    if estado_raw and cidade_slug and tipo_imovel and slug_item:
        url_fonte = f"https://mrv.com.br/imoveis/{estado_raw}/{cidade_slug}/{tipo_imovel}-{slug_item}"

    # Imagem principal
    imagem_url = extrair_imagem_principal(item.get("imagens") or [])

    # Texto completo para atributos binarios
    descricao_txt = ""
    desc = item.get("descricao") or {}
    if isinstance(desc, dict):
        descricao_txt = desc.get("plaintext", "") or ""

    lazer_txt = itens_lazer or ""
    texto_completo = f"{descricao_txt} {lazer_txt}"
    atributos = detectar_atributos_binarios(texto_completo)

    # Monta registro de atualizacao
    registro = {
        "endereco": endereco,
        "total_unidades": total_unidades,
        "numero_vagas": total_garagem,
        "area_terreno_m2": area_terreno,
        "metragens_descricao": metragens_descricao,
        "area_min_m2": area_min,
        "area_max_m2": area_max,
        "itens_lazer": itens_lazer,
        "registro_incorporacao": ri,
    }

    # Campos extras que precisam ser garantidos no banco
    extras = {
        "cep": cep,
        "latitude": latitude,
        "longitude": longitude,
        "imagem_url": imagem_url,
        "tipo_imovel": tipo_imovel,
    }

    # Garante colunas extras
    for col, val in extras.items():
        if val is not None:
            garantir_coluna(col, "TEXT")
            registro[col] = val

    # URL corrigida
    if url_fonte:
        registro["url_fonte"] = url_fonte

    # Atributos binarios
    registro.update(atributos)

    # Remove Nones
    registro = {k: v for k, v in registro.items() if v is not None}

    return registro


# ============================================================
# BUSCA REGISTROS NO BANCO
# ============================================================

def buscar_registros_mrv(limite=None):
    """Retorna lista de (nome, cidade, estado, slug) dos registros MRV no banco."""
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT nome, cidade, estado, slug FROM empreendimentos WHERE empresa = ?"
    if limite:
        query += f" LIMIT {limite}"
    rows = cursor.execute(query, (EMPRESA_NOME,)).fetchall()
    conn.close()
    return rows


# ============================================================
# MAIN
# ============================================================

def scrape_detalhes(limite=None):
    registros = buscar_registros_mrv(limite=limite)
    total = len(registros)
    logger.info(f"Iniciando coleta de detalhes | {total} empreendimentos")

    atualizados = 0
    nao_encontrados = 0
    erros = 0

    for i, row in enumerate(registros, 1):
        nome, cidade, estado, slug = row

        if not slug:
            logger.warning(f"[{i}/{total}] Sem slug: {nome}")
            nao_encontrados += 1
            continue

        estado_slug = slugify(estado or "")
        cidade_slug = slugify(cidade or "")

        logger.info(f"[{i}/{total}] {nome} | {cidade} | {estado_slug}")

        item, tipo = buscar_detalhe(estado_slug, cidade_slug, slug)

        if not item:
            logger.warning(f"  Nao encontrado em nenhum tipo: {slug}")
            nao_encontrados += 1
            time.sleep(DELAY)
            continue

        try:
            registro = processar_detalhe(item, nome)
            atualizar_empreendimento(EMPRESA_NOME, nome, registro)
            atualizados += 1
            logger.info(
                f"  OK | unidades={registro.get('total_unidades')} "
                f"area={registro.get('area_min_m2')}-{registro.get('area_max_m2')}m²"
            )
        except Exception as e:
            logger.error(f"  Erro ao processar {nome}: {e}")
            erros += 1

        time.sleep(DELAY)

    logger.info(
        f"\n=== MRV DETALHES CONCLUIDO ==="
        f"\n  Atualizados: {atualizados}"
        f"\n  Nao encontrados: {nao_encontrados}"
        f"\n  Erros: {erros}"
        f"\n  Total processado: {total}"
    )


def testar():
    logger.info("=== MODO TESTE: 3 empreendimentos ===")
    registros = buscar_registros_mrv(limite=3)

    for row in registros:
        nome, cidade, estado, slug = row
        estado_slug = slugify(estado or "")
        cidade_slug = slugify(cidade or "")

        print(f"\n{nome} | {cidade} | {estado_slug}")
        item, tipo = buscar_detalhe(estado_slug, cidade_slug, slug)

        if not item:
            print("  NAO ENCONTRADO")
            continue

        registro = processar_detalhe(item, nome)
        print(json.dumps(
            {k: v for k, v in registro.items()
             if not k.startswith("lazer_") and not k.startswith("apto_") and not k.startswith("prog_")},
            ensure_ascii=False,
            indent=2
        ))
        print(f"  itens_lazer: {registro.get('itens_lazer', '')[:80]}...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper MRV - Detalhes via API GraphQL")
    parser.add_argument("--limite", type=int, help="Limite de registros a processar")
    parser.add_argument("--testar", action="store_true", help="Modo teste: 3 registros")
    args = parser.parse_args()

    if args.testar:
        testar()
    else:
        scrape_detalhes(limite=args.limite)

"""
Script para geocodificar empreendimentos a partir do endereço completo.
Usa Nominatim (OpenStreetMap) com fallback por CEP e depois por cidade.
Roda uma vez — salva coordenadas no SQLite.
"""

import re
import time
import sqlite3
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen, Request

import pandas as pd

from src.database import SQLITE_PATH, get_sqlite_connection

CACHE_PATH = Path("data") / "geocode_cache.json"
HEADERS = {"User-Agent": "analise-geo-vendas-tenda/1.0"}


def _nominatim_search(query: str) -> tuple[float | None, float | None]:
    """Busca coordenadas no Nominatim."""
    url = f"https://nominatim.openstreetmap.org/search?q={quote(query)}&format=json&limit=1&countrycodes=br"
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None, None


def _extrair_rua(endereco: str) -> str:
    """Extrai nome da rua do endereço SIOPI: 'RUA FULANO, nº 0, ..., CIDADE/SP' → 'RUA FULANO'."""
    if not endereco:
        return ""
    # Pega tudo antes da primeira vírgula
    rua = endereco.split(",")[0].strip()
    return rua


def _extrair_bairro(endereco: str) -> str:
    """Tenta extrair bairro do endereço."""
    if not endereco:
        return ""
    partes = endereco.split(",")
    # Bairro geralmente é a 3ª ou 4ª parte (depois de rua e número)
    for parte in partes[2:]:
        parte = parte.strip()
        # Ignora partes que são CEP, quadra/lote, cidade
        if any(x in parte.upper() for x in ["CEP", "QD", "LT", "/"]):
            continue
        if re.match(r"^\d", parte):
            continue
        if len(parte) > 3:
            return parte
    return ""


def geocodificar_empreendimento(endereco: str, cidade: str, estado: str) -> tuple[float | None, float | None]:
    """
    Tenta geocodificar em 3 níveis:
    1. Rua + bairro + cidade + estado
    2. Rua + cidade + estado
    3. Bairro + cidade + estado (fallback)
    """
    rua = _extrair_rua(endereco)
    bairro = _extrair_bairro(endereco)

    # Tentativa 1: rua + bairro + cidade
    if rua and bairro:
        query = f"{rua}, {bairro}, {cidade}, {estado}, Brasil"
        lat, lon = _nominatim_search(query)
        if lat:
            return lat, lon
        time.sleep(1.1)

    # Tentativa 2: rua + cidade
    if rua:
        query = f"{rua}, {cidade}, {estado}, Brasil"
        lat, lon = _nominatim_search(query)
        if lat:
            return lat, lon
        time.sleep(1.1)

    # Tentativa 3: bairro + cidade
    if bairro:
        query = f"{bairro}, {cidade}, {estado}, Brasil"
        lat, lon = _nominatim_search(query)
        if lat:
            return lat, lon
        time.sleep(1.1)

    # Fallback: só cidade
    query = f"{cidade}, {estado}, Brasil"
    lat, lon = _nominatim_search(query)
    return lat, lon


def main():
    conn = get_sqlite_connection()

    # Lista empreendimentos únicos (1 endereço por empreendimento)
    emprs = pd.read_sql("""
        SELECT empreendimento,
               MIN(endereco_empreendimento) as endereco_empreendimento,
               MIN(cidade) as cidade,
               MIN(estado) as estado
        FROM vendas
        WHERE endereco_empreendimento IS NOT NULL
        GROUP BY empreendimento
    """, conn)

    print(f"Total empreendimentos para geocodificar: {len(emprs)}")

    # Carrega cache
    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())
        print(f"Cache carregado: {len(cache)} entradas")

    geocodificados = 0
    do_cache = 0
    falhas = 0

    for idx, row in emprs.iterrows():
        empr = row["empreendimento"]
        endereco = str(row["endereco_empreendimento"]).strip()
        cidade = str(row["cidade"]).strip()
        estado = str(row["estado"]).strip()

        # Cache key pelo endereço completo (não CEP)
        cache_key = f"addr_{empr}"

        if cache_key in cache:
            lat, lon = cache[cache_key]
            do_cache += 1
        else:
            print(f"  [{idx+1}/{len(emprs)}] {empr[:45]} ({cidade}/{estado})...", end=" ", flush=True)
            lat, lon = geocodificar_empreendimento(endereco, cidade, estado)

            if lat and lon:
                print(f"OK ({lat:.6f}, {lon:.6f})")
                geocodificados += 1
            else:
                print("FALHA")
                falhas += 1

            cache[cache_key] = (lat, lon)
            time.sleep(1.1)

        # Atualiza no banco
        if lat and lon:
            conn.execute("""
                UPDATE vendas SET latitude = ?, longitude = ?
                WHERE empreendimento = ?
            """, (lat, lon, empr))

    conn.commit()
    conn.close()

    # Salva cache
    CACHE_PATH.write_text(json.dumps(cache))

    print(f"\nResultado: {geocodificados} novos, {do_cache} do cache, {falhas} falhas")
    print(f"Cache salvo em {CACHE_PATH}")

    # Estatísticas
    conn2 = get_sqlite_connection()
    total = pd.read_sql("SELECT COUNT(*) as n FROM vendas", conn2).iloc[0, 0]
    com_coords = pd.read_sql(
        "SELECT COUNT(*) as n FROM vendas WHERE latitude IS NOT NULL", conn2
    ).iloc[0, 0]

    # Verificar dispersão
    pontos = pd.read_sql(
        "SELECT DISTINCT latitude, longitude FROM vendas WHERE latitude IS NOT NULL", conn2
    )
    conn2.close()

    print(f"Registros com coordenadas: {com_coords}/{total}")
    print(f"Pontos geográficos únicos: {len(pontos)}")


if __name__ == "__main__":
    main()

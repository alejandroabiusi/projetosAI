"""
Geocodifica empreendimentos salvando APENAS no cache JSON (sem tocar no banco).
Seguro para rodar em paralelo com o Streamlit.
Depois de terminar, rode: python aplicar_geocode_cache.py
"""

import re
import time
import sqlite3
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen, Request

import pandas as pd

from src.database import SQLITE_PATH

CACHE_PATH = Path("data") / "geocode_cache.json"
HEADERS = {"User-Agent": "analise-geo-vendas-tenda/1.0"}


def _nominatim_search(query: str) -> tuple[float | None, float | None]:
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
    if not endereco:
        return ""
    return endereco.split(",")[0].strip()


def _extrair_bairro(endereco: str) -> str:
    if not endereco:
        return ""
    partes = endereco.split(",")
    for parte in partes[2:]:
        parte = parte.strip()
        if any(x in parte.upper() for x in ["CEP", "QD", "LT", "/"]):
            continue
        if re.match(r"^\d", parte):
            continue
        if len(parte) > 3:
            return parte
    return ""


def geocodificar_empreendimento(endereco, cidade, estado):
    rua = _extrair_rua(endereco)
    bairro = _extrair_bairro(endereco)

    if rua and bairro:
        lat, lon = _nominatim_search(f"{rua}, {bairro}, {cidade}, {estado}, Brasil")
        if lat:
            return lat, lon
        time.sleep(1.1)

    if rua:
        lat, lon = _nominatim_search(f"{rua}, {cidade}, {estado}, Brasil")
        if lat:
            return lat, lon
        time.sleep(1.1)

    if bairro:
        lat, lon = _nominatim_search(f"{bairro}, {cidade}, {estado}, Brasil")
        if lat:
            return lat, lon
        time.sleep(1.1)

    lat, lon = _nominatim_search(f"{cidade}, {estado}, Brasil")
    return lat, lon


def main():
    # Lê empreendimentos do banco (read-only, rápido)
    conn = sqlite3.connect(str(SQLITE_PATH), timeout=30)
    emprs = pd.read_sql("""
        SELECT empreendimento,
               MIN(endereco_empreendimento) as endereco_empreendimento,
               MIN(cidade) as cidade,
               MIN(estado) as estado
        FROM vendas
        WHERE endereco_empreendimento IS NOT NULL
          AND latitude IS NULL
        GROUP BY empreendimento
        ORDER BY COUNT(*) DESC
    """, conn)
    conn.close()

    print(f"Empreendimentos sem coordenadas: {len(emprs)}")

    # Carrega cache
    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        print(f"Cache existente: {len(cache)} entradas")

    novos = 0
    ja_cache = 0
    falhas = 0

    for idx, row in emprs.iterrows():
        empr = row["empreendimento"]
        endereco = str(row["endereco_empreendimento"]).strip()
        cidade = str(row["cidade"]).strip()
        estado = str(row["estado"]).strip()

        cache_key = f"addr_{empr}"

        if cache_key in cache and cache[cache_key] and cache[cache_key][0]:
            ja_cache += 1
            continue

        print(f"  [{novos + falhas + 1}/{len(emprs) - ja_cache}] {empr[:50]} ({cidade}/{estado})...",
              end=" ", flush=True)

        lat, lon = geocodificar_empreendimento(endereco, cidade, estado)

        if lat and lon:
            print(f"OK ({lat:.4f}, {lon:.4f})")
            novos += 1
        else:
            print("FALHA")
            falhas += 1

        cache[cache_key] = [lat, lon] if lat else [None, None]

        # Salva cache a cada 10 geocodificações (para não perder progresso)
        if (novos + falhas) % 10 == 0:
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

        time.sleep(1.1)

    # Salva cache final
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    total_com = sum(1 for v in cache.values() if v and len(v) == 2 and v[0])
    print(f"\nResultado: {novos} novos, {ja_cache} já no cache, {falhas} falhas")
    print(f"Cache total: {len(cache)} entradas ({total_com} com coordenadas)")
    print(f"\nQuando terminar, rode: python aplicar_geocode_cache.py")


if __name__ == "__main__":
    main()

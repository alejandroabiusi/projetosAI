"""
Re-geocodifica empreendimentos usando CEP (mais preciso que endereço).
Usa Nominatim buscando por CEP brasileiro.
Salva no cache e atualiza o banco diretamente.
"""

import json
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen, Request

import pandas as pd

from src.database import SQLITE_PATH

CACHE_PATH = Path("data") / "geocode_cep_cache.json"
HEADERS = {"User-Agent": "analise-geo-vendas-tenda/1.0"}


def _geocode_cep(cep: str) -> tuple[float | None, float | None]:
    """Geocodifica um CEP brasileiro via Nominatim."""
    # Formata CEP: 04194260 -> 04194-260
    cep_fmt = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else cep

    # Tentativa 1: busca pelo CEP formatado
    query = f"{cep_fmt}, Brasil"
    url = f"https://nominatim.openstreetmap.org/search?q={quote(query)}&format=json&limit=1&countrycodes=br"
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass

    time.sleep(1.1)

    # Tentativa 2: busca pelo CEP sem formatação (postalcode)
    url2 = f"https://nominatim.openstreetmap.org/search?postalcode={cep_fmt}&country=br&format=json&limit=1"
    try:
        req = Request(url2, headers=HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass

    return None, None


def main():
    conn = sqlite3.connect(str(SQLITE_PATH), timeout=30)

    # Lista CEPs únicos de empreendimentos
    df = pd.read_sql("""
        SELECT cep_empreendimento as cep,
               COUNT(DISTINCT empreendimento) as n_emprs,
               COUNT(*) as n_vendas
        FROM vendas
        WHERE cep_empreendimento IS NOT NULL
          AND cep_empreendimento != ''
        GROUP BY cep_empreendimento
        ORDER BY n_vendas DESC
    """, conn)

    print(f"CEPs únicos para geocodificar: {len(df)}")
    print(f"Cobrindo {df['n_emprs'].sum()} empreendimentos, {df['n_vendas'].sum()} vendas")

    # Carrega cache
    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        print(f"Cache existente: {len(cache)} entradas")

    novos = 0
    ja_cache = 0
    falhas = 0
    atualizados = 0

    for idx, row in df.iterrows():
        cep = row["cep"]

        if cep in cache and cache[cep] and cache[cep][0]:
            lat, lon = cache[cep]
            ja_cache += 1
        elif cep in cache:
            # Já tentou e falhou
            ja_cache += 1
            continue
        else:
            print(f"  [{novos + falhas + 1}/{len(df) - ja_cache}] CEP {cep} ({row['n_emprs']} emprs, {row['n_vendas']} vendas)...",
                  end=" ", flush=True)

            lat, lon = _geocode_cep(cep)

            if lat and lon:
                print(f"OK ({lat:.6f}, {lon:.6f})")
                novos += 1
            else:
                print("FALHA")
                falhas += 1

            cache[cep] = [lat, lon] if lat else [None, None]

            # Salva cache a cada 20
            if (novos + falhas) % 20 == 0:
                CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

            time.sleep(1.1)

        # Atualiza coordenadas no banco
        if lat and lon:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE vendas SET latitude = ?, longitude = ? WHERE cep_empreendimento = ?",
                (lat, lon, cep)
            )
            atualizados += cursor.rowcount

        # Commit a cada 100 CEPs
        if (idx + 1) % 100 == 0:
            conn.commit()

    conn.commit()

    # Salva cache final
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    # Estatísticas
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM vendas WHERE latitude IS NOT NULL")
    total_com = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM vendas")
    total = cursor.fetchone()[0]

    conn.close()

    print(f"\nResultado: {novos} novos, {ja_cache} do cache, {falhas} falhas")
    print(f"Registros atualizados: {atualizados}")
    print(f"Registros com coordenadas: {total_com}/{total}")


if __name__ == "__main__":
    main()

"""
Re-geocodifica empreendimentos que tiveram coordenadas invalidadas.
Usa endereço + cidade como query no Nominatim (mesma abordagem do script original).
"""

import json
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen, Request

import pandas as pd

CACHE_PATH = Path("data") / "geocode_cache.json"
HEADERS = {"User-Agent": "analise-geo-vendas-tenda/1.0"}


def _geocode_endereco(endereco: str, cidade: str, estado: str) -> tuple:
    """Geocodifica usando endereço completo via Nominatim."""
    # Limpa endereço: pega só rua + bairro
    parts = endereco.split(",")
    rua = parts[0].strip() if parts else ""
    # Busca bairro no endereço
    bairro = ""
    for p in parts:
        p = p.strip()
        if not any(x in p.lower() for x in ["cep", "bl.", "ap", "n\u00ba", "n "]):
            if p != rua and len(p) > 3:
                bairro = p
                break

    queries = [
        f"{rua}, {bairro}, {cidade}, {estado}, Brasil" if bairro else f"{rua}, {cidade}, {estado}, Brasil",
        f"{bairro}, {cidade}, {estado}, Brasil" if bairro else f"{cidade}, {estado}, Brasil",
        f"{cidade}, {estado}, Brasil",
    ]

    for query in queries:
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

    return None, None


def main():
    conn = sqlite3.connect("data/vendas.db", timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # CEPs sem coordenadas
    df = pd.read_sql("""
        SELECT DISTINCT cep_empreendimento as cep,
               endereco_empreendimento as endereco,
               cidade, estado,
               COUNT(*) as vendas
        FROM vendas
        WHERE latitude IS NULL AND cep_empreendimento IS NOT NULL
        GROUP BY cep_empreendimento, cidade, estado
        ORDER BY vendas DESC
    """, conn)

    print(f"CEPs para re-geocodificar: {len(df)}")
    print(f"Vendas afetadas: {df['vendas'].sum()}")

    # Carrega cache
    cache = json.load(open(CACHE_PATH, "r", encoding="utf-8")) if CACHE_PATH.exists() else {}

    novos = 0
    falhas = 0
    atualizados = 0

    for idx, row in df.iterrows():
        cep = row["cep"]
        cidade = row["cidade"]
        estado = row["estado"]
        key = f"{cep}_{cidade}_{estado}"

        if key in cache and cache[key] and cache[key][0]:
            lat, lon = cache[key]
            # Aplicar ao banco
            cur = conn.cursor()
            cur.execute(
                "UPDATE vendas SET latitude = ?, longitude = ? WHERE cep_empreendimento = ? AND cidade = ? AND estado = ?",
                (lat, lon, cep, cidade, estado)
            )
            atualizados += cur.rowcount
            continue

        endereco = row["endereco"] or ""
        print(f"  [{novos + falhas + 1}/{len(df)}] {cep} ({cidade}/{estado}, {row['vendas']} vendas)...",
              end=" ", flush=True)

        lat, lon = _geocode_endereco(endereco, cidade, estado)

        if lat and lon:
            print(f"OK ({lat:.4f}, {lon:.4f})")
            cache[key] = [lat, lon]
            novos += 1
            cur = conn.cursor()
            cur.execute(
                "UPDATE vendas SET latitude = ?, longitude = ? WHERE cep_empreendimento = ? AND cidade = ? AND estado = ?",
                (lat, lon, cep, cidade, estado)
            )
            atualizados += cur.rowcount
        else:
            print("FALHA")
            cache[key] = [None, None]
            falhas += 1

        # Salva cache e commit a cada 50
        if (novos + falhas) % 50 == 0:
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
            conn.commit()

    conn.commit()
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    # Stats
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM vendas WHERE latitude IS NOT NULL")
    total_with = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM vendas")
    total = cur.fetchone()[0]
    conn.close()

    print(f"\nResultado: {novos} novos, {falhas} falhas, {atualizados} registros atualizados")
    print(f"Coordenadas: {total_with}/{total} ({100*total_with/total:.1f}%)")


if __name__ == "__main__":
    main()

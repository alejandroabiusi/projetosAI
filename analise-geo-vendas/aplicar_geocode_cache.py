"""
Aplica coordenadas do geocode_cache.json no banco SQLite.
Rode APÓS geocodificar_cache_only.py (pode rodar com Streamlit parado ou rodando).
"""

import json
import sqlite3
from pathlib import Path

from src.database import SQLITE_PATH

CACHE_PATH = Path("data") / "geocode_cache.json"


def main():
    if not CACHE_PATH.exists():
        print("Cache não encontrado!")
        return

    cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    com_coords = {k: v for k, v in cache.items() if v and len(v) == 2 and v[0] and v[1]}
    print(f"Cache: {len(cache)} entradas, {len(com_coords)} com coordenadas")

    conn = sqlite3.connect(str(SQLITE_PATH), timeout=60)
    cursor = conn.cursor()

    updates = 0
    for k, v in com_coords.items():
        if k.startswith("addr_"):
            empr_name = k[5:]
            cursor.execute(
                "UPDATE vendas SET latitude = ?, longitude = ? "
                "WHERE empreendimento = ? AND latitude IS NULL",
                (v[0], v[1], empr_name)
            )
        else:
            # CEP-based key
            cep = k.split("_")[0]
            cursor.execute(
                "UPDATE vendas SET latitude = ?, longitude = ? "
                "WHERE cep_empreendimento = ? AND latitude IS NULL",
                (v[0], v[1], cep)
            )
        updates += cursor.rowcount

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM vendas WHERE latitude IS NOT NULL")
    rows_com = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM vendas")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT empreendimento) FROM vendas WHERE latitude IS NOT NULL")
    emprs_com = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT empreendimento) FROM vendas WHERE latitude IS NULL")
    emprs_sem = cursor.fetchone()[0]

    print(f"Registros atualizados: {updates}")
    print(f"Registros com coordenadas: {rows_com}/{total}")
    print(f"Empreendimentos: {emprs_com} com coords, {emprs_sem} sem coords")
    conn.close()


if __name__ == "__main__":
    main()

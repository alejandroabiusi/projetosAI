"""
Script para mapear CEPs dos clientes para bairros usando a API ViaCEP.
Roda uma vez — salva mapeamento CEP → bairro/cidade num JSON local.
"""

import json
import re
import sqlite3
import time
from pathlib import Path
from urllib.request import urlopen

import pandas as pd

from src.database import get_sqlite_connection

CACHE_PATH = Path("data") / "cep_bairros.json"


def _limpar_cep(cep: str) -> str:
    """Remove formatação do CEP: '12043-341' → '12043341'."""
    return re.sub(r"\D", "", str(cep)).strip()


def _buscar_cep(cep: str) -> dict | None:
    """Busca dados do CEP na API OpenCEP (sem token, sem bloqueio)."""
    cep_limpo = _limpar_cep(cep)
    if len(cep_limpo) != 8:
        return None
    try:
        url = f"https://opencep.com/v1/{cep_limpo}"
        with urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            if "erro" in data:
                return None
            return {
                "bairro": data.get("bairro", ""),
                "cidade": data.get("localidade", ""),
                "uf": data.get("uf", ""),
            }
    except Exception:
        return None


def main():
    conn = get_sqlite_connection()

    # Lista CEPs únicos dos clientes
    df = pd.read_sql("""
        SELECT DISTINCT cep_cliente
        FROM vendas
        WHERE cep_cliente IS NOT NULL
    """, conn)
    conn.close()

    # Limpa CEPs
    df["cep_limpo"] = df["cep_cliente"].apply(_limpar_cep)
    df = df[df["cep_limpo"].str.len() == 8]
    ceps_unicos = df["cep_limpo"].unique()

    print(f"Total CEPs únicos para mapear: {len(ceps_unicos)}")

    # Carrega cache existente
    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        print(f"Cache carregado: {len(cache)} entradas")

    novos = 0
    falhas = 0
    do_cache = 0
    total = len(ceps_unicos)

    for i, cep in enumerate(ceps_unicos):
        if cep in cache:
            do_cache += 1
            continue

        resultado = _buscar_cep(cep)

        if resultado and resultado["bairro"]:
            cache[cep] = resultado
            novos += 1
            if novos % 100 == 0:
                print(f"  [{i+1}/{total}] {novos} novos, {falhas} falhas...")
                # Salva cache parcial a cada 100
                CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        else:
            # CEP genérico ou inválido — salva sem bairro
            if resultado:
                cache[cep] = resultado
            else:
                cache[cep] = {"bairro": "", "cidade": "", "uf": ""}
            falhas += 1

        # ViaCEP — delay conservador para evitar bloqueio
        time.sleep(0.6)

    # Salva cache final
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    # Estatísticas
    com_bairro = sum(1 for v in cache.values() if v.get("bairro"))
    print(f"\nResultado: {novos} novos, {do_cache} do cache, {falhas} sem bairro")
    print(f"Total mapeados: {len(cache)} CEPs ({com_bairro} com bairro)")
    print(f"Cache salvo em {CACHE_PATH}")


if __name__ == "__main__":
    main()

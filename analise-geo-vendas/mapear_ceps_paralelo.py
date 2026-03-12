"""
Mapeia CEPs dos clientes para bairros usando 2 APIs em paralelo:
- OpenCEP: lê do início para o fim
- CEP Aberto: lê do fim para o início
Ambas compartilham o mesmo cache para não duplicar consultas.
"""

import json
import re
import sqlite3
import threading
import time
from pathlib import Path
from urllib.request import urlopen, Request

import pandas as pd

from src.database import get_sqlite_connection

CACHE_PATH = Path("data") / "cep_bairros.json"
TOKEN_CEPABERTO = "25a8cf894f8d5285b9e4b7c25ae0fa71"

# Lock para acesso ao cache compartilhado
cache_lock = threading.Lock()
cache = {}
stats = {"opencep_ok": 0, "opencep_fail": 0, "cepaberto_ok": 0, "cepaberto_fail": 0, "skip": 0}


def _limpar_cep(cep: str) -> str:
    return re.sub(r"\D", "", str(cep)).strip()


def _buscar_opencep(cep: str) -> dict | None:
    try:
        url = f"https://opencep.com/v1/{cep}"
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


def _buscar_cepaberto(cep: str) -> dict | None:
    try:
        url = f"https://www.cepaberto.com/api/v3/cep?cep={cep}"
        req = Request(url)
        req.add_header("Authorization", f"Token token={TOKEN_CEPABERTO}")
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if not data or "cidade" not in data:
                return None
            cidade = data.get("cidade", {})
            estado = data.get("estado", {})
            return {
                "bairro": data.get("bairro", ""),
                "cidade": cidade.get("nome", "") if isinstance(cidade, dict) else str(cidade),
                "uf": estado.get("sigla", "") if isinstance(estado, dict) else str(estado),
            }
    except Exception:
        return None


def _processar_ceps(ceps: list, api_nome: str, buscar_fn, delay: float):
    """Processa uma lista de CEPs usando uma API específica."""
    global cache, stats

    for i, cep in enumerate(ceps):
        # Verifica no cache (thread-safe)
        with cache_lock:
            if cep in cache:
                stats["skip"] += 1
                continue

        resultado = buscar_fn(cep)

        with cache_lock:
            # Re-checa (outra thread pode ter adicionado)
            if cep in cache:
                stats["skip"] += 1
                continue

            if resultado and resultado.get("bairro"):
                cache[cep] = resultado
                stats[f"{api_nome}_ok"] += 1
            else:
                cache[cep] = resultado or {"bairro": "", "cidade": "", "uf": ""}
                stats[f"{api_nome}_fail"] += 1

            total_done = stats["opencep_ok"] + stats["opencep_fail"] + stats["cepaberto_ok"] + stats["cepaberto_fail"]

            # Status a cada 200
            if total_done % 200 == 0 and total_done > 0:
                com_bairro = sum(1 for v in cache.values() if v.get("bairro"))
                print(f"  [{total_done} novos] Cache: {len(cache)} CEPs ({com_bairro} com bairro) | "
                      f"OpenCEP: {stats['opencep_ok']}ok/{stats['opencep_fail']}fail | "
                      f"CEPAberto: {stats['cepaberto_ok']}ok/{stats['cepaberto_fail']}fail",
                      flush=True)

            # Salva cache a cada 500
            if total_done % 500 == 0 and total_done > 0:
                CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

        time.sleep(delay)


def main():
    global cache

    conn = get_sqlite_connection()
    df = pd.read_sql("""
        SELECT DISTINCT cep_cliente FROM vendas
        WHERE cep_cliente IS NOT NULL
    """, conn)
    conn.close()

    df["cep_limpo"] = df["cep_cliente"].apply(_limpar_cep)
    df = df[df["cep_limpo"].str.len() == 8]
    ceps_todos = df["cep_limpo"].unique().tolist()

    print(f"Total CEPs únicos: {len(ceps_todos)}")

    # Carrega cache existente
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        print(f"Cache carregado: {len(cache)} entradas")

    # Remove CEPs já no cache
    ceps_pendentes = [c for c in ceps_todos if c not in cache]
    print(f"CEPs pendentes: {len(ceps_pendentes)}")

    if not ceps_pendentes:
        print("Todos os CEPs já estão no cache!")
        return

    # Divide: OpenCEP pega do início, CEP Aberto pega do fim
    meio = len(ceps_pendentes) // 2
    ceps_opencep = ceps_pendentes[:meio]
    ceps_cepaberto = list(reversed(ceps_pendentes[meio:]))

    print(f"OpenCEP: {len(ceps_opencep)} CEPs (inicio -> meio)")
    print(f"CEP Aberto: {len(ceps_cepaberto)} CEPs (fim -> meio)")
    print("Iniciando...\n")

    t1 = threading.Thread(
        target=_processar_ceps,
        args=(ceps_opencep, "opencep", _buscar_opencep, 0.6),
    )
    t2 = threading.Thread(
        target=_processar_ceps,
        args=(ceps_cepaberto, "cepaberto", _buscar_cepaberto, 0.6),
    )

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Salva cache final
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    com_bairro = sum(1 for v in cache.values() if v.get("bairro"))
    print(f"\nFinalizado!")
    print(f"Cache total: {len(cache)} CEPs ({com_bairro} com bairro)")
    print(f"OpenCEP: {stats['opencep_ok']} ok, {stats['opencep_fail']} falhas")
    print(f"CEP Aberto: {stats['cepaberto_ok']} ok, {stats['cepaberto_fail']} falhas")


if __name__ == "__main__":
    main()

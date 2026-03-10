"""
Reset completo e coleta do zero
================================
Apaga banco, backups antigos e arquivos de progresso,
depois roda todos os scrapers em sequencia com backup entre cada etapa.

Uso:
    python run_coleta.py
    python run_coleta.py --a-partir 3
"""

import os
import sys
import shutil
import argparse
import subprocess
import sqlite3
from datetime import datetime

# ============================================================
# CAMINHOS
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
SCRAPERS_DIR = os.path.join(PROJECT_ROOT, "scrapers")
LOGS_DIR     = os.path.join(PROJECT_ROOT, "logs")
BACKUPS_DIR  = os.path.join(DATA_DIR, "backups")
DB_PATH      = os.path.join(DATA_DIR, "empreendimentos.db")

ARQUIVOS_PROGRESSO = [
    os.path.join(LOGS_DIR, "planoeplano_empreendimentos_progresso.json"),
    os.path.join(LOGS_DIR, "cury_empreendimentos_progresso.json"),
]

ARQUIVOS_LIXO = [
    os.path.join(DATA_DIR, "empreendimentos_recuperado.db"),
]

ETAPAS = [
    {
        "numero": 1,
        "nome": "Plano&Plano",
        "script": os.path.join(SCRAPERS_DIR, "planoeplano_empreendimentos.py"),
        "args": ["--reset-progresso", "--sem-imagens"],
    },
    {
        "numero": 2,
        "nome": "Generico (8 empresas)",
        "script": os.path.join(SCRAPERS_DIR, "generico_empreendimentos.py"),
        "args": ["--empresa", "todas"],
    },
    {
        "numero": 3,
        "nome": "Cury",
        "script": os.path.join(SCRAPERS_DIR, "cury_empreendimentos.py"),
        "args": ["--reset-progresso"],
    },
    {
        "numero": 4,
        "nome": "MRV - Listagem",
        "script": os.path.join(SCRAPERS_DIR, "mrv_empreendimentos.py"),
        "args": [],
    },
    {
        "numero": 5,
        "nome": "MRV - Detalhes",
        "script": os.path.join(SCRAPERS_DIR, "mrv_detalhes.py"),
        "args": [],
    },
]


# ============================================================
# HELPERS
# ============================================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def limpar_ambiente():
    log("Limpando ambiente...")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        log(f"  Banco apagado: {DB_PATH}")

    for f in ARQUIVOS_PROGRESSO:
        if os.path.exists(f):
            os.remove(f)
            log(f"  Progresso apagado: {os.path.basename(f)}")

    for f in ARQUIVOS_LIXO:
        if os.path.exists(f):
            os.remove(f)
            log(f"  Lixo apagado: {os.path.basename(f)}")

    if os.path.exists(BACKUPS_DIR):
        shutil.rmtree(BACKUPS_DIR)
        log("  Backups antigos apagados.")

    os.makedirs(BACKUPS_DIR, exist_ok=True)
    log("Ambiente limpo.")


def fazer_backup(numero):
    if not os.path.exists(DB_PATH):
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = os.path.join(BACKUPS_DIR, f"backup_apos_etapa{numero}_{ts}.db")
    shutil.copy2(DB_PATH, destino)
    log(f"Backup salvo: {os.path.basename(destino)}")


def contar_registros():
    try:
        conn = sqlite3.connect(DB_PATH)
        total = conn.execute("SELECT COUNT(*) FROM empreendimentos").fetchone()[0]
        conn.close()
        return total
    except Exception:
        return 0


def contar_por_empresa():
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT empresa, COUNT(*) FROM empreendimentos GROUP BY empresa ORDER BY empresa"
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def rodar_etapa(etapa):
    script = etapa["script"]
    if not os.path.exists(script):
        log(f"ERRO: Script nao encontrado: {script}")
        return False
    cmd = [sys.executable, script] + etapa["args"]
    resultado = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return resultado.returncode == 0


# ============================================================
# MAIN
# ============================================================

def main(a_partir=1, sem_reset=False):
    inicio_total = datetime.now()

    log("=" * 60)
    log("COLETA COMPLETA DE EMPREENDIMENTOS")
    log("=" * 60)

    if a_partir == 1 and not sem_reset:
        limpar_ambiente()
    else:
        log(f"Retomando a partir da etapa {a_partir}, sem reset.")
        os.makedirs(BACKUPS_DIR, exist_ok=True)

    etapas_ok = 0
    etapas_erro = 0

    for etapa in ETAPAS:
        if etapa["numero"] < a_partir:
            continue

        log("")
        log(f"{'=' * 40}")
        log(f"ETAPA {etapa['numero']}/5: {etapa['nome']}")
        log(f"{'=' * 40}")

        antes = contar_registros()
        inicio = datetime.now()

        ok = rodar_etapa(etapa)

        duracao = (datetime.now() - inicio).seconds
        depois = contar_registros()
        adicionados = depois - antes

        fazer_backup(etapa["numero"])

        status = "OK" if ok else "COM PROBLEMAS"
        log(f"Etapa {etapa['numero']} {status} | {duracao}s | +{adicionados} registros | Total: {depois}")

        if ok:
            etapas_ok += 1
        else:
            etapas_erro += 1
            log("Continuando para proxima etapa...")

    duracao_total = (datetime.now() - inicio_total).seconds

    log("")
    log("=" * 60)
    log("RESULTADO FINAL")
    log(f"  Duracao total: {duracao_total}s ({duracao_total // 60}min)")
    log(f"  Etapas OK: {etapas_ok} | Com problemas: {etapas_erro}")
    log("")
    log("Registros por empresa:")
    for empresa, total in contar_por_empresa():
        log(f"  {empresa}: {total}")
    log(f"  TOTAL: {contar_registros()}")
    log("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-partir", type=int, default=1,
                        help="Retomar a partir desta etapa (1-5)")
    parser.add_argument("--sem-reset", action="store_true",
                        help="Nao apagar banco e progresso ao retomar")
    args = parser.parse_args()
    main(a_partir=args.a_partir, sem_reset=args.sem_reset)

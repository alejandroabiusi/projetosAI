"""
Orquestrador de atualizacao recorrente
=======================================
Atualiza a base de empreendimentos sem apagar dados existentes.
Detecta novos empreendimentos e mudancas, registra no changelog,
e envia email com resumo.

Uso:
    python run_atualizacao.py
    python run_atualizacao.py --sem-email
    python run_atualizacao.py --sem-mapa
"""

import os
import sys
import glob
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

sys.path.insert(0, PROJECT_ROOT)
from config.settings import ATUALIZACAO, EMAIL
from data.database import (
    snapshot_empreendimentos,
    comparar_snapshots,
    registrar_run,
    obter_changelog,
    contar_empreendimentos,
)

# ============================================================
# ETAPAS (todos os scrapers disponiveis)
# ============================================================

ETAPAS = [
    {"nome": "Plano&Plano",    "script": os.path.join(SCRAPERS_DIR, "planoeplano_empreendimentos.py"), "args": ["--sem-imagens"]},
    {"nome": "Generico",       "script": os.path.join(SCRAPERS_DIR, "generico_empreendimentos.py"),    "args": ["--empresa", "todas"]},
    {"nome": "Cury",           "script": os.path.join(SCRAPERS_DIR, "cury_empreendimentos.py"),        "args": []},
    {"nome": "MRV Listagem",   "script": os.path.join(SCRAPERS_DIR, "mrv_empreendimentos.py"),         "args": []},
    {"nome": "MRV Detalhes",   "script": os.path.join(SCRAPERS_DIR, "mrv_detalhes.py"),                "args": []},
    {"nome": "Direcional",     "script": os.path.join(SCRAPERS_DIR, "direcional_empreendimentos.py"),  "args": []},
    {"nome": "Vivaz",          "script": os.path.join(SCRAPERS_DIR, "vivaz_empreendimentos.py"),       "args": []},
    {"nome": "Metrocasa",      "script": os.path.join(SCRAPERS_DIR, "metrocasa_empreendimentos.py"),   "args": []},
    {"nome": "Viva Benx",      "script": os.path.join(SCRAPERS_DIR, "vivabenx_empreendimentos.py"),    "args": []},
]

ETAPAS_ENRIQUECIMENTO = [
    {"nome": "Enriquecer dados",    "script": os.path.join(PROJECT_ROOT, "enriquecer_dados.py"),      "args": []},
    {"nome": "Enriquecer unidades", "script": os.path.join(PROJECT_ROOT, "enriquecer_unidades.py"),   "args": ["tudo"]},
    {"nome": "Corrigir nomes",      "script": os.path.join(PROJECT_ROOT, "corrigir_nomes.py"),        "args": []},
    {"nome": "Validar coordenadas", "script": os.path.join(PROJECT_ROOT, "validar_coordenadas.py"),   "args": []},
]


# ============================================================
# HELPERS
# ============================================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fazer_backup():
    """Faz backup do banco atual, mantendo no maximo max_backups."""
    if not os.path.exists(DB_PATH):
        return
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = os.path.join(BACKUPS_DIR, f"backup_{ts}.db")
    shutil.copy2(DB_PATH, destino)
    log(f"Backup salvo: {os.path.basename(destino)}")

    # Limitar numero de backups
    max_backups = ATUALIZACAO.get("max_backups", 10)
    backups = sorted(glob.glob(os.path.join(BACKUPS_DIR, "backup_*.db")))
    while len(backups) > max_backups:
        os.remove(backups.pop(0))


def rodar_script(script, args, timeout):
    """Roda um script Python com timeout. Retorna True se sucesso."""
    if not os.path.exists(script):
        log(f"  AVISO: Script nao encontrado: {script}")
        return False
    cmd = [sys.executable, script] + args
    try:
        resultado = subprocess.run(cmd, cwd=PROJECT_ROOT, timeout=timeout)
        return resultado.returncode == 0
    except subprocess.TimeoutExpired:
        log(f"  TIMEOUT apos {timeout}s")
        return False
    except Exception as e:
        log(f"  ERRO: {e}")
        return False


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
            "SELECT empresa, COUNT(*) FROM empreendimentos GROUP BY empresa ORDER BY COUNT(*) DESC"
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


# ============================================================
# MAIN
# ============================================================

def main(sem_email=False, sem_mapa=False):
    inicio = datetime.now()
    run_id = inicio.strftime("%Y%m%d_%H%M%S")
    timeout = ATUALIZACAO.get("timeout_por_scraper", 1800)
    retry_falhas = ATUALIZACAO.get("retry_falhas", True)

    os.makedirs(LOGS_DIR, exist_ok=True)

    log("=" * 60)
    log("ATUALIZACAO DE EMPREENDIMENTOS")
    log(f"Run ID: {run_id}")
    log("=" * 60)

    # 1. Backup
    if ATUALIZACAO.get("backup_antes", True):
        fazer_backup()

    # 2. Snapshot antes
    log("\nTirando snapshot do estado atual...")
    snapshot_antes = snapshot_empreendimentos()
    total_antes = contar_registros()
    log(f"  {total_antes} empreendimentos, {len(snapshot_antes)} chaves unicas")

    # 3. Rodar scrapers
    etapas_ok = 0
    etapas_erro = 0
    falhas = []

    log("\n--- SCRAPERS ---")
    for i, etapa in enumerate(ETAPAS, 1):
        log(f"\n[{i}/{len(ETAPAS)}] {etapa['nome']}")
        antes = contar_registros()
        ok = rodar_script(etapa["script"], etapa["args"], timeout)
        depois = contar_registros()
        delta = depois - antes

        if ok:
            etapas_ok += 1
            log(f"  OK | +{delta} registros | Total: {depois}")
        else:
            etapas_erro += 1
            falhas.append(etapa)
            log(f"  FALHA | Total: {depois}")

    # Retry de falhas
    if retry_falhas and falhas:
        log(f"\n--- RETRY ({len(falhas)} falhas) ---")
        for etapa in falhas[:]:
            log(f"  Retentando: {etapa['nome']}")
            ok = rodar_script(etapa["script"], etapa["args"], timeout)
            if ok:
                etapas_ok += 1
                etapas_erro -= 1
                falhas.remove(etapa)
                log(f"  OK no retry")
            else:
                log(f"  FALHA novamente")

    # 4. Enriquecimento
    log("\n--- ENRIQUECIMENTO ---")
    for etapa in ETAPAS_ENRIQUECIMENTO:
        log(f"  {etapa['nome']}...")
        ok = rodar_script(etapa["script"], etapa["args"], timeout)
        if not ok:
            log(f"  AVISO: {etapa['nome']} falhou")

    # 5. Snapshot depois e comparar
    log("\nComparando snapshots...")
    snapshot_depois = snapshot_empreendimentos()
    total_depois = contar_registros()
    diff = comparar_snapshots(snapshot_antes, snapshot_depois, run_id)
    log(f"  Novos: {diff['novos']} | Mudancas: {diff['mudancas']}")

    # 6. Gerar mapa
    if not sem_mapa:
        log("\nGerando mapa...")
        rodar_script(os.path.join(PROJECT_ROOT, "gerar_mapa.py"), [], timeout)

    # 7. Registrar run
    fim = datetime.now()
    status = "ok" if etapas_erro == 0 else ("parcial" if etapas_ok > 0 else "falha")
    stats = {
        "etapas_ok": etapas_ok,
        "etapas_erro": etapas_erro,
        "novos": diff["novos"],
        "mudancas": diff["mudancas"],
        "total_apos": total_depois,
    }
    registrar_run(run_id, inicio, fim, status, stats)

    # 8. Email
    if not sem_email and EMAIL.get("destinatarios"):
        log("\nEnviando email...")
        try:
            from notificar_email import enviar_email_atualizacao
            changelog = obter_changelog(run_id)
            enviar_email_atualizacao(stats, changelog, falhas)
            log("  Email enviado!")
        except Exception as e:
            log(f"  ERRO ao enviar email: {e}")
    elif not sem_email and not EMAIL.get("destinatarios"):
        log("\nEmail nao enviado (sem destinatarios configurados em settings.py)")

    # 9. Resumo final
    duracao = int((fim - inicio).total_seconds())
    log("")
    log("=" * 60)
    log("RESULTADO FINAL")
    log(f"  Status: {status.upper()}")
    log(f"  Duracao: {duracao}s ({duracao // 60}min)")
    log(f"  Etapas OK: {etapas_ok} | Falhas: {etapas_erro}")
    log(f"  Novos empreendimentos: {diff['novos']}")
    log(f"  Mudancas detectadas: {diff['mudancas']}")
    log(f"  Total apos: {total_depois}")
    if falhas:
        log(f"  Scrapers com falha: {', '.join(e['nome'] for e in falhas)}")
    log("")
    log("Registros por empresa:")
    for empresa, total in contar_por_empresa():
        log(f"  {empresa}: {total}")
    log("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Atualizacao recorrente de empreendimentos")
    parser.add_argument("--sem-email", action="store_true", help="Nao enviar email de notificacao")
    parser.add_argument("--sem-mapa", action="store_true", help="Nao regenerar mapa HTML")
    args = parser.parse_args()
    main(sem_email=args.sem_email, sem_mapa=args.sem_mapa)

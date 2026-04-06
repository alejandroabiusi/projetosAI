"""
Orquestrador Autônomo de Enriquecimento
========================================
Roda todas as etapas de enriquecimento em sequência com:
- Checkpoint por etapa (retomável se interrompido)
- Log completo em arquivo
- Timeout por etapa
- Relatório final de completude

Uso:
    python run_enriquecimento_completo.py           # Roda tudo
    python run_enriquecimento_completo.py --reset    # Reseta checkpoints e roda do zero
    python run_enriquecimento_completo.py --a-partir 5  # Retoma a partir da etapa 5

Projetado para rodar autônomo durante a madrugada (sem depender do Claude).
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
import sqlite3
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import DATA_DIR, LOGS_DIR

CHECKPOINT_FILE = os.path.join(DATA_DIR, "enriquecimento_checkpoint.json")
LOG_FILE = os.path.join(LOGS_DIR, "enriquecimento_completo.log")
RELATORIO_FILE = os.path.join(PROJECT_ROOT, "docs", "relatorio_enriquecimento.md")
DB_PATH = os.path.join(DATA_DIR, "empreendimentos.db")

# ============================================================
# ETAPAS
# ============================================================

ETAPAS = [
    {
        "id": 1,
        "nome": "Enriquecer dados (geocodificação + endereços)",
        "cmd": [sys.executable, "enriquecer_dados.py", "--empresa", "todas"],
        "timeout": 3600,  # 60 min
        "descricao": "Geocodifica empreendimentos sem coordenadas via CEP local + Nominatim",
    },
    {
        "id": 2,
        "nome": "Enriquecer unidades (total_unidades, vagas, amenidades)",
        "cmd": [sys.executable, "enriquecer_unidades.py", "tudo"],
        "timeout": 1800,  # 30 min
        "descricao": "Extrai dados de unidades via APIs e Selenium",
    },
    {
        "id": 3,
        "nome": "Qualificar produtos (tipologias, lazer, imagens)",
        "cmd": [sys.executable, "qualificar_produto.py"],
        "timeout": 5400,  # 90 min
        "descricao": "Qualificação profunda de tipologias, lazer, classificação de imagens",
    },
    {
        "id": 4,
        "nome": "Corrigir nomes",
        "cmd": [sys.executable, "corrigir_nomes.py"],
        "timeout": 600,  # 10 min
        "descricao": "Corrige nomes via APIs (Vivaz, Metrocasa, Kazzas)",
    },
    {
        "id": 5,
        "nome": "Validar coordenadas",
        "cmd": [sys.executable, "validar_coordenadas.py", "relatorio"],
        "timeout": 300,  # 5 min
        "descricao": "Valida coordenadas vs cidade (haversine)",
    },
    {
        "id": 6,
        "nome": "Corrigir fases VIC/Smart",
        "cmd": [sys.executable, "scrapers/corrigir_fases_vic.py"],
        "timeout": 1800,  # 30 min
        "descricao": "Re-detecta fase de 100+ registros VIC/Smart marcados incorretamente como Breve Lançamento",
    },
    {
        "id": 7,
        "nome": "Download de imagens (novas empresas)",
        "cmd": [sys.executable, "baixar_imagens.py"],
        "timeout": 7200,  # 120 min
        "descricao": "Baixa todas as imagens possíveis dos produtos (fachadas, plantas, decorados, lazer)",
    },
    {
        "id": 8,
        "nome": "Gerar mapa",
        "cmd": [sys.executable, "gerar_mapa.py"],
        "timeout": 120,  # 2 min
        "descricao": "Regenera mapa HTML com todos os empreendimentos geocodificados",
    },
    {
        "id": 9,
        "nome": "Auditoria final",
        "cmd": None,  # Executada inline
        "timeout": 60,
        "descricao": "Gera relatório de completude dos dados",
    },
]


# ============================================================
# LOGGER
# ============================================================

def setup_logger():
    os.makedirs(LOGS_DIR, exist_ok=True)
    logger = logging.getLogger("enriquecimento")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ============================================================
# CHECKPOINT
# ============================================================

def carregar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"etapas_completas": [], "inicio": None, "snapshot_antes": None}


def salvar_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)


# ============================================================
# SNAPSHOT DE COMPLETUDE
# ============================================================

def snapshot_completude():
    """Retorna dict com métricas de completude do banco."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    stats = {}
    cur.execute("SELECT COUNT(*) FROM empreendimentos")
    stats["total"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT empresa) FROM empreendimentos")
    stats["empresas"] = cur.fetchone()[0]

    for campo in ["latitude", "cidade", "fase", "dormitorios_descricao", "endereco",
                   "preco_a_partir", "total_unidades", "itens_lazer", "area_min_m2"]:
        cur.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {campo} IS NOT NULL AND {campo} != ''")
        stats[f"com_{campo}"] = cur.fetchone()[0]

    # Imagens baixadas
    downloads_dir = os.path.join(PROJECT_ROOT, "downloads")
    total_imgs = 0
    if os.path.exists(downloads_dir):
        for root, dirs, files in os.walk(downloads_dir):
            total_imgs += len([f for f in files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))])
    stats["total_imagens"] = total_imgs

    conn.close()
    return stats


def gerar_relatorio(antes, depois, logger):
    """Gera relatório MD comparando antes/depois."""
    os.makedirs(os.path.dirname(RELATORIO_FILE), exist_ok=True)

    lines = [
        f"# Relatório de Enriquecimento — {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        "",
        "## Resumo",
        "",
        f"| Métrica | Antes | Depois | Delta |",
        f"|---------|-------|--------|-------|",
        f"| Total registros | {antes['total']} | {depois['total']} | +{depois['total']-antes['total']} |",
        f"| Empresas | {antes['empresas']} | {depois['empresas']} | +{depois['empresas']-antes['empresas']} |",
    ]

    for campo in ["latitude", "cidade", "fase", "dormitorios_descricao", "endereco",
                   "preco_a_partir", "total_unidades", "itens_lazer", "area_min_m2"]:
        key = f"com_{campo}"
        a = antes.get(key, 0)
        d = depois.get(key, 0)
        pct_a = f"{100*a/antes['total']:.0f}%" if antes['total'] else "0%"
        pct_d = f"{100*d/depois['total']:.0f}%" if depois['total'] else "0%"
        lines.append(f"| {campo} | {a} ({pct_a}) | {d} ({pct_d}) | +{d-a} |")

    lines.append(f"| Imagens baixadas | {antes.get('total_imagens', 0)} | {depois.get('total_imagens', 0)} | +{depois.get('total_imagens', 0)-antes.get('total_imagens', 0)} |")

    lines.extend([
        "",
        "## Completude Final",
        "",
    ])

    for campo in ["latitude", "cidade", "fase", "dormitorios_descricao", "endereco"]:
        key = f"com_{campo}"
        d = depois.get(key, 0)
        total = depois['total']
        sem = total - d
        pct = f"{100*d/total:.1f}%" if total else "0%"
        lines.append(f"- **{campo}**: {d}/{total} ({pct}) — {sem} sem dados")

    lines.extend([
        "",
        "## Empresas sem coordenadas (top 10)",
        "",
    ])

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT empresa, COUNT(*) as sem,
            (SELECT COUNT(*) FROM empreendimentos e2 WHERE e2.empresa=e1.empresa) as total
        FROM empreendimentos e1 WHERE (latitude IS NULL OR latitude = '')
        GROUP BY empresa ORDER BY sem DESC LIMIT 10
    """)
    for e, sem, total in cur.fetchall():
        lines.append(f"- {e}: {sem}/{total}")

    lines.extend([
        "",
        "## Concentração de Breve Lançamento (>30%)",
        "",
    ])
    cur.execute("""
        SELECT empresa, COUNT(*) as total,
            SUM(CASE WHEN fase = 'Breve Lançamento' THEN 1 ELSE 0 END) as breves,
            ROUND(100.0 * SUM(CASE WHEN fase = 'Breve Lançamento' THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
        FROM empreendimentos GROUP BY empresa
        HAVING pct > 30 ORDER BY pct DESC
    """)
    for row in cur.fetchall():
        lines.append(f"- {row[0]}: {row[2]}/{row[1]} ({row[3]}%)")

    conn.close()

    with open(RELATORIO_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"Relatório salvo em: {RELATORIO_FILE}")


# ============================================================
# EXECUÇÃO
# ============================================================

def executar_etapa(etapa, logger):
    """Executa uma etapa como subprocess."""
    nome = etapa["nome"]
    cmd = etapa["cmd"]
    timeout = etapa["timeout"]

    logger.info(f"{'='*60}")
    logger.info(f"ETAPA {etapa['id']}: {nome}")
    logger.info(f"  {etapa['descricao']}")
    logger.info(f"  Timeout: {timeout}s")

    if cmd is None:
        # Etapa inline (auditoria)
        return True

    inicio = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            timeout=timeout,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        duracao = time.time() - inicio
        logger.info(f"  Retorno: {result.returncode} | Duração: {duracao:.0f}s")

        # Log últimas 20 linhas do stdout
        stdout_lines = (result.stdout or "").strip().split("\n")
        for line in stdout_lines[-20:]:
            logger.info(f"  > {line}")

        if result.returncode != 0:
            stderr_lines = (result.stderr or "").strip().split("\n")
            for line in stderr_lines[-10:]:
                logger.warning(f"  ERR> {line}")
            # Não interrompe — registra como warning e continua
            logger.warning(f"  Etapa {etapa['id']} retornou código {result.returncode}, continuando...")

        return True

    except subprocess.TimeoutExpired:
        duracao = time.time() - inicio
        logger.warning(f"  TIMEOUT após {duracao:.0f}s — continuando com próxima etapa")
        return True  # Continua mesmo com timeout

    except Exception as e:
        logger.error(f"  ERRO: {e}")
        return True  # Continua mesmo com erro


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reseta checkpoints")
    parser.add_argument("--a-partir", type=int, default=0, help="Retoma a partir da etapa N")
    args = parser.parse_args()

    logger = setup_logger()

    logger.info("=" * 60)
    logger.info("ORQUESTRADOR DE ENRIQUECIMENTO INICIADO")
    logger.info(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Checkpoint
    if args.reset:
        checkpoint = {"etapas_completas": [], "inicio": None, "snapshot_antes": None}
        salvar_checkpoint(checkpoint)
        logger.info("Checkpoints resetados")
    else:
        checkpoint = carregar_checkpoint()

    # Snapshot antes
    if not checkpoint.get("snapshot_antes"):
        checkpoint["snapshot_antes"] = snapshot_completude()
        checkpoint["inicio"] = datetime.now().isoformat()
        salvar_checkpoint(checkpoint)
        logger.info(f"Snapshot antes: {checkpoint['snapshot_antes']['total']} registros, "
                     f"{checkpoint['snapshot_antes'].get('com_latitude', 0)} com coords")

    # Executar etapas
    for etapa in ETAPAS:
        etapa_id = etapa["id"]

        # Pular se já completada ou se --a-partir definido
        if etapa_id in checkpoint.get("etapas_completas", []):
            logger.info(f"Etapa {etapa_id} já completada, pulando...")
            continue

        if args.a_partir and etapa_id < args.a_partir:
            logger.info(f"Etapa {etapa_id} pulada (--a-partir {args.a_partir})")
            continue

        # Etapa especial: auditoria final
        if etapa["cmd"] is None:
            depois = snapshot_completude()
            gerar_relatorio(checkpoint["snapshot_antes"], depois, logger)
            checkpoint["etapas_completas"].append(etapa_id)
            salvar_checkpoint(checkpoint)
            continue

        # Executar
        ok = executar_etapa(etapa, logger)
        if ok:
            checkpoint["etapas_completas"].append(etapa_id)
            salvar_checkpoint(checkpoint)
            logger.info(f"  Checkpoint salvo: etapa {etapa_id} concluída")

    # Relatório final
    depois = snapshot_completude()
    antes = checkpoint["snapshot_antes"]

    logger.info("")
    logger.info("=" * 60)
    logger.info("ENRIQUECIMENTO COMPLETO")
    logger.info(f"  Início: {checkpoint.get('inicio', '?')}")
    logger.info(f"  Fim: {datetime.now().isoformat()}")
    logger.info(f"  Coords: {antes.get('com_latitude', 0)} -> {depois.get('com_latitude', 0)} (+{depois.get('com_latitude', 0)-antes.get('com_latitude', 0)})")
    logger.info(f"  Cidades: {antes.get('com_cidade', 0)} -> {depois.get('com_cidade', 0)}")
    logger.info(f"  Fases: {antes.get('com_fase', 0)} -> {depois.get('com_fase', 0)}")
    logger.info(f"  Imagens: {antes.get('total_imagens', 0)} -> {depois.get('total_imagens', 0)} (+{depois.get('total_imagens', 0)-antes.get('total_imagens', 0)})")
    logger.info(f"  Relatório: {RELATORIO_FILE}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

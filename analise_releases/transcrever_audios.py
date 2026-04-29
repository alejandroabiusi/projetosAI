"""
Pipeline de transcrição de áudios de teleconferências usando faster-whisper.

Processa uma empresa por vez. Salva:
  1. Arquivo .txt por áudio em downloads/{empresa}/transcricoes/
  2. Tabela SQLite 'transcricoes' em data/transcricoes.db

Uso:
    python transcrever_audios.py                    # Todas as empresas (uma por vez)
    python transcrever_audios.py cury               # Apenas Cury
    python transcrever_audios.py planoeplano mrv     # PlanoePlano depois MRV
    python transcrever_audios.py --modelo small      # Modelo menor (mais rápido)
    python transcrever_audios.py --ano 2025          # Apenas áudios de 2025
    python transcrever_audios.py --ano 2025 2024     # Apenas 2025 e 2024
"""

import os
import sys
import re
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

# Diretórios
PROJECT_ROOT = Path(__file__).parent
DOWNLOADS_DIR = PROJECT_ROOT / "downloads"
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "transcricoes.db"

# Empresas com áudios
EMPRESAS = ["planoeplano", "cury", "mrv", "cyrela", "mouradubeux", "direcional", "tenda"]

# Modelo padrão (balance qualidade/velocidade)
MODELO_PADRAO = "large-v3"


def criar_schema(conn):
    """Cria tabela de transcrições se não existir."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transcricoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empresa TEXT NOT NULL,
            periodo TEXT,
            ano INTEGER,
            trimestre INTEGER,
            arquivo_audio TEXT NOT NULL,
            arquivo_texto TEXT,
            texto_completo TEXT,
            duracao_segundos REAL,
            idioma_detectado TEXT,
            num_segmentos INTEGER,
            modelo_whisper TEXT,
            data_transcricao TEXT,
            UNIQUE(empresa, arquivo_audio)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS segmentos_transcricao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcricao_id INTEGER NOT NULL,
            inicio REAL,
            fim REAL,
            texto TEXT,
            FOREIGN KEY (transcricao_id) REFERENCES transcricoes(id)
        )
    """)
    conn.commit()


def extrair_periodo(nome_arquivo):
    """Extrai período (ex: '3T2024') e ano/trimestre do nome do arquivo."""
    match = re.search(r"(\d)T(\d{4})", nome_arquivo)
    if match:
        trimestre = int(match.group(1))
        ano = int(match.group(2))
        return f"{trimestre}T{ano}", ano, trimestre
    return None, None, None


def ja_transcrito(conn, empresa, arquivo_audio):
    """Verifica se o áudio já foi transcrito."""
    row = conn.execute(
        "SELECT id FROM transcricoes WHERE empresa = ? AND arquivo_audio = ?",
        (empresa, arquivo_audio)
    ).fetchone()
    return row is not None


def transcrever_empresa(empresa, modelo_nome, conn, anos_filtro=None):
    """Transcreve áudios de uma empresa. Se anos_filtro, filtra por ano."""
    from faster_whisper import WhisperModel

    audio_dir = DOWNLOADS_DIR / empresa / "audios"
    transcricao_dir = DOWNLOADS_DIR / empresa / "transcricoes"

    if not audio_dir.exists():
        print(f"  Sem diretório de áudios para {empresa}")
        return {"novos": 0, "existentes": 0, "erros": 0}

    # Listar arquivos de áudio
    audios = sorted([
        f for f in audio_dir.iterdir()
        if f.suffix.lower() in (".mp3", ".mp4", ".m4a", ".wav", ".ogg")
    ])

    # Filtrar por ano se especificado
    if anos_filtro:
        audios = [f for f in audios if any(str(a) in f.name for a in anos_filtro)]

    if not audios:
        print(f"  Sem áudios para {empresa}")
        return {"novos": 0, "existentes": 0, "erros": 0}

    # Criar diretório de transcrições
    transcricao_dir.mkdir(parents=True, exist_ok=True)

    contadores = {"novos": 0, "existentes": 0, "erros": 0}

    # Carregar modelo (uma vez por empresa)
    print(f"\n  Carregando modelo {modelo_nome}...")
    model = WhisperModel(modelo_nome, device="cpu", compute_type="int8")
    print(f"  Modelo carregado.")

    for i, audio_path in enumerate(audios, 1):
        nome = audio_path.name
        periodo, ano, trimestre = extrair_periodo(nome)

        # Verificar se já foi transcrito
        if ja_transcrito(conn, empresa, nome):
            print(f"  [{i}/{len(audios)}] Já transcrito: {nome}")
            contadores["existentes"] += 1
            continue

        print(f"  [{i}/{len(audios)}] Transcrevendo: {nome} ({audio_path.stat().st_size / 1024 / 1024:.1f} MB)...")
        t0 = time.time()

        try:
            # Transcrever
            segments, info = model.transcribe(
                str(audio_path),
                language="pt",
                beam_size=5,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )

            # Coletar segmentos
            todos_segmentos = []
            textos = []
            for seg in segments:
                todos_segmentos.append({
                    "inicio": round(seg.start, 2),
                    "fim": round(seg.end, 2),
                    "texto": seg.text.strip(),
                })
                textos.append(seg.text.strip())

            texto_completo = "\n".join(textos)
            duracao = info.duration
            idioma = info.language

            elapsed = time.time() - t0
            ratio = duracao / elapsed if elapsed > 0 else 0
            print(f"    Concluído: {duracao/60:.1f} min de áudio em {elapsed/60:.1f} min ({ratio:.1f}x)")

            # Salvar .txt
            txt_nome = audio_path.stem + ".txt"
            txt_path = transcricao_dir / txt_nome
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"# Transcrição: {nome}\n")
                f.write(f"# Empresa: {empresa}\n")
                f.write(f"# Período: {periodo or 'N/A'}\n")
                f.write(f"# Duração: {duracao/60:.1f} minutos\n")
                f.write(f"# Modelo: {modelo_nome}\n")
                f.write(f"# Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"# Segmentos: {len(todos_segmentos)}\n\n")

                for seg in todos_segmentos:
                    mins_i = int(seg["inicio"] // 60)
                    secs_i = int(seg["inicio"] % 60)
                    f.write(f"[{mins_i:02d}:{secs_i:02d}] {seg['texto']}\n")

            # Salvar no SQLite
            cur = conn.execute("""
                INSERT INTO transcricoes
                    (empresa, periodo, ano, trimestre, arquivo_audio, arquivo_texto,
                     texto_completo, duracao_segundos, idioma_detectado, num_segmentos,
                     modelo_whisper, data_transcricao)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                empresa, periodo, ano, trimestre, nome, txt_nome,
                texto_completo, duracao, idioma, len(todos_segmentos),
                modelo_nome, datetime.now().isoformat()
            ))
            transcricao_id = cur.lastrowid

            # Salvar segmentos
            for seg in todos_segmentos:
                conn.execute("""
                    INSERT INTO segmentos_transcricao (transcricao_id, inicio, fim, texto)
                    VALUES (?, ?, ?, ?)
                """, (transcricao_id, seg["inicio"], seg["fim"], seg["texto"]))

            conn.commit()
            contadores["novos"] += 1

        except Exception as e:
            print(f"    ERRO: {e}")
            contadores["erros"] += 1

    # Liberar modelo da memória
    del model

    return contadores


def main():
    args = sys.argv[1:]
    modelo = MODELO_PADRAO
    anos_filtro = None

    # Parse --modelo
    if "--modelo" in args:
        idx = args.index("--modelo")
        if idx + 1 < len(args):
            modelo = args[idx + 1]
            args = args[:idx] + args[idx + 2:]

    # Parse --ano (pode receber múltiplos anos)
    if "--ano" in args:
        idx = args.index("--ano")
        anos_filtro = []
        i = idx + 1
        while i < len(args) and args[i].isdigit():
            anos_filtro.append(int(args[i]))
            i += 1
        args = args[:idx] + args[i:]
        if not anos_filtro:
            print("Erro: --ano requer pelo menos um ano (ex: --ano 2025 2024)")
            sys.exit(1)

    # Determinar empresas
    if args:
        empresas = []
        for nome in args:
            nome_lower = nome.lower()
            if nome_lower in EMPRESAS:
                empresas.append(nome_lower)
            else:
                print(f"Empresa '{nome}' não encontrada. Opções: {', '.join(EMPRESAS)}")
                sys.exit(1)
    else:
        empresas = [e for e in EMPRESAS if (DOWNLOADS_DIR / e / "audios").exists()]

    anos_label = f", Anos: {anos_filtro}" if anos_filtro else ""
    print("=" * 60)
    print("TRANSCRIÇÃO DE ÁUDIOS DE TELECONFERÊNCIAS")
    print(f"Empresas: {', '.join(empresas)}")
    print(f"Modelo: {modelo}{anos_label}")
    print("=" * 60)

    # Criar/conectar banco
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    criar_schema(conn)

    todos_resultados = {}
    for empresa in empresas:
        print(f"\n{'=' * 60}")
        print(f"EMPRESA: {empresa.upper()}")
        print(f"{'=' * 60}")

        resultado = transcrever_empresa(empresa, modelo, conn, anos_filtro=anos_filtro)
        todos_resultados[empresa] = resultado

    conn.close()

    # Resumo
    print(f"\n{'=' * 60}")
    print("RESUMO GERAL")
    print(f"{'=' * 60}")
    total_novos = 0
    total_erros = 0
    for empresa, r in todos_resultados.items():
        print(f"  {empresa:15s}: {r['novos']:3d} novos | {r['existentes']:3d} existentes | {r['erros']:3d} erros")
        total_novos += r["novos"]
        total_erros += r["erros"]
    print(f"\n  TOTAL: {total_novos} transcrições, {total_erros} erros")
    print(f"  Banco: {DB_PATH}")


if __name__ == "__main__":
    main()

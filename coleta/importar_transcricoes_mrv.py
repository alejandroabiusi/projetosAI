"""
Importa transcrições oficiais da MRV (PDFs do RI) para o banco SQLite transcricoes.db.

- Extrai texto de cada PDF usando pypdf (PdfReader)
- Insere na tabela 'transcricoes' com modelo_whisper='pdf_oficial_mrv'
- Pula registros que já existem (mesmo empresa + arquivo_audio)
- Mantém transcrições whisper existentes do mesmo trimestre
"""

import os
import re
import sqlite3
from datetime import datetime
from pypdf import PdfReader

PDF_DIR = r"C:\Projetos_AI\coleta\downloads\mrv\transcricoes_ri"
DB_PATH = r"C:\Projetos_AI\coleta\data\transcricoes.db"
EMPRESA = "mrv"
MODELO = "pdf_oficial_mrv"

# Regex para extrair trimestre e ano do nome do arquivo
PATTERN = re.compile(r"MRV_Transcricao_(\d)T(\d{4})\.pdf")


def extrair_texto_pdf(caminho: str) -> str:
    """Extrai todo o texto de um PDF usando pypdf."""
    reader = PdfReader(caminho)
    partes = []
    for page in reader.pages:
        texto = page.extract_text()
        if texto:
            partes.append(texto)
    return "\n".join(partes)


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    arquivos = sorted(
        f for f in os.listdir(PDF_DIR) if f.endswith(".pdf") and PATTERN.match(f)
    )

    inseridos = 0
    pulados = 0
    erros = 0

    for arquivo in arquivos:
        match = PATTERN.match(arquivo)
        trimestre = int(match.group(1))
        ano = int(match.group(2))
        periodo = f"{trimestre}T{ano}"

        # Verifica se já existe registro com mesmo (empresa, arquivo_audio)
        cur.execute(
            "SELECT 1 FROM transcricoes WHERE empresa = ? AND arquivo_audio = ?",
            (EMPRESA, arquivo),
        )
        if cur.fetchone():
            print(f"  PULADO (já existe): {arquivo}")
            pulados += 1
            continue

        # Extrai texto do PDF
        caminho = os.path.join(PDF_DIR, arquivo)
        try:
            texto = extrair_texto_pdf(caminho)
        except Exception as e:
            print(f"  ERRO ao ler {arquivo}: {e}")
            erros += 1
            continue

        if not texto.strip():
            print(f"  AVISO: {arquivo} sem texto extraído (PDF pode ser escaneado)")

        data_transcricao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cur.execute(
            """
            INSERT INTO transcricoes
                (empresa, periodo, ano, trimestre, arquivo_audio, arquivo_texto,
                 texto_completo, duracao_segundos, idioma_detectado, num_segmentos,
                 modelo_whisper, data_transcricao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                EMPRESA,
                periodo,
                ano,
                trimestre,
                arquivo,       # arquivo_audio = nome do PDF
                None,          # arquivo_texto
                texto,         # texto_completo
                None,          # duracao_segundos
                "pt",          # idioma_detectado
                None,          # num_segmentos
                MODELO,        # modelo_whisper
                data_transcricao,
            ),
        )
        inseridos += 1
        print(f"  INSERIDO: {arquivo} ({periodo}) - {len(texto)} caracteres")

    conn.commit()
    conn.close()

    print(f"\nResumo: {inseridos} inseridos, {pulados} pulados, {erros} erros")
    print(f"Total de PDFs processados: {len(arquivos)}")


if __name__ == "__main__":
    main()

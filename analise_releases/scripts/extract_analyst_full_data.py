"""
Extract all analyst questions from Tenda earnings calls and cross-company calls.
Also extract text from Tenda Release 4T2025 PDF.
"""
import sys
import io
import os
import re
import json
import sqlite3

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'transcricoes.db')
OUTPUT_JSON = os.path.join(BASE_DIR, 'data', 'analyst_full_data.json')
PDF_PATH = os.path.join(BASE_DIR, 'downloads', 'tenda', 'releases', 'Tenda_Release_4T2025.pdf')
OUTPUT_TXT = os.path.join(BASE_DIR, 'data', 'tenda_release_4t25.txt')

# Key analysts who cover Tenda
TENDA_ANALYSTS = {
    "Andre Mazini": "Citi",
    "Gustavo Cambauva": "BTG Pactual",
    "Rafael Rehder": "Safra",
    "Ygor Altero": "XP",
    "Tainan Costa": "UBS",
    "Marcelo Motta": "JP Morgan",
    "Fanny Oreng": "Santander",
    "Carla Graca": "Bank of America",
    "Elvis Credendio": "Itau BBA",
    "Herman Lee": "Bradesco BBI",
}

# Regex to match speaker tags: **[Name - Role (Bank)]:** or **[Name - Role]:**
SPEAKER_PATTERN = re.compile(r'\*\*\[([^\]]+)\]:\*\*')


def extract_analyst_blocks(texto, analyst_name):
    """Extract all text blocks spoken by a given analyst from a transcript."""
    # Split text by speaker tags
    parts = SPEAKER_PATTERN.split(texto)
    # parts alternates: text_before, speaker_tag_content, text_after, speaker_tag_content, ...
    blocks = []
    for i in range(1, len(parts), 2):
        speaker_info = parts[i]
        speech_text = parts[i + 1].strip() if i + 1 < len(parts) else ""
        # Check if this speaker matches the analyst
        # Speaker info format: "Name - Analista (Bank)" or "Name - Role"
        name_part = speaker_info.split(' - ')[0].strip()
        if name_part.lower() == analyst_name.lower() and speech_text:
            blocks.append(speech_text)
    return blocks


def find_analyst_in_speakers(speakers_json, analyst_name):
    """Check if analyst appears in the speakers_detectados JSON."""
    if not speakers_json:
        return None
    try:
        speakers = json.loads(speakers_json)
    except (json.JSONDecodeError, TypeError):
        return None
    for s in speakers:
        if s.get('nome', '').lower() == analyst_name.lower():
            return s
    return None


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all Tenda transcripts
    cursor.execute("""
        SELECT empresa, periodo, ano, trimestre, texto_processado, speakers_detectados
        FROM transcricoes
        WHERE empresa = 'tenda'
        ORDER BY ano DESC, trimestre DESC
    """)
    tenda_rows = cursor.fetchall()

    # Get all non-Tenda transcripts for periods 3T2025 and 4T2025
    cursor.execute("""
        SELECT empresa, periodo, ano, trimestre, texto_processado, speakers_detectados
        FROM transcricoes
        WHERE empresa != 'tenda' AND periodo IN ('3T2025', '4T2025')
        ORDER BY empresa, ano DESC, trimestre DESC
    """)
    other_rows = cursor.fetchall()

    result = {}

    for analyst_name, banco in TENDA_ANALYSTS.items():
        print(f"\nProcessing analyst: {analyst_name} ({banco})")

        tenda_questions = []
        tenda_calls = []

        # Extract from ALL Tenda calls
        for row in tenda_rows:
            # Check if analyst is in speakers
            speaker_info = find_analyst_in_speakers(row['speakers_detectados'], analyst_name)
            if speaker_info or (row['texto_processado'] and analyst_name.lower() in row['texto_processado'].lower()):
                blocks = extract_analyst_blocks(row['texto_processado'], analyst_name)
                if blocks:
                    full_text = "\n\n".join(blocks)
                    tenda_questions.append({
                        "periodo": row['periodo'],
                        "texto": full_text
                    })
                    tenda_calls.append(row['periodo'])
                    print(f"  Tenda {row['periodo']}: {len(blocks)} blocks, {len(full_text)} chars")

        # Extract from other companies (3T2025, 4T2025)
        other_questions = []
        for row in other_rows:
            speaker_info = find_analyst_in_speakers(row['speakers_detectados'], analyst_name)
            if speaker_info or (row['texto_processado'] and analyst_name.lower() in row['texto_processado'].lower()):
                blocks = extract_analyst_blocks(row['texto_processado'], analyst_name)
                if blocks:
                    full_text = "\n\n".join(blocks)
                    other_questions.append({
                        "empresa": row['empresa'],
                        "periodo": row['periodo'],
                        "texto": full_text
                    })
                    print(f"  {row['empresa']} {row['periodo']}: {len(blocks)} blocks, {len(full_text)} chars")

        # Sort tenda_calls by most recent
        result[analyst_name] = {
            "banco": banco,
            "tenda_questions": tenda_questions,
            "other_company_questions": other_questions,
            "num_tenda_calls": len(tenda_calls),
            "recent_tenda_calls": tenda_calls  # already sorted DESC
        }

    conn.close()

    # Save JSON
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nSaved analyst data to {OUTPUT_JSON}")

    # Summary
    print("\n=== SUMMARY ===")
    for name, data in result.items():
        print(f"{name} ({data['banco']}): {data['num_tenda_calls']} Tenda calls, "
              f"{len(data['tenda_questions'])} Tenda Q blocks, "
              f"{len(data['other_company_questions'])} other company Q blocks")
        if data['recent_tenda_calls']:
            print(f"  Recent Tenda calls: {', '.join(data['recent_tenda_calls'][:5])}")

    # === PDF EXTRACTION ===
    print(f"\n=== Extracting PDF: {PDF_PATH} ===")
    try:
        from pypdf import PdfReader
        reader = PdfReader(PDF_PATH)
        full_text = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if page_text:
                full_text.append(f"--- Página {i+1} ---\n{page_text}")

        combined = "\n\n".join(full_text)
        with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
            f.write(combined)
        print(f"PDF extracted: {len(reader.pages)} pages, {len(combined)} chars")
        print(f"Saved to {OUTPUT_TXT}")
    except ImportError:
        print("ERROR: pypdf not installed. Run: pip install pypdf")
    except Exception as e:
        print(f"ERROR extracting PDF: {e}")


if __name__ == '__main__':
    main()

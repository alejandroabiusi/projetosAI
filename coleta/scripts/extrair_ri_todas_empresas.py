#!/usr/bin/env python3
"""
Extrai Registro de Incorporação de TODAS as URLs de empreendimentos.
Aplica os mesmos regex patterns testados do enriquecer_ri.py.

Uso:
    python scripts/extrair_ri_todas_empresas.py --limite 5000
    python scripts/extrair_ri_todas_empresas.py --empresa "Tenda" --dry-run
    python scripts/extrair_ri_todas_empresas.py --delay 2.0
"""

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "empreendimentos.db"

# ── Regex patterns (copiados exatamente do enriquecer_ri.py) ────────────────
RI_TRIGGER_PATTERNS = [
    re.compile(
        r'incorpora[çc][ãa]o\s+registrada\s+sob\s+.{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'Registro\s+de\s+[Ii]ncorpora[çc][ãa]o\s+(?:prenotado|registrado|sob|n[ºo°]|R[\-\.]).{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'Memorial\s+de\s+[Ii]ncorpora[çc][ãa]o\s+prenotado.{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'R\.?\s*I\.?\s*(?:n[ºo°]|registrad|sob).{5,400}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'(?:com\s+)?incorpora[çc][ãa]o.{5,300}?matr[ií]cula.{5,200}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL,
    ),
]

MATRICULA_PATTERN = re.compile(
    r'matr[ií]cula\s*(?:n[ºo°\.]\s*)?(\d[\d\.\-/]+)', re.IGNORECASE
)

CARTORIO_PATTERNS = [
    re.compile(
        r'(?:do|no)\s+(\d+[ºª°]?\s*(?:Oficial|Of[ií]cio|Cart[oó]rio)\s+de\s+Registro\s+de\s+Im[oó]veis\s+de\s+[^.,;]+)',
        re.IGNORECASE,
    ),
    re.compile(
        r'(Cart[oó]rio\s+(?:do\s+)?\d+[ºª°]?\s*Of[ií]cio[^.,;]*)',
        re.IGNORECASE,
    ),
    re.compile(
        r'(\d+[ºª°]?\s*Of[ií]cio\s+de\s+(?:Registro\s+de\s+)?(?:Im[oó]veis\s+de\s+)?[^.,;]+)',
        re.IGNORECASE,
    ),
]

DATA_RI_PATTERN = re.compile(
    r'(?:em|de|data)\s+(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', re.IGNORECASE
)

NUMERO_RI_PATTERN = re.compile(
    r'(?:sob|n[ºo°]\.?\s*)\s*(R[\.\-]?\s*\d+[\d\.\-/]*)', re.IGNORECASE
)

# ── Headers ─────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


# ── Funções ─────────────────────────────────────────────────────────────────
def extrair_texto_pagina(html: str) -> str:
    """Extrai texto limpo do HTML, removendo nav/footer/header/script/style."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "footer", "header", "script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def extrair_ri_do_texto(texto: str) -> dict | None:
    """Busca padroes de R.I. no texto. Retorna dict ou None."""
    if not texto:
        return None
    texto_limpo = " ".join(texto.split())

    ri_textos = []
    for pattern in RI_TRIGGER_PATTERNS:
        matches = pattern.findall(texto_limpo)
        for m in matches:
            m_clean = m.strip()
            if len(m_clean) > 20 and m_clean not in ri_textos:
                ri_textos.append(m_clean)

    if not ri_textos:
        return None

    ri_texto = max(ri_textos, key=len)
    resultado = {"registro_incorporacao": ri_texto[:2000]}

    # Matricula
    mat = MATRICULA_PATTERN.search(ri_texto)
    if mat:
        resultado["matricula_ri"] = mat.group(1).strip()

    # Cartorio
    for cp in CARTORIO_PATTERNS:
        cart = cp.search(ri_texto)
        if cart:
            resultado["cartorio_ri"] = cart.group(1).strip()
            break

    # Data
    data = DATA_RI_PATTERN.search(ri_texto)
    if data:
        dia, mes, ano = data.group(1).zfill(2), data.group(2).zfill(2), data.group(3)
        if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31:
            resultado["data_ri"] = f"{ano}-{mes}-{dia}"

    # Numero
    num = NUMERO_RI_PATTERN.search(ri_texto)
    if num:
        resultado["numero_ri"] = num.group(1).strip()

    # Multiplos trechos
    if len(ri_textos) > 1:
        resultado["registro_incorporacao"] = " | ".join(t[:800] for t in ri_textos)[:2000]
        todas_mat = []
        for t in ri_textos:
            mm = MATRICULA_PATTERN.search(t)
            if mm:
                todas_mat.append(mm.group(1).strip())
        if todas_mat:
            resultado["matricula_ri"] = " | ".join(todas_mat)

    return resultado


def fetch_url(url: str, timeout: int = 10) -> str | None:
    """GET na URL e retorna HTML ou None em caso de erro."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except Exception:
        return None


def atualizar_empreendimento(cur, emp_id: int, dados: dict):
    """UPDATE apenas campos que estao NULL/vazio no banco."""
    campos_update = []
    valores = []
    for campo in ["registro_incorporacao", "matricula_ri", "cartorio_ri", "data_ri", "numero_ri"]:
        if campo in dados:
            campos_update.append(f"{campo} = CASE WHEN ({campo} IS NULL OR {campo} = '') THEN ? ELSE {campo} END")
            valores.append(dados[campo])
    if not campos_update:
        return
    valores.append(emp_id)
    sql = f"UPDATE empreendimentos SET {', '.join(campos_update)} WHERE id = ?"
    cur.execute(sql, valores)


def main():
    parser = argparse.ArgumentParser(description="Extrair RI de todas as URLs sem RI")
    parser.add_argument("--limite", type=int, default=5000, help="Limite de URLs (default 5000)")
    parser.add_argument("--empresa", type=str, default=None, help="Filtrar por empresa")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay entre requests em segundos")
    parser.add_argument("--dry-run", action="store_true", help="Nao salva no banco")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERRO: banco nao encontrado em {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # Selecionar URLs sem RI
    sql = """
        SELECT id, empresa, nome, url_fonte FROM empreendimentos
        WHERE url_fonte IS NOT NULL AND url_fonte != ''
        AND (registro_incorporacao IS NULL OR registro_incorporacao = '')
    """
    params = []
    if args.empresa:
        sql += " AND empresa = ?"
        params.append(args.empresa)
    sql += " ORDER BY empresa, nome LIMIT ?"
    params.append(args.limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    total = len(rows)

    print(f"=== Extrair RI de todas as empresas ===", flush=True)
    print(f"URLs a processar: {total}", flush=True)
    print(f"Delay: {args.delay}s | Dry-run: {args.dry_run}", flush=True)
    print(flush=True)

    # Stats
    encontrados = 0
    erros_http = 0
    sem_ri = 0
    stats_empresa = {}
    batch_size = 50

    for i, (emp_id, empresa, nome, url) in enumerate(rows, 1):
        # Fetch
        html = fetch_url(url)
        if html is None:
            erros_http += 1
            if i % batch_size == 0 or i == total:
                print(f"  [{i}/{total}] HTTP erro - {empresa} | {nome}", flush=True)
        else:
            texto = extrair_texto_pagina(html)
            resultado = extrair_ri_do_texto(texto)

            if resultado:
                encontrados += 1
                stats_empresa[empresa] = stats_empresa.get(empresa, 0) + 1

                if not args.dry_run:
                    atualizar_empreendimento(cur, emp_id, resultado)

                # Sempre mostrar achados
                campos = list(resultado.keys())
                print(f"  [{i}/{total}] RI ENCONTRADO - {empresa} | {nome} | campos: {campos}", flush=True)
            else:
                sem_ri += 1

        # Progresso a cada batch
        if i % batch_size == 0:
            pct = i / total * 100
            print(f"--- Progresso: {i}/{total} ({pct:.1f}%) | Encontrados: {encontrados} | Erros HTTP: {erros_http} ---", flush=True)
            if not args.dry_run:
                conn.commit()

        # Delay entre requests
        if i < total:
            time.sleep(args.delay)

    # Commit final
    if not args.dry_run:
        conn.commit()

    conn.close()

    # Relatorio final
    print()
    print("=" * 60)
    print(f"RESULTADO FINAL")
    print(f"=" * 60)
    print(f"  Total processado:  {total}")
    print(f"  RI encontrado:     {encontrados} ({encontrados/total*100:.1f}%)" if total else "")
    print(f"  Sem RI na pagina:  {sem_ri}")
    print(f"  Erros HTTP:        {erros_http}")
    print(f"  Dry-run:           {args.dry_run}")
    print()

    if stats_empresa:
        print("Top empresas com RI encontrado:")
        for emp, qtd in sorted(stats_empresa.items(), key=lambda x: -x[1])[:30]:
            print(f"  {emp:40s} {qtd:4d}")
    else:
        print("Nenhum RI encontrado.")


if __name__ == "__main__":
    main()

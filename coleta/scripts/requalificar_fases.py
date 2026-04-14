"""
Requalificar fases de empresas com contaminação detectada.
Revisita cada URL e extrai a fase REAL do empreendimento.
"""
import sqlite3
import requests
import re
import time
import sys
import io
import os
import argparse
import logging
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

# Fases canônicas e seus sinônimos
FASE_PATTERNS = [
    ("Breve Lançamento", [
        r"breve\s*lan[çc]amento", r"em\s*breve", r"coming\s*soon",
        r"pre[- ]?lan[çc]amento", r"futuro\s*lan[çc]amento",
    ]),
    ("Lançamento", [
        r"lan[çc]amento", r"rec[ée]m[- ]?lan[çc]ado", r"novo",
    ]),
    ("Em Construção", [
        r"em\s*constru[çc][ãa]o", r"em\s*obras?", r"obra\s*em\s*andamento",
        r"em\s*execu[çc][ãa]o",
    ]),
    ("Pronto para Morar", [
        r"pronto", r"entregue", r"im[óo]vel\s*pronto", r"pronto\s*(?:para|pra)\s*morar",
        r"chaves?\s*na\s*m[ãa]o", r"mudança\s*imediata",
    ]),
    ("100% Vendido", [
        r"100\s*%?\s*vendido", r"esgotado", r"vendas?\s*encerrad",
    ]),
]


def extrair_fase(html, url):
    """Extrai fase do empreendimento da página."""
    soup = BeautifulSoup(html, "html.parser")

    # Remover nav, footer, header
    for tag in soup.find_all(["nav", "footer", "header"]):
        tag.decompose()

    # 1. Buscar em badges/labels com classes de status
    for tag in soup.find_all(["span", "div", "p", "label", "badge", "a"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        if any(kw in classes or kw in tag_id for kw in ["status", "fase", "stage", "badge", "label", "tag"]):
            texto_tag = tag.get_text().strip().lower()
            if len(texto_tag) < 50:  # Evitar pegar parágrafos longos
                for fase_canonica, patterns in FASE_PATTERNS:
                    for pat in patterns:
                        if re.search(pat, texto_tag, re.IGNORECASE):
                            return fase_canonica

    # 2. Buscar na URL (muitos sites usam /lancamentos/, /obras/, /prontos/)
    url_lower = url.lower()
    if "/lancamento" in url_lower or "/lançamento" in url_lower:
        # Pode ser lançamento mas verificar no texto
        pass
    if "/pronto" in url_lower or "/entregue" in url_lower:
        return "Pronto para Morar"
    if "/obra" in url_lower or "/construcao" in url_lower or "/construção" in url_lower:
        return "Em Construção"

    # 3. Buscar no texto geral (menos confiável — pegar da seção principal)
    # Priorizar: seções hero, banner, info principal
    texto = soup.get_text(separator=" ").lower()

    # "100% vendido" é bem específico
    if re.search(r"100\s*%?\s*vendido|esgotado|vendas?\s*encerrad", texto):
        return "100% Vendido"

    # Buscar em contexto curto (evitar pegar de texto descritivo)
    for tag in soup.find_all(["h1", "h2", "h3", "strong", "b"]):
        t = tag.get_text().strip().lower()
        if len(t) < 40:
            for fase_canonica, patterns in FASE_PATTERNS:
                for pat in patterns:
                    if re.search(pat, t):
                        return fase_canonica

    # Fallback: texto geral mas com contexto
    # "obra" perto de "%" (evolução)
    if re.search(r"\d+\s*%\s*(?:da\s*obra|conclu|execut|andamento)", texto):
        return "Em Construção"

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", type=str, default=None, help="Empresa específica")
    parser.add_argument("--todas-suspeitas", action="store_true", help="Rodar nas 6 empresas com fase contaminada")
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    EMPRESAS_CONTAMINADAS = ["Diálogo", "You,inc", "HM Engenharia", "Calper", "Pafil", "MBigucci"]

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()

    if args.empresa:
        empresas = [args.empresa]
    elif args.todas_suspeitas:
        empresas = EMPRESAS_CONTAMINADAS
    else:
        empresas = EMPRESAS_CONTAMINADAS

    for empresa in empresas:
        cur.execute("""
            SELECT id, nome, url_fonte, fase FROM empreendimentos
            WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
            ORDER BY nome
        """, (empresa,))
        rows = cur.fetchall()
        log.info(f"\n=== {empresa}: {len(rows)} URLs ===")

        atualizados = 0
        erros = 0
        sem_fase = 0
        mantidos = 0

        session = requests.Session()
        session.headers.update(HEADERS)

        for i, (emp_id, nome, url, fase_atual) in enumerate(rows, 1):
            try:
                resp = session.get(url, timeout=10, allow_redirects=True)
                if resp.status_code != 200:
                    erros += 1
                    continue

                nova_fase = extrair_fase(resp.text, url)

                if nova_fase is None:
                    sem_fase += 1
                    if i <= 5 or i % 25 == 0:
                        log.info(f"  [{i}] {nome[:35]}: não encontrou fase (mantém '{fase_atual}')")
                elif nova_fase != fase_atual:
                    if not args.dry_run:
                        cur.execute("UPDATE empreendimentos SET fase = ? WHERE id = ?", (nova_fase, emp_id))
                    atualizados += 1
                    if i <= 10 or i % 25 == 0:
                        log.info(f"  [{i}] {nome[:35]}: '{fase_atual}' -> '{nova_fase}'")
                else:
                    mantidos += 1

            except Exception:
                erros += 1

            time.sleep(args.delay)

            if not args.dry_run and i % 50 == 0:
                conn.commit()

        if not args.dry_run:
            conn.commit()

        log.info(f"  Resultado {empresa}: {atualizados} atualizados, {mantidos} mantidos, {sem_fase} sem fase, {erros} erros")

    # Verificação final
    log.info(f"\n{'='*60}")
    log.info("VERIFICAÇÃO PÓS-CORREÇÃO:")
    for empresa in empresas:
        cur.execute("""
            SELECT fase, COUNT(*) FROM empreendimentos
            WHERE empresa = ? GROUP BY fase ORDER BY COUNT(*) DESC
        """, (empresa,))
        fases = cur.fetchall()
        log.info(f"  {empresa}:")
        for f, c in fases:
            log.info(f"    {str(f):25s}: {c}")

    conn.close()
    log.info("DONE")


if __name__ == "__main__":
    main()

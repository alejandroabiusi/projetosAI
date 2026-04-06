"""
Corrige fases incorretas da VIC Engenharia e Smart Construtora.
===============================================================
Bug: O detector de fase capturava "Breve Lançamento" dos menus de
navegação do site, resultando em 100+ registros marcados incorretamente.

Fix: Re-visita cada página e detecta a fase usando apenas elementos
de status (badges, tags) e conteúdo principal, ignorando navegação.

Uso:
    python scrapers/corrigir_fases_vic.py
    python scrapers/corrigir_fases_vic.py --dry-run
"""

import os
import sys
import re
import time
import logging
import argparse
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import REQUESTS, LOGS_DIR
from data.database import get_connection

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "text/html",
}


def setup_logger():
    logger = logging.getLogger("corrigir_fases")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "corrigir_fases.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


def detectar_fase_strict(html, url):
    """Detecta fase usando apenas elementos de status, ignorando navegação."""
    soup = BeautifulSoup(html, "html.parser")

    FASES = [
        (["breve lançamento", "breve lancamento", "futuro lançamento"], "Breve Lançamento"),
        (["lançamento", "lancamento"], "Lançamento"),
        (["em obra", "em construção", "em construcao", "obras em andamento", "obras iniciadas"], "Em Construção"),
        (["100% vendido"], "100% Vendido"),
        (["pronto para morar", "pronto", "entregue", "imóvel pronto"], "Pronto"),
    ]

    def match_fase(t):
        t = t.lower()
        for keywords, fase in FASES:
            for kw in keywords:
                if kw in t:
                    return fase
        return None

    # 1. Taxonomia WordPress via classes CSS
    for el in soup.find_all(class_=re.compile(r'progresso[-_]|estagio[-_]', re.I)):
        classes_str = " ".join(el.get("class", []))
        fase = match_fase(classes_str.replace("-", " "))
        if fase:
            return fase

    # 2. Badges e tags de status (NÃO dentro de nav/menu/header/footer)
    for sel in ['span.status', '.status', '.badge', '.tag', '.fase',
                '[class*="status"]', '[class*="fase"]', '[class*="estagio"]']:
        for el in soup.select(sel):
            # Ignorar se está em navegação
            if el.find_parent(['nav', 'header', 'footer']):
                continue
            if el.find_parent(class_=re.compile(r'nav|menu|header|footer|sidebar', re.I)):
                continue
            # Ignorar se está em card/listagem de outros empreendimentos
            if el.find_parent(class_=re.compile(r'card|listing|grid|carousel|slider|swiper|related|outros', re.I)):
                continue
            fase = match_fase(el.get_text())
            if fase:
                return fase

    # 3. Links de taxonomia (fora de nav)
    for el in soup.select('a[href*="estagio_obra"], a[href*="estagio-obra"], a[href*="status-obra"]'):
        if el.find_parent(['nav', 'header', 'ul', 'ol', 'footer']):
            continue
        fase = match_fase(el.get_text())
        if fase:
            return fase

    # 4. H2 "Status da obra" indica Em Construção
    for h2 in soup.find_all('h2'):
        h2_text = h2.get_text(strip=True).lower()
        if 'status da obra' in h2_text:
            return "Em Construção"

    # 5. Conteúdo principal (<main>/<article>) - sem menus
    main_soup = soup.find('main') or soup.find('article') or soup.find(id='content')
    if main_soup:
        # Remove nav/header/footer dentro do main
        for tag in main_soup.find_all(['nav', 'header', 'footer']):
            tag.decompose()
        main_text = main_soup.get_text(separator="\n", strip=True)
        fase = match_fase(main_text)
        if fase:
            return fase

    # 6. Fallback: primeiros 500 chars do body (sem nav)
    body = soup.find('body')
    if body:
        body_copy = body.__copy__() if hasattr(body, '__copy__') else body
        texto = body.get_text(separator="\n", strip=True)[:500].lower()
        for keywords, fase in FASES:
            if fase in ("Breve Lançamento", "Lançamento"):
                continue  # Muito genérico para fallback
            for kw in keywords:
                if kw in texto:
                    return fase

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Não atualiza o banco")
    args = parser.parse_args()

    logger = setup_logger()
    conn = get_connection()
    cur = conn.cursor()

    # Buscar registros com "Breve Lançamento" da VIC e Smart
    cur.execute("""
        SELECT id, empresa, nome, url_fonte, fase
        FROM empreendimentos
        WHERE empresa IN ('VIC Engenharia', 'Smart Construtora')
        AND fase = 'Breve Lançamento'
    """)
    registros = cur.fetchall()
    logger.info(f"Total registros para corrigir: {len(registros)}")

    corrigidos = 0
    erros = 0
    sem_mudanca = 0

    for row in registros:
        rid, empresa, nome, url_fonte, fase_atual = row
        if not url_fonte:
            continue

        try:
            resp = requests.get(url_fonte, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"  [{rid}] {nome}: status {resp.status_code}")
                erros += 1
                continue

            nova_fase = detectar_fase_strict(resp.text, url_fonte)

            if nova_fase and nova_fase != fase_atual:
                logger.info(f"  [{rid}] {nome}: {fase_atual} -> {nova_fase}")
                if not args.dry_run:
                    cur.execute("UPDATE empreendimentos SET fase=?, data_atualizacao=datetime('now') WHERE id=?",
                               (nova_fase, rid))
                corrigidos += 1
            elif nova_fase == fase_atual:
                sem_mudanca += 1
            else:
                logger.info(f"  [{rid}] {nome}: sem fase detectada (mantendo {fase_atual})")
                sem_mudanca += 1

        except Exception as e:
            logger.error(f"  [{rid}] {nome}: erro {e}")
            erros += 1

        time.sleep(1.5)

    if not args.dry_run:
        conn.commit()

    conn.close()

    logger.info("=" * 60)
    logger.info(f"RELATÓRIO - Correção de Fases")
    logger.info(f"  Total processados: {len(registros)}")
    logger.info(f"  Corrigidos: {corrigidos}")
    logger.info(f"  Sem mudança: {sem_mudanca}")
    logger.info(f"  Erros: {erros}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

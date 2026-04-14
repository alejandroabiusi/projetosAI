"""
Requalificar tipologias de TODOS os empreendimentos da base.
Revisita cada URL e re-extrai dormitórios com regex corrigido:
- Ranges "2 a 4 dorms" -> preenche 2d, 3d, 4d (intermediários)
- Aceita "03 dorms" (zero à esquerda)
- Aceita "3 suítes" como 3 dorms
- Studio: SÓ se em contexto de planta/metragem/título, NÃO menu solto
- Remove nav/footer/header/aside ANTES de extrair
- Busca em seções de planta/tipologia PRIMEIRO, fallback para texto geral
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
from datetime import datetime
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()


def extrair_tipologias(html, url, titulo_pagina):
    """Extrai tipologias com regex robusto. Retorna dict de flags."""
    soup = BeautifulSoup(html, "html.parser")

    # Remover nav, footer, header, aside, menu
    for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
        tag.decompose()

    # Tentar pegar seção de plantas/tipologias primeiro (mais confiável)
    secao_tipologia = ""
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", []))
        section_id = section.get("id", "")
        text_check = (classes + " " + section_id).lower()
        if any(kw in text_check for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur"]):
            secao_tipologia += " " + section.get_text(separator=" ")

    # Texto geral (sem nav/footer)
    texto_geral = soup.get_text(separator=" ").lower()

    # Usar seção de tipologia se encontrada, senão texto geral
    texto = secao_tipologia.lower() if secao_tipologia.strip() else texto_geral
    titulo = titulo_pagina.lower() if titulo_pagina else ""

    flags = {
        "apto_studio": 0, "apto_1_dorm": 0, "apto_2_dorms": 0,
        "apto_3_dorms": 0, "apto_4_dorms": 0, "apto_suite": 0,
    }

    # === RANGES com "a": "2 a 4 dorms" -> marca 2, 3, 4 (preenche intermediários) ===
    range_matches = re.findall(r"(\d)\s*a\s*(\d)\s*(?:dorm|quarto|su[ií]te)", texto)
    for start, end in range_matches:
        s, e = int(start), int(end)
        for d in range(s, e + 1):
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # === ENUMERAÇÕES com "e" ou ",": "2 e 3 dorms", "1, 2 e 3 dorms" -> marca cada um ===
    enum_matches = re.findall(r"(\d)(?:\s*(?:e|,)\s*(\d))+\s*(?:dorm|quarto)", texto)
    # Também pegar padrões mais longos: "1, 2 e 3 dorms"
    enum_full = re.findall(r"((?:\d\s*(?:,|e)\s*)+\d)\s*(?:dorm|quarto)", texto)
    for match in enum_full:
        nums = re.findall(r"\d", match)
        for n in nums:
            d = int(n)
            if d == 1: flags["apto_1_dorm"] = 1
            elif d == 2: flags["apto_2_dorms"] = 1
            elif d == 3: flags["apto_3_dorms"] = 1
            elif d == 4: flags["apto_4_dorms"] = 1

    # === INDIVIDUAIS (aceita zero à esquerda, "dorm" singular/plural) ===
    if re.search(r"\b0?1\s*(?:dorm|quarto)", texto):
        flags["apto_1_dorm"] = 1
    if re.search(r"\b0?2\s*(?:dorm|quarto|su[ií]te)", texto):
        flags["apto_2_dorms"] = 1
    if re.search(r"\b0?3\s*(?:dorm|quarto|su[ií]te)", texto):
        flags["apto_3_dorms"] = 1
    if re.search(r"\b0?4\s*(?:dorm|quarto|su[ií]te)", texto):
        flags["apto_4_dorms"] = 1

    # === SUÍTE ===
    if re.search(r"su[ií]te", texto):
        flags["apto_suite"] = 1

    # === STUDIO (restritivo — só em contexto de tipologia/planta/metragem/título) ===
    studio_in_titulo = "studio" in titulo
    studio_em_contexto = bool(re.search(r"studio.{0,30}m[²2]", texto) or
                              re.search(r"\d+m[²2].{0,30}studio", texto) or
                              re.search(r"planta.{0,80}studio", texto) or
                              re.search(r"studio.{0,80}planta", texto) or
                              re.search(r"studio\s+(?:e|,|\|)\s+\d", texto) or
                              re.search(r"\d\s+(?:e|,|\|)\s+studio", texto))

    # Se tem seção de tipologia e studio aparece nela, OK
    studio_na_secao = "studio" in secao_tipologia.lower() if secao_tipologia.strip() else False

    if studio_in_titulo or studio_em_contexto or studio_na_secao:
        flags["apto_studio"] = 1

    # Reconstruir dormitorios_descricao
    partes = []
    if flags["apto_studio"]: partes.append("Studio")
    if flags["apto_1_dorm"]: partes.append("1 dorm")
    if flags["apto_2_dorms"]: partes.append("2 dorms")
    if flags["apto_3_dorms"]: partes.append("3 dorms")
    if flags["apto_4_dorms"]: partes.append("4 dorms")
    if flags["apto_suite"]: partes.append("c/suíte")

    dorms_desc = " e ".join(partes) if partes else None

    return flags, dorms_desc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limite", type=int, default=7000)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--empresa", type=str, default=None)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()

    where = "WHERE url_fonte IS NOT NULL AND url_fonte != ''"
    params = []
    if args.empresa:
        where += " AND empresa = ?"
        params.append(args.empresa)

    cur.execute(f"""
        SELECT id, empresa, nome, url_fonte, apto_studio, apto_1_dorm,
               apto_2_dorms, apto_3_dorms, apto_4_dorms, apto_suite, dormitorios_descricao
        FROM empreendimentos {where}
        ORDER BY empresa, nome
        LIMIT ? OFFSET ?
    """, params + [args.limite, args.offset])

    rows = cur.fetchall()
    log.info(f"=== Requalificar tipologias: {len(rows)} URLs (offset={args.offset}) ===")

    session = requests.Session()
    session.headers.update(HEADERS)

    total_processados = 0
    total_atualizados = 0
    total_erros = 0
    total_sem_mudanca = 0
    changes_log = []

    for i, row in enumerate(rows, 1):
        emp_id, empresa, nome, url = row[0], row[1], row[2], row[3]
        old_flags = {
            "apto_studio": row[4], "apto_1_dorm": row[5], "apto_2_dorms": row[6],
            "apto_3_dorms": row[7], "apto_4_dorms": row[8], "apto_suite": row[9],
        }
        old_desc = row[10]

        try:
            resp = session.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                total_erros += 1
                continue

            # Extrair título
            soup_title = BeautifulSoup(resp.text[:5000], "html.parser")
            title_tag = soup_title.find("title")
            titulo = title_tag.get_text() if title_tag else ""

            new_flags, new_desc = extrair_tipologias(resp.text, url, titulo)

            # Comparar: só atualizar se extraiu ALGO e é diferente
            extraiu_algo = any(v == 1 for v in new_flags.values())

            if not extraiu_algo:
                # Não conseguiu extrair nada — manter dados existentes
                total_sem_mudanca += 1
            else:
                # Verificar se mudou
                mudou = False
                for campo in new_flags:
                    if new_flags[campo] != old_flags.get(campo):
                        mudou = True
                        break

                if mudou and not args.dry_run:
                    cur.execute("""UPDATE empreendimentos SET
                        apto_studio=?, apto_1_dorm=?, apto_2_dorms=?,
                        apto_3_dorms=?, apto_4_dorms=?, apto_suite=?,
                        dormitorios_descricao=?
                        WHERE id=?""",
                        (new_flags["apto_studio"], new_flags["apto_1_dorm"],
                         new_flags["apto_2_dorms"], new_flags["apto_3_dorms"],
                         new_flags["apto_4_dorms"], new_flags["apto_suite"],
                         new_desc, emp_id))
                    total_atualizados += 1

                    if i <= 10 or i % 100 == 0:
                        log.info(f"  [{i}] {empresa} | {nome[:30]}: {new_desc}")
                elif not mudou:
                    total_sem_mudanca += 1

            total_processados += 1

        except requests.exceptions.SSLError:
            total_erros += 1
        except requests.exceptions.Timeout:
            total_erros += 1
        except requests.exceptions.ConnectionError:
            total_erros += 1
        except Exception as e:
            total_erros += 1

        # Commit a cada 100
        if not args.dry_run and i % 100 == 0:
            conn.commit()

        # Progresso a cada 50
        if i % 50 == 0:
            log.info(f"--- Progresso: {i}/{len(rows)} | atualiz={total_atualizados} erros={total_erros} sem_mudanca={total_sem_mudanca} ---")

        time.sleep(args.delay)

    if not args.dry_run:
        conn.commit()

    # Mini-auditoria pós-execução
    log.info(f"\n{'='*60}")
    log.info(f"RESULTADO TIPOLOGIAS")
    log.info(f"{'='*60}")
    log.info(f"  Processados:    {total_processados}")
    log.info(f"  Atualizados:    {total_atualizados}")
    log.info(f"  Sem mudança:    {total_sem_mudanca}")
    log.info(f"  Erros HTTP:     {total_erros}")

    # Checar se ainda há suspeitos
    cur.execute("""
        SELECT empresa, COUNT(*) as total, SUM(apto_studio) as st
        FROM empreendimentos WHERE apto_studio IS NOT NULL
        GROUP BY empresa HAVING total >= 5 AND CAST(st AS FLOAT)/total > 0.9
        ORDER BY total DESC
    """)
    suspeitos = cur.fetchall()
    if suspeitos:
        log.info(f"\n  ALERTA: {len(suspeitos)} empresas ainda com studio >90%:")
        for r in suspeitos:
            log.info(f"    {r[0]:30s} ({r[1]:3d}): studio={r[2]} ({100*r[2]/r[1]:.0f}%)")
    else:
        log.info(f"\n  OK: Nenhuma empresa com studio >90%")

    conn.close()
    log.info("DONE")


if __name__ == "__main__":
    main()

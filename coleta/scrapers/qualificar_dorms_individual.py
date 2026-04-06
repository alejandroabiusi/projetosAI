"""
Qualificação individual de dormitórios — visita cada página e extrai
tipologias da ficha técnica real do produto, não do menu/sidebar.
"""
import os
import sys
import re
import time
import sqlite3
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "empreendimentos.db")


def extrair_dorms_pagina(url):
    """Visita uma página e extrai dormitórios da ficha técnica."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None

    dorms_found = set()

    # 1. Seções de tipologia/plantas
    for el in soup.find_all(attrs={"class": re.compile(r"tipo|planta|dorm|quarto|ficha|tab-pane|card-tipo", re.I)}):
        text = el.get_text(separator=" ", strip=True)
        for m in re.findall(r"(\d+)\s*(?:quartos?|dorms?\.?|dormit[oó]rios?|su[ií]tes?)", text, re.I):
            num = int(m)
            if 1 <= num <= 4:
                dorms_found.add(num)
        if re.search(r"\bstudios?\b", text, re.I):
            dorms_found.add(0)

    # 2. Títulos curtos (h2/h3/h4/span) que mencionam quartos
    for tag in soup.find_all(["h2", "h3", "h4", "span", "strong"]):
        text = tag.get_text(strip=True)
        if len(text) < 40:
            for m in re.findall(r"(\d+)\s*(?:quartos?|dorms?)", text, re.I):
                num = int(m)
                if 1 <= num <= 4:
                    dorms_found.add(num)
            if re.search(r"\bstudio", text, re.I):
                dorms_found.add(0)

    # 3. Meta description ou og:description
    for meta in soup.find_all("meta", attrs={"name": "description"}) + soup.find_all("meta", attrs={"property": "og:description"}):
        content = meta.get("content", "")
        for m in re.findall(r"(\d+)\s*(?:quartos?|dorms?)", content, re.I):
            num = int(m)
            if 1 <= num <= 4:
                dorms_found.add(num)

    return dorms_found if dorms_found else None


def qualificar_empresa(empresa):
    """Qualifica dormitórios de todos os produtos de uma empresa."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT id, nome, url_fonte FROM empreendimentos WHERE empresa=? AND prog_mcmv=1 AND url_fonte IS NOT NULL ORDER BY nome",
        (empresa,),
    )
    recs = cur.fetchall()
    print(f"{empresa}: {len(recs)} para qualificar")

    fixed = 0
    for i, (rid, nome, url) in enumerate(recs):
        dorms = extrair_dorms_pagina(url)

        if dorms:
            parts = []
            if 0 in dorms:
                parts.append("Studio")
            for n in sorted(dorms - {0}):
                parts.append(f"{n} quartos" if n > 1 else "1 quarto")
            new_desc = " | ".join(parts)

            st = 1 if 0 in dorms else 0
            d1 = 1 if 1 in dorms else 0
            d2 = 1 if 2 in dorms else 0
            d3 = 1 if 3 in dorms else 0

            cur.execute(
                "UPDATE empreendimentos SET dormitorios_descricao=?, apto_studio=?, apto_1_dorm=?, apto_2_dorms=?, apto_3_dorms=? WHERE id=?",
                (new_desc, st, d1, d2, d3, rid),
            )
            fixed += 1

        if (i + 1) % 20 == 0:
            conn.commit()
            print(f"  {fixed}/{i+1} qualificados")

        time.sleep(0.5)

    conn.commit()

    cur.execute(
        "SELECT SUM(apto_1_dorm), SUM(apto_2_dorms), SUM(apto_3_dorms), COUNT(*) FROM empreendimentos WHERE empresa=? AND prog_mcmv=1",
        (empresa,),
    )
    d1, d2, d3, t = cur.fetchone()
    print(f"\n{empresa} final: 1D={d1}/{t} ({100*d1/t:.0f}%) | 2D={d2}/{t} ({100*d2/t:.0f}%) | 3D={d3}/{t} ({100*d3/t:.0f}%)")
    print(f"Qualificados: {fixed}/{len(recs)}")

    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", required=True)
    args = parser.parse_args()

    qualificar_empresa(args.empresa)

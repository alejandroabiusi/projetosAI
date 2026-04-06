"""
Varredura de lazer, vaga e edifício garagem — visita cada página
e re-extrai todos os atributos de lazer + vaga das seções relevantes.

Processa em batches de 200, salvando progresso em JSON.

Uso:
    python scrapers/varrer_lazer_vaga.py
    python scrapers/varrer_lazer_vaga.py --reset
    python scrapers/varrer_lazer_vaga.py --batch 200
"""

import os
import sys
import re
import json
import time
import sqlite3
import warnings
import argparse
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.database import detectar_atributos_binarios, ATRIBUTOS_LAZER

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "empreendimentos.db")
PROGRESSO_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "varredura_lazer_progresso.json")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Termos de lazer para buscar no texto limpo (sem nav/footer)
TERMOS_LAZER = [
    "piscina", "churrasqueira", "fitness", "academia", "playground",
    "brinquedoteca", "salão de festas", "salao de festas", "pet care",
    "pet place", "coworking", "co-working", "bicicletário", "bicicletario",
    "quadra", "mini quadra", "beach tennis", "delivery", "sala delivery",
    "horta", "lavanderia", "redário", "redario", "rooftop",
    "sauna", "piquenique", "picnic", "sport bar",
    "cinema", "cine open", "mini mercado", "mini market",
    "espaço beleza", "espaco beleza", "sala de estudos",
    "gourmet", "espaço gourmet", "praça", "praca",
    "solarium", "solário", "sala de jogos", "salão de jogos",
    "varanda",
]

# Termos de vaga
TERMOS_VAGA = {
    "vaga": "apto_vaga_garagem",
    "garagem": "apto_vaga_garagem",
    "estacionamento": "apto_vaga_garagem",
    "edifício garagem": "vaga_edificio_garagem",
    "edificio garagem": "vaga_edificio_garagem",
    "edifício-garagem": "vaga_edificio_garagem",
    "prédio garagem": "vaga_edificio_garagem",
    "predio garagem": "vaga_edificio_garagem",
    "pilotis": "vaga_pilotis",
    "subsolo": "vaga_subsolo",
    "coberta": "vaga_coberta",
    "descoberta": "vaga_descoberta",
}


def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": []}


def salvar_progresso(prog):
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(prog, f)


def extrair_lazer_pagina(url):
    """Visita página e extrai itens de lazer do conteúdo principal (sem nav/footer)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12, verify=False)
        if resp.status_code != 200:
            return None, None
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remover nav, footer, header, sidebar, menus
        for tag in soup.find_all(["nav", "footer", "header", "aside"]):
            tag.decompose()
        for tag in soup.find_all(class_=re.compile(r"menu|nav|footer|sidebar|widget", re.I)):
            tag.decompose()

        texto = soup.get_text(separator=" ", strip=True).lower()

        # Extrair lazer
        itens = set()
        for termo in TERMOS_LAZER:
            if termo in texto:
                itens.add(termo.title())

        # Extrair vaga
        vagas = {}
        for termo, col in TERMOS_VAGA.items():
            if termo in texto:
                vagas[col] = 1

        return itens, vagas

    except Exception:
        return None, None


def processar_batch(batch_size=200):
    prog = carregar_progresso()
    processados_set = set(prog["processados"])

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT id, empresa, nome, url_fonte FROM empreendimentos "
        "WHERE prog_mcmv=1 AND url_fonte IS NOT NULL AND url_fonte != '' "
        "ORDER BY id"
    )
    todos = cur.fetchall()

    # Filtrar já processados
    pendentes = [(rid, emp, nome, url) for rid, emp, nome, url in todos if rid not in processados_set]

    batch = pendentes[:batch_size]
    print(f"Batch: {len(batch)} de {len(pendentes)} pendentes ({len(processados_set)} já processados)", flush=True)

    if not batch:
        print("Nada para processar!", flush=True)
        conn.close()
        return

    updated = 0
    for i, (rid, emp, nome, url) in enumerate(batch):
        itens, vagas = extrair_lazer_pagina(url)

        if itens is not None:
            # Atualizar itens_lazer
            if itens:
                lazer_str = " | ".join(sorted(itens))
                cur.execute("UPDATE empreendimentos SET itens_lazer=? WHERE id=?", (lazer_str, rid))

            # Atualizar flags de lazer via detectar_atributos_binarios
            if itens:
                texto_lazer = " ".join(itens).lower()
                binarios = detectar_atributos_binarios(texto_lazer)
                lazer_cols = {k: v for k, v in binarios.items() if k.startswith("lazer_")}
                if lazer_cols:
                    sets = ", ".join(f"{k}={v}" for k, v in lazer_cols.items())
                    cur.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", (rid,))

            # Atualizar vagas
            if vagas:
                sets = ", ".join(f"{k}={v}" for k, v in vagas.items())
                cur.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", (rid,))

            updated += 1

        processados_set.add(rid)

        if (i + 1) % 50 == 0:
            conn.commit()
            prog["processados"] = list(processados_set)
            salvar_progresso(prog)
            print(f"  {updated} atualizados, {i+1}/{len(batch)}", flush=True)

        time.sleep(0.3)

    conn.commit()
    prog["processados"] = list(processados_set)
    salvar_progresso(prog)

    print(f"\nBatch concluído: {updated}/{len(batch)} atualizados", flush=True)
    print(f"Total processados: {len(processados_set)}/{len(todos)}", flush=True)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--batch", type=int, default=200)
    args = parser.parse_args()

    if args.reset and os.path.exists(PROGRESSO_FILE):
        os.remove(PROGRESSO_FILE)
        print("Progresso resetado", flush=True)

    processar_batch(args.batch)

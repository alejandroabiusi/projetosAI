"""Re-scrape Magik JC para corrigir enderecos (filtrar sede)."""
import sys
sys.stdout.reconfigure(errors="replace")
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import requests
import time
import re
import logging
from bs4 import BeautifulSoup

print("Imports OK", flush=True)

sys.path.insert(0, ".")
from scrapers.generico_empreendimentos import EMPRESAS, extrair_dados_empreendimento

print("Scraper import OK", flush=True)

logger = logging.getLogger("rescrape")
logging.basicConfig(level=logging.INFO, format="%(message)s")

config = EMPRESAS["magik_jc"]
DB = "data/empreendimentos.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur2 = conn.cursor()

cur.execute("SELECT id, nome, url_fonte FROM empreendimentos WHERE empresa='Magik JC'")
registros = cur.fetchall()

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

atualiz = 0
erros = 0
for i, r in enumerate(registros, 1):
    url = r["url_fonte"]
    if not url:
        continue
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"[{i}/{len(registros)}] {r['nome']}: HTTP {resp.status_code}")
            erros += 1
            time.sleep(1)
            continue

        dados = extrair_dados_empreendimento(resp.text, url, config, logger)

        updates = {}
        for campo in ["endereco", "bairro", "total_unidades"]:
            if dados.get(campo):
                updates[campo] = dados[campo]

        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            vals = list(updates.values()) + [r["id"]]
            cur2.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
            atualiz += 1
            print(f"[{i}/{len(registros)}] {r['nome']}: {list(updates.keys())}")
        else:
            print(f"[{i}/{len(registros)}] {r['nome']}: sem dados novos")

        time.sleep(1.5)
    except Exception as e:
        print(f"[{i}/{len(registros)}] {r['nome']}: ERRO {e}")
        erros += 1
        time.sleep(1)

conn.commit()
conn.close()
print(f"\nTotal: {atualiz} atualizados, {erros} erros")

"""Corrige nomes dos empreendimentos extraindo o nome real das páginas/APIs."""
import sys
sys.stdout.reconfigure(errors="replace")
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("nomes")

DB = "data/empreendimentos.db"


def extrair_nome_pagina(html, empresa):
    """Extrai nome real do empreendimento a partir do HTML da pagina."""
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string if soup.title else ""

    if empresa == "Plano&Plano":
        # Title format: "Apartamento à venda no X | NOME | Plano&Plano"
        parts = [p.strip() for p in title.split("|")]
        if len(parts) >= 2:
            return parts[1].strip()
        # Fallback: h2
        for h2 in soup.find_all("h2"):
            t = h2.get_text(strip=True)
            if t and len(t) < 60 and "quer saber" not in t.lower() and "localiz" not in t.lower():
                return t

    elif empresa == "Direcional":
        # Title format: "NOME - Direcional" or similar
        parts = [p.strip() for p in title.split("|")]
        if len(parts) >= 1:
            nome = parts[0].strip()
            nome = re.sub(r"\s*[-|]\s*Direcional.*$", "", nome).strip()
            if nome and len(nome) < 80:
                return nome

    elif empresa == "Magik JC":
        # h1 has the product name, sometimes with suffix
        h1 = soup.find("h1")
        if h1:
            nome = h1.get_text(strip=True)
            # Remove suffix like "em Bairro - Cidade / SP"
            nome = re.sub(r"\s+em\s+[\w\s]+\s*[-/]\s*\w+\s*/?\s*\w*$", "", nome).strip()
            if nome and len(nome) < 80:
                return nome

    elif empresa == "Pacaembu":
        # Title: "NOME - Cidade - SP"
        if title:
            nome = re.sub(r"\s*-\s*[\w\s]+\s*-\s*SP\s*$", "", title).strip()
            nome = re.sub(r"\s*[-|]\s*Pacaembu.*$", "", nome).strip()
            if nome and len(nome) < 80:
                return nome

    elif empresa == "Conx":
        # Title has nome, h1 has "nome Residencial Conx"
        h1 = soup.find("h1")
        if h1:
            nome = h1.get_text(strip=True)
            nome = re.sub(r"\s*Residencial\s+Conx\s*$", "", nome, flags=re.IGNORECASE).strip()
            nome = re.sub(r"\s*Conx\s+Vendas\s*$", "", nome, flags=re.IGNORECASE).strip()
            if nome and len(nome) < 80:
                return nome

    elif empresa == "Vibra Residencial":
        # Title or h1 has "SOBRE O NOME"
        for tag in soup.find_all(["h1", "h2"]):
            t = tag.get_text(strip=True)
            t = re.sub(r"^SOBRE\s+O\s+", "", t, flags=re.IGNORECASE).strip()
            if t and len(t) < 60 and "vibra" not in t.lower():
                return t

    elif empresa == "Benx":
        h1 = soup.find("h1")
        if h1:
            nome = h1.get_text(strip=True)
            if nome and len(nome) < 80:
                return nome

    elif empresa == "Kazzas":
        # h1 or title
        h1 = soup.find("h1")
        if h1:
            nome = h1.get_text(strip=True)
            if nome and len(nome) < 80 and nome.lower() != "kazzas":
                return nome

    elif empresa == "Mundo Apto":
        h1 = soup.find("h1")
        if h1:
            nome = h1.get_text(strip=True)
            if nome and len(nome) < 80 and nome.lower() != "mundo apto":
                return nome

    return None


def corrigir_via_requests(empresas_alvo):
    """Corrige nomes via requests (empresas com HTML server-side)."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    for empresa in empresas_alvo:
        cur.execute("SELECT id, nome, url_fonte FROM empreendimentos WHERE empresa=? AND url_fonte IS NOT NULL",
                    (empresa,))
        registros = cur.fetchall()
        logger.info(f"\n=== {empresa}: {len(registros)} registros ===")

        atualizados = 0
        erros = 0
        iguais = 0

        for i, r in enumerate(registros, 1):
            try:
                resp = session.get(r["url_fonte"], timeout=15)
                if resp.status_code != 200:
                    erros += 1
                    continue

                nome_real = extrair_nome_pagina(resp.text, empresa)

                if not nome_real:
                    if i <= 3:
                        logger.info(f"  [{i}/{len(registros)}] {r['nome'][:40]:40s} | sem nome extraido")
                    continue

                # Compare
                if nome_real.strip().lower() != r["nome"].strip().lower():
                    cur2.execute("UPDATE empreendimentos SET nome=? WHERE id=?", (nome_real, r["id"]))
                    atualizados += 1
                    logger.info(f"  [{i}/{len(registros)}] {r['nome'][:35]:35s} -> {nome_real}")
                else:
                    iguais += 1

                time.sleep(1)
            except Exception as e:
                erros += 1
                if i <= 3:
                    logger.info(f"  [{i}/{len(registros)}] ERRO: {e}")

        logger.info(f"  {empresa}: {atualizados} corrigidos, {iguais} ja corretos, {erros} erros")

    conn.commit()
    conn.close()


def corrigir_via_api_vivaz():
    """Corrige nomes Vivaz via API."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    cur.execute("SELECT id, nome, slug FROM empreendimentos WHERE empresa='Vivaz' AND slug IS NOT NULL")
    registros = cur.fetchall()
    logger.info(f"\n=== Vivaz (API): {len(registros)} registros ===")

    atualizados = 0
    for i, r in enumerate(registros, 1):
        try:
            resp = requests.post(
                "https://www.meuvivaz.com.br/imovel/informacoes/",
                json={"Url": r["slug"]},
                headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    nome_api = data.get("imovel", {}).get("Nome", "")
                    if nome_api and nome_api.strip().lower() != r["nome"].strip().lower():
                        cur2.execute("UPDATE empreendimentos SET nome=? WHERE id=?", (nome_api, r["id"]))
                        atualizados += 1
                        logger.info(f"  [{i}/{len(registros)}] {r['nome'][:35]:35s} -> {nome_api}")
            time.sleep(1.2)
        except Exception as e:
            pass

    conn.commit()
    conn.close()
    logger.info(f"  Vivaz: {atualizados} corrigidos")


def corrigir_via_api_metrocasa():
    """Corrige nomes Metrocasa via API."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    all_items = []
    for page in range(1, 10):
        r = session.get(f"https://www.metrocasa.com.br/api/properties?page={page}&perPage=50", timeout=15)
        data = r.json()
        items = data.get("list", [])
        if not items:
            break
        all_items.extend(items)

    logger.info(f"\n=== Metrocasa (API): {len(all_items)} items ===")

    atualizados = 0
    for item in all_items:
        slug = item.get("slug", "")
        nome_api = item.get("title", "")
        if not nome_api:
            continue

        cur.execute("SELECT id, nome FROM empreendimentos WHERE empresa='Metrocasa' AND url_fonte LIKE ?",
                    (f"%{slug}%",))
        row = cur.fetchone()
        if row and nome_api.strip().lower() != row[1].strip().lower():
            cur.execute("UPDATE empreendimentos SET nome=? WHERE id=?", (nome_api, row[0]))
            atualizados += 1
            logger.info(f"  {row[1][:35]:35s} -> {nome_api}")

    conn.commit()
    conn.close()
    logger.info(f"  Metrocasa: {atualizados} corrigidos")


def corrigir_via_api_kazzas():
    """Corrige nomes Kazzas via WP API."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    all_items = []
    page = 1
    while True:
        r = session.get(f"https://kazzas.com.br/wp-json/wp/v2/empreendimento?per_page=50&page={page}", timeout=15)
        if r.status_code != 200:
            break
        data = r.json()
        if not data:
            break
        all_items.extend(data)
        page += 1

    logger.info(f"\n=== Kazzas (WP API): {len(all_items)} items ===")

    atualizados = 0
    for item in all_items:
        slug = item.get("slug", "")
        nome_api = item.get("title", {}).get("rendered", "")
        if not nome_api:
            continue

        cur.execute("SELECT id, nome FROM empreendimentos WHERE empresa='Kazzas' AND url_fonte LIKE ?",
                    (f"%{slug}%",))
        row = cur.fetchone()
        if row and nome_api.strip().lower() != row[1].strip().lower():
            cur.execute("UPDATE empreendimentos SET nome=? WHERE id=?", (nome_api, row[0]))
            atualizados += 1
            logger.info(f"  {row[1][:35]:35s} -> {nome_api}")

    conn.commit()
    conn.close()
    logger.info(f"  Kazzas: {atualizados} corrigidos")


if __name__ == "__main__":
    logger.info("=== CORRIGINDO NOMES DOS EMPREENDIMENTOS ===\n")

    # 1. APIs (fast)
    corrigir_via_api_vivaz()
    corrigir_via_api_metrocasa()
    corrigir_via_api_kazzas()

    # 2. Requests (medium)
    corrigir_via_requests([
        "Plano&Plano",
        "Direcional",
        "Magik JC",
        "Pacaembu",
        "Conx",
        "Vibra Residencial",
        "Benx",
    ])

    logger.info("\n=== CONCLUIDO ===")

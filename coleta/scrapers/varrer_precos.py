"""
Varredura de preços e renda — visita cada página e extrai:
- preco_a_partir (R$ valor)
- renda_minima (R$ valor)
- preco_referencia (texto descritivo: "2 dorms 42m²", "unidade tipo", etc.)

Processa em batches de 200, salvando progresso em JSON.
Sobrescreve valores existentes (preço pode ter mudado).

Uso:
    python scrapers/varrer_precos.py
    python scrapers/varrer_precos.py --reset
    python scrapers/varrer_precos.py --batch 200
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

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "empreendimentos.db")
PROGRESSO_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "varredura_precos_progresso.json")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": []}


def salvar_progresso(prog):
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(prog, f)


def extrair_precos_pagina(url):
    """Visita página e extrai preço, renda e referência do preço."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12, verify=False)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None

    # Remover nav, footer, header
    for tag in soup.find_all(["nav", "footer", "header", "aside"]):
        tag.decompose()
    for tag in soup.find_all(class_=re.compile(r"menu|nav|footer|sidebar|widget", re.I)):
        tag.decompose()

    texto = soup.get_text(separator="\n", strip=True)
    resultado = {}

    # ============================================================
    # 1. PREÇO — "R$ XXX.XXX" ou "a partir de R$ XXX.XXX"
    # ============================================================
    # Buscar todos os preços na página
    precos_encontrados = []
    for m in re.finditer(r"R\$\s*([\d.,]+)", texto):
        try:
            valor_str = m.group(1)
            # Tratar formatos: "205.000,00" ou "205000" ou "205.000"
            if "," in valor_str:
                valor = float(valor_str.replace(".", "").replace(",", "."))
            else:
                valor = float(valor_str.replace(".", ""))
                # Se ficou < 1000, provavelmente era "205.000" sem vírgula = 205000
                if valor < 1000 and "." in m.group(1):
                    valor = float(valor_str.replace(".", ""))

            # Filtrar: preço de imóvel MCMV é entre 100k e 600k
            if 80000 <= valor <= 600000:
                # Capturar contexto (30 chars antes e depois)
                start = max(0, m.start() - 60)
                end = min(len(texto), m.end() + 60)
                contexto = texto[start:end].replace("\n", " ").strip()
                precos_encontrados.append((valor, contexto))
        except (ValueError, TypeError):
            pass

    if precos_encontrados:
        # Pegar o menor preço (geralmente é o "a partir de")
        precos_encontrados.sort(key=lambda x: x[0])
        melhor_preco, contexto_preco = precos_encontrados[0]
        resultado["preco_a_partir"] = melhor_preco

        # Tentar extrair referência do preço (ex: "2 dorms 42m²", "unidade tipo")
        referencia = _extrair_referencia_preco(contexto_preco, texto)
        if referencia:
            resultado["preco_referencia"] = referencia

    # ============================================================
    # 2. RENDA — "renda a partir de R$ X.XXX" ou "renda familiar R$ X.XXX"
    # ============================================================
    renda_patterns = [
        r"renda\s+(?:familiar\s+)?(?:a\s+partir\s+de\s+)?R\$\s*([\d.,]+)",
        r"renda\s+(?:m[ií]nima\s+)?(?:de\s+)?R\$\s*([\d.,]+)",
        r"a\s+partir\s+de\s+R\$\s*([\d.,]+)\s*(?:de\s+)?renda",
        r"R\$\s*([\d.,]+)\s*(?:de\s+)?renda\s+(?:familiar|m[ií]nima)",
    ]

    for pattern in renda_patterns:
        m = re.search(pattern, texto, re.IGNORECASE)
        if m:
            try:
                valor_str = m.group(1)
                if "," in valor_str:
                    valor = float(valor_str.replace(".", "").replace(",", "."))
                else:
                    valor = float(valor_str.replace(".", ""))

                # Renda MCMV: 1.500 a 10.000
                if 1000 <= valor <= 15000:
                    resultado["renda_minima"] = valor
                    break
            except (ValueError, TypeError):
                pass

    # Se não achou renda via patterns, tentar via texto "renda" + valor próximo
    if "renda_minima" not in resultado:
        # Buscar "renda" no texto e pegar o valor mais próximo
        for m in re.finditer(r"renda", texto, re.IGNORECASE):
            trecho = texto[m.start():min(len(texto), m.start() + 100)]
            renda_match = re.search(r"R\$\s*([\d.,]+)", trecho)
            if renda_match:
                try:
                    val_str = renda_match.group(1)
                    if "," in val_str:
                        val = float(val_str.replace(".", "").replace(",", "."))
                    else:
                        val = float(val_str.replace(".", ""))
                    if 1000 <= val <= 15000:
                        resultado["renda_minima"] = val
                        break
                except (ValueError, TypeError):
                    pass

    return resultado if resultado else None


def _extrair_referencia_preco(contexto, texto_completo):
    """Extrai a referência do preço (qual unidade/tipologia o preço se refere)."""
    # Patterns comuns:
    # "a partir de R$ 205.000* | *Ref. 2 dorms 42m²"
    # "R$ 205.000,00¹ ¹Referente à unidade tipo 2 dormitórios"
    # "2 quartos a partir de R$ 205.000"

    referencia = None

    # 1. Buscar asterisco/nota de rodapé próximo ao preço
    ref_patterns = [
        r"[*¹²³⁴]\s*(?:Ref\.?(?:erente)?|Valor)\s*(?:[àa])?\s*(.{10,80}?)(?:\.|$|\n)",
        r"(?:refer[eê]ncia|referente)\s*(?:[àa])?\s*(.{10,80}?)(?:\.|$|\n)",
        r"(?:unidade\s+(?:tipo|de))\s+(.{5,60}?)(?:\.|$|\n)",
    ]

    for pattern in ref_patterns:
        m = re.search(pattern, texto_completo, re.IGNORECASE)
        if m:
            ref = m.group(1).strip()
            if len(ref) > 5 and len(ref) < 100:
                referencia = ref
                break

    # 2. Se não achou nota, extrair do contexto: "2 dorms a partir de R$..."
    if not referencia:
        m = re.search(
            r"(\d+\s*(?:dorms?\.?|quartos?|dormit[oó]rios?|su[ií]tes?|studios?)"
            r"(?:\s*(?:com|c/)?\s*(?:su[ií]te|varanda|garden))?"
            r"(?:\s*[\d.,]+\s*m[²2])?)",
            contexto, re.IGNORECASE
        )
        if m:
            referencia = m.group(1).strip()

    return referencia


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

    pendentes = [(rid, emp, nome, url) for rid, emp, nome, url in todos if rid not in processados_set]

    batch = pendentes[:batch_size]
    print(f"Batch: {len(batch)} de {len(pendentes)} pendentes ({len(processados_set)} já processados)", flush=True)

    if not batch:
        print("Nada para processar!", flush=True)
        conn.close()
        return

    precos_encontrados = 0
    rendas_encontradas = 0
    refs_encontradas = 0

    for i, (rid, emp, nome, url) in enumerate(batch):
        dados = extrair_precos_pagina(url)

        if dados:
            sets = []
            vals = []

            if "preco_a_partir" in dados:
                sets.append("preco_a_partir=?")
                vals.append(dados["preco_a_partir"])
                precos_encontrados += 1

            if "renda_minima" in dados:
                sets.append("renda_minima=?")
                vals.append(dados["renda_minima"])
                rendas_encontradas += 1

            if "preco_referencia" in dados:
                sets.append("preco_referencia=?")
                vals.append(dados["preco_referencia"])
                refs_encontradas += 1

            if sets:
                sets.append("data_atualizacao=datetime('now')")
                vals.append(rid)
                cur.execute(f"UPDATE empreendimentos SET {', '.join(sets)} WHERE id=?", vals)

        processados_set.add(rid)

        if (i + 1) % 50 == 0:
            conn.commit()
            prog["processados"] = list(processados_set)
            salvar_progresso(prog)
            print(f"  {precos_encontrados} preços, {rendas_encontradas} rendas, {refs_encontradas} refs | {i+1}/{len(batch)}", flush=True)

        time.sleep(0.3)

    conn.commit()
    prog["processados"] = list(processados_set)
    salvar_progresso(prog)

    print(f"\nBatch concluído:", flush=True)
    print(f"  Preços encontrados: {precos_encontrados}/{len(batch)}", flush=True)
    print(f"  Rendas encontradas: {rendas_encontradas}/{len(batch)}", flush=True)
    print(f"  Referências: {refs_encontradas}/{len(batch)}", flush=True)
    print(f"  Total processados: {len(processados_set)}/{len(todos)}", flush=True)

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

"""
Enriquece dados de total_unidades, numero_vagas, varanda e quartos.
Fontes:
  1. API Vivaz -> vagas, quartos, varanda, bairro
  2. Selenium (Cury, Metrocasa, Kazzas, Conx, Benx, Pacaembu, Mundo Apto) -> total_unidades, vagas
  3. dormitorios_descricao parsing -> apto_1_dorm, apto_2_dorms, apto_3_dorms flags
  4. apto_terraco -> lazer_varanda (copia)
"""
import sys
sys.stdout.reconfigure(errors="replace")
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import requests
import time
import re
import json
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("enriquecer")

DB = "data/empreendimentos.db"


# ============================================================
# 1. Vivaz API enrichment
# ============================================================
def enriquecer_vivaz():
    """Usa API Vivaz para extrair vagas, quartos, varanda, bairro."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    cur.execute("""SELECT id, nome, slug, numero_vagas, bairro
                   FROM empreendimentos WHERE empresa='Vivaz' AND slug IS NOT NULL""")
    registros = cur.fetchall()
    logger.info(f"\n=== VIVAZ API: {len(registros)} registros ===")

    atualizados = 0
    erros = 0
    for i, r in enumerate(registros, 1):
        try:
            resp = requests.post(
                "https://www.meuvivaz.com.br/imovel/informacoes/",
                json={"Url": r["slug"]},
                headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
                timeout=15
            )
            if resp.status_code != 200:
                logger.info(f"  [{i}/{len(registros)}] {r['nome']}: HTTP {resp.status_code}")
                erros += 1
                time.sleep(1)
                continue

            data = resp.json()
            if not data.get("success"):
                logger.info(f"  [{i}/{len(registros)}] {r['nome']}: API success=false")
                erros += 1
                time.sleep(1)
                continue

            imovel = data.get("imovel", {})
            updates = {}

            # Vagas
            vagas = imovel.get("VagasGaragem")
            if vagas is not None and (not r["numero_vagas"] or r["numero_vagas"] == ""):
                updates["numero_vagas"] = str(vagas)

            # Varanda
            if imovel.get("OpcaoDeVaranda"):
                updates["lazer_varanda"] = 1
                updates["apto_terraco"] = 1

            # Bairro
            loc = imovel.get("Localizacao", {})
            if loc.get("Bairro") and (not r["bairro"] or r["bairro"] == ""):
                updates["bairro"] = loc["Bairro"]

            # Quartos -> atualizar flags
            quartos_str = imovel.get("Quartos", "")
            if quartos_str:
                updates["dormitorios_descricao"] = quartos_str
                q = quartos_str.lower()
                if "1" in q or "studio" in q:
                    updates["apto_1_dorm"] = 1
                if "2" in q:
                    updates["apto_2_dorms"] = 1
                if "3" in q:
                    updates["apto_3_dorms"] = 1

            if updates:
                sets = ", ".join(f"{k}=?" for k in updates)
                vals = list(updates.values()) + [r["id"]]
                cur2.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
                atualizados += 1
                if i <= 5 or i % 20 == 0:
                    logger.info(f"  [{i}/{len(registros)}] {r['nome']}: {list(updates.keys())}")
            time.sleep(1.2)

        except Exception as e:
            logger.info(f"  [{i}/{len(registros)}] {r['nome']}: ERRO {e}")
            erros += 1
            time.sleep(1)

    conn.commit()
    conn.close()
    logger.info(f"  Vivaz: {atualizados} atualizados, {erros} erros")
    return atualizados


# ============================================================
# 2. Selenium enrichment for total_unidades
# ============================================================
def enriquecer_selenium():
    """Usa Selenium para extrair total_unidades e vagas das paginas."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    # Empresas que precisam Selenium e tem baixa completude de total_unidades
    empresas = ["Cury", "Metrocasa", "Kazzas", "Conx", "Benx", "Pacaembu", "Mundo Apto"]

    cur.execute(f"""SELECT id, nome, empresa, url_fonte, total_unidades, numero_vagas
                    FROM empreendimentos
                    WHERE empresa IN ({','.join('?' for _ in empresas)})
                    AND url_fonte IS NOT NULL AND url_fonte != ''
                    AND (total_unidades IS NULL OR total_unidades = 0)
                    ORDER BY empresa, nome""", empresas)
    registros = cur.fetchall()
    logger.info(f"\n=== SELENIUM: {len(registros)} registros sem total_unidades ===")

    if not registros:
        conn.close()
        return 0

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    logger.info("  Chrome iniciado")

    atualizados = 0
    erros = 0

    try:
        for i, r in enumerate(registros, 1):
            try:
                driver.set_page_load_timeout(20)
                try:
                    driver.get(r["url_fonte"])
                except Exception:
                    pass  # Timeout is OK, page may have loaded enough
                time.sleep(3)  # Wait for JS rendering

                # Quick scroll to trigger lazy loading
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(" ", strip=True)

                updates = {}

                # Extract total_unidades
                unidades = _extrair_total_unidades(text)
                if unidades and unidades > 0:
                    updates["total_unidades"] = unidades

                # Extract vagas
                if not r["numero_vagas"] or r["numero_vagas"] == "" or r["numero_vagas"] == "0":
                    vagas = _extrair_vagas(text)
                    if vagas:
                        updates["numero_vagas"] = str(vagas)

                # Extract amenities
                text_lower = text.lower()
                amenities = _extrair_amenidades(text_lower)
                updates.update(amenities)

                if updates:
                    sets = ", ".join(f"{k}=?" for k in updates)
                    vals = list(updates.values()) + [r["id"]]
                    cur2.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
                    atualizados += 1
                    logger.info(f"  [{i}/{len(registros)}] {r['empresa']:12s} | {r['nome'][:35]:35s} | {updates}")
                else:
                    if i <= 10 or i % 20 == 0:
                        logger.info(f"  [{i}/{len(registros)}] {r['empresa']:12s} | {r['nome'][:35]:35s} | sem dados")

                time.sleep(0.5)

            except Exception as e:
                logger.info(f"  [{i}/{len(registros)}] {r['empresa']:12s} | {r['nome'][:35]:35s} | ERRO: {e}")
                erros += 1
                time.sleep(0.5)

    finally:
        driver.quit()
        logger.info("  Chrome fechado")

    conn.commit()
    conn.close()
    logger.info(f"  Selenium: {atualizados} atualizados, {erros} erros")
    return atualizados


def _extrair_total_unidades(text):
    """Extrai total de unidades do texto da pagina."""
    patterns = [
        # "520 unidades residenciais" or "520 unidades"
        r"(?<!\d[\-./])(\d[\d.]*)\s*(?:unidades?\s*(?:residenciais?|habitacionais?)?|UHs?)",
        # "total de 520 apartamentos"
        r"(?:total\s*(?:de\s*)?)?(\d[\d.]*)\s*apartamentos?",
        # "520 casas"
        r"(?<!\d[\-./])(\d[\d.]*)\s*(?:casas|sobrados)",
        # "unidades: 520"
        r"(?:unidades|apartamentos)\s*[:=]\s*(\d[\d.]*)",
        # "3 torres com 520 unidades"
        r"(\d[\d.]*)\s*torres?\s*(?:com|e)\s*(\d[\d.]*)\s*(?:unidades|apartamentos|aptos?)",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            # For the torre pattern, return the second group
            if "torres" in pat:
                val = m.group(2)
            else:
                val = m.group(1)
            val = val.replace(".", "")
            try:
                n = int(val)
                if 20 <= n <= 5000:  # Reasonable range for a single development
                    # Check context - skip if preceded by phone-like pattern
                    start = max(0, m.start() - 10)
                    prefix = text[start:m.start()]
                    if re.search(r"[\d\-()]{4,}", prefix):
                        continue  # Likely phone number
                    return n
            except ValueError:
                pass
    return None


def _extrair_vagas(text):
    """Extrai numero de vagas do texto."""
    patterns = [
        r"(\d+)\s*vagas?\s*(?:de\s*)?(?:garagem|estacionamento|auto)",
        r"vagas?\s*[:=]\s*(\d+)",
        r"(\d+)\s*vagas?",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                n = int(m.group(1))
                if 0 < n <= 5:  # Per unit
                    return n
            except ValueError:
                pass
    return None


def _extrair_amenidades(text_lower):
    """Extrai amenidades do texto."""
    updates = {}
    if "piscina" in text_lower:
        updates["lazer_piscina"] = 1
    if any(w in text_lower for w in ["churrasqueira", "churras", "grill", "bbq"]):
        updates["lazer_churrasqueira"] = 1
    if any(w in text_lower for w in ["varanda", "sacada", "terraco", "terraço", "balcony"]):
        updates["lazer_varanda"] = 1
        updates["apto_terraco"] = 1
    return updates


# ============================================================
# 3. Parse dormitorios_descricao -> flags
# ============================================================
def atualizar_flags_dorms():
    """Atualiza apto_1_dorm, apto_2_dorms, apto_3_dorms a partir de dormitorios_descricao."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""SELECT id, dormitorios_descricao FROM empreendimentos
                   WHERE dormitorios_descricao IS NOT NULL AND dormitorios_descricao != ''""")
    registros = cur.fetchall()
    logger.info(f"\n=== FLAGS DORMS: {len(registros)} registros com dormitorios_descricao ===")

    atualizados = 0
    for r in registros:
        desc = r[1].lower()
        d1 = 1 if re.search(r"(?:^|\b)1\s*(?:dorm|quarto|qto)", desc) or "studio" in desc else 0
        d2 = 1 if re.search(r"(?:^|\b)2\s*(?:dorm|quarto|qto)", desc) else 0
        d3 = 1 if re.search(r"(?:^|\b)3\s*(?:dorm|quarto|qto)", desc) else 0

        cur.execute("""UPDATE empreendimentos SET apto_1_dorm=?, apto_2_dorms=?, apto_3_dorms=?
                       WHERE id=?""", (d1, d2, d3, r[0]))
        atualizados += 1

    conn.commit()
    conn.close()
    logger.info(f"  Flags dorms: {atualizados} atualizados")
    return atualizados


# ============================================================
# 4. Copy apto_terraco -> lazer_varanda
# ============================================================
def copiar_terraco_varanda():
    """Copia apto_terraco=1 -> lazer_varanda=1."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("UPDATE empreendimentos SET lazer_varanda=1 WHERE apto_terraco=1")
    n = cur.rowcount
    conn.commit()
    conn.close()
    logger.info(f"\n=== VARANDA: {n} registros atualizados (apto_terraco -> lazer_varanda) ===")
    return n


# ============================================================
# 5. Direcional/Plano&Plano - re-parse from existing pages
# ============================================================
def enriquecer_direcional_planoplano():
    """Re-scrape Direcional e Plano&Plano para total_unidades faltantes via requests."""
    from bs4 import BeautifulSoup

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    cur.execute("""SELECT id, nome, empresa, url_fonte
                   FROM empreendimentos
                   WHERE empresa IN ('Direcional', 'Plano&Plano')
                   AND url_fonte IS NOT NULL AND url_fonte != ''
                   AND (total_unidades IS NULL OR total_unidades = 0)""")
    registros = cur.fetchall()
    logger.info(f"\n=== DIRECIONAL/PLANO: {len(registros)} sem total_unidades ===")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    atualizados = 0
    for i, r in enumerate(registros, 1):
        try:
            resp = session.get(r["url_fonte"], timeout=15)
            if resp.status_code != 200:
                continue

            text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)
            unidades = _extrair_total_unidades(text)
            if unidades:
                cur2.execute("UPDATE empreendimentos SET total_unidades=? WHERE id=?", (unidades, r["id"]))
                atualizados += 1
                logger.info(f"  [{i}/{len(registros)}] {r['empresa']:12s} | {r['nome'][:35]:35s} | {unidades} unidades")

            time.sleep(1.5)
        except Exception as e:
            logger.info(f"  [{i}/{len(registros)}] {r['empresa']:12s} | ERRO: {e}")

    conn.commit()
    conn.close()
    logger.info(f"  Direcional/Plano: {atualizados} atualizados")
    return atualizados


# ============================================================
# 6. Magik JC - re-scrape for total_unidades
# ============================================================
def enriquecer_magik():
    """Re-scrape Magik JC pages for total_unidades."""
    from bs4 import BeautifulSoup

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    cur.execute("""SELECT id, nome, url_fonte
                   FROM empreendimentos
                   WHERE empresa='Magik JC'
                   AND url_fonte IS NOT NULL
                   AND (total_unidades IS NULL OR total_unidades = 0)""")
    registros = cur.fetchall()
    logger.info(f"\n=== MAGIK JC: {len(registros)} sem total_unidades ===")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    atualizados = 0
    for i, r in enumerate(registros, 1):
        try:
            resp = session.get(r["url_fonte"], timeout=15)
            if resp.status_code != 200:
                continue

            text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)
            unidades = _extrair_total_unidades(text)
            if unidades:
                cur2.execute("UPDATE empreendimentos SET total_unidades=? WHERE id=?", (unidades, r["id"]))
                atualizados += 1
                logger.info(f"  [{i}/{len(registros)}] {r['nome'][:40]:40s} | {unidades} unidades")

            time.sleep(1.5)
        except Exception as e:
            logger.info(f"  [{i}/{len(registros)}] ERRO: {e}")

    conn.commit()
    conn.close()
    logger.info(f"  Magik: {atualizados} atualizados")
    return atualizados


# ============================================================
# Relatorio de completude
# ============================================================
def relatorio():
    """Mostra completude atual."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM empreendimentos")
    total = cur.fetchone()[0]

    campos = ["total_unidades", "numero_vagas", "lazer_piscina", "lazer_churrasqueira",
              "apto_terraco", "lazer_varanda", "apto_1_dorm", "apto_2_dorms", "apto_3_dorms",
              "qty_1dorm", "qty_2dorms", "qty_3dorms"]

    print(f"\n=== COMPLETUDE GERAL ({total} registros) ===")
    for c in campos:
        try:
            cur.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {c} IS NOT NULL AND {c} != '' AND {c} != 0")
            n = cur.fetchone()[0]
            pct = 100 * n / total
            bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
            print(f"  {c:25s}: {n:4d}/{total} ({pct:5.1f}%) [{bar}]")
        except Exception as e:
            print(f"  {c:25s}: ERRO - {e}")

    print(f"\n=== TOTAL_UNIDADES POR EMPRESA ===")
    cur.execute("""SELECT empresa, COUNT(*) as total,
                   SUM(CASE WHEN total_unidades IS NOT NULL AND total_unidades > 0 THEN 1 ELSE 0 END) as preenchido
                   FROM empreendimentos GROUP BY empresa ORDER BY total DESC""")
    for r in cur.fetchall():
        pct = 100 * r[2] / r[1] if r[1] > 0 else 0
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(f"  {r[0]:20s}: {r[2]:3d}/{r[1]:3d} ({pct:5.1f}%) [{bar}]")

    conn.close()


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enriquece dados de unidades e amenidades")
    parser.add_argument("modo", choices=["vivaz", "selenium", "requests", "magik", "flags", "varanda", "tudo", "relatorio"],
                        help="Modo de operacao")
    args = parser.parse_args()

    if args.modo == "relatorio":
        relatorio()
    elif args.modo == "vivaz":
        enriquecer_vivaz()
        relatorio()
    elif args.modo == "selenium":
        enriquecer_selenium()
        relatorio()
    elif args.modo == "requests":
        enriquecer_direcional_planoplano()
        enriquecer_magik()
        relatorio()
    elif args.modo == "magik":
        enriquecer_magik()
    elif args.modo == "flags":
        atualizar_flags_dorms()
        copiar_terraco_varanda()
        relatorio()
    elif args.modo == "varanda":
        copiar_terraco_varanda()
    elif args.modo == "tudo":
        logger.info("=== ENRIQUECIMENTO COMPLETO ===")
        # 1. Quick fixes first
        atualizar_flags_dorms()
        copiar_terraco_varanda()
        # 2. API (fast)
        enriquecer_vivaz()
        # 3. Requests (medium)
        enriquecer_direcional_planoplano()
        enriquecer_magik()
        # 4. Selenium (slow)
        enriquecer_selenium()
        # 5. Report
        relatorio()

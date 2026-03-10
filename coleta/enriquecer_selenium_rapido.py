"""Selenium rapido - testa poucos registros por empresa para ver o que e possivel extrair."""
import sys
sys.stdout.reconfigure(errors="replace")
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import time
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("sel_rapido")

DB = "data/empreendimentos.db"


def _extrair_total_unidades(text):
    patterns = [
        r"(?<!\d[\-./])(\d[\d.]*)\s*(?:unidades?\s*(?:residenciais?|habitacionais?)?|UHs?)",
        r"(?:total\s*(?:de\s*)?)?(\d[\d.]*)\s*apartamentos?",
        r"(?<!\d[\-./])(\d[\d.]*)\s*(?:casas|sobrados)",
        r"(?:unidades|apartamentos)\s*[:=]\s*(\d[\d.]*)",
        r"(\d[\d.]*)\s*torres?\s*(?:com|e)\s*(\d[\d.]*)\s*(?:unidades|apartamentos|aptos?)",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            if "torres" in pat:
                val = m.group(2)
            else:
                val = m.group(1)
            val = val.replace(".", "")
            try:
                n = int(val)
                if 20 <= n <= 5000:
                    start = max(0, m.start() - 10)
                    prefix = text[start:m.start()]
                    if re.search(r"[\d\-()]{4,}", prefix):
                        continue
                    return n
            except ValueError:
                pass
    return None


def _extrair_vagas(text):
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
                if 0 < n <= 5:
                    return n
            except ValueError:
                pass
    return None


def main():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    empresas = ["Cury", "Metrocasa", "Kazzas", "Conx", "Pacaembu", "Mundo Apto", "Benx"]
    SAMPLE_SIZE = 5  # test 5 per company

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-images")
    options.page_load_strategy = "eager"  # Don't wait for all resources

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_page_load_timeout(15)
    logger.info("Chrome iniciado (modo rapido)")

    total_atualizados = 0

    try:
        for empresa in empresas:
            cur.execute("""SELECT id, nome, url_fonte, numero_vagas
                           FROM empreendimentos
                           WHERE empresa=?
                           AND url_fonte IS NOT NULL AND url_fonte != ''
                           AND (total_unidades IS NULL OR total_unidades = 0)
                           LIMIT ?""", (empresa, SAMPLE_SIZE))
            registros = cur.fetchall()
            if not registros:
                continue

            logger.info(f"\n=== {empresa} ({len(registros)} amostras) ===")
            found_unidades = 0
            found_vagas = 0

            for r in registros:
                try:
                    try:
                        driver.get(r["url_fonte"])
                    except Exception:
                        pass
                    time.sleep(3)

                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(" ", strip=True)

                    unidades = _extrair_total_unidades(text)
                    vagas = _extrair_vagas(text)

                    updates = {}
                    if unidades:
                        updates["total_unidades"] = unidades
                        found_unidades += 1
                    if vagas and (not r["numero_vagas"] or r["numero_vagas"] == ""):
                        updates["numero_vagas"] = str(vagas)
                        found_vagas += 1

                    # Amenities
                    text_lower = text.lower()
                    if "piscina" in text_lower:
                        updates["lazer_piscina"] = 1
                    if any(w in text_lower for w in ["churrasqueira", "churras", "grill"]):
                        updates["lazer_churrasqueira"] = 1
                    if any(w in text_lower for w in ["varanda", "sacada", "terraco", "terraço"]):
                        updates["lazer_varanda"] = 1
                        updates["apto_terraco"] = 1

                    if updates:
                        sets = ", ".join(f"{k}=?" for k in updates)
                        vals = list(updates.values()) + [r["id"]]
                        cur2.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
                        total_atualizados += 1

                    status = f"units={unidades}" if unidades else "no units"
                    status += f" vagas={vagas}" if vagas else ""
                    logger.info(f"  {r['nome'][:40]:40s} | {status}")

                except Exception as e:
                    logger.info(f"  {r['nome'][:40]:40s} | ERRO: {e}")

            logger.info(f"  -> Unidades: {found_unidades}/{len(registros)}, Vagas: {found_vagas}/{len(registros)}")

    finally:
        driver.quit()
        logger.info("\nChrome fechado")

    conn.commit()
    conn.close()
    logger.info(f"\nTotal atualizados: {total_atualizados}")


if __name__ == "__main__":
    main()

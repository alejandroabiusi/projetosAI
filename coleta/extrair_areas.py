"""
Extração de área do terreno e cálculo de áreas construídas estimadas.
=====================================================================
1. Scraping: busca area_terreno_m2 nas páginas dos empreendimentos
2. Cálculo: area_construida_min/max_est = total_unidades × area_min/max_m2
3. Cálculo: coeficiente_adensamento_min/max = area_construida / area_terreno

Uso:
    python extrair_areas.py
    python extrair_areas.py --empresa "Plano&Plano"
    python extrair_areas.py --apenas-calcular      # Pula scraping, só calcula
    python extrair_areas.py --limite 5
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import REQUESTS as REQ_CONFIG, LOGS_DIR
from data.database import get_connection, garantir_coluna, atualizar_empreendimento

# ============================================================
# CONFIGURACAO
# ============================================================

HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# Empresas que precisam de Selenium (SPAs)
EMPRESAS_SELENIUM = {"Cury", "Metrocasa", "HM Engenharia"}

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("extrair_areas")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "extrair_areas.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ============================================================
# EXTRACAO DE AREA DO TERRENO (HTML)
# ============================================================

def _parse_numero_br(texto):
    """Converte número em formato brasileiro (1.234,56) para float."""
    texto = texto.strip()
    # Formato: 1.234,56 ou 1234,56
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    return float(texto)


# Padrões de regex para area do terreno, ordenados do mais específico ao mais genérico
TERRENO_PATTERNS = [
    # "Área do terreno: 4.913,89 m²"
    r"[áa]rea\s+(?:total\s+)?do\s+terreno[:\s]+([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "Terreno: 4.913,89 m²"
    r"[Tt]erreno[:\s]+([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "terreno de 4.913,89 m²"
    r"terreno\s+de\s+([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "Área do terreno\n4.913,89 m²" (label e valor em linhas separadas)
    r"[áa]rea\s+(?:total\s+)?do\s+terreno\s*\n\s*([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "Terreno\n4.913,89 m²"
    r"[Tt]erreno\s*\n\s*([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "4.913,89 m² de terreno"
    r"([0-9.]+(?:,[0-9]+)?)\s*m[²2]\s+de\s+terreno",
    # "Área total: 4.913,89m²" (pode ser terreno em contextos específicos)
    r"[áa]rea\s+total[:\s]+([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # "lote de X m²" ou "lote: X m²"
    r"lote[:\s]+(?:de\s+)?([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
    # Variante com "metragem do terreno"
    r"metragem\s+do\s+terreno[:\s]+([0-9.]+(?:,[0-9]+)?)\s*m[²2]",
]

# Padrão para contexto de "área total" que NÃO é terreno (área do apartamento)
FALSO_POSITIVO_AREA_TOTAL = re.compile(
    r"(?:apartamento|apto|unidade|privativ|[úu]til|coberta)\s+",
    re.IGNORECASE
)


def extrair_area_terreno(texto):
    """Extrai área do terreno de um texto HTML."""
    if not texto:
        return None

    for pattern in TERRENO_PATTERNS:
        matches = list(re.finditer(pattern, texto, re.IGNORECASE | re.MULTILINE))
        for match in matches:
            try:
                valor = _parse_numero_br(match.group(1))
                # Sanity checks
                if valor < 50:
                    continue  # Muito pequeno para terreno
                if valor > 500000:
                    continue  # Muito grande, provavelmente erro

                # Para "área total", verificar se não é área do apartamento
                if "total" in pattern.lower() and "terreno" not in pattern.lower():
                    # Verificar contexto ao redor
                    start = max(0, match.start() - 100)
                    contexto = texto[start:match.start()].lower()
                    if FALSO_POSITIVO_AREA_TOTAL.search(contexto):
                        continue
                    # Se valor < 200, provavelmente é área do apto
                    if valor < 200:
                        continue

                return valor
            except (ValueError, TypeError):
                continue

    return None


def extrair_area_terreno_json_ld(soup):
    """Tenta extrair área do terreno de JSON-LD estruturado."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0]
            # Schema.org RealEstateListing
            floor_size = data.get("floorSize") or {}
            if isinstance(floor_size, dict):
                val = floor_size.get("value")
                if val:
                    return float(str(val).replace(",", "."))
            # Propriedade customizada
            for key in ["landArea", "areaTerreno", "lotSize"]:
                val = data.get(key)
                if val:
                    if isinstance(val, dict):
                        val = val.get("value", val.get("minValue"))
                    if val:
                        return float(str(val).replace(",", "."))
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            continue
    return None


def extrair_area_terreno_meta(soup):
    """Tenta extrair de meta tags OG ou customizadas."""
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") or meta.get("name", "")
        if "terreno" in prop.lower() or "land" in prop.lower():
            val = meta.get("content", "")
            try:
                return float(re.sub(r"[^\d.,]", "", val).replace(".", "").replace(",", "."))
            except (ValueError, TypeError):
                pass
    return None


def extrair_area_terreno_pagina(soup, texto):
    """Combina todas as estratégias de extração."""
    # 1. JSON-LD
    val = extrair_area_terreno_json_ld(soup)
    if val and 50 <= val <= 500000:
        return val

    # 2. Meta tags
    val = extrair_area_terreno_meta(soup)
    if val and 50 <= val <= 500000:
        return val

    # 3. Regex no texto
    val = extrair_area_terreno(texto)
    if val:
        return val

    return None


# ============================================================
# SELENIUM
# ============================================================

_driver = None


def _get_driver():
    global _driver
    if _driver is None:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--user-agent={HEADERS['User-Agent']}")
        try:
            _driver = webdriver.Chrome(options=options)
        except Exception:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            _driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        _driver.set_page_load_timeout(30)
    return _driver


def _close_driver():
    global _driver
    if _driver:
        _driver.quit()
        _driver = None


# ============================================================
# CALCULOS
# ============================================================

def calcular_areas_construidas(conn):
    """Calcula area_construida_min/max_est e coeficiente_adensamento para todos os registros."""
    cursor = conn.cursor()

    # Garantir colunas
    for col in ["area_construida_min_est", "area_construida_max_est",
                "coeficiente_adensamento_min", "coeficiente_adensamento_max"]:
        garantir_coluna(col, "REAL")

    cursor.execute("""
        SELECT empresa, nome, total_unidades, area_min_m2, area_max_m2, area_terreno_m2
        FROM empreendimentos
        WHERE total_unidades IS NOT NULL AND total_unidades > 0
        AND (area_min_m2 IS NOT NULL OR area_max_m2 IS NOT NULL)
    """)
    rows = cursor.fetchall()

    atualizados = 0
    for row in rows:
        empresa, nome = row["empresa"], row["nome"]
        total_un = float(row["total_unidades"])
        area_min = float(row["area_min_m2"]) if row["area_min_m2"] else None
        area_max = float(row["area_max_m2"]) if row["area_max_m2"] else None
        area_terreno = float(row["area_terreno_m2"]) if row["area_terreno_m2"] else None

        dados = {}

        # Se só tem um dos dois, usar o mesmo para ambos
        if area_min and not area_max:
            area_max = area_min
        if area_max and not area_min:
            area_min = area_max

        if area_min and area_max:
            ac_min = total_un * area_min
            ac_max = total_un * area_max
            dados["area_construida_min_est"] = round(ac_min, 2)
            dados["area_construida_max_est"] = round(ac_max, 2)

            if area_terreno and area_terreno > 0:
                dados["coeficiente_adensamento_min"] = round(ac_min / area_terreno, 2)
                dados["coeficiente_adensamento_max"] = round(ac_max / area_terreno, 2)

        if dados:
            atualizar_empreendimento(empresa, nome, dados)
            atualizados += 1

    return atualizados


# ============================================================
# SCRAPING DE AREA DO TERRENO
# ============================================================

def scrape_area_terreno(args):
    """Busca area_terreno_m2 nas páginas dos empreendimentos que não têm."""
    conn = get_connection()
    cursor = conn.cursor()

    garantir_coluna("area_terreno_m2", "REAL")

    conditions = ["url_fonte IS NOT NULL", "url_fonte != ''",
                  "(area_terreno_m2 IS NULL OR area_terreno_m2 = 0)"]
    params = []

    if args.empresa and args.empresa.lower() != "todas":
        conditions.append("empresa = ?")
        params.append(args.empresa)

    where = " AND ".join(conditions)
    query = f"SELECT empresa, nome, slug, url_fonte FROM empreendimentos WHERE {where} ORDER BY empresa"
    if args.limite > 0:
        query += f" LIMIT {args.limite}"

    rows = cursor.execute(query, params).fetchall()
    conn.close()

    total = len(rows)
    logger.info(f"Empreendimentos sem area_terreno_m2: {total}")

    if not total:
        return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    encontrados = 0
    erros = 0
    usando_selenium = False

    try:
        for i, row in enumerate(rows, 1):
            empresa, nome, slug, url = row["empresa"], row["nome"], row["slug"], row["url_fonte"]

            if i % 50 == 0 or i == 1:
                logger.info(f"  Progresso: {i}/{total} ({encontrados} encontrados)")

            try:
                html = None
                if empresa in EMPRESAS_SELENIUM:
                    if not usando_selenium:
                        logger.info(f"  Iniciando Chrome para {empresa}...")
                        usando_selenium = True
                    driver = _get_driver()
                    try:
                        driver.get(url)
                        time.sleep(4)
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(2)
                        html = driver.page_source
                    except Exception as e:
                        logger.debug(f"  Selenium erro {nome}: {e}")
                else:
                    try:
                        resp = session.get(url, timeout=20)
                        if resp.status_code == 200:
                            html = resp.text
                    except Exception as e:
                        logger.debug(f"  Requests erro {nome}: {e}")

                if not html:
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "html.parser")
                texto = soup.get_text(separator="\n", strip=True)

                area = extrair_area_terreno_pagina(soup, texto)

                if area:
                    atualizar_empreendimento(empresa, nome, {"area_terreno_m2": area})
                    encontrados += 1
                    logger.info(f"    {empresa} | {nome}: {area:.2f} m²")

                # Delay
                delay = 3 if empresa in EMPRESAS_SELENIUM else 1
                time.sleep(delay)

            except Exception as e:
                logger.error(f"  ERRO {nome}: {e}")
                erros += 1

    finally:
        _close_driver()

    logger.info(f"  Scraping concluido: {encontrados} terrenos encontrados, {erros} erros")
    return encontrados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Extração de área do terreno e cálculo de áreas construídas")
    parser.add_argument("--empresa", type=str, default="todas",
                        help="Empresa a processar (nome no banco ou 'todas')")
    parser.add_argument("--limite", type=int, default=0,
                        help="Limite de registros para scraping (0=todos)")
    parser.add_argument("--apenas-calcular", action="store_true",
                        help="Pular scraping, apenas calcular areas construidas")
    parser.add_argument("--apenas-scraping", action="store_true",
                        help="Apenas scraping de area_terreno, sem calcular")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("EXTRACAO DE AREAS E COEFICIENTES")
    logger.info(f"Empresa: {args.empresa} | Limite: {args.limite}")
    logger.info("=" * 60)

    # Garantir todas as colunas
    for col in ["area_terreno_m2", "area_construida_min_est", "area_construida_max_est",
                "coeficiente_adensamento_min", "coeficiente_adensamento_max"]:
        garantir_coluna(col, "REAL")

    # Fase 1: Scraping de area_terreno_m2
    novos_terrenos = 0
    if not args.apenas_calcular:
        logger.info("\n--- FASE 1: Scraping de area_terreno_m2 ---")
        novos_terrenos = scrape_area_terreno(args)

    # Fase 2: Cálculos
    if not args.apenas_scraping:
        logger.info("\n--- FASE 2: Cálculo de áreas construídas e coeficientes ---")
        conn = get_connection()
        calculados = calcular_areas_construidas(conn)
        conn.close()
        logger.info(f"  Calculados: {calculados} registros")

    # Relatório
    logger.info("\n" + "=" * 60)
    conn = get_connection()
    cursor = conn.cursor()

    for col in ["area_terreno_m2", "area_construida_min_est", "coeficiente_adensamento_min"]:
        cursor.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {col} IS NOT NULL AND {col} > 0")
        logger.info(f"  {col}: {cursor.fetchone()[0]}/2290")

    # Top 5 coeficientes
    cursor.execute("""
        SELECT empresa, nome, coeficiente_adensamento_max, area_terreno_m2, area_construida_max_est
        FROM empreendimentos
        WHERE coeficiente_adensamento_max IS NOT NULL
        ORDER BY coeficiente_adensamento_max DESC LIMIT 5
    """)
    rows = cursor.fetchall()
    if rows:
        logger.info("\n  Top 5 coeficientes de adensamento:")
        for r in rows:
            logger.info(f"    {r[0]}: {r[1]} — CA={r[2]:.1f}x (terreno={r[3]:.0f}m², construida={r[4]:.0f}m²)")

    conn.close()
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

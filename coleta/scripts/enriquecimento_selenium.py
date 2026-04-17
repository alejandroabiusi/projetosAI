"""
Enriquecimento via Selenium — Empresas SPA que precisam de JS rendering.
========================================================================
Processa empresas cujos sites retornam conteúdo vazio via requests
(Next.js, React SPA, Cloudflare WAF, etc.)

Empresas-alvo:
  - Helbor (87 fases, 90 tipologias)
  - Vitacon (76 tipologias, 82 imagens)
  - HSM (30 tudo vazio)
  - Trisul (65 - Cloudflare WAF)

Uso:
    python scripts/enriquecimento_selenium.py
    python scripts/enriquecimento_selenium.py --empresa Helbor
    python scripts/enriquecimento_selenium.py --dry-run
"""

import sqlite3
import re
import os
import sys
import io
import json
import time
import logging
import argparse
from datetime import datetime
from collections import defaultdict

if sys.stdout and hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
LOG_PATH = os.path.join(PROJECT_ROOT, "build_logs", "enriquecimento_selenium.log")

os.makedirs(os.path.join(PROJECT_ROOT, "build_logs"), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8", mode="a"),
    ]
)
log = logging.getLogger()

# Import extraction functions from the main script
from enriquecimento_autonomo import (
    extrair_tipologias_detalhadas, extrair_tipologias_flags,
    extrair_fase, extrair_coordenadas, extrair_endereco,
    extrair_imagem, extrair_lazer, extrair_itens_lazer_raw,
    extrair_itens_marketizaveis, extrair_metragem, extrair_preco,
    extrair_unidades_vagas, extrair_dados_next_data,
    converter_metragens_para_tipologias, clean_soup, get_title,
    snapshot_completude, _normalizar_tipo,
)

EMPRESAS_SPA = ["Helbor", "Vitacon", "HSM", "Trisul"]


def criar_driver():
    """Cria instancia do Chrome headless."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_page_load_timeout(30)
    return driver


def fetch_selenium(url, driver, scroll=True, wait=4):
    """Carrega pagina com Selenium, scroll e espera."""
    try:
        driver.get(url)
        time.sleep(wait)
        if scroll:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(1)
        return driver.page_source, driver.current_url
    except Exception as e:
        log.info(f"  Selenium erro: {e}")
        return None, None


def processar_empresa_selenium(empresa, conn, driver, dry_run=False):
    """Processa empresa via Selenium."""
    cur = conn.cursor()

    cur.execute("""
        SELECT id, nome, url_fonte, empresa,
               tipologias_detalhadas, fase, area_min_m2, area_max_m2,
               latitude, longitude, endereco, cidade, estado, bairro,
               imagem_url, itens_lazer, itens_lazer_raw, itens_marketizaveis,
               dormitorios_descricao, preco_a_partir, total_unidades, numero_vagas,
               metragens_descricao
        FROM empreendimentos
        WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
        ORDER BY nome
    """, (empresa,))
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    if not rows:
        return {"total": 0}

    log.info(f"\n{'='*60}")
    log.info(f"SELENIUM: {empresa} ({len(rows)} produtos)")
    log.info(f"{'='*60}")

    stats = {"total": len(rows), "processados": 0, "erros": 0,
             "atualizados": defaultdict(int)}

    for i, row in enumerate(rows, 1):
        dados = dict(zip(col_names, row))
        emp_id = dados["id"]
        nome = dados["nome"]
        url = dados["url_fonte"]

        # Checar se precisa processar (tem campos faltantes)
        needs_work = (
            not dados.get("tipologias_detalhadas") or
            not dados.get("fase") or
            not dados.get("imagem_url") or
            not dados.get("latitude") or
            (not dados.get("area_min_m2") or dados.get("area_min_m2") == 0)
        )
        if not needs_work:
            continue

        try:
            html, final_url = fetch_selenium(url, driver)
            if not html or len(html) < 500:
                stats["erros"] += 1
                continue

            from bs4 import BeautifulSoup
            soup = clean_soup(html)
            titulo = get_title(html)
            texto = soup.get_text(separator=" ", strip=True)
            texto_lower = texto.lower()

            novos = {}

            # __NEXT_DATA__ (sites Next.js)
            next_data = extrair_dados_next_data(html)

            # Tipologias
            if not dados.get("tipologias_detalhadas"):
                # Primeiro: converter metragens_descricao
                if dados.get("metragens_descricao"):
                    val = converter_metragens_para_tipologias(dados["metragens_descricao"])
                    if val:
                        novos["tipologias_detalhadas"] = val
                if "tipologias_detalhadas" not in novos:
                    val = extrair_tipologias_detalhadas(soup, texto)
                    if not val and next_data:
                        quartos = next_data.get("_quartos")
                        metr = next_data.get("_metragem_titulo") or (str(int(next_data["_area"])) if next_data.get("_area") else None)
                        if quartos and metr:
                            tn = _normalizar_tipo(quartos)
                            if tn:
                                val = f"{tn} {metr}m\u00b2"
                    if val:
                        novos["tipologias_detalhadas"] = val

            # Fase
            if not dados.get("fase"):
                fase = extrair_fase(soup, texto_lower, final_url or url)
                if not fase and next_data.get("_fase_raw"):
                    raw = next_data["_fase_raw"].lower()
                    if "pronto" in raw or "entregue" in raw: fase = "Pronto para Morar"
                    elif "constru" in raw or "obra" in raw: fase = "Em Constru\u00e7\u00e3o"
                    elif "lan\u00e7a" in raw or "lanca" in raw: fase = "Lan\u00e7amento"
                    elif "breve" in raw: fase = "Breve Lan\u00e7amento"
                    elif "vendido" in raw or "esgotado" in raw: fase = "100% Vendido"
                if fase:
                    novos["fase"] = fase

            # Metragem
            if not dados.get("area_min_m2") or dados.get("area_min_m2") == 0:
                amin, amax = extrair_metragem(texto)
                if not amin and next_data.get("_area"):
                    amin = amax = next_data["_area"]
                if amin:
                    novos["area_min_m2"] = amin
                    novos["area_max_m2"] = amax

            # Coordenadas
            if not dados.get("latitude"):
                lat, lon = extrair_coordenadas(html)
                if lat is None and next_data.get("_latitude"):
                    lat = next_data["_latitude"]
                    lon = next_data.get("_longitude")
                if lat is not None:
                    novos["latitude"] = str(lat)
                    novos["longitude"] = str(lon)

            # Imagem
            if not dados.get("imagem_url"):
                img = extrair_imagem(html)
                if not img and next_data.get("_imagem"):
                    img = next_data["_imagem"]
                if img:
                    novos["imagem_url"] = img

            # Endereco
            if not dados.get("endereco") or not dados.get("cidade"):
                end = extrair_endereco(soup, html, texto)
                if not end["endereco"] and next_data.get("_endereco"):
                    end["endereco"] = next_data["_endereco"]
                if not end["cidade"] and next_data.get("_cidade"):
                    end["cidade"] = next_data["_cidade"]
                for campo in ["endereco", "cidade", "estado", "bairro"]:
                    if end.get(campo) and not dados.get(campo):
                        novos[campo] = end[campo]

            # Dormitorios
            if not dados.get("dormitorios_descricao"):
                flags, desc = extrair_tipologias_flags(soup, titulo, texto)
                if any(v == 1 for v in flags.values()):
                    for campo, valor in flags.items():
                        if not dados.get(campo):
                            novos[campo] = valor
                    if desc:
                        novos["dormitorios_descricao"] = desc

            # Lazer
            if not dados.get("itens_lazer"):
                lf, lt = extrair_lazer(soup, texto)
                if lf:
                    for campo, valor in lf.items():
                        novos[campo] = valor
                    if lt:
                        novos["itens_lazer"] = lt

            # Gravar
            if novos and not dry_run:
                sets = ", ".join([f"{k}=?" for k in novos.keys()])
                vals = list(novos.values()) + [emp_id]
                cur.execute(f"UPDATE empreendimentos SET {sets} WHERE id=?", vals)
                for campo in novos:
                    stats["atualizados"][campo] += 1

            stats["processados"] += 1

            if i <= 3 or i % 20 == 0 or i == len(rows):
                n = len(novos)
                campos = ", ".join(sorted(novos.keys())[:5]) if novos else "-"
                log.info(f"  [{i}/{len(rows)}] {nome[:40]}: +{n} ({campos})")

        except Exception as e:
            stats["erros"] += 1
            log.info(f"  [{i}/{len(rows)}] {nome[:40]}: ERRO: {e}")

        if not dry_run and i % 20 == 0:
            conn.commit()

    if not dry_run:
        conn.commit()

    log.info(f"\n  Resultado {empresa}: {stats['processados']} processados, {stats['erros']} erros")
    if stats["atualizados"]:
        log.info(f"  Campos: {dict(stats['atualizados'])}")

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    snap_antes = snapshot_completude(conn)

    empresas = [args.empresa] if args.empresa else EMPRESAS_SPA

    log.info(f"SELENIUM ENRICHMENT — {len(empresas)} empresas")
    driver = criar_driver()
    log.info("Chrome headless iniciado")

    total_stats = {"empresas": 0, "atualizacoes": 0}

    try:
        for emp in empresas:
            stats = processar_empresa_selenium(emp, conn, driver, dry_run=args.dry_run)
            total_stats["empresas"] += 1
            total_stats["atualizacoes"] += sum(stats.get("atualizados", {}).values())
    finally:
        driver.quit()
        log.info("Chrome encerrado")

    snap_depois = snapshot_completude(conn)
    conn.close()

    log.info(f"\nRESUMO: {total_stats['empresas']} empresas, {total_stats['atualizacoes']} atualizacoes")
    for campo in sorted(snap_depois.keys()):
        if campo == "total": continue
        antes = snap_antes.get(campo, 0)
        depois = snap_depois[campo]
        if depois > antes:
            log.info(f"  {campo}: {antes} -> {depois} (+{depois-antes})")


if __name__ == "__main__":
    main()

"""
Scraper de empreendimentos da Metrocasa.
========================================
Usa Selenium porque o site e uma SPA Next.js com dados renderizados
via React Server Components (sem API publica acessivel).

Extrai dados da listagem em /imoveis e detalhes de cada pagina /imoveis/{slug}.

Uso:
    python scrapers/metrocasa_empreendimentos.py
    python scrapers/metrocasa_empreendimentos.py --limite 3
    python scrapers/metrocasa_empreendimentos.py --atualizar
"""

import os
import sys
import re
import time
import json
import logging
import argparse
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import LOGS_DIR
from data.database import (
    inserir_empreendimento,
    empreendimento_existe,
    atualizar_empreendimento,
    detectar_atributos_binarios,
    contar_empreendimentos,
)

EMPRESA = "Metrocasa"
BASE_URL = "https://www.metrocasa.com.br"
PROGRESSO_FILE = os.path.join(LOGS_DIR, "metrocasa_empreendimentos_progresso.json")

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("scraper.metrocasa")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "metrocasa_empreendimentos.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ============================================================
# SELENIUM HELPERS
# ============================================================

def criar_driver():
    """Cria instancia do Chrome headless."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_page_load_timeout(30)
    return driver


def carregar_pagina(driver, url, espera=5):
    """Carrega pagina e faz scroll para ativar lazy loading."""
    driver.get(url)
    time.sleep(espera)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(1)
    return BeautifulSoup(driver.page_source, "html.parser")


# ============================================================
# PROGRESSO
# ============================================================

def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": [], "erros": []}


def salvar_progresso(progresso):
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(progresso, f, indent=2)


# ============================================================
# PARSING DA LISTAGEM
# ============================================================

def extrair_empreendimentos_listagem(driver):
    """
    Extrai empreendimentos da pagina de listagem /imoveis.
    O texto segue o padrao: Status | Metrocasa | Nome | Dormitorios | Endereco | CTA
    """
    soup = carregar_pagina(driver, f"{BASE_URL}/imoveis", espera=6)
    texto = soup.get_text(separator="\n", strip=True)

    empreendimentos = []

    # Pattern: captura blocos com Status (Lancamento/Em construcao/Pronto) seguido de Metrocasa e dados
    padrao_bloco = re.compile(
        r"(Lan[çc]amento|Em constru[çc][ãa]o|Pronto|Pronto para morar|Em breve|Breve lan[çc]amento)"
        r"\s*\n\s*Metrocasa\s*\n\s*"
        r"(.+?)\n"           # nome
        r"\s*(.+?)\n"        # dormitorios
        r"\s*(.+?)\n"        # endereco
        r"\s*Quero Morar Aqui",
        re.IGNORECASE | re.DOTALL
    )

    for m in padrao_bloco.finditer(texto):
        fase_raw = m.group(1).strip()
        nome = m.group(2).strip()
        dorms = m.group(3).strip()
        endereco = m.group(4).strip()

        # Normalizar fase
        fase_map = {
            "lançamento": "Lançamento",
            "lancamento": "Lançamento",
            "em construção": "Em Construção",
            "em construcao": "Em Construção",
            "pronto": "Imóvel Pronto",
            "pronto para morar": "Imóvel Pronto",
            "em breve": "Breve Lançamento",
            "breve lançamento": "Breve Lançamento",
            "breve lancamento": "Breve Lançamento",
        }
        fase = fase_map.get(fase_raw.lower(), fase_raw)

        # Gerar slug
        import unicodedata
        slug = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
        slug = re.sub(r"[^\w\s-]", "", slug.lower())
        slug = re.sub(r"[\s_]+", "-", slug).strip("-")

        empreendimentos.append({
            "nome": nome,
            "slug": slug,
            "fase": fase,
            "dormitorios_descricao": dorms,
            "endereco": endereco,
        })

    # Also try to find links via JS click handlers
    slugs_from_links = set()
    try:
        links = driver.execute_script("""
            var results = [];
            document.querySelectorAll('a[href*="/imoveis/"]').forEach(function(a) {
                var href = a.getAttribute('href');
                if (href && href !== '/imoveis') {
                    results.push(href);
                }
            });
            return results;
        """)
        for href in links:
            slug = href.split("/imoveis/")[-1].strip("/")
            if slug and not slug.startswith("_") and not slug.startswith("page"):
                slugs_from_links.add(slug)
    except Exception:
        pass

    logger.info(f"Empreendimentos encontrados na listagem: {len(empreendimentos)}")
    if slugs_from_links:
        logger.info(f"Slugs encontrados via links: {len(slugs_from_links)}")

    return empreendimentos


# ============================================================
# PARSING DA PAGINA DE DETALHE
# ============================================================

def extrair_detalhes(driver, slug):
    """Extrai dados detalhados de uma pagina de empreendimento."""
    url = f"{BASE_URL}/imoveis/{slug}"
    soup = carregar_pagina(driver, url, espera=5)
    texto = soup.get_text(separator="\n", strip=True)

    dados = {}

    # Preco - "A partir de R$ 245.000" ou "R$ 245.000"
    precos = re.findall(r"R\$\s*([\d.,]+)", texto)
    for p in precos:
        try:
            valor = float(p.replace(".", "").replace(",", "."))
            if valor > 50000:
                dados["preco_a_partir"] = valor
                break
        except ValueError:
            continue

    # Metragens - "24m²", "24 a 41m²", etc.
    metragens = re.findall(r"(\d+(?:[.,]\d+)?)\s*(?:[Aa]\s*(\d+(?:[.,]\d+)?)\s*)?m[²2]", texto)
    if metragens:
        nums = []
        metr_strs = []
        for m in metragens:
            v1 = float(m[0].replace(",", "."))
            if 10.0 <= v1 <= 500.0:
                nums.append(v1)
                metr_strs.append(f"{m[0]}m²")
            if m[1]:
                v2 = float(m[1].replace(",", "."))
                if 10.0 <= v2 <= 500.0:
                    nums.append(v2)
                    metr_strs.append(f"{m[1]}m²")
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)
            dados["metragens_descricao"] = " | ".join(sorted(set(metr_strs)))

    # Total unidades
    uni_match = re.search(r"(\d+)\s*(?:unidades|apartamentos|aptos)", texto, re.IGNORECASE)
    if uni_match:
        dados["total_unidades"] = int(uni_match.group(1))

    # Torres
    torres_match = re.search(r"(\d+)\s*torre", texto, re.IGNORECASE)
    if torres_match:
        dados["numero_torres"] = int(torres_match.group(1))

    # Andares
    andares_match = re.search(r"(\d+)\s*(?:andares?|pavimentos?)", texto, re.IGNORECASE)
    if andares_match:
        dados["numero_andares"] = andares_match.group(0).strip()

    # Vagas
    vagas_match = re.search(r"(\d+)\s*vagas?", texto, re.IGNORECASE)
    if vagas_match:
        dados["numero_vagas"] = vagas_match.group(0).strip()

    # Itens de lazer
    termos_lazer = [
        "piscina", "churrasqueira", "fitness", "academia", "playground",
        "brinquedoteca", "salão de festas", "salao de festas", "pet care",
        "pet place", "coworking", "bicicletário", "bicicletario", "quadra",
        "delivery", "horta", "lavanderia", "rooftop", "sauna", "spa",
        "gourmet", "salão de jogos", "salao de jogos", "solarium",
        "cinema", "deck", "portaria", "hall", "guarita",
    ]
    texto_lower = texto.lower()
    itens = set()
    for termo in termos_lazer:
        if termo in texto_lower:
            itens.add(termo.title())
    if itens:
        dados["itens_lazer"] = " | ".join(sorted(itens))

    # Atributos binarios
    atributos = detectar_atributos_binarios(texto)
    dados.update(atributos)

    return dados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Metrocasa")
    parser.add_argument("--atualizar", action="store_true", help="Atualizar empreendimentos existentes")
    parser.add_argument("--limite", type=int, default=0, help="Limitar numero de empreendimentos (0=todos)")
    parser.add_argument("--reset-progresso", action="store_true", help="Resetar progresso")
    args = parser.parse_args()

    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            logger.info("Progresso resetado.")
        return

    progresso = carregar_progresso()

    logger.info("=" * 60)
    logger.info(f"SCRAPER METROCASA (metrocasa.com.br)")
    logger.info(f"Empreendimentos no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)

    driver = criar_driver()

    try:
        # Fase 1: Extrair listagem
        logger.info("[FASE 1] Extraindo listagem de empreendimentos...")
        empreendimentos = extrair_empreendimentos_listagem(driver)

        if not empreendimentos:
            logger.error("Nenhum empreendimento encontrado na listagem!")
            return

        if args.limite > 0:
            empreendimentos = empreendimentos[:args.limite]
            logger.info(f"Limitado a {args.limite} empreendimentos")

        # Filtrar ja processados
        if not args.atualizar:
            empreendimentos = [e for e in empreendimentos if e["slug"] not in progresso["processados"]]
            logger.info(f"Pendentes: {len(empreendimentos)}")

        if not empreendimentos:
            logger.info("Todos os empreendimentos ja foram processados.")
            return

        # Fase 2: Processar cada empreendimento
        logger.info(f"[FASE 2] Processando {len(empreendimentos)} empreendimentos...")
        novos = 0
        atualizados = 0
        erros = 0
        pulados = 0

        for i, emp in enumerate(empreendimentos, 1):
            nome = emp["nome"]
            slug = emp["slug"]
            logger.info(f"\n[{i}/{len(empreendimentos)}] {nome}")

            try:
                # Dados basicos da listagem
                dados = {
                    "empresa": EMPRESA,
                    "nome": nome,
                    "slug": slug,
                    "url_fonte": f"{BASE_URL}/imoveis/{slug}",
                    "cidade": "São Paulo",
                    "estado": "SP",
                    "fase": emp.get("fase"),
                    "dormitorios_descricao": emp.get("dormitorios_descricao"),
                    "endereco": emp.get("endereco"),
                }

                # Extrair bairro do nome se possivel
                # Nomes como "Estação Patriarca", "Vila Guilherme", etc. geralmente sao o bairro
                # Mas nomes como "Clube Itaquera" tem o bairro "Itaquera"
                # Vamos tentar detectar no endereco
                end = emp.get("endereco", "")
                if end:
                    # Nao temos bairro explícito, mas podemos inferir do nome do empreendimento
                    pass

                # Enriquecer com detalhes da pagina individual
                detalhes = extrair_detalhes(driver, slug)
                dados.update(detalhes)

                # Verificar existencia
                existe = empreendimento_existe(EMPRESA, nome)

                if existe and not args.atualizar:
                    logger.info(f"  Ja existe, pulando.")
                    pulados += 1
                elif existe and args.atualizar:
                    atualizar_empreendimento(EMPRESA, nome, dados)
                    atualizados += 1
                    logger.info(f"  Atualizado.")
                else:
                    inserir_empreendimento(dados)
                    novos += 1
                    logger.info(f"  Inserido: {nome} | {dados.get('fase', 'N/A')} | {dados.get('endereco', '?')}")

                progresso["processados"].append(slug)
                salvar_progresso(progresso)

            except Exception as e:
                logger.error(f"  Erro: {e}")
                erros += 1
                progresso["erros"].append(slug)
                salvar_progresso(progresso)

            time.sleep(2)

        # Relatorio final
        logger.info("\n" + "=" * 60)
        logger.info("RELATORIO FINAL - METROCASA")
        logger.info(f"  Novos: {novos}")
        logger.info(f"  Atualizados: {atualizados}")
        logger.info(f"  Pulados: {pulados}")
        logger.info(f"  Erros: {erros}")
        logger.info(f"  Total no banco: {contar_empreendimentos(EMPRESA)}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        logger.info("Chrome fechado.")


if __name__ == "__main__":
    main()

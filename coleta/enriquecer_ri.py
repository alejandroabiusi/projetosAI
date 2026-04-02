"""
Enriquecimento de Registro de Incorporação (R.I.)
==================================================
Visita a URL de cada empreendimento no banco e busca no texto da página
padrões de registro de incorporação, matrícula e cartório.

Extrai:
  - registro_incorporacao: texto completo do R.I. encontrado na página
  - matricula_ri: número da matrícula (ex: "392.731")
  - cartorio_ri: nome do cartório (ex: "9º Oficial de Registro de Imóveis de São Paulo")
  - data_ri: data do registro extraída do texto (formato YYYY-MM-DD)

Estratégia:
  - Para empresas com tipo_acesso "requests": HTTP GET direto
  - Para empresas com tipo_acesso "selenium": navegador headless
  - Para MRV/Vivaz (API): pula (MRV já tem RI via mrv_detalhes.py)
  - Pula empreendimentos que já têm registro_incorporacao preenchido (a menos que --forcar)

Uso:
    python enriquecer_ri.py
    python enriquecer_ri.py --empresa "Plano&Plano"
    python enriquecer_ri.py --limite 10
    python enriquecer_ri.py --forcar
    python enriquecer_ri.py --sem-selenium
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import sqlite3
import requests as req_lib
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import REQUESTS as REQ_CONFIG, LOGS_DIR, SELENIUM
from data.database import get_connection, garantir_coluna, atualizar_empreendimento_por_url

# ============================================================
# CONFIGURAÇÃO
# ============================================================

EMPRESA_CONFIG = {
    "MRV":               {"tipo_acesso": "api_mrv",   "pular": True},   # já tem RI via mrv_detalhes.py
    "Vivaz":             {"tipo_acesso": "api_vivaz",  "pular": False},
    "Plano&Plano":       {"tipo_acesso": "requests"},
    "Direcional":        {"tipo_acesso": "requests"},
    "Magik JC":          {"tipo_acesso": "requests"},
    "Kazzas":            {"tipo_acesso": "requests"},
    "Vibra Residencial": {"tipo_acesso": "requests"},
    "Pacaembu":          {"tipo_acesso": "requests"},
    "Mundo Apto":        {"tipo_acesso": "requests"},
    "Conx":              {"tipo_acesso": "requests"},
    "Viva Benx":         {"tipo_acesso": "requests"},
    "Cury":              {"tipo_acesso": "selenium"},   # já tem RI, mas pode enriquecer matrícula/cartório
    "Metrocasa":         {"tipo_acesso": "selenium"},
    "VIC Engenharia":    {"tipo_acesso": "requests"},
    "Vasco Construtora": {"tipo_acesso": "requests"},
    "Vinx":              {"tipo_acesso": "requests"},
    "Riformato":         {"tipo_acesso": "requests"},
    "ACLF":              {"tipo_acesso": "requests"},
    "BM7":               {"tipo_acesso": "requests"},
    "FYP Engenharia":    {"tipo_acesso": "requests"},
    "Smart Construtora": {"tipo_acesso": "requests"},
    "Jotanunes":         {"tipo_acesso": "requests"},
    "Cavazani":          {"tipo_acesso": "requests"},
    "Carrilho":          {"tipo_acesso": "requests"},
    "BP8":               {"tipo_acesso": "requests"},
    "ACL Incorporadora": {"tipo_acesso": "requests"},
    "Novolar":           {"tipo_acesso": "requests"},
    "Árbore":            {"tipo_acesso": "requests"},
    "SUGOI":             {"tipo_acesso": "requests"},
    "Emccamp":           {"tipo_acesso": "requests"},
    "EPH":               {"tipo_acesso": "requests"},
    "Ampla":             {"tipo_acesso": "requests"},
    "Novvo":             {"tipo_acesso": "requests"},
    "M.Lar":             {"tipo_acesso": "requests"},
    "Ún1ca":             {"tipo_acesso": "requests"},
    "Construtora Open":  {"tipo_acesso": "requests"},
    "Grafico":           {"tipo_acesso": "requests"},
    "Stanza":            {"tipo_acesso": "requests"},
    "Graal Engenharia":  {"tipo_acesso": "requests"},
    "Rev3":              {"tipo_acesso": "requests"},
    "HM Engenharia":     {"tipo_acesso": "requests"},
    "Sousa Araujo":      {"tipo_acesso": "requests"},
    "Econ Construtora":  {"tipo_acesso": "requests"},
    "Benx":              {"tipo_acesso": "requests"},
}

HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

PROGRESSO_FILE = os.path.join(LOGS_DIR, "enriquecer_ri_progresso.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "enriquecer_ri.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ============================================================
# REGEX PATTERNS PARA EXTRAÇÃO DE R.I.
# ============================================================

# Padrões que indicam texto de registro de incorporação
# Captura o trecho completo (até 500 chars) a partir do match
RI_TRIGGER_PATTERNS = [
    # "incorporação registrada sob R.3, em 19/09/2025, na matrícula nº 392.731 do 9º Oficial..."
    re.compile(
        r'incorpora[çc][ãa]o\s+registrada\s+sob\s+.{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL
    ),
    # "Registro de Incorporação R-01.123.321 do 1º Ofício de Araçatuba."
    re.compile(
        r'Registro\s+de\s+[Ii]ncorpora[çc][ãa]o\s+(?:prenotado|registrado|sob|n[ºo°]|R[\-\.]).{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL
    ),
    # "Memorial de Incorporação prenotado sob o nº 145307 no 2º Ofício..."
    re.compile(
        r'Memorial\s+de\s+[Ii]ncorpora[çc][ãa]o\s+prenotado.{5,500}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL
    ),
    # "R.I. nº R-01/123.456 do 1º Ofício..." ou "R.I. registrado..."
    re.compile(
        r'R\.?\s*I\.?\s*(?:n[ºo°]|registrad|sob).{5,400}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL
    ),
    # Fallback mais amplo: "incorporação" seguido de "matrícula" próximo
    re.compile(
        r'(?:com\s+)?incorpora[çc][ãa]o.{5,300}?matr[ií]cula.{5,200}?(?:Of[ií]cio|Cart[oó]rio|Registro)[^.]*\.',
        re.IGNORECASE | re.DOTALL
    ),
]

# Extração de matrícula
MATRICULA_PATTERN = re.compile(
    r'matr[ií]cula\s*(?:n[ºo°\.]\s*)?(\d[\d\.\-/]+)',
    re.IGNORECASE
)

# Extração de cartório
CARTORIO_PATTERNS = [
    # "do 9º Oficial de Registro de Imóveis de São Paulo"
    re.compile(
        r'(?:do|no)\s+(\d+[ºª°]?\s*(?:Oficial|Of[ií]cio|Cart[oó]rio)\s+de\s+Registro\s+de\s+Im[oó]veis\s+de\s+[^.,;]+)',
        re.IGNORECASE
    ),
    # "Cartório do 1º Ofício"
    re.compile(
        r'(Cart[oó]rio\s+(?:do\s+)?\d+[ºª°]?\s*Of[ií]cio[^.,;]*)',
        re.IGNORECASE
    ),
    # "1º Ofício de Araçatuba"
    re.compile(
        r'(\d+[ºª°]?\s*Of[ií]cio\s+de\s+(?:Registro\s+de\s+)?(?:Im[oó]veis\s+de\s+)?[^.,;]+)',
        re.IGNORECASE
    ),
]

# Extração de data do R.I.
DATA_RI_PATTERN = re.compile(
    r'(?:em|de|data)\s+(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})',
    re.IGNORECASE
)

# Extração do número do registro (R.3, R-01/123.456, etc.)
NUMERO_RI_PATTERN = re.compile(
    r'(?:sob|n[ºo°]\.?\s*)\s*(R[\.\-]?\s*\d+[\d\.\-/]*)',
    re.IGNORECASE
)


# ============================================================
# FUNÇÕES DE EXTRAÇÃO
# ============================================================

def extrair_ri_do_texto(texto):
    """
    Busca no texto completo da página os padrões de R.I.
    Retorna dict com os campos encontrados ou None.
    """
    if not texto:
        return None

    # Normalizar espaços
    texto_limpo = " ".join(texto.split())

    # Tentar cada padrão de trigger
    ri_textos = []
    for pattern in RI_TRIGGER_PATTERNS:
        matches = pattern.findall(texto_limpo)
        for m in matches:
            m_clean = m.strip()
            if len(m_clean) > 20 and m_clean not in ri_textos:
                ri_textos.append(m_clean)

    if not ri_textos:
        return None

    # Usar o texto mais completo (o maior)
    ri_texto = max(ri_textos, key=len)

    resultado = {
        "registro_incorporacao": ri_texto[:2000],  # limitar tamanho
    }

    # Extrair matrícula
    mat_match = MATRICULA_PATTERN.search(ri_texto)
    if mat_match:
        resultado["matricula_ri"] = mat_match.group(1).strip()

    # Extrair cartório
    for cp in CARTORIO_PATTERNS:
        cart_match = cp.search(ri_texto)
        if cart_match:
            resultado["cartorio_ri"] = cart_match.group(1).strip()
            break

    # Extrair data do R.I.
    data_match = DATA_RI_PATTERN.search(ri_texto)
    if data_match:
        dia = data_match.group(1).zfill(2)
        mes = data_match.group(2).zfill(2)
        ano = data_match.group(3)
        if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31:
            resultado["data_ri"] = f"{ano}-{mes}-{dia}"

    # Extrair número do registro
    num_match = NUMERO_RI_PATTERN.search(ri_texto)
    if num_match:
        resultado["numero_ri"] = num_match.group(1).strip()

    # Se encontrou vários trechos de R.I. (empreendimentos com múltiplos módulos),
    # concatenar todos separados por " | "
    if len(ri_textos) > 1:
        resultado["registro_incorporacao"] = " | ".join(t[:800] for t in ri_textos)[:2000]

        # Extrair todas as matrículas
        todas_mat = []
        for t in ri_textos:
            mm = MATRICULA_PATTERN.search(t)
            if mm:
                todas_mat.append(mm.group(1).strip())
        if todas_mat:
            resultado["matricula_ri"] = " | ".join(todas_mat)

    return resultado


# ============================================================
# FETCH HTML
# ============================================================

def fetch_html_requests(url, session):
    """Baixa HTML via requests."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.text
        else:
            log.warning(f"HTTP {resp.status_code} para {url}")
            return None
    except Exception as e:
        log.warning(f"Erro requests {url}: {e}")
        return None


def criar_driver():
    """Cria driver Selenium headless."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={HEADERS['User-Agent']}")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(SELENIUM.get("timeout", 30))
    return driver


def fetch_html_selenium(url, driver):
    """Baixa HTML via Selenium."""
    try:
        driver.get(url)
        time.sleep(3)
        # Scroll até o final da página (R.I. costuma ficar no rodapé)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        return driver.page_source
    except Exception as e:
        log.warning(f"Erro Selenium {url}: {e}")
        return None


# ============================================================
# PROGRESSO (RESUMÍVEL)
# ============================================================

def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": {}, "erros": {}}


def salvar_progresso(progresso):
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(progresso, f, indent=2)


# ============================================================
# BUSCAR REGISTROS
# ============================================================

def buscar_registros(empresa, forcar=False, limite=None):
    """Busca empreendimentos para enriquecer R.I."""
    conn = get_connection()
    cur = conn.cursor()

    # Garantir colunas existem
    for col in ["matricula_ri", "cartorio_ri", "data_ri", "numero_ri"]:
        garantir_coluna(col, "TEXT")

    if forcar:
        where = "WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''"
        params = [empresa]
    else:
        where = """WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
                   AND (registro_incorporacao IS NULL OR registro_incorporacao = '')"""
        params = [empresa]

    sql = f"""
        SELECT id, empresa, nome, url_fonte, registro_incorporacao,
               COALESCE(matricula_ri,'') as matricula_ri,
               COALESCE(cartorio_ri,'') as cartorio_ri
        FROM empreendimentos {where}
        ORDER BY id
    """
    if limite:
        sql += f" LIMIT {limite}"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def processar_empresa(empresa, config, forcar=False, limite=None, sem_selenium=False):
    """Processa todos os empreendimentos de uma empresa."""
    tipo = config.get("tipo_acesso", "requests")

    if config.get("pular"):
        log.info(f"[{empresa}] Pulando (já tem R.I. via scraper próprio)")
        return 0, 0

    if tipo.startswith("api_"):
        log.info(f"[{empresa}] Pulando tipo {tipo} (API, sem página HTML)")
        return 0, 0

    if tipo == "selenium" and sem_selenium:
        log.info(f"[{empresa}] Pulando Selenium (--sem-selenium)")
        return 0, 0

    registros = buscar_registros(empresa, forcar, limite)
    if not registros:
        log.info(f"[{empresa}] Nenhum registro para processar")
        return 0, 0

    log.info(f"[{empresa}] {len(registros)} registros para processar (tipo: {tipo})")

    progresso = carregar_progresso()
    processados = progresso.get("processados", {})

    session = None
    driver = None

    if tipo == "requests":
        session = req_lib.Session()
        session.headers.update(HEADERS)
    elif tipo == "selenium":
        driver = criar_driver()

    ok_count = 0
    skip_count = 0

    try:
        for i, (eid, emp, nome, url, ri_atual, mat_atual, cart_atual) in enumerate(registros):
            eid_str = str(eid)

            # Pular se já processado neste ciclo
            if eid_str in processados and not forcar:
                skip_count += 1
                continue

            log.info(f"  [{i+1}/{len(registros)}] {nome[:50]} → {url[:80]}")

            # Fetch HTML
            html = None
            if tipo == "requests":
                html = fetch_html_requests(url, session)
                time.sleep(0.5)  # rate limiting
            elif tipo == "selenium":
                html = fetch_html_selenium(url, driver)

            if not html:
                processados[eid_str] = {"status": "sem_html"}
                salvar_progresso(progresso)
                continue

            # Extrair texto da página
            soup = BeautifulSoup(html, "html.parser")

            # Remover scripts e styles para texto limpo
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            texto = soup.get_text(separator=" ")

            # Tentar extrair R.I.
            resultado = extrair_ri_do_texto(texto)

            if resultado:
                # Preparar campos para atualização
                campos = {}

                # Só sobrescreve registro_incorporacao se ainda não tinha ou se --forcar
                if not ri_atual or forcar:
                    campos["registro_incorporacao"] = resultado.get("registro_incorporacao")

                # Sempre preenche matrícula/cartório/data se encontrou e não tinha
                if resultado.get("matricula_ri") and (not mat_atual or forcar):
                    campos["matricula_ri"] = resultado["matricula_ri"]
                if resultado.get("cartorio_ri") and (not cart_atual or forcar):
                    campos["cartorio_ri"] = resultado["cartorio_ri"]
                if resultado.get("data_ri"):
                    campos["data_ri"] = resultado["data_ri"]
                if resultado.get("numero_ri"):
                    campos["numero_ri"] = resultado["numero_ri"]

                if campos:
                    atualizar_empreendimento_por_url(emp, url, campos)
                    ok_count += 1
                    log.info(f"    ✓ R.I. encontrado: {resultado.get('numero_ri', '')} "
                             f"mat={resultado.get('matricula_ri', '')} "
                             f"cart={resultado.get('cartorio_ri', '')[:40] if resultado.get('cartorio_ri') else ''}")
                else:
                    log.debug(f"    — R.I. já preenchido, nada a atualizar")

                processados[eid_str] = {"status": "ok", "campos": list(campos.keys())}
            else:
                processados[eid_str] = {"status": "sem_ri"}
                log.debug(f"    — Nenhum R.I. encontrado na página")

            # Salvar progresso a cada 10 registros
            if (i + 1) % 10 == 0:
                salvar_progresso(progresso)

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        salvar_progresso(progresso)

    return ok_count, len(registros) - skip_count


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enriquecer Registro de Incorporação")
    parser.add_argument("--empresa", type=str, help="Processar apenas uma empresa")
    parser.add_argument("--limite", type=int, help="Limitar registros por empresa")
    parser.add_argument("--forcar", action="store_true", help="Reprocessar mesmo com R.I. existente")
    parser.add_argument("--sem-selenium", action="store_true", help="Pular empresas que usam Selenium")
    parser.add_argument("--reset-progresso", action="store_true", help="Limpar progresso anterior")
    args = parser.parse_args()

    if args.reset_progresso and os.path.exists(PROGRESSO_FILE):
        os.remove(PROGRESSO_FILE)
        log.info("Progresso resetado")

    # Garantir colunas no banco
    for col in ["matricula_ri", "cartorio_ri", "data_ri", "numero_ri"]:
        garantir_coluna(col, "TEXT")

    log.info("=" * 60)
    log.info("ENRIQUECIMENTO DE REGISTRO DE INCORPORAÇÃO")
    log.info("=" * 60)

    empresas = [args.empresa] if args.empresa else list(EMPRESA_CONFIG.keys())
    total_ok = 0
    total_proc = 0

    for empresa in empresas:
        if empresa not in EMPRESA_CONFIG:
            # Empresa não configurada — tentar com requests por padrão
            config = {"tipo_acesso": "requests"}
        else:
            config = EMPRESA_CONFIG[empresa]

        ok, proc = processar_empresa(empresa, config, args.forcar, args.limite, args.sem_selenium)
        total_ok += ok
        total_proc += proc

    log.info("=" * 60)
    log.info(f"CONCLUÍDO: {total_ok} R.I. encontrados em {total_proc} páginas visitadas")
    log.info("=" * 60)

    # Relatório final
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE registro_incorporacao IS NOT NULL AND registro_incorporacao != ''")
    total_ri = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE matricula_ri IS NOT NULL AND matricula_ri != ''")
    total_mat = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE cartorio_ri IS NOT NULL AND cartorio_ri != ''")
    total_cart = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM empreendimentos")
    total = cur.fetchone()[0]
    conn.close()

    log.info(f"\nCobertura atual:")
    log.info(f"  Registro Incorporação: {total_ri}/{total} ({total_ri/total*100:.1f}%)")
    log.info(f"  Matrícula:             {total_mat}/{total} ({total_mat/total*100:.1f}%)")
    log.info(f"  Cartório:              {total_cart}/{total} ({total_cart/total*100:.1f}%)")


if __name__ == "__main__":
    main()

"""
Verificacao de status de empreendimentos existentes.
=====================================================
Visita URLs do banco, atualiza fases que mudaram, e reconcilia
URLs que desapareceram (redirect, renomeacao ou remocao).

Ao contrario da reconciliacao da Direcional (que remove do banco),
este script marca empreendimentos mortos com fase="Removido".

Uso:
    python scrapers/verificar_status.py                    # Verificar tudo
    python scrapers/verificar_status.py --empresa MRV      # So uma empresa
    python scrapers/verificar_status.py --sem-selenium     # Pular Cury/Metrocasa
    python scrapers/verificar_status.py --dry-run           # So verificar, nao atualizar
    python scrapers/verificar_status.py --limite 5          # Max 5 por empresa (teste)
"""

import os
import sys
import re
import time
import json
import difflib
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from html import unescape

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import REQUESTS as REQUESTS_CONFIG, LOGS_DIR
from data.database import (
    get_connection,
    buscar_empreendimentos_por_empresa,
    atualizar_empreendimento_por_url,
    atualizar_empreendimento,
    registrar_mudanca,
    registrar_reconciliacao,
    registrar_run,
    contar_empreendimentos,
)

# Importar detectar_fase do generico
from scrapers.generico_empreendimentos import detectar_fase


# ============================================================
# CONFIGURACAO
# ============================================================

DELAY = 2
PROGRESSO_FILE = os.path.join(LOGS_DIR, "verificar_status_progresso.json")

HEADERS = {
    "User-Agent": REQUESTS_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

HEADERS_JSON = {
    "User-Agent": REQUESTS_CONFIG["headers"]["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}

# Mapeamento empresa -> tipo de verificacao
TIPO_VERIFICACAO = {
    "MRV": "mrv",
    "Vivaz": "vivaz",
    "Direcional": "direcional",
    "Viva Benx": "vivabenx",
    "Cury": "cury",
    "Metrocasa": "metrocasa",
    "Plano&Plano": "planoeplano",
    # Todas as outras usam generico
}

# Soft 404 patterns
SOFT_404_PATTERNS = [
    "página não encontrada",
    "pagina nao encontrada",
    "não encontramos",
    "nao encontramos",
    "page not found",
    "404",
    "esta página não existe",
    "esta pagina nao existe",
    "conteúdo não disponível",
    "conteudo nao disponivel",
]


# ============================================================
# LOGGING
# ============================================================

os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("verificar_status")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "verificar_status.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ============================================================
# PROGRESSO (resumivel)
# ============================================================

def carregar_progresso(run_id):
    """Carrega progresso de execucao anterior, se mesmo run_id."""
    if os.path.exists(PROGRESSO_FILE):
        try:
            with open(PROGRESSO_FILE, "r", encoding="utf-8") as f:
                prog = json.load(f)
            if prog.get("run_id") == run_id:
                return prog
        except (json.JSONDecodeError, KeyError):
            pass
    return {
        "run_id": run_id,
        "processados": [],
        "resultados": {
            "ok": 0, "fase_mudou": 0, "redirect": 0,
            "found_by_name": 0, "found_by_location": 0,
            "removido": 0, "erro": 0,
        },
    }


def salvar_progresso(progresso):
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(progresso, f, indent=2, ensure_ascii=False)


# ============================================================
# HELPERS
# ============================================================

def listar_empresas():
    """Retorna lista de empresas distintas no banco."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT empresa FROM empreendimentos ORDER BY empresa")
    empresas = [row[0] for row in cursor.fetchall()]
    conn.close()
    return empresas


def haversine_metros(lat1, lon1, lat2, lon2):
    """Distancia em metros entre dois pontos (lat/lon em graus)."""
    R = 6_371_000
    rlat1, rlat2 = radians(lat1), radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(rlat1) * cos(rlat2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def verificar_url(url, base_url, padrao_produto=None):
    """
    Verifica se uma URL de empreendimento ainda esta viva.

    Retorna:
        ('ok', url_final, soup)              - pagina viva
        ('redirect_produto', url_nova, soup)  - redirect para outro produto
        ('morta', None, None)                - pagina morta/removida
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    except requests.RequestException as e:
        logger.debug(f"  Erro de conexao: {e}")
        return ("morta", None, None)

    url_final = resp.url.rstrip("/")
    url_original = url.rstrip("/")

    # HTTP 404/410
    if resp.status_code in (404, 410):
        return ("morta", None, None)

    # Qualquer outro erro
    if resp.status_code >= 400:
        return ("morta", None, None)

    # Redirect para homepage ou pagina de listagem
    base_clean = base_url.rstrip("/")
    if url_final == base_clean or url_final == base_clean + "/":
        return ("morta", None, None)

    # Detectar redirect para pagina de listagem
    listagem_patterns = [
        "/empreendimentos", "/imoveis", "/portfolio", "/busca",
        "/lancamentos", "/apartamentos", "/produtos",
    ]
    if url_final != url_original:
        path_final = url_final.replace(base_clean, "").rstrip("/")
        if any(path_final == p or path_final == p + "/" for p in listagem_patterns):
            return ("morta", None, None)

    soup = BeautifulSoup(resp.text, "html.parser")
    texto = soup.get_text(separator=" ", strip=True)

    # Conteudo muito curto (shell vazio)
    if len(texto) < 1000:
        return ("morta", None, None)

    # Soft 404
    texto_lower = texto.lower()[:2000]
    for pattern in SOFT_404_PATTERNS:
        if pattern in texto_lower:
            return ("morta", None, None)

    # Redirect para outro produto
    if url_final != url_original:
        # Se redirecionou para outra URL de produto valida
        if padrao_produto and re.search(padrao_produto, url_final):
            return ("redirect_produto", url_final, soup)
        # Heuristica: se a URL final ainda contem segmento de produto
        for seg in ["/empreendimento/", "/empreendimentos/", "/imoveis/", "/imovel/"]:
            if seg in url_final:
                return ("redirect_produto", url_final, soup)

    return ("ok", url_final, soup)


# ============================================================
# BATCH HANDLERS — coletam listagem completa para comparar
# ============================================================

def batch_mrv():
    """Busca todos os empreendimentos MRV via API GraphQL. Retorna {url: {nome, fase}}."""
    API_BASE = (
        "https://mrv.com.br/graphql/execute.json/mrv/search-result-card"
        ";basePath=/content/dam/mrv/content-fragments/detalhe-empreendimento/{estado}/"
    )
    ESTADOS = [
        "sao-paulo", "minas-gerais", "rio-de-janeiro", "parana",
        "santa-catarina", "rio-grande-do-sul", "goias", "bahia",
        "pernambuco", "ceara", "espirito-santo", "mato-grosso-do-sul",
        "mato-grosso", "para", "maranhao", "paraiba",
        "rio-grande-do-norte", "alagoas", "sergipe", "amazonas",
        "tocantins", "piaui", "distrito-federal",
    ]

    ativos = {}
    for estado in ESTADOS:
        url = API_BASE.format(estado=estado)
        try:
            resp = requests.get(url, headers={**HEADERS_JSON, "Referer": "https://mrv.com.br/"}, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            dados = resp.json()
            items = dados.get("data", {}).get("empreendimentosList", {}).get("items", [])
            for item in items:
                nome = item.get("nomeImovel", "").strip()
                status = unescape(item.get("statusImovel", "") or "")
                path_parts = (item.get("_path", "")).strip("/").split("/")
                if len(path_parts) >= 4:
                    p_estado = path_parts[-4].lower()
                    p_tipo = path_parts[-3].lower()
                    p_cidade = path_parts[-2].lower()
                    p_slug = path_parts[-1].lower()
                    url_fonte = f"https://mrv.com.br/imoveis/{p_estado}/{p_cidade}/{p_tipo}-{p_slug}"
                else:
                    slug_final = path_parts[-1] if path_parts else ""
                    url_fonte = f"https://mrv.com.br/{estado}/{slug_final}"
                ativos[url_fonte] = {"nome": nome, "fase": _mapear_fase_mrv(status)}
            logger.info(f"  MRV {estado}: {len(items)} empreendimentos")
        except Exception as e:
            logger.warning(f"  MRV {estado}: erro {e}")
        time.sleep(1)

    logger.info(f"  MRV total: {len(ativos)} empreendimentos ativos na API")
    return ativos


def _mapear_fase_mrv(status):
    if not status:
        return None
    mapa = {
        "lançamento": "Lançamento", "lancamento": "Lançamento",
        "em construção": "Em Construção", "em construcao": "Em Construção",
        "prontos": "Imóvel Pronto", "pronto": "Imóvel Pronto",
        "breve lançamento": "Breve Lançamento", "breve lancamento": "Breve Lançamento",
    }
    return mapa.get(status.lower(), status)


def batch_vivaz():
    """Busca todos os empreendimentos Vivaz via API REST. Retorna {url: {nome, fase}}."""
    BASE = "https://www.meuvivaz.com.br"
    headers = {**HEADERS_JSON, "Referer": f"{BASE}/", "Origin": BASE}

    try:
        resp = requests.post(
            f"{BASE}/imovel/produto-por-estado",
            headers=headers,
            json={"ImovelOrigemId": 3},
            timeout=30,
        )
        data = resp.json()
    except Exception as e:
        logger.warning(f"  Vivaz API erro: {e}")
        return {}

    ativos = {}
    if data and data.get("success"):
        for item in data.get("Imoveis", []):
            nome = item.get("Nome", "").strip()
            slug = item.get("UrlAmigavel", "")
            status = item.get("Status", "")
            fase = _mapear_fase_vivaz(status)
            url_fonte = f"{BASE}/empreendimentos/{slug}" if slug else None
            if url_fonte:
                ativos[url_fonte] = {"nome": nome, "fase": fase}

    logger.info(f"  Vivaz total: {len(ativos)} empreendimentos ativos na API")
    return ativos


def _mapear_fase_vivaz(status):
    if not status:
        return None
    mapa = {
        "lançamento": "Lançamento", "lancamento": "Lançamento",
        "em breve": "Breve Lançamento",
        "em obras": "Em Construção",
        "pronto": "Imóvel Pronto", "pronto para morar": "Imóvel Pronto",
    }
    return mapa.get(status.lower(), status)


def batch_direcional():
    """Busca URLs ativas do sitemap Direcional. Retorna {url: {nome, fase}}."""
    SITEMAP_URL = "https://www.direcional.com.br/empreendimento-sitemap.xml"
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"  Direcional sitemap erro: {e}")
        return {}

    urls = []
    for match in re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", resp.text):
        url = match.strip()
        if "/empreendimentos/" in url and url.rstrip("/") != "https://www.direcional.com.br/empreendimentos":
            urls.append(url)

    # Para batch, retornamos apenas as URLs encontradas (sem nome/fase — sera checado individualmente)
    ativos = {url: {"nome": None, "fase": None} for url in urls}
    logger.info(f"  Direcional sitemap: {len(ativos)} URLs ativas")
    return ativos


def batch_vivabenx():
    """Busca URLs ativas do sitemap Viva Benx. Retorna {url: {nome, fase}}."""
    SITEMAP_URL = "https://www.vivabenx.com.br/sitemap.xml"
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"  Viva Benx sitemap erro: {e}")
        return {}

    ativos = {}
    for match in re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", resp.text):
        url = match.strip()
        if "/empreendimento/" in url:
            ativos[url] = {"nome": None, "fase": None}

    logger.info(f"  Viva Benx sitemap: {len(ativos)} URLs ativas")
    return ativos


# ============================================================
# RECONCILIACAO — URL morta, tentar encontrar substituto
# ============================================================

def reconciliar_empreendimento(emp, ativos_site, registros_empresa, run_id, dry_run):
    """
    Tenta reconciliar empreendimento com URL morta.

    Retorna tipo de resolucao: 'redirect', 'found_by_name', 'found_by_location', 'removido'
    """
    url = emp["url_fonte"]
    nome = emp["nome"]
    fase_ant = emp["fase"]
    empresa = None

    # Encontrar empresa a partir dos registros
    for r in registros_empresa:
        if r["url_fonte"] == url:
            # Pegar empresa do contexto (sera passada como parametro depois)
            break

    # Passo 1: Redirect (ja detectado antes de chamar esta funcao)
    # Se chegou aqui, nao houve redirect valido

    # Passo 2: Busca por nome
    nomes_ativos = {}
    for url_ativo, info in ativos_site.items():
        if info.get("nome"):
            nomes_ativos[url_ativo] = info["nome"]

    if nomes_ativos:
        melhor_ratio = 0
        melhor_url = None
        melhor_nome = None
        for url_ativo, nome_ativo in nomes_ativos.items():
            ratio = difflib.SequenceMatcher(None, nome.lower(), nome_ativo.lower()).ratio()
            if ratio > melhor_ratio:
                melhor_ratio = ratio
                melhor_url = url_ativo
                melhor_nome = nome_ativo

        if melhor_ratio >= 0.85:
            logger.info(f"    -> MATCH POR NOME ({melhor_ratio:.0%}): {melhor_nome}")
            logger.info(f"       Nova URL: {melhor_url}")
            return "found_by_name", melhor_url, melhor_nome

    # Passo 3: Busca por localizacao
    lat = emp.get("latitude")
    lon = emp.get("longitude")
    if lat is not None and lon is not None:
        try:
            lat, lon = float(lat), float(lon)
        except (ValueError, TypeError):
            lat, lon = None, None

    if lat is not None and lon is not None:
        # Comparar com registros ativos que tem coordenadas
        melhor_dist = float("inf")
        melhor_match = None
        for reg in registros_empresa:
            if reg["url_fonte"] == url:
                continue
            # So comparar com URLs que estao ativas no site
            if reg["url_fonte"] not in ativos_site:
                continue
            rlat = reg.get("latitude")
            rlon = reg.get("longitude")
            if rlat is None or rlon is None:
                continue
            try:
                rlat, rlon = float(rlat), float(rlon)
            except (ValueError, TypeError):
                continue
            dist = haversine_metros(lat, lon, rlat, rlon)
            if dist < 200 and dist < melhor_dist:
                melhor_dist = dist
                melhor_match = reg

        if melhor_match:
            logger.info(f"    -> MATCH POR LOCALIZACAO ({melhor_dist:.0f}m): {melhor_match['nome']}")
            return "found_by_location", melhor_match["url_fonte"], melhor_match["nome"]

    # Passo 4: Removido
    logger.info(f"    -> REMOVIDO (sem substituto)")
    return "removido", None, None


# ============================================================
# VERIFICACAO POR TIPO DE SITE
# ============================================================

def verificar_empresa_batch(empresa, registros, ativos_site, run_id, dry_run, limite):
    """Verifica empresa usando listagem batch (API/sitemap) + comparacao."""
    resultados = {"ok": 0, "fase_mudou": 0, "redirect": 0,
                  "found_by_name": 0, "found_by_location": 0,
                  "removido": 0, "erro": 0}

    processados = []

    for i, reg in enumerate(registros):
        if limite and i >= limite:
            break

        url = reg["url_fonte"]
        if not url:
            continue

        nome = reg["nome"]
        fase_ant = reg["fase"]

        # Ja removido? Pular
        if fase_ant == "Removido":
            continue

        url_normalizada = url.rstrip("/")

        # Checar se URL esta na listagem ativa
        url_encontrada = None
        for url_ativa in ativos_site:
            if url_ativa.rstrip("/") == url_normalizada:
                url_encontrada = url_ativa
                break

        if url_encontrada:
            # URL ativa — checar se fase mudou
            info = ativos_site[url_encontrada]
            fase_nova = info.get("fase")

            if fase_nova and fase_ant and fase_nova != fase_ant:
                logger.info(f"  {nome}: fase mudou {fase_ant} -> {fase_nova}")
                if not dry_run:
                    atualizar_empreendimento_por_url(empresa, url, {"fase": fase_nova})
                    registrar_mudanca(run_id, empresa, nome, "fase_mudou",
                                      "fase", fase_ant, fase_nova)
                resultados["fase_mudou"] += 1
            else:
                resultados["ok"] += 1
        else:
            # URL nao encontrada na listagem — URL morta
            logger.info(f"  {nome}: URL ausente da listagem ({url})")

            tipo, url_nova, nome_novo = reconciliar_empreendimento(
                reg, ativos_site, registros, run_id, dry_run
            )

            if tipo in ("found_by_name", "found_by_location"):
                if not dry_run:
                    atualizar_empreendimento_por_url(empresa, url, {"url_fonte": url_nova})
                    registrar_reconciliacao(
                        empresa, "renomeado" if tipo == "found_by_name" else "relancado",
                        url, nome_anterior=nome, nome_novo=nome_novo,
                        url_nova=url_nova, fase_anterior=fase_ant,
                    )
            elif tipo == "removido":
                if not dry_run:
                    atualizar_empreendimento_por_url(empresa, url, {"fase": "Removido"})
                    registrar_mudanca(run_id, empresa, nome, "fase_mudou",
                                      "fase", fase_ant, "Removido")
                    registrar_reconciliacao(
                        empresa, "cancelado", url,
                        nome_anterior=nome, fase_anterior=fase_ant,
                        observacao="URL ausente da listagem batch",
                    )

            resultados[tipo] += 1
        processados.append(url)

    return resultados, processados


def verificar_empresa_individual(empresa, registros, run_id, dry_run, limite,
                                  base_url=None, padrao_produto=None,
                                  use_selenium=False):
    """Verifica empresa visitando cada URL individualmente."""
    resultados = {"ok": 0, "fase_mudou": 0, "redirect": 0,
                  "found_by_name": 0, "found_by_location": 0,
                  "removido": 0, "erro": 0}

    processados = []
    driver = None

    if use_selenium:
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            from config.settings import SELENIUM

            options = Options()
            if SELENIUM.get("headless", True):
                options.add_argument("--headless=new")
            options.add_argument(f"user-agent={SELENIUM['user_agent']}")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options,
            )
            driver.set_page_load_timeout(SELENIUM.get("timeout", 30))
        except Exception as e:
            logger.error(f"  Falha ao iniciar Selenium: {e}")
            return resultados, processados

    try:
        for i, reg in enumerate(registros):
            if limite and i >= limite:
                break

            url = reg["url_fonte"]
            if not url:
                continue

            nome = reg["nome"]
            fase_ant = reg["fase"]

            if fase_ant == "Removido":
                continue

            logger.info(f"  [{i+1}/{min(len(registros), limite or len(registros))}] {nome}")

            try:
                if use_selenium and driver:
                    resultado = _verificar_url_selenium(driver, url, base_url)
                else:
                    resultado = verificar_url(url, base_url or "", padrao_produto)

                status, url_nova, soup = resultado

                if status == "ok":
                    # Re-detectar fase
                    if soup:
                        texto = soup.get_text(separator="\n", strip=True)
                        fase_nova = detectar_fase(texto, soup)
                        if fase_nova and fase_ant and fase_nova != fase_ant:
                            logger.info(f"    Fase mudou: {fase_ant} -> {fase_nova}")
                            if not dry_run:
                                atualizar_empreendimento_por_url(empresa, url, {"fase": fase_nova})
                                registrar_mudanca(run_id, empresa, nome, "fase_mudou",
                                                  "fase", fase_ant, fase_nova)
                            resultados["fase_mudou"] += 1
                        else:
                            resultados["ok"] += 1
                    else:
                        resultados["ok"] += 1

                elif status == "redirect_produto":
                    logger.info(f"    Redirect: {url} -> {url_nova}")
                    if not dry_run:
                        atualizar_empreendimento_por_url(empresa, url, {"url_fonte": url_nova})
                        registrar_reconciliacao(
                            empresa, "renomeado", url,
                            nome_anterior=nome, url_nova=url_nova,
                            fase_anterior=fase_ant,
                            observacao="URL redirecionou para outro produto",
                        )
                    resultados["redirect"] += 1

                elif status == "morta":
                    logger.info(f"    URL morta: {url}")
                    if not dry_run:
                        atualizar_empreendimento_por_url(empresa, url, {"fase": "Removido"})
                        registrar_mudanca(run_id, empresa, nome, "fase_mudou",
                                          "fase", fase_ant, "Removido")
                        registrar_reconciliacao(
                            empresa, "cancelado", url,
                            nome_anterior=nome, fase_anterior=fase_ant,
                            observacao="URL morta (404/redirect/soft404)",
                        )
                    resultados["removido"] += 1

            except Exception as e:
                logger.error(f"    Erro: {e}")
                resultados["erro"] += 1

            processados.append(url)
            time.sleep(DELAY)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    return resultados, processados


def _verificar_url_selenium(driver, url, base_url):
    """Verifica URL usando Selenium (para Cury/Metrocasa)."""
    try:
        driver.get(url)
        time.sleep(3)  # Esperar JS carregar

        url_final = driver.current_url.rstrip("/")
        url_original = url.rstrip("/")

        # Redirect para homepage
        if base_url:
            base_clean = base_url.rstrip("/")
            if url_final == base_clean or url_final == base_clean + "/":
                return ("morta", None, None)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        texto = soup.get_text(separator=" ", strip=True)

        if len(texto) < 1000:
            return ("morta", None, None)

        texto_lower = texto.lower()[:2000]
        for pattern in SOFT_404_PATTERNS:
            if pattern in texto_lower:
                return ("morta", None, None)

        if url_final != url_original:
            for seg in ["/empreendimento/", "/empreendimentos/", "/imoveis/", "/imovel/"]:
                if seg in url_final:
                    return ("redirect_produto", url_final, soup)

        return ("ok", url_final, soup)

    except Exception as e:
        logger.debug(f"  Selenium erro: {e}")
        return ("morta", None, None)


# ============================================================
# HANDLERS POR EMPRESA
# ============================================================

# Base URLs e padroes para empresas genericas
# Importamos do generico para obter base_url e padrao_link
def _get_generico_config(empresa):
    """Retorna (base_url, padrao_produto) para empresa generica."""
    try:
        from scrapers.generico_empreendimentos import EMPRESAS
        for key, cfg in EMPRESAS.items():
            if cfg["nome_banco"] == empresa:
                return cfg["base_url"], cfg.get("padrao_link")
    except ImportError:
        pass
    return None, None


def verificar_empresa(empresa, registros, run_id, dry_run, limite, sem_selenium):
    """Seleciona handler e verifica empresa."""
    tipo = TIPO_VERIFICACAO.get(empresa, "generico")

    logger.info(f"\n{'='*50}")
    logger.info(f"Empresa: {empresa} ({len(registros)} registros, tipo={tipo})")
    logger.info(f"{'='*50}")

    # Filtrar registros ja removidos
    registros_ativos = [r for r in registros if r.get("fase") != "Removido"]
    if not registros_ativos:
        logger.info("  Nenhum registro ativo para verificar.")
        return {"ok": 0, "fase_mudou": 0, "redirect": 0, "found_by_name": 0,
                "found_by_location": 0, "removido": 0, "erro": 0}, []

    # Batch handlers (API/sitemap)
    if tipo == "mrv":
        logger.info("  Buscando listagem via API GraphQL...")
        ativos = batch_mrv()
        return verificar_empresa_batch(empresa, registros_ativos, ativos,
                                        run_id, dry_run, limite)

    elif tipo == "vivaz":
        logger.info("  Buscando listagem via API REST...")
        ativos = batch_vivaz()
        return verificar_empresa_batch(empresa, registros_ativos, ativos,
                                        run_id, dry_run, limite)

    elif tipo == "direcional":
        logger.info("  Buscando sitemap XML...")
        ativos = batch_direcional()
        return verificar_empresa_batch(empresa, registros_ativos, ativos,
                                        run_id, dry_run, limite)

    elif tipo == "vivabenx":
        logger.info("  Buscando sitemap XML...")
        ativos = batch_vivabenx()
        return verificar_empresa_batch(empresa, registros_ativos, ativos,
                                        run_id, dry_run, limite)

    # Selenium handlers
    elif tipo == "cury":
        if sem_selenium:
            logger.info("  Pulando (--sem-selenium)")
            return {"ok": 0, "fase_mudou": 0, "redirect": 0, "found_by_name": 0,
                    "found_by_location": 0, "removido": 0, "erro": 0}, []
        return verificar_empresa_individual(
            empresa, registros_ativos, run_id, dry_run, limite,
            base_url="https://cury.net", use_selenium=True,
        )

    elif tipo == "metrocasa":
        if sem_selenium:
            logger.info("  Pulando (--sem-selenium)")
            return {"ok": 0, "fase_mudou": 0, "redirect": 0, "found_by_name": 0,
                    "found_by_location": 0, "removido": 0, "erro": 0}, []
        return verificar_empresa_individual(
            empresa, registros_ativos, run_id, dry_run, limite,
            base_url="https://www.metrocasa.com.br", use_selenium=True,
        )

    # Plano&Plano (requests individual)
    elif tipo == "planoeplano":
        return verificar_empresa_individual(
            empresa, registros_ativos, run_id, dry_run, limite,
            base_url="https://www.planoeplano.com.br",
            padrao_produto=r"planoeplano\.com\.br/(?:portfolio|imoveis)/[\w-]+",
        )

    # Generico (requests individual)
    else:
        base_url, padrao_produto = _get_generico_config(empresa)
        return verificar_empresa_individual(
            empresa, registros_ativos, run_id, dry_run, limite,
            base_url=base_url or "",
            padrao_produto=padrao_produto,
        )


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Verificar status de empreendimentos existentes")
    parser.add_argument("--empresa", type=str, default=None,
                        help="Verificar apenas uma empresa (nome exato)")
    parser.add_argument("--sem-selenium", action="store_true",
                        help="Pular empresas que requerem Selenium (Cury, Metrocasa)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Apenas verificar, nao atualizar o banco")
    parser.add_argument("--limite", type=int, default=0,
                        help="Maximo de empreendimentos por empresa (0=todos)")
    args = parser.parse_args()

    inicio = datetime.now()
    run_id = f"verif_{inicio.strftime('%Y%m%d_%H%M%S')}"
    limite = args.limite if args.limite > 0 else None

    logger.info("=" * 60)
    logger.info("VERIFICACAO DE STATUS DE EMPREENDIMENTOS")
    logger.info(f"Run ID: {run_id}")
    if args.dry_run:
        logger.info("*** MODO DRY-RUN — nenhuma alteracao sera feita ***")
    if args.sem_selenium:
        logger.info("Selenium desabilitado (Cury e Metrocasa serao pulados)")
    if args.empresa:
        logger.info(f"Empresa filtrada: {args.empresa}")
    if limite:
        logger.info(f"Limite por empresa: {limite}")
    logger.info("=" * 60)

    # Carregar progresso (para retomar execucao interrompida)
    progresso = carregar_progresso(run_id)

    # Listar empresas
    if args.empresa:
        empresas = [args.empresa]
    else:
        empresas = listar_empresas()

    logger.info(f"\n{len(empresas)} empresa(s) para verificar")

    # Contadores globais
    totais = {"ok": 0, "fase_mudou": 0, "redirect": 0,
              "found_by_name": 0, "found_by_location": 0,
              "removido": 0, "erro": 0}

    for empresa in empresas:
        registros = buscar_empreendimentos_por_empresa(empresa)

        # Filtrar URLs ja processadas neste run
        registros_pendentes = [
            r for r in registros
            if r["url_fonte"] and r["url_fonte"] not in progresso["processados"]
        ]

        if not registros_pendentes:
            logger.info(f"\n{empresa}: todos ja processados, pulando.")
            continue

        try:
            resultados, processados = verificar_empresa(
                empresa, registros_pendentes, run_id, args.dry_run, limite, args.sem_selenium
            )

            # Atualizar totais
            for k, v in resultados.items():
                totais[k] = totais.get(k, 0) + v

            # Salvar progresso
            progresso["processados"].extend(processados)
            for k, v in resultados.items():
                progresso["resultados"][k] = progresso["resultados"].get(k, 0) + v
            salvar_progresso(progresso)

        except Exception as e:
            logger.error(f"Erro ao verificar {empresa}: {e}")
            totais["erro"] += 1

    # Relatorio final
    fim = datetime.now()
    duracao = int((fim - inicio).total_seconds())

    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL — VERIFICACAO DE STATUS")
    logger.info("=" * 60)
    logger.info(f"  Duracao: {duracao}s ({duracao // 60}min)")
    logger.info(f"  URLs processadas: {len(progresso['processados'])}")
    logger.info(f"  OK (sem mudanca): {totais['ok']}")
    logger.info(f"  Fase mudou: {totais['fase_mudou']}")
    logger.info(f"  Redirect: {totais['redirect']}")
    logger.info(f"  Match por nome: {totais['found_by_name']}")
    logger.info(f"  Match por localizacao: {totais['found_by_location']}")
    logger.info(f"  Removido: {totais['removido']}")
    logger.info(f"  Erros: {totais['erro']}")
    if args.dry_run:
        logger.info("  *** DRY-RUN: nenhuma alteracao foi feita ***")
    logger.info("=" * 60)

    # Registrar run (mesmo em dry-run, para historico)
    if not args.dry_run:
        stats = {
            "etapas_ok": len(empresas),
            "etapas_erro": 0,
            "novos": 0,
            "mudancas": totais["fase_mudou"] + totais["redirect"] + totais["found_by_name"]
                        + totais["found_by_location"] + totais["removido"],
            "total_apos": contar_empreendimentos(),
        }
        registrar_run(run_id, inicio, fim, "ok", stats)

    # Limpar progresso ao finalizar com sucesso
    if os.path.exists(PROGRESSO_FILE):
        os.remove(PROGRESSO_FILE)


if __name__ == "__main__":
    main()

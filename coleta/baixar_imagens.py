"""
Download e inventario de imagens de empreendimentos.
=====================================================
Tres fases:
  1. Inventario de disco: escaneia downloads/{empresa}/imagens/
  2. Popular DB: atualiza campos imagem_url e imagens_{cat} no banco
  3. Download de faltantes: baixa imagens da web para empreendimentos sem imagens

Prioridade: plantas (floor plans) > fachada > areas_comuns > decorado > geral > obra

Uso:
    python baixar_imagens.py
    python baixar_imagens.py --empresa "Cury"
    python baixar_imagens.py --apenas-inventario
    python baixar_imagens.py --limite 5
    python baixar_imagens.py --max-por-empreendimento 10
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import hashlib
import requests
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import REQUESTS as REQ_CONFIG, LOGS_DIR, DOWNLOADS_DIR, SELENIUM
from data.database import (
    get_connection,
    garantir_coluna,
    atualizar_empreendimento,
)

# ============================================================
# CONFIGURACAO
# ============================================================

EMPRESA_CONFIG = {
    # --- Empresas com imagens existentes ---
    "MRV":               {"tipo_acesso": "api_mrv",    "download_key": "mrv"},
    "Vivaz":             {"tipo_acesso": "api_vivaz",   "download_key": "vivaz"},
    "Plano&Plano":       {"tipo_acesso": "requests",   "download_key": "planoeplano"},
    "Direcional":        {"tipo_acesso": "requests",   "download_key": "direcional"},
    "Magik JC":          {"tipo_acesso": "requests",   "download_key": "magik_jc"},
    "Kazzas":            {"tipo_acesso": "requests",   "download_key": "kazzas"},
    "Vibra Residencial": {"tipo_acesso": "requests",   "download_key": "vibra"},
    "Pacaembu":          {"tipo_acesso": "requests",   "download_key": "pacaembu"},
    "Mundo Apto":        {"tipo_acesso": "requests",   "download_key": "mundo_apto"},
    "Conx":              {"tipo_acesso": "requests",   "download_key": "conx"},
    "Benx":              {"tipo_acesso": "requests",   "download_key": "benx"},
    "Cury":              {"tipo_acesso": "selenium",   "download_key": "cury"},
    "Metrocasa":         {"tipo_acesso": "selenium",   "download_key": "metrocasa"},
    # --- Empresas sem imagens (requests) ---
    "VIC Engenharia":    {"tipo_acesso": "requests",   "download_key": "vic"},
    "HM Engenharia":     {"tipo_acesso": "selenium",   "download_key": "eme"},
    "Grafico":           {"tipo_acesso": "requests",   "download_key": "grafico"},
    "Econ Construtora":  {"tipo_acesso": "requests",   "download_key": "econ"},
    "Novolar":           {"tipo_acesso": "requests",   "download_key": "novolar"},
    "Graal Engenharia":  {"tipo_acesso": "requests",   "download_key": "graal"},
    "Árbore":            {"tipo_acesso": "requests",   "download_key": "arbore"},
    "Viva Benx":         {"tipo_acesso": "requests",   "download_key": "vivabenx"},
    "Vasco Construtora": {"tipo_acesso": "requests",   "download_key": "vasco"},
    "Vinx":              {"tipo_acesso": "requests",   "download_key": "vinx"},
    "Riformato":         {"tipo_acesso": "requests",   "download_key": "riformato"},
    "SUGOI":             {"tipo_acesso": "requests",   "download_key": "sugoi"},
    "ACLF":              {"tipo_acesso": "requests",   "download_key": "aclf"},
    "Emccamp":           {"tipo_acesso": "requests",   "download_key": "emccamp"},
    "BM7":               {"tipo_acesso": "requests",   "download_key": "bm7"},
    "FYP Engenharia":    {"tipo_acesso": "requests",   "download_key": "fyp"},
    "Stanza":            {"tipo_acesso": "requests",   "download_key": "stanza"},
    "Sousa Araujo":      {"tipo_acesso": "requests",   "download_key": "sousa_araujo"},
    "Smart Construtora": {"tipo_acesso": "requests",   "download_key": "smart"},
    "EPH":               {"tipo_acesso": "requests",   "download_key": "eph"},
    "Jotanunes":         {"tipo_acesso": "requests",   "download_key": "jotanunes"},
    "Cavazani":          {"tipo_acesso": "requests",   "download_key": "cavazani"},
    "Rev3":              {"tipo_acesso": "requests",   "download_key": "rev3"},
    "Carrilho":          {"tipo_acesso": "requests",   "download_key": "carrilho"},
    "BP8":               {"tipo_acesso": "requests",   "download_key": "bp8"},
    "ACL Incorporadora": {"tipo_acesso": "requests",   "download_key": "acl"},
    "Ampla":             {"tipo_acesso": "requests",   "download_key": "ampla"},
    "Novvo":             {"tipo_acesso": "requests",   "download_key": "novvo"},
    "M.Lar":             {"tipo_acesso": "requests",   "download_key": "mlar"},
    "Construtora Open":  {"tipo_acesso": "requests",   "download_key": "open"},
    "Ún1ca":             {"tipo_acesso": "requests",   "download_key": "unica"},
}

CATEGORIAS = ["plantas", "fachada", "areas_comuns", "decorado", "geral", "obra"]

HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/*,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

IMG_HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

PROGRESSO_FILE = os.path.join(LOGS_DIR, "baixar_imagens_progresso.json")

# Termos para ignorar imagens (logos, icons, etc.)
SKIP_TERMS = [
    "logo", "icon", "avatar", "pixel", "svg", "data:", "base64",
    "1x1", "placeholder", "dot.png", "governament", "reclame",
    "whatsapp", "facebook", "twitter", "instagram", "linkedin",
    "favicon", "spinner", "loading", "arrow", "close", "menu",
    "selo", "badge", "certificado", "button", "btn",
]

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("baixar_imagens")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "baixar_imagens.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


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
        json.dump(progresso, f, indent=2, ensure_ascii=False)


# ============================================================
# SELENIUM
# ============================================================

def criar_driver():
    """Cria instancia do Chrome headless."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={SELENIUM['user_agent']}")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_page_load_timeout(30)
    return driver


# ============================================================
# API VIVAZ - IMAGENS
# ============================================================

# ============================================================
# API MRV - IMAGENS
# ============================================================

MRV_API_BASE = (
    "https://mrv.com.br/graphql/execute.json/mrv/search-result-card"
    ";basePath=/content/dam/mrv/content-fragments/detalhe-empreendimento/{estado}/"
)

MRV_ESTADOS = [
    "sao-paulo", "minas-gerais", "rio-de-janeiro", "parana", "santa-catarina",
    "rio-grande-do-sul", "goias", "bahia", "pernambuco", "ceara", "espirito-santo",
    "mato-grosso-do-sul", "mato-grosso", "para", "maranhao", "paraiba",
    "rio-grande-do-norte", "sergipe", "alagoas", "amazonas", "piaui",
    "distrito-federal", "tocantins",
]

MRV_TIPO_MAP = {
    "perspectivas de fachada": "fachada",
    "fachada": "fachada",
    "perspectiva": "fachada",
    "planta": "plantas",
    "plantas": "plantas",
    "implantação": "plantas",
    "implantacao": "plantas",
    "decorado": "decorado",
    "interiores": "decorado",
    "lazer": "areas_comuns",
    "área comum": "areas_comuns",
    "areas comuns": "areas_comuns",
    "obra": "obra",
    "andamento": "obra",
}


def _mrv_categorizar(tipo_titulo):
    """Categoriza imagem MRV pelo tipoTitulo da API."""
    if not tipo_titulo:
        return "geral"
    t = tipo_titulo.lower().strip()
    for key, cat in MRV_TIPO_MAP.items():
        if key in t:
            return cat
    return "geral"


def baixar_imagens_mrv_api(session, max_imgs, progresso):
    """Baixa imagens de todos os empreendimentos MRV via API GraphQL."""
    import sqlite3
    from html import unescape

    conn = get_connection()
    # Mapear nome -> slug do banco
    rows = conn.execute(
        "SELECT nome, slug, imagem_url FROM empreendimentos WHERE empresa = 'MRV'"
    ).fetchall()
    conn.close()

    db_map = {}
    for row in rows:
        db_map[row["nome"]] = {"slug": row["slug"], "tem_img": bool(row["imagem_url"])}

    total_baixadas = 0

    for estado in MRV_ESTADOS:
        url = MRV_API_BASE.format(estado=estado)
        try:
            r = session.get(url, headers={"Accept": "application/json"}, timeout=30)
            if r.status_code != 200:
                continue
            data = r.json()
            items = data.get("data", {}).get("empreendimentosList", {}).get("items", [])
        except Exception as e:
            logger.warning(f"  MRV API erro {estado}: {e}")
            continue

        for item in items:
            nome = unescape(item.get("nomeImovel", "")).strip()
            if not nome or nome not in db_map:
                continue
            if db_map[nome]["tem_img"]:
                continue

            slug = db_map[nome]["slug"]
            chave = f"MRV:{slug}"
            if chave in progresso.get("processados", []):
                continue

            # Extrair imagens da API
            imagens = []
            for grupo in (item.get("imagens") or []):
                for key, val in grupo.items():
                    if isinstance(val, list):
                        for img in val:
                            img_url = img.get("urlImagem")
                            if not img_url:
                                continue
                            alt = unescape(img.get("alt", "") or "")
                            tipo = unescape(img.get("tipoTitulo", "") or "")
                            cat = _mrv_categorizar(tipo)
                            imagens.append({"url": img_url, "alt": alt or tipo, "categoria": cat})

            if not imagens:
                progresso.setdefault("processados", []).append(chave)
                continue

            selecionadas = filtrar_e_priorizar(imagens, max_imgs)
            slug_pasta = normalizar_slug(slug) if slug else normalizar_slug(nome)
            pasta_base = os.path.join(DOWNLOADS_DIR, "mrv", "imagens", slug_pasta)

            baixadas = 0
            dados_db = {}

            for img in selecionadas:
                cat = img["categoria"]
                pasta_cat = os.path.join(pasta_base, cat)

                ext = ".jpg"
                url_lower = img["url"].lower()
                for e in [".png", ".webp", ".jpeg"]:
                    if e in url_lower:
                        ext = e
                        break

                url_hash = hashlib.md5(img["url"].encode()).hexdigest()[:8]
                desc = _gerar_descricao_arquivo(img.get("alt", ""), cat)
                nome_arquivo = f"{slug_pasta}_{cat}_{desc}_{url_hash}{ext}" if desc else f"{slug_pasta}_{cat}_{url_hash}{ext}"
                if len(nome_arquivo) > 120:
                    nome_arquivo = f"{slug_pasta}_{cat}_{url_hash}{ext}"
                caminho = os.path.join(pasta_cat, nome_arquivo)

                if os.path.exists(caminho):
                    baixadas += 1
                    continue

                if baixar_imagem(img["url"], caminho, session):
                    baixadas += 1
                    col = f"imagens_{cat}"
                    rel_path = os.path.relpath(caminho, os.path.dirname(DOWNLOADS_DIR)).replace("\\", "/")
                    dados_db.setdefault(col, []).append(rel_path)

            if dados_db:
                update = {}
                for col, paths in dados_db.items():
                    garantir_coluna(col, "TEXT")
                    update[col] = " | ".join(paths)
                for prioridade in CATEGORIAS:
                    key = f"imagens_{prioridade}"
                    if key in dados_db and dados_db[key]:
                        update["imagem_url"] = dados_db[key][0]
                        break
                atualizar_empreendimento("MRV", nome, update)

            if baixadas > 0:
                total_baixadas += baixadas
                logger.info(f"    {nome}: {baixadas} imagens")

            progresso.setdefault("processados", []).append(chave)
            db_map[nome]["tem_img"] = True

        time.sleep(1)

    return total_baixadas


VIVAZ_API_HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Content-Type": "application/json; charset=utf-8",
    "Accept": "*/*",
    "Origin": "https://www.meuvivaz.com.br",
    "Referer": "https://www.meuvivaz.com.br/",
}

VIVAZ_TIPO_MAP = {
    "Portaria": "fachada",
    "Fachada": "fachada",
    "Planta": "plantas",
    "Implantação": "plantas",
    "Implantacao": "plantas",
    "Decorado": "decorado",
    "Cozinha": "decorado",
    "Living": "decorado",
    "Dormitório": "decorado",
    "Dormitorio": "decorado",
    "Quarto": "decorado",
    "Sala": "decorado",
    "Banho": "decorado",
    "Banheiro": "decorado",
    "Piscina": "areas_comuns",
    "Lazer": "areas_comuns",
    "Churrasqueira": "areas_comuns",
    "Fitness": "areas_comuns",
    "Playground": "areas_comuns",
    "Área Comum": "areas_comuns",
    "Obra": "obra",
}


def baixar_imagens_vivaz(nome, slug, url_fonte, download_key, max_imgs, session, progresso):
    """Baixa imagens via API Vivaz (site é SPA, requests não funciona)."""
    # Extrair slug da URL
    api_slug = url_fonte.split("/empreendimentos/")[-1] if "/empreendimentos/" in url_fonte else slug

    try:
        r = session.post(
            "https://www.meuvivaz.com.br/imovel/informacoes",
            json={"Url": api_slug},
            headers=VIVAZ_API_HEADERS,
            timeout=15,
        )
        if r.status_code != 200 or "application/json" not in r.headers.get("content-type", ""):
            logger.warning(f"    API Vivaz retornou {r.status_code}")
            return 0
        data = r.json()
    except Exception as e:
        logger.warning(f"    API Vivaz erro: {e}")
        return 0

    imovel = data.get("imovel", {})
    imagens = []

    # ImagemPrincipal
    img_princ = imovel.get("ImagemPrincipal") or {}
    url_princ = img_princ.get("ImgOriginal") or img_princ.get("Img1944x1080") or img_princ.get("Img400x375")
    if url_princ:
        imagens.append({"url": url_princ, "alt": img_princ.get("Legenda", ""), "categoria": "fachada"})

    # Galeria
    for item in (imovel.get("Galeria") or []):
        url_img = item.get("ImgOriginal") or item.get("Img2408x1080") or item.get("Img666x600")
        if not url_img:
            continue
        tipo = item.get("TipoImagem", "")
        cat = VIVAZ_TIPO_MAP.get(tipo, categorizar_imagem(url_img, tipo))
        imagens.append({"url": url_img, "alt": tipo, "categoria": cat})

    if not imagens:
        logger.info(f"    API Vivaz: nenhuma imagem")
        return 0

    selecionadas = filtrar_e_priorizar(imagens, max_imgs)
    slug_pasta = normalizar_slug(slug) if slug else normalizar_slug(nome)
    pasta_base = os.path.join(DOWNLOADS_DIR, download_key, "imagens", slug_pasta)

    baixadas = 0
    dados_db = {}

    for img in selecionadas:
        cat = img["categoria"]
        pasta_cat = os.path.join(pasta_base, cat)

        ext = ".jpg"
        url_lower = img["url"].lower()
        for e in [".png", ".webp", ".jpeg"]:
            if e in url_lower:
                ext = e
                break

        url_hash = hashlib.md5(img["url"].encode()).hexdigest()[:10]
        nome_arquivo = f"{slug_pasta}_{url_hash}{ext}"
        caminho = os.path.join(pasta_cat, nome_arquivo)

        if os.path.exists(caminho):
            baixadas += 1
            continue

        if baixar_imagem(img["url"], caminho, session):
            baixadas += 1
            col = f"imagens_{cat}"
            rel_path = os.path.relpath(caminho, os.path.dirname(DOWNLOADS_DIR)).replace("\\", "/")
            dados_db.setdefault(col, []).append(rel_path)

    # Atualizar DB
    if dados_db:
        update = {}
        for col, paths in dados_db.items():
            garantir_coluna(col, "TEXT")
            update[col] = " | ".join(paths)

        for prioridade in CATEGORIAS:
            key = f"imagens_{prioridade}"
            if key in dados_db and dados_db[key]:
                update["imagem_url"] = dados_db[key][0]
                break

        atualizar_empreendimento("Vivaz", nome, update)

    return baixadas


# ============================================================
# FASE 1: INVENTARIO DE DISCO
# ============================================================

def inventariar_imagens_disco(download_key):
    """Escaneia downloads/{key}/imagens/ e retorna inventario por slug."""
    imagens_dir = os.path.join(DOWNLOADS_DIR, download_key, "imagens")

    if not os.path.exists(imagens_dir):
        return {}

    inventario = {}

    for pasta_slug in os.listdir(imagens_dir):
        caminho_slug = os.path.join(imagens_dir, pasta_slug)
        if not os.path.isdir(caminho_slug):
            continue

        categorias = {}
        total_imgs = 0

        conteudo = os.listdir(caminho_slug)
        tem_subcategorias = any(
            os.path.isdir(os.path.join(caminho_slug, item))
            for item in conteudo
        )

        if tem_subcategorias:
            for sub in conteudo:
                sub_path = os.path.join(caminho_slug, sub)
                if os.path.isdir(sub_path):
                    arquivos = []
                    for f in os.listdir(sub_path):
                        fp = os.path.join(sub_path, f)
                        if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) and os.path.getsize(fp) > 5000:
                            arquivos.append(fp)
                    if arquivos:
                        categorias[sub] = sorted(arquivos)
                        total_imgs += len(arquivos)
        else:
            arquivos = []
            for f in conteudo:
                fp = os.path.join(caminho_slug, f)
                if os.path.isfile(fp) and f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) and os.path.getsize(fp) > 5000:
                    arquivos.append(fp)
            if arquivos:
                categorias["geral"] = sorted(arquivos)
                total_imgs += len(arquivos)

        if total_imgs > 0:
            inventario[pasta_slug] = {"categorias": categorias, "total": total_imgs}

    return inventario


def normalizar_slug(slug):
    """Normaliza slug para comparacao."""
    if not slug:
        return ""
    return slug.lower().replace(" ", "-").replace("_", "-").strip("-")


def encontrar_pasta_slug(slug_db, inventario):
    """Tenta encontrar a pasta de imagens correspondente a um slug do DB."""
    if not slug_db:
        return None

    slug_norm = normalizar_slug(slug_db)

    # Match exato
    if slug_db in inventario:
        return slug_db
    if slug_norm in {normalizar_slug(k) for k in inventario}:
        for k in inventario:
            if normalizar_slug(k) == slug_norm:
                return k

    # Match parcial (slug contido na pasta ou vice-versa)
    for pasta in inventario:
        pasta_norm = normalizar_slug(pasta)
        if slug_norm in pasta_norm or pasta_norm in slug_norm:
            return pasta

    return None


# ============================================================
# FASE 2: POPULAR DB DE DISCO
# ============================================================

def popular_db_de_disco(empresa, download_key, inventario):
    """Popula campos de imagem no DB a partir dos arquivos em disco."""
    conn = get_connection()
    registros = conn.execute(
        "SELECT nome, slug FROM empreendimentos WHERE empresa = ?",
        (empresa,)
    ).fetchall()
    conn.close()

    # Garantir colunas de imagem
    garantir_coluna("imagem_url", "TEXT")
    for cat in CATEGORIAS:
        garantir_coluna(f"imagens_{cat}", "TEXT")

    atualizados = 0

    for row in registros:
        nome, slug_db = row["nome"], row["slug"]
        pasta_match = encontrar_pasta_slug(slug_db, inventario)

        if not pasta_match:
            continue

        dados = inventario[pasta_match]
        dados_update = {}

        for cat, paths in dados["categorias"].items():
            col = f"imagens_{cat}" if cat in CATEGORIAS else "imagens_geral"
            # Armazenar caminhos relativos ao projeto
            rel_paths = []
            for p in paths[:20]:  # max 20 por categoria
                rel = os.path.relpath(p, os.path.dirname(DOWNLOADS_DIR))
                rel_paths.append(rel.replace("\\", "/"))
            if rel_paths:
                dados_update[col] = " | ".join(rel_paths)

        # imagem_url = primeira imagem (prioridade: plantas > fachada > etc.)
        for prioridade in CATEGORIAS:
            if prioridade in dados["categorias"]:
                first_path = dados["categorias"][prioridade][0]
                dados_update["imagem_url"] = os.path.relpath(first_path, os.path.dirname(DOWNLOADS_DIR)).replace("\\", "/")
                break

        if dados_update:
            atualizar_empreendimento(empresa, nome, dados_update)
            atualizados += 1

    logger.info(f"  DB populado de disco: {atualizados} registros")
    return atualizados


# ============================================================
# FASE 3: DOWNLOAD DE IMAGENS FALTANTES
# ============================================================

def categorizar_imagem(src, alt="", context=""):
    """Categoriza imagem pelo URL, alt text e contexto HTML (section/heading proxima)."""
    texto = (alt + " " + src + " " + context).lower()

    if any(t in texto for t in ["planta", "implanta", "floor", "layout", "tipologia"]):
        return "plantas"
    elif any(t in texto for t in ["fachada", "portaria", "exterior", "perspectiva",
                                    "external", "frente", "entrada"]):
        return "fachada"
    elif any(t in texto for t in ["decorado", "cozinha", "living", "dormitorio",
                                    "dormitório", "banho", "quarto", "sala", "interior",
                                    "suíte", "suite", "lavabo", "varanda", "terraço",
                                    "terraco", "sacada"]):
        return "decorado"
    elif any(t in texto for t in ["obra", "constru", "andamento", "evoluc", "acompanhe"]):
        return "obra"
    elif any(t in texto for t in ["churrasqueira", "fitness", "piscina", "playground",
                                    "brinquedoteca", "salao", "salão", "pet", "coworking",
                                    "bicicletario", "bicicletário", "quadra", "lazer",
                                    "area comum", "área comum", "leisure", "academia",
                                    "sauna", "rooftop", "gourmet", "cinema", "game",
                                    "lounge", "espaco", "espaço", "garden"]):
        return "areas_comuns"

    return "geral"


def _extrair_contexto_html(elem):
    """Extrai contexto do elemento HTML: section id/class, heading mais proximo, etc."""
    contexto_parts = []
    # Subir nos pais procurando section/div com id ou class relevante
    for parent in elem.parents:
        if parent.name in ("section", "div"):
            pid = parent.get("id", "")
            pclass = " ".join(parent.get("class", []))
            if pid:
                contexto_parts.append(pid)
            if pclass:
                contexto_parts.append(pclass)
            if pid or pclass:
                break
    # Heading mais proximo anterior
    prev_heading = elem.find_previous(["h1", "h2", "h3", "h4"])
    if prev_heading:
        contexto_parts.append(prev_heading.get_text(strip=True))
    return " ".join(contexto_parts)[:200]


def _gerar_descricao_arquivo(alt_text, categoria):
    """Gera descricao curta para o nome do arquivo a partir do alt-text."""
    if not alt_text or len(alt_text.strip()) < 3:
        return ""
    # Limpar e normalizar
    desc = alt_text.strip().lower()
    desc = re.sub(r'[^\w\s-]', '', desc)
    desc = re.sub(r'\s+', '_', desc)
    # Remover palavras genéricas
    for word in ["imagem", "foto", "image", "photo", "de", "do", "da", "dos", "das",
                 "para", "com", "em", "no", "na", "um", "uma"]:
        desc = re.sub(rf'\b{word}\b_?', '', desc)
    desc = re.sub(r'_+', '_', desc).strip('_')
    # Limitar
    if len(desc) > 40:
        desc = desc[:40].rsplit('_', 1)[0]
    return desc


def extrair_urls_imagens(soup, url_fonte):
    """Extrai URLs de imagens de uma pagina HTML."""
    imgs = []
    seen = set()

    base_domain = re.match(r"https?://[^/]+", url_fonte)
    base_url = base_domain.group() if base_domain else ""

    # img[src], img[data-src]
    for img in soup.find_all("img"):
        for attr in ["src", "data-src", "data-lazy-src"]:
            src = img.get(attr, "")
            if not src:
                continue
            alt = img.get("alt", "")
            context = _extrair_contexto_html(img)
            src = _normalizar_url(src, base_url)
            if src and src not in seen and _url_valida(src):
                seen.add(src)
                imgs.append({"url": src, "alt": alt, "context": context,
                             "categoria": categorizar_imagem(src, alt, context)})

    # div[data-bg], div[data-image], etc.
    for attr in ["data-bg", "data-image", "data-src"]:
        for elem in soup.find_all(True, attrs={attr: True}):
            src = elem[attr]
            alt = elem.get("alt", "")
            context = _extrair_contexto_html(elem)
            src = _normalizar_url(src, base_url)
            if src and src not in seen and _url_valida(src):
                seen.add(src)
                imgs.append({"url": src, "alt": alt, "context": context,
                             "categoria": categorizar_imagem(src, alt, context)})

    # <source srcset="..."> (lazy loading via <picture>)
    for source in soup.find_all("source", srcset=True):
        srcset = source["srcset"]
        media = source.get("media", "")
        if "max-width" in media:
            continue  # Pula versoes mobile

        # srcset pode ter multiplas URLs com tamanhos: "url1, url2 2x"
        for part in srcset.split(","):
            src = part.strip().split(" ")[0].strip()
            if not src:
                continue
            # Pular retina (@2x) e webp (preferir jpg se disponivel)
            if "@2x" in src or "@3x" in src:
                continue
            if "-webp.webp" in src.lower():
                continue
            alt_elem = source.find_parent("picture")
            alt = ""
            if alt_elem:
                img_tag = alt_elem.find("img")
                if img_tag:
                    alt = img_tag.get("alt", "")
            context = _extrair_contexto_html(source)
            src = _normalizar_url(src, base_url)
            if src and src not in seen and _url_valida(src):
                seen.add(src)
                imgs.append({"url": src, "alt": alt, "context": context,
                             "categoria": categorizar_imagem(src, alt, context)})

    # background-image em style inline
    for elem in soup.find_all(True, style=True):
        style = elem["style"]
        m = re.search(r"background-image:\s*url\(['\"]?([^)\"']+)", style)
        if m:
            src = _normalizar_url(m.group(1), base_url)
            if src and src not in seen and _url_valida(src):
                seen.add(src)
                context = _extrair_contexto_html(elem)
                imgs.append({"url": src, "alt": "", "context": context,
                             "categoria": categorizar_imagem(src, "", context)})

    return imgs


def _normalizar_url(src, base_url):
    """Normaliza URL de imagem."""
    if not src or not src.strip():
        return None
    src = src.strip()
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = base_url + src
    elif not src.startswith("http"):
        return None
    return src


def _url_valida(src):
    """Verifica se URL e de imagem valida (nao logo/icon/etc)."""
    src_lower = src.lower()

    # CDNs de imagem com formato dinâmico (Cloudinary, imgix, etc.)
    is_cdn_image = any(cdn in src_lower for cdn in [
        "cloudinary.com/", "imgix.net/", "imagekit.io/",
    ])

    # Deve ser formato de imagem OU CDN de imagem
    if not is_cdn_image:
        if not any(ext in src_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            return False

    # Nao deve conter termos de skip
    for term in SKIP_TERMS:
        if term in src_lower:
            return False

    # Para CDNs, filtrar imagens muito pequenas (thumbnails, logos)
    if is_cdn_image:
        # Cloudinary: c_limit,w_120 = muito pequeno
        m = re.search(r'w_(\d+)', src_lower)
        if m and int(m.group(1)) < 200:
            return False

    return True


def filtrar_e_priorizar(imagens, max_total=10):
    """Prioriza imagens: plantas primeiro, limite total."""
    por_categoria = {}
    for img in imagens:
        cat = img["categoria"]
        por_categoria.setdefault(cat, []).append(img)

    selecionadas = []
    restante = max_total

    # Primeiro passo: 1 de cada categoria na ordem de prioridade
    for cat in CATEGORIAS:
        if cat in por_categoria and restante > 0:
            selecionadas.append(por_categoria[cat][0])
            restante -= 1

    # Segundo passo: preencher com mais imagens na ordem de prioridade
    for cat in CATEGORIAS:
        if restante <= 0:
            break
        for img in por_categoria.get(cat, [])[1:]:
            if restante <= 0:
                break
            if img not in selecionadas:
                selecionadas.append(img)
                restante -= 1

    return selecionadas


def _detectar_extensao(url, content_type=""):
    """Detecta extensão da imagem pela URL ou Content-Type."""
    url_lower = url.lower()
    for ext in [".png", ".webp", ".jpeg", ".jpg"]:
        if ext in url_lower:
            return ext
    # CDN com formato dinâmico — usar content-type
    if content_type:
        ct = content_type.lower()
        if "png" in ct:
            return ".png"
        if "webp" in ct:
            return ".webp"
        if "jpeg" in ct or "jpg" in ct:
            return ".jpg"
    return ".jpg"


def baixar_imagem(url, caminho_destino, session):
    """Baixa uma imagem e salva em disco."""
    try:
        resp = session.get(url, headers=IMG_HEADERS, timeout=20, stream=True)
        if resp.status_code == 200:
            content = resp.content
            if len(content) > 5000:  # min 5KB
                os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)
                with open(caminho_destino, "wb") as f:
                    f.write(content)
                return True
    except Exception as e:
        logger.debug(f"  Erro download img: {e}")
    return False


def fetch_html(url, session=None, driver=None, tipo_acesso="requests"):
    """Fetch HTML por requests ou selenium."""
    if tipo_acesso == "selenium" and driver:
        try:
            driver.get(url)
            time.sleep(4)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            return driver.page_source
        except Exception as e:
            logger.warning(f"  Selenium erro: {e}")
            return None
    elif session:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"  Requests erro: {e}")
    return None


def baixar_imagens_empreendimento(nome, slug, url_fonte, download_key, max_imgs,
                                   session, driver, tipo_acesso, progresso):
    """Baixa imagens de um empreendimento individual."""
    html = fetch_html(url_fonte, session, driver, tipo_acesso)
    if not html:
        return 0

    soup = BeautifulSoup(html, "html.parser")
    imagens = extrair_urls_imagens(soup, url_fonte)

    if not imagens:
        logger.info(f"    Nenhuma imagem encontrada")
        return 0

    selecionadas = filtrar_e_priorizar(imagens, max_imgs)

    slug_pasta = normalizar_slug(slug) if slug else normalizar_slug(nome)
    pasta_base = os.path.join(DOWNLOADS_DIR, download_key, "imagens", slug_pasta)

    baixadas = 0
    dados_db = {}

    for img in selecionadas:
        cat = img["categoria"]
        pasta_cat = os.path.join(pasta_base, cat)

        # Determinar extensao (suporta CDNs com formato dinâmico)
        ext = _detectar_extensao(img["url"])

        # Nome descritivo: slug_categoria_descricao_hash.ext
        url_hash = hashlib.md5(img["url"].encode()).hexdigest()[:8]
        desc = _gerar_descricao_arquivo(img.get("alt", ""), cat)
        nome_arquivo = f"{slug_pasta}_{cat}_{desc}_{url_hash}{ext}" if desc else f"{slug_pasta}_{cat}_{url_hash}{ext}"
        # Limitar tamanho do nome
        if len(nome_arquivo) > 120:
            nome_arquivo = f"{slug_pasta}_{cat}_{url_hash}{ext}"
        caminho = os.path.join(pasta_cat, nome_arquivo)

        if os.path.exists(caminho):
            baixadas += 1  # ja existe
            continue

        if baixar_imagem(img["url"], caminho, session or requests):
            baixadas += 1
            col = f"imagens_{cat}"
            rel_path = os.path.relpath(caminho, os.path.dirname(DOWNLOADS_DIR)).replace("\\", "/")
            dados_db.setdefault(col, []).append(rel_path)

    # Atualizar DB
    if dados_db:
        update = {}
        for col, paths in dados_db.items():
            garantir_coluna(col, "TEXT")
            update[col] = " | ".join(paths)

        # imagem_url = primeira imagem de plantas ou fachada
        for prioridade in CATEGORIAS:
            key = f"imagens_{prioridade}"
            if key in dados_db and dados_db[key]:
                update["imagem_url"] = dados_db[key][0]
                break

        atualizar_empreendimento(
            next(e for e, c in EMPRESA_CONFIG.items() if c["download_key"] == download_key),
            nome, update
        )

    return baixadas


# ============================================================
# PROCESSAMENTO POR EMPRESA
# ============================================================

def processar_empresa(empresa, config, atualizar, limite, max_imgs,
                       apenas_inventario, sem_download, progresso):
    """Processa todas as 3 fases para uma empresa."""
    download_key = config["download_key"]
    tipo_acesso = config["tipo_acesso"]

    # Fase 1: Inventario
    logger.info(f"  [Fase 1] Inventario de disco...")
    inventario = inventariar_imagens_disco(download_key)
    total_disco = sum(v["total"] for v in inventario.values())
    logger.info(f"  Encontradas: {len(inventario)} pastas, {total_disco} imagens")

    # Fase 2: Popular DB
    logger.info(f"  [Fase 2] Populando DB de disco...")
    atualizados_disco = popular_db_de_disco(empresa, download_key, inventario)

    if apenas_inventario or sem_download:
        return {"disco": atualizados_disco, "baixados": 0, "erros": 0, "sem_imgs": 0}

    # Fase 3: Download de faltantes
    logger.info(f"  [Fase 3] Baixando imagens faltantes...")

    # MRV usa API GraphQL dedicada
    if tipo_acesso == "api_mrv":
        session = requests.Session()
        session.headers.update(HEADERS)
        n = baixar_imagens_mrv_api(session, max_imgs, progresso)
        salvar_progresso(progresso)
        return {"disco": atualizados_disco, "baixados": n, "erros": 0, "sem_imgs": 0}

    conn = get_connection()
    registros = conn.execute(
        "SELECT nome, slug, url_fonte, imagem_url FROM empreendimentos WHERE empresa = ?",
        (empresa,)
    ).fetchall()
    conn.close()

    # Filtrar para quem nao tem imagens (nem em disco nem no DB)
    registros_sem = []
    for row in registros:
        nome, slug, url_fonte = row["nome"], row["slug"], row["url_fonte"]
        imagem_url_db = row["imagem_url"]
        if not slug or not url_fonte:
            continue

        # Ja tem imagem no DB? Pula (a menos que --atualizar)
        if imagem_url_db and not atualizar:
            continue

        pasta = encontrar_pasta_slug(slug, inventario)
        if not pasta or atualizar:
            registros_sem.append((nome, slug, url_fonte))

    if limite > 0:
        registros_sem = registros_sem[:limite]

    logger.info(f"  Sem imagens em disco: {len(registros_sem)}")

    if not registros_sem:
        return {"disco": atualizados_disco, "baixados": 0, "erros": 0, "sem_imgs": 0}

    # Setup acesso
    session = requests.Session()
    session.headers.update(HEADERS)
    driver = None

    if tipo_acesso == "selenium":
        logger.info(f"  Iniciando Chrome...")
        driver = criar_driver()

    baixados_total = 0
    erros = 0
    sem_imgs = 0

    try:
        for i, (nome, slug, url_fonte) in enumerate(registros_sem, 1):
            chave = f"{empresa}:{slug}"
            if chave in progresso.get("processados", []) and not atualizar:
                continue

            logger.info(f"  [{i}/{len(registros_sem)}] {nome}")

            try:
                if tipo_acesso == "api_vivaz":
                    n = baixar_imagens_vivaz(
                        nome, slug, url_fonte, download_key, max_imgs,
                        session, progresso
                    )
                else:
                    n = baixar_imagens_empreendimento(
                        nome, slug, url_fonte, download_key, max_imgs,
                        session, driver, tipo_acesso, progresso
                    )
                if n > 0:
                    baixados_total += n
                    logger.info(f"    {n} imagens baixadas")
                else:
                    sem_imgs += 1

                progresso.setdefault("processados", []).append(chave)

            except Exception as e:
                logger.error(f"    Erro: {e}")
                erros += 1
                progresso.setdefault("erros", []).append(chave)

            salvar_progresso(progresso)
            time.sleep(1 if tipo_acesso == "api_vivaz" else 2)

    finally:
        if driver:
            driver.quit()
            logger.info(f"  Chrome fechado")

    return {"disco": atualizados_disco, "baixados": baixados_total, "erros": erros, "sem_imgs": sem_imgs}


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Download e inventario de imagens de empreendimentos")
    parser.add_argument("--empresa", type=str, default="todas",
                        help="Empresa a processar (nome no banco ou 'todas')")
    parser.add_argument("--limite", type=int, default=0,
                        help="Limite de empreendimentos por empresa para download (0=todos)")
    parser.add_argument("--atualizar", action="store_true",
                        help="Rebaixar imagens mesmo para quem ja tem")
    parser.add_argument("--apenas-inventario", action="store_true",
                        help="Apenas inventariar disco e popular DB (sem baixar)")
    parser.add_argument("--sem-download", action="store_true",
                        help="Nao baixar novas imagens")
    parser.add_argument("--max-por-empreendimento", type=int, default=20,
                        help="Max imagens por empreendimento (default: 20)")
    parser.add_argument("--reset-progresso", action="store_true",
                        help="Resetar progresso de downloads")
    args = parser.parse_args()

    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            logger.info("Progresso resetado.")
        return

    # Garantir colunas de imagem
    garantir_coluna("imagem_url", "TEXT")
    for cat in CATEGORIAS:
        garantir_coluna(f"imagens_{cat}", "TEXT")

    progresso = carregar_progresso()

    logger.info("=" * 60)
    logger.info("DOWNLOAD E INVENTARIO DE IMAGENS")
    logger.info(f"Empresa: {args.empresa} | Limite: {args.limite} | Max/emp: {args.max_por_empreendimento}")
    logger.info("=" * 60)

    if args.empresa.lower() == "todas":
        empresas = list(EMPRESA_CONFIG.items())
    else:
        if args.empresa in EMPRESA_CONFIG:
            empresas = [(args.empresa, EMPRESA_CONFIG[args.empresa])]
        else:
            # Auto-detect: empresa no banco mas nao no config
            key = re.sub(r'[^\w]', '_', args.empresa).lower().strip('_')
            logger.info(f"Empresa nao configurada, usando download_key='{key}' com requests")
            empresas = [(args.empresa, {"tipo_acesso": "requests", "download_key": key})]

    total_disco = 0
    total_baixados = 0
    total_erros = 0

    for empresa_nome, config in empresas:
        logger.info(f"\n--- {empresa_nome} ---")
        resultado = processar_empresa(
            empresa_nome, config, args.atualizar, args.limite,
            args.max_por_empreendimento, args.apenas_inventario,
            args.sem_download, progresso
        )
        total_disco += resultado["disco"]
        total_baixados += resultado["baixados"]
        total_erros += resultado["erros"]

    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL - IMAGENS")
    logger.info(f"  DB populados de disco: {total_disco}")
    logger.info(f"  Novas imagens baixadas: {total_baixados}")
    logger.info(f"  Erros: {total_erros}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

"""
Qualificacao profunda de empreendimentos.
==========================================
Visita cada URL uma vez e executa todos os extratores no mesmo HTML:
  - tipologias_detalhadas (tipologia + metragem)
  - aptos_por_andar
  - modelo_vaga (flags binarios + texto)
  - itens_lazer_raw (lista exaustiva do site)
  - itens_marketizaveis (diferenciais premium)
  - classificacao de imagens em disco (heuristica)

Estrategia:
  - Um script, uma passada por URL
  - So atualiza NULLs (flag --forcar para override)
  - Importa EMPRESA_CONFIG do enriquecer_dados.py

Uso:
    python qualificar_produto.py
    python qualificar_produto.py --empresa MRV
    python qualificar_produto.py --limite 5
    python qualificar_produto.py --forcar
    python qualificar_produto.py --apenas-imagens
    python qualificar_produto.py --sem-imagens
    python qualificar_produto.py --dry-run
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import shutil
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import REQUESTS as REQ_CONFIG, LOGS_DIR, DOWNLOADS_DIR, SELENIUM
from data.database import (
    get_connection,
    garantir_coluna,
    atualizar_empreendimento,
)
from enriquecer_dados import EMPRESA_CONFIG

# ============================================================
# CONFIGURACAO
# ============================================================

HEADERS = {
    "User-Agent": REQ_CONFIG["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

PROGRESSO_FILE = os.path.join(LOGS_DIR, "qualificar_produto_progresso.json")

NOVAS_COLUNAS = [
    ("tipologias_detalhadas", "TEXT"),
    ("aptos_por_andar", "TEXT"),
    ("modelo_vaga", "TEXT"),
    ("itens_lazer_raw", "TEXT"),
    ("itens_marketizaveis", "TEXT"),
    ("vaga_pilotis", "INTEGER DEFAULT 0"),
    ("vaga_subsolo", "INTEGER DEFAULT 0"),
    ("vaga_edificio_garagem", "INTEGER DEFAULT 0"),
    ("vaga_coberta", "INTEGER DEFAULT 0"),
    ("vaga_descoberta", "INTEGER DEFAULT 0"),
    ("vaga_rotativa", "INTEGER DEFAULT 0"),
    ("imagens_classificadas", "INTEGER DEFAULT 0"),
]

# ============================================================
# LOGGING
# ============================================================

os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("qualificar_produto")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "qualificar_produto.log"), encoding="utf-8")
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
        with open(PROGRESSO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processados": [], "erros": []}


def salvar_progresso(progresso):
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(progresso, f, indent=2, ensure_ascii=False)


# ============================================================
# ACESSO A PAGINAS
# ============================================================

def fetch_html_requests(url, session):
    """Faz GET e retorna HTML."""
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"  HTTP {resp.status_code} para {url}")
    except Exception as e:
        logger.warning(f"  Erro requests: {e}")
    return None


def fetch_html_selenium(url, driver):
    """Carrega pagina com Selenium e retorna HTML."""
    try:
        driver.get(url)
        time.sleep(4)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)
        return driver.page_source
    except Exception as e:
        logger.warning(f"  Erro selenium: {e}")
    return None


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
# BUSCA DE REGISTROS
# ============================================================

def buscar_registros(empresa, forcar=False, limite=0):
    """Busca registros com url_fonte para qualificacao."""
    conn = get_connection()
    cursor = conn.cursor()

    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}

    conditions = ["empresa = ?", "url_fonte IS NOT NULL", "url_fonte != ''"]
    params = [empresa]

    if not forcar:
        # Registros que ainda nao tem tipologias detalhadas OU itens_lazer_raw OU itens_marketizaveis
        filtros = []
        for col in ["tipologias_detalhadas", "itens_lazer_raw", "itens_marketizaveis"]:
            if col in colunas_existentes:
                filtros.append(f"({col} IS NULL OR {col} = '')")
            else:
                filtros.append("1=1")
        conditions.append(f"({' OR '.join(filtros)})")

    where = " AND ".join(conditions)

    cols_select = ["id", "nome", "slug", "url_fonte", "empresa"]
    for extra in ["tipologias_detalhadas", "aptos_por_andar", "modelo_vaga",
                   "itens_lazer_raw", "itens_marketizaveis",
                   "preco_a_partir", "renda_minima",
                   "total_torres", "total_andares", "total_unidades",
                   "imagens_classificadas"]:
        if extra in colunas_existentes:
            cols_select.append(extra)

    query = f"SELECT {', '.join(cols_select)} FROM empreendimentos WHERE {where}"
    if limite > 0:
        query += f" LIMIT {limite}"

    rows = cursor.execute(query, params).fetchall()
    conn.close()
    return rows, cols_select


# ============================================================
# EXTRATORES
# ============================================================

def extrair_tipologias_detalhadas(soup, texto):
    """Extrai tipologias com metragem: 'Studio 26m² | 2D 42m² | 2D+Suite 55m²'"""
    tipologias = []

    # 1. Procurar secoes HTML com class/id de tipologia
    secoes = soup.find_all(attrs={"class": re.compile(r"tipolog|planta|ficha|tipo|plant|floor", re.I)})
    secoes += soup.find_all(attrs={"id": re.compile(r"tipolog|planta|ficha|tipo|plant|floor", re.I)})

    secao_texto = ""
    for sec in secoes:
        secao_texto += " " + sec.get_text(separator=" ", strip=True)

    texto_busca = secao_texto if secao_texto.strip() else texto

    # Patterns para tipologia + metragem
    patterns = [
        # "Studio 26m²", "1 Dormitório 32m²", "2 Dorms + Suíte 55m²"
        r'(studio|loft|kitnet|(?:\d)\s*(?:dorm(?:it[oó]rio)?s?|quartos?|suites?)(?:\s*\+?\s*su[ií]te)?)\s*[:\-–|]?\s*(\d{2,4})\s*m[²2]',
        # "2 dormitórios de 55m²"
        r'(\d)\s*(?:dorm(?:it[oó]rio)?s?|quartos?)\s*(?:de|com)?\s*(\d{2,4})\s*m[²2]',
        # "55m² | 2 dormitórios"
        r'(\d{2,4})\s*m[²2]\s*[:\-–|]?\s*(\d)\s*(?:dorm(?:it[oó]rio)?s?|quartos?)',
        # "Planta 2D 42m²"
        r'(?:planta\s+)?(\d[dD]\s*(?:\+\s*su[ií]te)?)\s*[:\-–|]?\s*(\d{2,4})\s*m[²2]',
        # "Studio | 26m²" com quebra de linha
        r'(studio|loft|kitnet)\s*(?:\||\\n|\s)+\s*(\d{2,4})\s*m[²2]',
    ]

    encontrados = set()

    for pat in patterns:
        for m in re.finditer(pat, texto_busca, re.IGNORECASE):
            g1, g2 = m.group(1).strip(), m.group(2).strip()

            # Determinar qual e tipologia e qual e metragem
            if g1.isdigit() and len(g1) >= 2:
                # Pattern "55m² | 2 dorms" — g1 e metragem, g2 e tipo
                metragem = g1
                tipo_raw = g2
            elif g2.isdigit() and len(g2) >= 2 and int(g2) > 15:
                metragem = g2
                tipo_raw = g1
            elif g1.isdigit() and int(g1) <= 6:
                # g1 e numero de dorms, g2 e metragem
                tipo_raw = g1
                metragem = g2
            else:
                tipo_raw = g1
                metragem = g2

            # Normalizar tipo
            tipo_norm = _normalizar_tipo(tipo_raw)
            if tipo_norm and metragem:
                key = f"{tipo_norm} {metragem}m²"
                if key not in encontrados:
                    encontrados.add(key)
                    tipologias.append(key)

    # JSON-LD fallback
    if not tipologias:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    data = data[0] if data else {}
                floor_plans = data.get("floorSize", [])
                if isinstance(floor_plans, dict):
                    floor_plans = [floor_plans]
                for fp in floor_plans:
                    val = fp.get("value", "")
                    if val:
                        tipologias.append(f"{val}m²")
            except (json.JSONDecodeError, AttributeError):
                pass

    return " | ".join(tipologias) if tipologias else None


def _normalizar_tipo(raw):
    """Normaliza tipo de tipologia: '2 dormitorios' → '2D', 'studio' → 'Studio'."""
    raw = raw.strip().lower()

    if raw in ("studio", "loft", "kitnet"):
        return raw.capitalize()

    # "2d+suite", "2D + Suíte"
    m = re.match(r'(\d)\s*d\s*(?:\+\s*su[ií]te)?', raw, re.I)
    if m:
        base = f"{m.group(1)}D"
        if "suite" in raw.lower() or "suíte" in raw.lower():
            return f"{base}+Suite"
        return base

    # "2 dorms", "2 dormitórios"
    m = re.match(r'(\d)\s*(?:dorm|quarto|suite)', raw, re.I)
    if m:
        num = m.group(1)
        if "suite" in raw.lower() or "suíte" in raw.lower():
            return f"{num}D+Suite"
        return f"{num}D"

    # Numero puro (do pattern invertido)
    if raw.isdigit() and int(raw) <= 6:
        return f"{raw}D"

    return None


def extrair_aptos_por_andar(texto):
    """Extrai aptos por andar: '4 aptos/andar' ou '6-8 aptos/andar'."""
    patterns = [
        r'(\d+)\s*(?:a\s*)?(\d+)?\s*(?:aptos?|apartamentos?|unidades?)\s*(?:por|/)\s*andar',
        r'(\d+)\s*(?:por|/)\s*andar',
        r'(\d+)\s*(?:aptos?|unidades?)\s*/\s*andar',
    ]

    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            n1 = m.group(1)
            n2 = m.group(2) if m.lastindex >= 2 and m.group(2) else None
            if n2:
                return f"{n1}-{n2} aptos/andar"
            return f"{n1} aptos/andar"

    return None


VAGA_KEYWORDS = {
    "pilotis": ["pilotis", "piloti"],
    "subsolo": ["subsolo", "sub-solo", "subterrâne", "subterrane"],
    "edificio_garagem": ["edifício garagem", "edificio garagem", "prédio garagem", "predio garagem"],
    "coberta": ["vaga coberta", "garagem coberta", "cobertas"],
    "descoberta": ["vaga descoberta", "descobertas"],
    "rotativa": ["vaga rotativa", "uso comum", "rotativa"],
}


def extrair_modelo_vaga(texto):
    """Extrai modelo de vaga: dict com flags binarios + texto resumo."""
    texto_lower = texto.lower()
    flags = {}
    partes = []

    for tipo, keywords in VAGA_KEYWORDS.items():
        encontrado = any(kw in texto_lower for kw in keywords)
        flags[f"vaga_{tipo}"] = 1 if encontrado else 0
        if encontrado:
            partes.append(tipo.replace("_", " "))

    resumo = " | ".join(partes) if partes else None
    return flags, resumo


def extrair_preco_renda(soup, texto):
    """Re-tenta preco e renda minima com patterns expandidos."""
    dados = {}

    # Preco
    patterns_preco = [
        r'(?:a\s*partir\s*(?:de)?|desde|pre[cç]o)\s*(?:de\s*)?R\$\s*([\d.,]+)',
        r'R\$\s*([\d]{2,3}(?:[.,]\d{3})+(?:[.,]\d{2})?)',
        r'(?:valor|investimento)\s*(?:de\s*)?R\$\s*([\d.,]+)',
    ]
    for pat in patterns_preco:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = m.group(1).replace(".", "").replace(",", ".")
            try:
                preco = float(val)
                if 50000 < preco < 10000000:
                    dados["preco_a_partir"] = preco
                    break
            except ValueError:
                pass

    # Renda minima
    patterns_renda = [
        r'renda\s*(?:m[ií]nima|a\s*partir|familiar)?\s*(?:de\s*)?R\$\s*([\d.,]+)',
        r'renda\s*(?:de\s*)?R\$\s*([\d.,]+)',
    ]
    for pat in patterns_renda:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = m.group(1).replace(".", "").replace(",", ".")
            try:
                renda = float(val)
                if 1000 < renda < 100000:
                    dados["renda_minima"] = renda
                    break
            except ValueError:
                pass

    return dados


def extrair_torres_andares_unidades(soup, texto):
    """Re-tenta torres, andares e unidades com patterns melhorados."""
    dados = {}

    # Torres
    patterns_torres = [
        r'(\d+)\s*(?:torres?|blocos?|edif[ií]cios?)',
        r'(?:torres?|blocos?|edif[ií]cios?)\s*[:=]?\s*(\d+)',
    ]
    for pat in patterns_torres:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 50:
                dados["total_torres"] = val
                break

    # Andares
    patterns_andares = [
        r'(\d+)\s*(?:andares|pavimentos|pisos)',
        r'(?:andares|pavimentos)\s*[:=]?\s*(\d+)',
    ]
    for pat in patterns_andares:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 2 <= val <= 80:
                dados["total_andares"] = val
                break

    # Unidades
    patterns_unidades = [
        r'(\d+)\s*(?:unidades|apartamentos|aptos)',
        r'(?:unidades|apartamentos)\s*[:=]?\s*(\d+)',
    ]
    for pat in patterns_unidades:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 5 <= val <= 10000:
                dados["total_unidades"] = val
                break

    return dados


# ============================================================
# LAZER RAW — extracao exaustiva
# ============================================================

LAZER_KEYWORDS = [
    "piscina", "churrasqueira", "fitness", "academia", "salão de festas",
    "salao de festas", "playground", "brinquedoteca", "pet place", "pet care",
    "pet play", "coworking", "bicicletário", "bicicletario", "quadra",
    "sauna", "spa", "ofurô", "ofuro", "deck", "solarium", "rooftop",
    "espaço gourmet", "espaco gourmet", "lounge", "cinema", "lan house",
    "game", "jogos", "lavanderia", "mini market", "market", "beauty",
    "espaço beleza", "salão beleza", "pilates", "yoga", "crossfit",
    "funcional", "pista de corrida", "pista corrida", "praca",
    "praça", "jardim", "horta", "fire pit", "firepit", "fogueira",
    "hammam", "hidro", "estar", "repouso", "descanso", "massagem",
    "sports bar", "bar", "adega", "wine", "beer garden",
    "cowork", "home office", "estudo", "biblioteca",
    "delivery space", "espaço delivery", "espaco delivery",
    "car wash", "lava jato", "oficina bike",
    "espaço zen", "espaco zen", "meditação", "meditacao",
    "pomar", "bosque", "lago", "espelho d'água", "espelho dagua",
    "parquinho", "baby", "kids", "teen", "infantil", "criança",
    "espaço mulher", "espaco mulher", "vestiário", "vestiario",
    "sala de ginástica", "sala ginastica", "musculação", "musculacao",
    "quadra poliesportiva", "quadra esportiva", "mini quadra",
    "squash", "beach tennis", "tênis", "tenis",
    "piscina coberta", "piscina aquecida", "piscina infantil", "piscina adulto",
    "redário", "redario", "gazebo", "pergolado", "mirante",
    "dog run", "dog place", "agility",
]


def extrair_itens_lazer_raw(soup, texto):
    """Extracao exaustiva de itens de lazer do site."""
    itens = []
    seen = set()

    def _add(item):
        item = item.strip().strip("•–-·").strip()
        item = re.sub(r'\s+', ' ', item)
        if len(item) < 3 or len(item) > 80:
            return
        # Filtrar lixo: telefones, emails, URLs, numeros puros
        if re.match(r'^[\d\s\(\)\-\+\.]+$', item):
            return
        if re.search(r'\(\d{2}\)\s*\d{4}', item):
            return
        if '@' in item or 'http' in item.lower():
            return
        key = item.lower()
        if key not in seen:
            seen.add(key)
            itens.append(item)

    # 1. Secoes HTML dedicadas a lazer/amenidades
    secoes = soup.find_all(attrs={"class": re.compile(r"lazer|amenid|infraestrutura|diferenc|leisure|amenities|facilities|common", re.I)})
    secoes += soup.find_all(attrs={"id": re.compile(r"lazer|amenid|infraestrutura|diferenc|leisure|amenities", re.I)})

    for sec in secoes:
        # Items <li>
        for li in sec.find_all("li"):
            txt = li.get_text(strip=True)
            if txt:
                _add(txt)

        # <h3>, <h4>, <span> com texto curto (provavelmente labels)
        for tag in sec.find_all(["h3", "h4", "h5", "span", "p", "strong"]):
            txt = tag.get_text(strip=True)
            if txt and len(txt) < 60:
                _add(txt)

    # 2. Alt-text de imagens com keywords de lazer
    for img in soup.find_all("img"):
        alt = (img.get("alt", "") or "").strip()
        if alt:
            alt_lower = alt.lower()
            if any(kw in alt_lower for kw in LAZER_KEYWORDS[:30]):
                _add(alt)

    # 3. Labels de icones: [class*=icon] + span/text
    for icon_el in soup.find_all(attrs={"class": re.compile(r"icon|ico|svg", re.I)}):
        sib = icon_el.find_next_sibling()
        if sib:
            txt = sib.get_text(strip=True)
            if txt and len(txt) < 60:
                _add(txt)
        # Texto inline no parent
        parent = icon_el.parent
        if parent:
            for child in parent.children:
                if hasattr(child, 'name') and child.name in ("span", "p", "strong", "label"):
                    txt = child.get_text(strip=True)
                    if txt and len(txt) < 60:
                        _add(txt)

    # 4. Fallback: keywords no texto completo
    if len(itens) < 3:
        texto_lower = texto.lower()
        for kw in LAZER_KEYWORDS:
            if kw in texto_lower:
                _add(kw.title())

    return " | ".join(itens) if itens else None


# ============================================================
# ITENS MARKETIZAVEIS
# ============================================================

MARKETIZAVEIS = [
    ("Cortina blackout", ["blackout", "cortina integrada"]),
    ("Veneziana integrada", ["veneziana integrada"]),
    ("Tomada USB", ["tomada usb", "usb"]),
    ("Ponto grill varanda", ["ponto grill", "preparação churrasqueira", "preparacao churrasqueira", "preparo churrasqueira"]),
    ("Prep ar condicionado", ["preparação ar", "preparacao ar", "infra split", "infra vrf", "ponto de ar condicionado",
                               "ar condicionado", "split já instalado"]),
    ("Aquecimento gás/solar", ["aquecimento a gás", "aquecimento gas", "aquecedor solar", "aquecimento solar"]),
    ("Medição individualizada", ["medição individual", "medicao individual", "hidrômetro individual", "hidrometro individual"]),
    ("Portaria com clausura", ["clausura", "eclusa pedestres", "eclusa de pedestres"]),
    ("Lazer no rooftop", ["lazer rooftop", "rooftop"]),
    ("Contrapiso nivelado", ["contrapiso nivelado", "varanda sala nivelad", "varanda e sala nivelad"]),
    ("Esquadria acústica", ["acústic", "acustic", "vidro acústic", "vidro acustic"]),
    ("Reuso de água", ["reuso água", "reuso agua", "reaproveitamento água", "reaproveitamento agua", "reúso", "reuso de água"]),
    ("Energia solar", ["energia solar", "fotovoltaic", "painel solar", "painéis solares"]),
    ("Smart home", ["smart home", "automação", "automacao", "casa inteligente", "home connect"]),
    ("Varanda gourmet", ["varanda gourmet", "varanda integrada", "terraço gourmet", "terraco gourmet"]),
    ("Carregador VE", ["carregador veículo", "carregador veiculo", "carro elétrico", "carro eletrico",
                        "veículo elétrico", "veiculo eletrico", "eletroposto"]),
    ("Pé-direito diferenciado", ["pé-direito diferenciado", "pe-direito diferenciado",
                                   "pé-direito elevado", "pe-direito elevado",
                                   "pé-direito duplo", "pe-direito duplo",
                                   "pé direito diferenciado", "pé direito elevado"]),
    ("Vidro de piso a teto", ["piso a teto", "floor to ceiling", "piso ao teto"]),
    ("Bicicletário com tomada", ["bicicletário elétric", "bicicletario eletric", "bike elétric"]),
    ("Infraestrutura água quente", ["água quente", "agua quente", "boiler"]),
]


def extrair_itens_marketizaveis(soup, texto):
    """Detecta diferenciais premium por keyword matching."""
    texto_lower = texto.lower()
    encontrados = []

    for nome_feature, keywords in MARKETIZAVEIS:
        for kw in keywords:
            if kw.lower() in texto_lower:
                encontrados.append(nome_feature)
                break

    return " | ".join(encontrados) if encontrados else None


# ============================================================
# CLASSIFICACAO DE IMAGENS (heuristica, offline)
# ============================================================

CATEGORIAS_IMG = {
    "plantas": ["planta", "implantacao", "implantação", "floor", "layout", "tipologia"],
    "implantacao": ["implanta"],
    "fachada": ["fachada", "portaria", "exterior", "perspectiva", "external"],
    "decorado": ["decorado", "interior", "cozinha", "living", "quarto", "dormitorio",
                  "dormitório", "banho", "sala", "suíte", "suite", "lavabo"],
    "obra": ["obra", "construcao", "construção", "andamento"],
    "areas_comuns": ["churrasqueira", "fitness", "piscina", "playground", "brinquedoteca",
                      "salao", "salão", "pet", "coworking", "bicicletario", "bicicletário",
                      "quadra", "lazer", "area comum", "área comum", "leisure",
                      "academia", "sauna", "rooftop", "gourmet", "cinema", "game"],
}


def _classificar_arquivo(filename, alt_text=""):
    """Classifica um arquivo de imagem por nome + alt text."""
    texto = (filename + " " + alt_text).lower()

    # implantacao e subcategoria de plantas, verificar primeiro
    if any(kw in texto for kw in CATEGORIAS_IMG["implantacao"]):
        return "plantas"  # implantacao vai na pasta plantas

    for cat, keywords in CATEGORIAS_IMG.items():
        if cat == "implantacao":
            continue
        if any(kw in texto for kw in keywords):
            return cat

    return "geral"


def _extrair_tipologia_de_alt(alt_text):
    """Tenta extrair tipologia/metragem do alt-text de uma planta."""
    if not alt_text:
        return None
    m = re.search(r'(\d)\s*(?:dorm|quarto|d)\w*\s*(\d{2,3})\s*m', alt_text, re.I)
    if m:
        return f"{m.group(1)}D_{m.group(2)}m2"
    m = re.search(r'(studio|loft)\s*(\d{2,3})\s*m', alt_text, re.I)
    if m:
        return f"{m.group(1)}_{m.group(2)}m2"
    return None


def classificar_imagens_empreendimento(empresa, slug, download_key, soup, dry_run=False):
    """Classifica imagens ja em disco por heuristica de nome/alt-text."""
    imagens_dir = os.path.join(DOWNLOADS_DIR, download_key, "imagens", slug)

    if not os.path.exists(imagens_dir):
        return False

    # Listar arquivos de imagem (flat — nao entrar em subpastas ja organizadas)
    conteudo = os.listdir(imagens_dir)
    tem_subpastas = any(
        os.path.isdir(os.path.join(imagens_dir, item))
        for item in conteudo
    )

    # Se ja tem subpastas, assumir que ja foi organizado
    if tem_subpastas:
        logger.debug(f"    {slug}: ja tem subpastas, pulando reorganizacao")
        return True

    arquivos_img = []
    for f in conteudo:
        fp = os.path.join(imagens_dir, f)
        if os.path.isfile(fp) and f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            if os.path.getsize(fp) > 5000:
                arquivos_img.append(f)

    if not arquivos_img:
        return False

    # Mapear src/filename → alt-text do HTML
    alt_map = {}
    if soup:
        for img_tag in soup.find_all("img"):
            src = img_tag.get("src", "") or img_tag.get("data-src", "") or ""
            alt = img_tag.get("alt", "") or ""
            if src:
                # Extrair nome do arquivo do src
                src_filename = src.split("/")[-1].split("?")[0]
                if src_filename:
                    alt_map[src_filename.lower()] = alt

    # Classificar cada arquivo
    classificacao = {}  # cat → [filenames]
    for f in arquivos_img:
        alt = alt_map.get(f.lower(), "")
        cat = _classificar_arquivo(f, alt)
        classificacao.setdefault(cat, []).append(f)

    if dry_run:
        logger.info(f"    [DRY-RUN] {slug}: {', '.join(f'{c}={len(fs)}' for c, fs in classificacao.items())}")
        return True

    # Reorganizar em subpastas
    for cat, files in classificacao.items():
        cat_dir = os.path.join(imagens_dir, cat)
        os.makedirs(cat_dir, exist_ok=True)
        for f in files:
            src_path = os.path.join(imagens_dir, f)
            dst_path = os.path.join(cat_dir, f)
            if os.path.exists(src_path) and not os.path.exists(dst_path):
                shutil.move(src_path, dst_path)

    # Atualizar colunas imagens_* no banco
    dados_update = {}
    for cat, files in classificacao.items():
        col = f"imagens_{cat}"
        rel_paths = []
        for f in files[:20]:
            rel = os.path.join(download_key, "imagens", slug, cat, f).replace("\\", "/")
            rel_paths.append(rel)
        if rel_paths:
            dados_update[col] = " | ".join(rel_paths)

    dados_update["imagens_classificadas"] = 1

    atualizar_empreendimento(empresa, slug_para_nome(slug), dados_update)

    logger.info(f"    {slug}: classificadas {len(arquivos_img)} imgs em {len(classificacao)} categorias")
    return True


def slug_para_nome(slug):
    """Converte slug de volta para algo buscavel — usa o banco."""
    conn = get_connection()
    cursor = conn.cursor()
    row = cursor.execute("SELECT nome FROM empreendimentos WHERE slug = ? LIMIT 1", (slug,)).fetchone()
    conn.close()
    if row:
        return row["nome"]
    return slug


# ============================================================
# PROCESSAMENTO POR EMPRESA
# ============================================================

def processar_empresa(empresa, config, args, progresso):
    """Processa todos os registros de uma empresa."""
    tipo_acesso = config.get("tipo_acesso", "requests")
    download_key = config.get("download_key", empresa.lower())

    # APIs nao tem HTML para extrair — pular extracao HTML
    if tipo_acesso.startswith("api_") and not args.apenas_imagens:
        logger.info(f"  {empresa}: tipo API ({tipo_acesso}), sem HTML para qualificar")
        if not args.sem_imagens:
            return _processar_apenas_imagens(empresa, download_key, args, progresso)
        return {"atualizados": 0, "erros": 0, "imgs": 0}

    rows, cols = buscar_registros(empresa, args.forcar, args.limite)
    if not rows:
        logger.info(f"  {empresa}: nenhum registro para qualificar")
        return {"atualizados": 0, "erros": 0, "imgs": 0}

    logger.info(f"  {empresa}: {len(rows)} registros para qualificar")

    atualizados = 0
    erros = 0
    imgs_class = 0

    session = None
    driver = None

    try:
        if not args.apenas_imagens:
            if tipo_acesso == "selenium":
                driver = criar_driver()
                logger.info(f"  Chrome iniciado para {empresa}")
            else:
                import requests as req_lib
                session = req_lib.Session()
                session.headers.update(HEADERS)

        for row in rows:
            # Converter Row para dict para usar .get()
            row_dict = dict(row)
            nome = row_dict["nome"]
            slug = row_dict["slug"]
            url = row_dict["url_fonte"]
            chave = f"{empresa}|{nome}"

            if chave in progresso["processados"]:
                continue

            try:
                dados_novos = {}
                soup = None

                # --- Extracao HTML ---
                if not args.apenas_imagens and url:
                    html = None
                    if tipo_acesso == "selenium" and driver:
                        html = fetch_html_selenium(url, driver)
                    elif session:
                        html = fetch_html_requests(url, session)

                    if html:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html, "lxml")
                        texto = soup.get_text(separator=" ", strip=True)

                        # Tipologias detalhadas
                        if args.forcar or not row_dict.get("tipologias_detalhadas"):
                            val = extrair_tipologias_detalhadas(soup, texto)
                            if val:
                                dados_novos["tipologias_detalhadas"] = val

                        # Aptos por andar
                        if args.forcar or not row_dict.get("aptos_por_andar"):
                            val = extrair_aptos_por_andar(texto)
                            if val:
                                dados_novos["aptos_por_andar"] = val

                        # Modelo vaga
                        if args.forcar or not row_dict.get("modelo_vaga"):
                            flags, resumo = extrair_modelo_vaga(texto)
                            if resumo:
                                dados_novos["modelo_vaga"] = resumo
                            dados_novos.update(flags)

                        # Preco / Renda (so NULLs)
                        if args.forcar or not row_dict.get("preco_a_partir"):
                            preco_dados = extrair_preco_renda(soup, texto)
                            if "preco_a_partir" in preco_dados:
                                if args.forcar or not row_dict.get("preco_a_partir"):
                                    dados_novos["preco_a_partir"] = preco_dados["preco_a_partir"]
                            if "renda_minima" in preco_dados:
                                if args.forcar or not row_dict.get("renda_minima"):
                                    dados_novos["renda_minima"] = preco_dados["renda_minima"]

                        # Torres / Andares / Unidades (so NULLs)
                        tau = extrair_torres_andares_unidades(soup, texto)
                        for campo, val in tau.items():
                            if args.forcar or not row_dict.get(campo):
                                dados_novos[campo] = val

                        # Itens lazer raw
                        if args.forcar or not row_dict.get("itens_lazer_raw"):
                            val = extrair_itens_lazer_raw(soup, texto)
                            if val:
                                dados_novos["itens_lazer_raw"] = val

                        # Itens marketizaveis
                        if args.forcar or not row_dict.get("itens_marketizaveis"):
                            val = extrair_itens_marketizaveis(soup, texto)
                            if val:
                                dados_novos["itens_marketizaveis"] = val

                    else:
                        logger.warning(f"    {nome}: sem HTML")

                # --- Salvar dados ---
                if dados_novos and not args.dry_run:
                    atualizar_empreendimento(empresa, nome, dados_novos)
                    atualizados += 1
                    logger.info(f"    {nome}: atualizado ({', '.join(dados_novos.keys())})")
                elif dados_novos and args.dry_run:
                    logger.info(f"    [DRY-RUN] {nome}: {json.dumps(dados_novos, ensure_ascii=False)[:200]}")
                    atualizados += 1
                else:
                    logger.debug(f"    {nome}: sem dados novos")

                # --- Classificar imagens ---
                if not args.sem_imagens:
                    if args.forcar or not row_dict.get("imagens_classificadas"):
                        ok = classificar_imagens_empreendimento(
                            empresa, slug, download_key, soup, dry_run=args.dry_run
                        )
                        if ok:
                            imgs_class += 1

                progresso["processados"].append(chave)
                salvar_progresso(progresso)

                # Delay
                delay = 3 if tipo_acesso == "selenium" else 1.5
                if not args.apenas_imagens:
                    time.sleep(delay)

            except Exception as e:
                logger.error(f"    ERRO {nome}: {e}")
                progresso["erros"].append({"empresa": empresa, "nome": nome, "erro": str(e)})
                salvar_progresso(progresso)
                erros += 1

    finally:
        if driver:
            driver.quit()
            logger.info(f"  Chrome fechado para {empresa}")

    return {"atualizados": atualizados, "erros": erros, "imgs": imgs_class}


def _processar_apenas_imagens(empresa, download_key, args, progresso):
    """Processa apenas classificacao de imagens (para APIs sem HTML)."""
    conn = get_connection()
    cursor = conn.cursor()

    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}

    conditions = ["empresa = ?", "url_fonte IS NOT NULL"]
    params = [empresa]

    if not args.forcar and "imagens_classificadas" in colunas_existentes:
        conditions.append("(imagens_classificadas IS NULL OR imagens_classificadas = 0)")

    where = " AND ".join(conditions)
    query = f"SELECT nome, slug FROM empreendimentos WHERE {where}"
    if args.limite > 0:
        query += f" LIMIT {args.limite}"

    rows = cursor.execute(query, params).fetchall()
    conn.close()

    imgs = 0
    for row in rows:
        nome, slug = row["nome"], row["slug"]
        chave = f"{empresa}|{nome}|imgs"
        if chave in progresso["processados"]:
            continue

        ok = classificar_imagens_empreendimento(
            empresa, slug, download_key, None, dry_run=args.dry_run
        )
        if ok:
            imgs += 1

        progresso["processados"].append(chave)
        salvar_progresso(progresso)

    return {"atualizados": 0, "erros": 0, "imgs": imgs}


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Qualificacao profunda de empreendimentos")
    parser.add_argument("--empresa", type=str, default="todas",
                        help="Empresa a processar (nome no banco ou 'todas')")
    parser.add_argument("--limite", type=int, default=0,
                        help="Limite de registros por empresa (0=todos)")
    parser.add_argument("--forcar", action="store_true",
                        help="Reprocessar registros que ja tem dados")
    parser.add_argument("--apenas-imagens", action="store_true",
                        help="Apenas classificar imagens (pula extracao HTML)")
    parser.add_argument("--sem-imagens", action="store_true",
                        help="Apenas extrair dados (pula classificacao de imagens)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nao salvar no banco (apenas mostrar)")
    parser.add_argument("--reset-progresso", action="store_true",
                        help="Resetar progresso")
    args = parser.parse_args()

    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            logger.info("Progresso resetado.")
        return

    # Garantir novas colunas
    for col_nome, col_tipo in NOVAS_COLUNAS:
        garantir_coluna(col_nome, col_tipo)

    progresso = carregar_progresso()

    logger.info("=" * 60)
    logger.info("QUALIFICACAO PROFUNDA DE EMPREENDIMENTOS")
    logger.info(f"Empresa: {args.empresa} | Limite: {args.limite} | Forcar: {args.forcar}")
    logger.info(f"Apenas imagens: {args.apenas_imagens} | Sem imagens: {args.sem_imagens} | Dry-run: {args.dry_run}")
    logger.info("=" * 60)

    # Ordenar: API primeiro (rapido), requests, selenium
    ordem = {"api_mrv": 0, "api_vivaz": 1, "requests": 2, "selenium": 3}

    if args.empresa.lower() == "todas":
        empresas = sorted(EMPRESA_CONFIG.items(), key=lambda x: ordem.get(x[1]["tipo_acesso"], 9))
    else:
        if args.empresa in EMPRESA_CONFIG:
            empresas = [(args.empresa, EMPRESA_CONFIG[args.empresa])]
        else:
            logger.error(f"Empresa desconhecida: {args.empresa}")
            logger.info(f"Disponiveis: {', '.join(EMPRESA_CONFIG.keys())}")
            return

    total_atualizado = 0
    total_erros = 0
    total_imgs = 0

    for empresa_nome, config in empresas:
        logger.info(f"\n--- {empresa_nome} ---")
        resultado = processar_empresa(empresa_nome, config, args, progresso)
        total_atualizado += resultado["atualizados"]
        total_erros += resultado["erros"]
        total_imgs += resultado["imgs"]

    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL - QUALIFICACAO")
    logger.info(f"  Atualizados: {total_atualizado}")
    logger.info(f"  Imagens classificadas: {total_imgs}")
    logger.info(f"  Erros: {total_erros}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

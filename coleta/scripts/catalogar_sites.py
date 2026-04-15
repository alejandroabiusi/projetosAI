"""
Catalogador de Sites — Fase 1 do requalificador iterativo.

Para cada empresa (todas, inclusive originais):
1. Visita 3 URLs de amostra
2. Analisa a estrutura HTML de cada uma
3. Para cada campo (coords, fase, tipologia, lazer, endereço, metragem, preço, imagem, RI):
   - Detecta ONDE a informação está no HTML (ou se não existe)
   - Registra o seletor/padrão que funciona
4. Salva catálogo em config/catalogo_sites.json

Depois, o script de teste (testar_catalogo.py) usa o catálogo para:
- Testar 5 amostras por empresa × campo
- Marcar cada empresa × campo como OK / Reprocessar / N/A
"""

import sqlite3
import requests
import re
import os
import sys
import io
import json
import time
import logging
from collections import defaultdict
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
CATALOGO_PATH = os.path.join(PROJECT_ROOT, "config", "catalogo_sites.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger()

CAMPOS = ["coords", "fase", "tipologia", "lazer", "endereco", "cidade",
          "metragem", "preco", "imagem", "ri"]


def fetch_page(url, session):
    try:
        r = session.get(url, timeout=12, allow_redirects=True)
        if r.status_code == 200:
            return r.text, r.url
    except:
        pass
    return None, None


def catalogar_coords(html):
    """Detecta onde estão as coordenadas."""
    metodos = []

    # Google Maps iframe
    if re.search(r'<iframe[^>]+google.*maps', html, re.I):
        if re.search(r'@-?\d+\.\d+,-?\d+\.\d+', html):
            metodos.append("maps_iframe_@")
        elif re.search(r'q=-?\d+\.\d+,-?\d+\.\d+', html):
            metodos.append("maps_iframe_q")
        elif re.search(r'll=-?\d+\.\d+,-?\d+\.\d+', html):
            metodos.append("maps_iframe_ll")
        else:
            metodos.append("maps_iframe_outros")

    # JSON lat/lng no JS
    if re.search(r'"lat(?:itude)?":\s*-?\d+\.\d{4}', html):
        metodos.append("json_lat_lng")

    # Meta tags OG
    soup = BeautifulSoup(html[:10000], "html.parser")
    if soup.find("meta", {"property": re.compile(r"latitude|place:location", re.I)}):
        metodos.append("meta_og_place")

    # Schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict) and data.get("geo"):
                metodos.append("schema_org_geo")
                break
        except:
            pass

    return metodos or ["nao_encontrado"]


def catalogar_fase(soup, html, url):
    """Detecta onde está a fase/status."""
    metodos = []

    # Badges/labels com classe
    for tag in soup.find_all(["span", "div", "p", "label", "a", "li"]):
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        if any(kw in classes or kw in tag_id for kw in ["status", "fase", "stage", "badge", "tag-"]):
            t = tag.get_text().strip().lower()
            if len(t) < 50 and any(kw in t for kw in ["lan", "constru", "obra", "pronto", "entregue", "vendido", "breve"]):
                metodos.append(f"badge_class:{classes[:30]}")
                break

    # URL
    url_l = url.lower()
    if any(kw in url_l for kw in ["/pronto", "/obra", "/lancamento", "/vendido", "/entregue", "/construcao"]):
        metodos.append("url_path")

    # Texto com evolução de obra (%)
    texto = soup.get_text(separator=" ").lower()
    if re.search(r"\d+\s*%\s*(?:da\s*obra|conclu|execut)", texto):
        metodos.append("texto_pct_obra")

    # Texto genérico
    if re.search(r"breve\s*lan[çc]amento|100\s*%?\s*vendido|pronto\s*para\s*morar|em\s*constru[çc]", texto):
        metodos.append("texto_generico")

    return metodos or ["nao_encontrado"]


def catalogar_tipologia(soup, html, titulo):
    """Detecta onde estão as tipologias."""
    metodos = []

    # Seção específica de plantas/tipologia
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["planta", "tipolog", "tipo-", "apartamento", "unidade", "ficha", "configur", "dormit"]):
            texto_secao = section.get_text(separator=" ").lower()
            if re.search(r"\d\s*(?:dorm|quarto|su[ií]te)|studio", texto_secao):
                metodos.append(f"secao_tipologia:{check[:40]}")
                break

    # Título da página
    titulo_l = titulo.lower()
    if re.search(r"\d\s*(?:dorm|quarto)|studio", titulo_l):
        metodos.append("titulo_pagina")

    # Texto geral
    texto = soup.get_text(separator=" ").lower()
    if re.search(r"\d\s*(?:dorm|quarto|su[ií]te)|studio", texto):
        if not metodos:  # só se não encontrou em seção específica
            metodos.append("texto_geral")

    return metodos or ["nao_encontrado"]


def catalogar_lazer(soup, html):
    """Detecta onde estão os itens de lazer."""
    metodos = []

    for section in soup.find_all(["section", "div", "ul"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        check = classes + " " + sid
        if any(kw in check for kw in ["lazer", "amenid", "comodid", "infraestr", "area-comum", "leisure", "diferencia"]):
            texto_secao = section.get_text(separator=" ").lower()
            if re.search(r"piscina|churras|fitness|academia|playground", texto_secao):
                metodos.append(f"secao_lazer:{check[:40]}")
                break

    # Lista com itens de lazer
    for ul in soup.find_all("ul"):
        lis = ul.find_all("li")
        if len(lis) >= 3:
            textos = [li.get_text().strip().lower() for li in lis[:10]]
            lazer_count = sum(1 for t in textos if re.search(r"piscina|churras|fitness|playground|sal[ãa]o|pet|quadra", t))
            if lazer_count >= 2:
                classes = " ".join(ul.get("class", [])).lower()
                metodos.append(f"lista_ul:{classes[:30]}")
                break

    # Texto geral (fallback)
    texto = soup.get_text(separator=" ").lower()
    if re.search(r"piscina|churrasqueir|fitness|academia", texto):
        if not metodos:
            metodos.append("texto_geral")

    return metodos or ["nao_encontrado"]


def catalogar_endereco(soup, html):
    """Detecta onde está o endereço."""
    metodos = []

    # Schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict) and data.get("address"):
                metodos.append("schema_org_address")
                break
        except:
            pass

    # Meta OG
    if soup.find("meta", {"property": re.compile(r"street-address|locality", re.I)}):
        metodos.append("meta_og")

    # Texto estruturado
    texto = soup.get_text(separator=" ")
    if re.search(r"(?:endere[çc]o|localiza[çc][ãa]o)\s*:", texto, re.I):
        metodos.append("texto_rotulado")
    elif re.search(r"(?:Rua|Av\.|Avenida|R\.|Al\.)\s+\w", texto):
        metodos.append("texto_rua")

    return metodos or ["nao_encontrado"]


def catalogar_cidade(soup, html, url):
    """Detecta onde está a cidade."""
    metodos = []

    # Meta OG
    if soup.find("meta", {"property": re.compile(r"locality", re.I)}):
        metodos.append("meta_og_locality")

    # Schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if isinstance(data, dict):
                addr = data.get("address", {})
                if isinstance(addr, dict) and addr.get("addressLocality"):
                    metodos.append("schema_org_locality")
                    break
        except:
            pass

    # URL
    url_parts = url.lower().split("/")
    for common in ["sao-paulo", "rio-de-janeiro", "curitiba", "belo-horizonte", "salvador",
                    "recife", "fortaleza", "goiania", "campinas", "sorocaba"]:
        if common in url:
            metodos.append("url_path_cidade")
            break

    # Texto
    texto = soup.get_text(separator=" ")
    if re.search(r"(?:cidade|localiza[çc][ãa]o)\s*:\s*\w", texto, re.I):
        metodos.append("texto_rotulado")

    return metodos or ["nao_encontrado"]


def catalogar_metragem(soup, texto):
    """Detecta onde está a metragem."""
    metodos = []

    # Seção de plantas
    for section in soup.find_all(["section", "div"]):
        classes = " ".join(section.get("class", [])).lower()
        sid = (section.get("id") or "").lower()
        if any(kw in classes + " " + sid for kw in ["planta", "tipolog", "ficha"]):
            t = section.get_text(separator=" ")
            if re.search(r"\d+(?:[.,]\d+)?\s*m[²2]", t):
                metodos.append(f"secao_planta:{(classes + ' ' + sid)[:30]}")
                break

    # Texto geral
    if re.search(r"\d+(?:[.,]\d+)?\s*(?:a\s*\d+(?:[.,]\d+)?\s*)?m[²2]", texto):
        if not metodos:
            metodos.append("texto_geral")

    return metodos or ["nao_encontrado"]


def catalogar_preco(texto):
    """Detecta onde está o preço."""
    if re.search(r"(?:a\s*partir\s*de|desde)\s*R\$\s*[\d.,]+", texto, re.I):
        return ["texto_a_partir"]
    if re.search(r"R\$\s*[\d.,]+(?:\s*mil)?", texto):
        return ["texto_rs"]
    return ["nao_encontrado"]


def catalogar_imagem(soup):
    """Detecta onde está a imagem principal."""
    metodos = []
    if soup.find("meta", {"property": "og:image"}):
        metodos.append("meta_og_image")
    for img in soup.find_all("img", limit=5):
        src = (img.get("src") or img.get("data-src") or "").lower()
        if any(kw in src for kw in ["fachada", "hero", "banner", "principal"]):
            metodos.append("img_fachada")
            break
    if not metodos:
        if soup.find("img"):
            metodos.append("img_generica")
    return metodos or ["nao_encontrado"]


def catalogar_ri(texto):
    """Detecta se tem Registro de Incorporação."""
    if re.search(r"incorpora[çc][ãa]o\s+registrada|registro\s+de\s+incorpora[çc]|memorial\s+de\s+incorpora[çc]|R\.?\s*I\.?\s*(?:n[ºo°]|registrad|sob)", texto, re.I):
        return ["texto_ri"]
    if re.search(r"matr[ií]cula\s*(?:n[ºo°])?\s*\d", texto, re.I):
        return ["texto_matricula"]
    return ["nao_encontrado"]


def catalogar_empresa(empresa, urls, session):
    """Cataloga a estrutura HTML de uma empresa a partir de 3 URLs."""
    catalogo = {
        "empresa": empresa,
        "urls_analisadas": [],
        "acessivel": True,
        "renderiza_js": False,
        "campos": {},
    }

    resultados_por_campo = defaultdict(list)
    paginas_acessiveis = 0

    for url in urls:
        html, final_url = fetch_page(url, session)
        if not html:
            catalogo["urls_analisadas"].append({"url": url, "status": "inacessivel"})
            continue

        paginas_acessiveis += 1
        soup_full = BeautifulSoup(html, "html.parser")
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["nav", "footer", "header", "aside", "menu"]):
            tag.decompose()

        titulo = get_title_safe(html)
        texto = soup.get_text(separator=" ")

        # Detectar se é JS-only
        body_text = soup.find("body")
        body_len = len(body_text.get_text(strip=True)) if body_text else 0
        if body_len < 100:
            catalogo["renderiza_js"] = True

        # Catalogar cada campo
        resultados_por_campo["coords"].append(catalogar_coords(html))
        resultados_por_campo["fase"].append(catalogar_fase(soup, html, final_url or url))
        resultados_por_campo["tipologia"].append(catalogar_tipologia(soup, html, titulo))
        resultados_por_campo["lazer"].append(catalogar_lazer(soup, html))
        resultados_por_campo["endereco"].append(catalogar_endereco(soup_full, html))
        resultados_por_campo["cidade"].append(catalogar_cidade(soup_full, html, final_url or url))
        resultados_por_campo["metragem"].append(catalogar_metragem(soup, texto))
        resultados_por_campo["preco"].append(catalogar_preco(texto))
        resultados_por_campo["imagem"].append(catalogar_imagem(soup_full))
        resultados_por_campo["ri"].append(catalogar_ri(texto))

        catalogo["urls_analisadas"].append({"url": url, "status": "ok"})
        time.sleep(1.5)

    if paginas_acessiveis == 0:
        catalogo["acessivel"] = False

    # Consolidar: para cada campo, qual método apareceu mais
    for campo in CAMPOS:
        todos_metodos = []
        for resultado in resultados_por_campo.get(campo, []):
            todos_metodos.extend(resultado)

        # Contar frequência
        freq = defaultdict(int)
        for m in todos_metodos:
            freq[m] += 1

        # Método mais frequente
        if freq:
            melhor = max(freq.items(), key=lambda x: x[1])
            disponivel = melhor[0] != "nao_encontrado"
            catalogo["campos"][campo] = {
                "disponivel": disponivel,
                "metodo_principal": melhor[0],
                "frequencia": melhor[1],
                "total_paginas": paginas_acessiveis,
                "todos_metodos": dict(freq),
            }
        else:
            catalogo["campos"][campo] = {
                "disponivel": False,
                "metodo_principal": "nao_encontrado",
                "frequencia": 0,
                "total_paginas": 0,
                "todos_metodos": {},
            }

    return catalogo


def get_title_safe(html):
    try:
        soup = BeautifulSoup(html[:5000], "html.parser")
        t = soup.find("title")
        return t.get_text().strip() if t else ""
    except:
        return ""


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", type=str, default=None)
    parser.add_argument("--limite", type=int, default=999)
    parser.add_argument("--min-produtos", type=int, default=3)
    parser.add_argument("--delay", type=float, default=1.5)
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    session = requests.Session()
    session.headers.update(HEADERS)

    # Carregar catálogo existente (para continuar de onde parou)
    catalogo_existente = {}
    if os.path.exists(CATALOGO_PATH):
        with open(CATALOGO_PATH, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                catalogo_existente = {e["empresa"]: e for e in data}
            except:
                pass

    # Listar empresas
    if args.empresa:
        empresas_lista = [args.empresa]
    else:
        cur.execute("""
            SELECT empresa, COUNT(*) as total FROM empreendimentos
            WHERE url_fonte IS NOT NULL AND url_fonte != ''
            GROUP BY empresa HAVING total >= ?
            ORDER BY total DESC LIMIT ?
        """, (args.min_produtos, args.limite))
        empresas_lista = [r[0] for r in cur.fetchall()]

    log.info(f"=== CATALOGAÇÃO DE SITES ===")
    log.info(f"Empresas: {len(empresas_lista)} | Delay: {args.delay}s")

    catalogos = list(catalogo_existente.values()) if catalogo_existente else []
    empresas_ja_catalogadas = set(catalogo_existente.keys())

    for i, empresa in enumerate(empresas_lista, 1):
        if empresa in empresas_ja_catalogadas:
            log.info(f"  [{i}/{len(empresas_lista)}] {empresa}: já catalogada, pulando")
            continue

        # Pegar 3 URLs aleatórias
        cur.execute("""
            SELECT url_fonte FROM empreendimentos
            WHERE empresa = ? AND url_fonte IS NOT NULL AND url_fonte != ''
            ORDER BY RANDOM() LIMIT 3
        """, (empresa,))
        urls = [r[0] for r in cur.fetchall()]

        if not urls:
            continue

        cat = catalogar_empresa(empresa, urls, session)
        catalogos.append(cat)

        # Resumo
        disponiveis = sum(1 for c in cat["campos"].values() if c["disponivel"])
        js = " (JS-ONLY!)" if cat["renderiza_js"] else ""
        acess = "OK" if cat["acessivel"] else "INACESSÍVEL"
        log.info(f"  [{i}/{len(empresas_lista)}] {empresa}: {acess}{js} | {disponiveis}/{len(CAMPOS)} campos disponíveis")

        # Salvar progresso a cada 10 empresas
        if i % 10 == 0:
            os.makedirs(os.path.dirname(CATALOGO_PATH), exist_ok=True)
            with open(CATALOGO_PATH, "w", encoding="utf-8") as f:
                json.dump(catalogos, f, ensure_ascii=False, indent=2)
            log.info(f"  >> Catálogo salvo ({len(catalogos)} empresas)")

    # Salvar final
    os.makedirs(os.path.dirname(CATALOGO_PATH), exist_ok=True)
    with open(CATALOGO_PATH, "w", encoding="utf-8") as f:
        json.dump(catalogos, f, ensure_ascii=False, indent=2)

    # Resumo global
    log.info(f"\n{'='*60}")
    log.info(f"RESUMO DA CATALOGAÇÃO")
    log.info(f"{'='*60}")
    log.info(f"Total empresas catalogadas: {len(catalogos)}")

    # Matriz de disponibilidade
    log.info(f"\nDisponibilidade por campo:")
    for campo in CAMPOS:
        disp = sum(1 for c in catalogos if c["campos"].get(campo, {}).get("disponivel", False))
        ndisp = sum(1 for c in catalogos if not c["campos"].get(campo, {}).get("disponivel", False))
        log.info(f"  {campo:15s}: {disp:3d} disponível | {ndisp:3d} não disponível")

    js_only = sum(1 for c in catalogos if c.get("renderiza_js"))
    inacess = sum(1 for c in catalogos if not c.get("acessivel"))
    log.info(f"\nJS-only (precisa Selenium): {js_only}")
    log.info(f"Inacessíveis: {inacess}")

    conn.close()
    log.info(f"\nCatálogo salvo em: {CATALOGO_PATH}")
    log.info("DONE")


if __name__ == "__main__":
    main()

"""
Scraper de empreendimentos da Viva Benx (MCMV).
================================================
Usa requests + BeautifulSoup (site server-side rendered).
Coleta links via sitemap XML e extrai dados de cada pagina.

Uso:
    python scrapers/vivabenx_empreendimentos.py
    python scrapers/vivabenx_empreendimentos.py --limite 3
    python scrapers/vivabenx_empreendimentos.py --atualizar
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import REQUESTS, LOGS_DIR
from data.database import (
    inserir_empreendimento,
    empreendimento_existe,
    atualizar_empreendimento,
    detectar_atributos_binarios,
    contar_empreendimentos,
)

EMPRESA = "Viva Benx"
BASE_URL = "https://www.vivabenx.com.br"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
PROGRESSO_FILE = os.path.join(LOGS_DIR, "vivabenx_empreendimentos_progresso.json")
DELAY = 2

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("scraper.vivabenx")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "vivabenx_empreendimentos.log"), encoding="utf-8")
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
        json.dump(progresso, f, indent=2)


# ============================================================
# COLETA DE LINKS VIA SITEMAP
# ============================================================

def coletar_links_sitemap():
    """Coleta URLs de empreendimentos do sitemap XML."""
    logger.info(f"Buscando sitemap: {SITEMAP_URL}")
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Erro ao buscar sitemap: {e}")
        return []

    urls = []
    for match in re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", resp.text):
        url_found = match.strip()
        if "/empreendimento/" in url_found and url_found.rstrip("/") != f"{BASE_URL}/empreendimento":
            urls.append(url_found)

    logger.info(f"Total de URLs de empreendimentos no sitemap: {len(urls)}")
    return urls


# ============================================================
# PARSING DE EMPREENDIMENTO
# ============================================================

def extrair_dados_empreendimento(html, url):
    """Extrai dados estruturados da pagina de um empreendimento Viva Benx."""
    soup = BeautifulSoup(html, "html.parser")
    texto_completo = soup.get_text(separator="\n", strip=True)

    dados = {
        "empresa": EMPRESA,
        "url_fonte": url,
        "cidade": "São Paulo",
        "estado": "SP",
    }

    # Slug
    slug = url.rstrip("/").split("/")[-1]
    dados["slug"] = slug

    # Nome - h1 principal ou title
    h1 = soup.find("h1")
    if h1:
        dados["nome"] = re.sub(r'\s+', ' ', h1.get_text(strip=True)).strip()
    else:
        title = soup.find("title")
        if title:
            nome = title.get_text(strip=True).split("|")[0].split("-")[0].strip()
            dados["nome"] = nome

    # Fase/Status
    fase = _extrair_fase(soup, texto_completo)
    if fase:
        dados["fase"] = fase

    # Endereco
    _extrair_endereco(soup, texto_completo, dados)

    # Coordenadas - Google Maps embed
    _extrair_coordenadas(soup, html, dados)

    # Dormitorios
    dorm_match = re.findall(
        r"\d+\s*(?:e\s*\d+\s*)?(?:quartos?|dorms?\.?|dormitórios?|dormitorios?|suítes?|studios?)",
        texto_completo, re.IGNORECASE
    )
    if dorm_match:
        vistos = set()
        unicos = []
        for d in dorm_match:
            d_norm = d.strip().lower()
            if d_norm not in vistos:
                vistos.add(d_norm)
                unicos.append(d.strip())
        dados["dormitorios_descricao"] = " | ".join(unicos[:4])

    # Metragens
    metragens = re.findall(
        r"\d+(?:[.,]\d+)?\s*(?:[Aa]\s*\d+(?:[.,]\d+)?\s*)?m[²2]",
        texto_completo
    )
    if metragens:
        vistos = set()
        unicos = []
        for m in metragens:
            m_norm = m.strip().lower()
            if m_norm not in vistos:
                vistos.add(m_norm)
                unicos.append(m.strip())
        dados["metragens_descricao"] = " | ".join(unicos[:5])

        nums = []
        for m in metragens:
            for n in re.findall(r"(\d+(?:[.,]\d+)?)", m):
                v = float(n.replace(",", "."))
                if 15.0 <= v <= 500.0:
                    nums.append(v)
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)

    # Preco
    precos = re.findall(r"R\$\s*([\d.,]+)", texto_completo)
    for p in precos:
        try:
            valor = float(p.replace(".", "").replace(",", "."))
            if valor > 50000:
                dados["preco_a_partir"] = valor
                break
        except ValueError:
            continue

    # Ficha tecnica
    uni_match = re.search(r"(\d+)\s*(?:unidades|apartamentos|aptos|apts)", texto_completo, re.IGNORECASE)
    if uni_match:
        dados["total_unidades"] = int(uni_match.group(1))

    torres_match = re.search(r"(\d+)\s*torre", texto_completo, re.IGNORECASE)
    if torres_match:
        dados["numero_torres"] = int(torres_match.group(1))

    vagas_match = re.search(r"(\d+)\s*vagas?", texto_completo, re.IGNORECASE)
    if vagas_match:
        dados["numero_vagas"] = vagas_match.group(0).strip()

    # MCMV
    texto_lower = texto_completo.lower()
    if any(kw in texto_lower for kw in ["minha casa minha vida", "mcmv", "his-2", "his 2", "hmp", "casa verde amarela"]):
        dados["prog_mcmv"] = True

    # Itens de lazer
    itens = _extrair_itens_lazer(texto_completo)
    if itens:
        dados["itens_lazer"] = " | ".join(itens)

    # Atributos binarios
    atributos = detectar_atributos_binarios(texto_completo)
    dados.update(atributos)

    return dados


def _extrair_fase(soup, texto_completo):
    """Extrai fase/status do empreendimento."""
    # 1. Procurar em elementos HTML de status
    for tag in soup.find_all(["span", "div", "p"], class_=re.compile(r"status|fase|tag|badge", re.I)):
        texto = tag.get_text(strip=True).lower()
        if any(f in texto for f in ["lançamento", "lancamento", "breve", "pronto", "obra", "entregue"]):
            return _normalizar_fase(tag.get_text(strip=True))

    # 2. Procurar no texto
    texto_lower = texto_completo.lower()
    if "breve lançamento" in texto_lower[:1000] or "breve lancamento" in texto_lower[:1000]:
        return "Breve Lançamento"
    if "lançamento" in texto_lower[:500] or "lancamento" in texto_lower[:500]:
        return "Lançamento"
    if "obras avançadas" in texto_lower or "obras avancadas" in texto_lower:
        return "Em Construção"
    if "em obra" in texto_lower or "em construção" in texto_lower:
        return "Em Construção"
    if "pronto" in texto_lower[:1000]:
        return "Pronto"
    return None


def _normalizar_fase(fase_raw):
    """Normaliza texto de fase."""
    f = fase_raw.strip().lower()
    if "breve" in f:
        return "Breve Lançamento"
    if "obras avançadas" in f or "obras avancadas" in f:
        return "Em Construção"
    if "em obra" in f or "construção" in f or "construcao" in f:
        return "Em Construção"
    if "lançamento" in f or "lancamento" in f:
        return "Lançamento"
    if "pronto" in f or "entregue" in f:
        return "Pronto"
    return fase_raw.strip()


def _extrair_endereco(soup, texto_completo, dados):
    """Extrai endereço, bairro da página."""
    # Procurar padrão "Rua/Av X, 123 - Bairro" no texto
    match_end = re.search(
        r"((?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rodovia)[^<\n]{5,80}?\d+)\s*[-–]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)",
        texto_completo
    )
    if match_end:
        dados["endereco"] = match_end.group(1).strip()
        dados["bairro"] = match_end.group(2).strip()
    else:
        # Endereço sem bairro
        enderecos = re.findall(
            r"(?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rodovia)[^<\n]{5,80}?\d+",
            texto_completo
        )
        if enderecos:
            dados["endereco"] = enderecos[0].strip()

    # Se não achou bairro, extrair do nome do empreendimento (Viva Benx BAIRRO)
    if not dados.get("bairro"):
        nome = dados.get("nome", "")
        bairro_do_nome = re.sub(r"^Viva Benx\s+", "", nome, flags=re.IGNORECASE).strip()
        # Remover sufixos como "I", "II", "III"
        bairro_do_nome = re.sub(r"\s+[IVX]+$", "", bairro_do_nome).strip()
        if bairro_do_nome and len(bairro_do_nome) > 2:
            dados["bairro"] = bairro_do_nome


def _extrair_coordenadas(soup, html, dados):
    """Extrai coordenadas de Google Maps embed ou scripts."""
    # 1. Iframe do Google Maps
    iframes = soup.find_all("iframe", src=True)
    for iframe in iframes:
        src = iframe["src"]
        if "google" in src and "maps" in src:
            # Padrão !2d{lng}!3d{lat}
            match = re.search(r"!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)", src)
            if match:
                dados["longitude"] = float(match.group(1))
                dados["latitude"] = float(match.group(2))
                return
            # Padrão q=lat,lng
            match = re.search(r"q=(-?\d+\.\d+),(-?\d+\.\d+)", src)
            if match:
                dados["latitude"] = float(match.group(1))
                dados["longitude"] = float(match.group(2))
                return

    # 2. Scripts com coordenadas
    for script in soup.find_all("script"):
        if not script.string:
            continue
        # google.maps.LatLng(lat, lng)
        match = re.search(r"LatLng\(\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*\)", script.string)
        if match:
            dados["latitude"] = float(match.group(1))
            dados["longitude"] = float(match.group(2))
            return
        # {lat: -23.xxx, lng: -46.xxx}
        match = re.search(r'"?lat"?\s*:\s*(-?\d+\.\d+)\s*,\s*"?lng"?\s*:\s*(-?\d+\.\d+)', script.string)
        if match:
            dados["latitude"] = float(match.group(1))
            dados["longitude"] = float(match.group(2))
            return

    # 3. Fallback: qualquer link com coordenadas
    match = re.search(r"!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)", html)
    if match:
        dados["longitude"] = float(match.group(1))
        dados["latitude"] = float(match.group(2))


def _extrair_itens_lazer(texto):
    """Extrai itens de lazer do texto da pagina."""
    termos = [
        "piscina", "churrasqueira", "fitness", "academia", "playground",
        "brinquedoteca", "salão de festas", "salao de festas", "pet care",
        "pet place", "coworking", "bicicletário", "bicicletario", "quadra",
        "delivery", "horta", "lavanderia", "redário", "redario", "rooftop",
        "sauna", "spa", "sport bar", "cinema", "cine", "mini mercado",
        "market", "espaço beleza", "espaco beleza", "gourmet", "salão de jogos",
        "salao de jogos", "solarium", "solário", "portaria",
        "mini quadra", "beach tennis", "sala funcional",
        "espaço gourmet", "espaco gourmet", "piquenique", "deck",
        "varanda", "terraço", "terraco",
    ]
    texto_lower = texto.lower()
    encontrados = set()
    for termo in termos:
        if termo in texto_lower:
            encontrados.add(termo.title())
    return sorted(encontrados)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Viva Benx")
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
    logger.info(f"SCRAPER VIVA BENX (MCMV)")
    logger.info(f"Empreendimentos no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)

    # Fase 1: Coletar links via sitemap
    logger.info("[FASE 1] Coletando links via sitemap...")
    links = coletar_links_sitemap()

    if not links:
        logger.error("Nenhum link encontrado no sitemap!")
        return

    if args.limite > 0:
        links = links[:args.limite]
        logger.info(f"Limitado a {args.limite} empreendimentos")

    # Filtrar ja processados
    if not args.atualizar:
        links = [url for url in links if url not in progresso["processados"]]
        logger.info(f"Pendentes: {len(links)}")

    if not links:
        logger.info("Todos os empreendimentos ja foram processados.")
        return

    # Fase 2: Processar cada empreendimento
    logger.info(f"[FASE 2] Processando {len(links)} empreendimentos...")
    novos = 0
    atualizados = 0
    erros = 0
    pulados = 0

    for i, url in enumerate(links, 1):
        slug = url.rstrip("/").split("/")[-1]
        logger.info(f"\n[{i}/{len(links)}] {slug}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"  Status {resp.status_code}")
                erros += 1
                progresso["erros"].append(url)
                salvar_progresso(progresso)
                continue

            dados = extrair_dados_empreendimento(resp.text, url)
            nome = dados.get("nome", "")

            if not nome:
                nome = slug.replace("-", " ").title()
                dados["nome"] = nome

            existe = empreendimento_existe(EMPRESA, nome)

            if existe and not args.atualizar:
                logger.info(f"  Ja existe, pulando.")
                pulados += 1
            elif existe and args.atualizar:
                atualizar_empreendimento(EMPRESA, nome, dados)
                atualizados += 1
                logger.info(f"  Atualizado: {nome}")
            else:
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {nome} | {dados.get('fase', 'N/A')} | {dados.get('bairro', 'N/A')}")

            progresso["processados"].append(url)
            salvar_progresso(progresso)

        except Exception as e:
            logger.error(f"  Erro: {e}")
            erros += 1
            progresso["erros"].append(url)
            salvar_progresso(progresso)

        time.sleep(DELAY)

    # Relatorio final
    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL - VIVA BENX")
    logger.info(f"  Novos: {novos}")
    logger.info(f"  Atualizados: {atualizados}")
    logger.info(f"  Pulados: {pulados}")
    logger.info(f"  Erros: {erros}")
    logger.info(f"  Total no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

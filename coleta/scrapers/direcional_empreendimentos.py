"""
Scraper de empreendimentos da Direcional Engenharia.
=====================================================
Usa requests + BeautifulSoup (site WordPress server-side).
Coleta links via sitemap XML e extrai dados de cada pagina.

Uso:
    python scrapers/direcional_empreendimentos.py
    python scrapers/direcional_empreendimentos.py --limite 3
    python scrapers/direcional_empreendimentos.py --atualizar
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

EMPRESA = "Direcional"
BASE_URL = "https://www.direcional.com.br"
SITEMAP_URL = f"{BASE_URL}/empreendimento-sitemap.xml"
PROGRESSO_FILE = os.path.join(LOGS_DIR, "direcional_empreendimentos_progresso.json")
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
logger = logging.getLogger("scraper.direcional")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "direcional_empreendimentos.log"), encoding="utf-8")
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

    # Parsear XML do sitemap com regex (evita dependencia de lxml)
    urls = []
    for match in re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", resp.text):
        url_found = match.strip()
        if "/empreendimentos/" in url_found and url_found.rstrip("/") != f"{BASE_URL}/empreendimentos":
            urls.append(url_found)

    logger.info(f"Total de URLs no sitemap: {len(urls)}")
    return urls


# ============================================================
# PARSING DE EMPREENDIMENTO
# ============================================================

def extrair_dados_empreendimento(html, url):
    """Extrai dados estruturados da pagina de um empreendimento Direcional."""
    soup = BeautifulSoup(html, "html.parser")
    texto_completo = soup.get_text(separator="\n", strip=True)

    dados = {
        "empresa": EMPRESA,
        "url_fonte": url,
    }

    # Slug
    slug = url.rstrip("/").split("/")[-1]
    dados["slug"] = slug

    # Nome - h1 principal
    h1 = soup.find("h1")
    if h1:
        nome = h1.get_text(strip=True)
        # Limpar nome (remover textos extras como "Lançamento" que podem estar junto)
        nome = re.sub(r'\s+', ' ', nome).strip()
        dados["nome"] = nome

    # Fase/Status — timeline de status da Direcional
    # O site renderiza timeline via JS com imagens lazy-loaded:
    #   complete_*.png        → fase concluída
    #   complete-lancamento_* → fase ATUAL (ativa)
    #   in-progress_*         → fase futura
    # A imagem fica em div.col-auto e o texto da fase no sibling seguinte.
    fase = None

    # 1. Procurar img com data-src ou src contendo "complete-lancamento"
    for img in soup.find_all("img", attrs={"data-src": re.compile(r"complete.lancamento")}):
        parent = img.parent
        if parent:
            for sib in parent.find_next_siblings():
                t = sib.get_text(strip=True)
                if t:
                    fase = t
                    break
        if fase:
            break
    # Fallback: buscar no src também
    if not fase:
        for img in soup.find_all("img", src=re.compile(r"complete.lancamento")):
            parent = img.parent
            if parent and parent.name != "noscript":
                for sib in parent.find_next_siblings():
                    t = sib.get_text(strip=True)
                    if t:
                        fase = t
                        break
            if fase:
                break

    # 2. Badges/tags genéricos
    if not fase:
        for tag in soup.find_all(["span", "div", "p"], class_=re.compile(r"status|fase|tag|badge|label", re.I)):
            texto = tag.get_text(strip=True).lower()
            if any(f in texto for f in ["lançamento", "lancamento", "breve", "pronto", "obra", "entregue"]):
                fase = tag.get_text(strip=True)
                break

    # Normalizar
    if fase:
        fase_lower = fase.lower()
        if "pronto" in fase_lower:
            fase = "Pronto"
        elif "obras avançadas" in fase_lower or "obras avancadas" in fase_lower:
            fase = "Em Construção"
        elif "em obra" in fase_lower or "em construção" in fase_lower or "em construcao" in fase_lower:
            fase = "Em Construção"
        elif "breve" in fase_lower and "lançamento" in fase_lower:
            fase = "Breve Lançamento"
        elif "futuro" in fase_lower and "lançamento" in fase_lower:
            fase = "Breve Lançamento"
        elif "lançamento" in fase_lower or "lancamento" in fase_lower:
            fase = "Lançamento"
        dados["fase"] = fase

    # Endereco - procurar padroes como "Rua/Av/Alameda... - Bairro, Cidade - UF"
    enderecos = re.findall(
        r"(?:Rua|Av\.|Avenida|R\.|Al\.|Alameda|Estrada|Rodovia)[^<\n]+?(?:\d+)[^<\n]*?[-–]\s*[A-ZÀ-Ú][^<\n]*?[-–]\s*[A-Z]{2}",
        texto_completo
    )
    if enderecos:
        dados["endereco"] = enderecos[0].strip()
        # Extrair bairro, cidade e estado do endereco
        match = re.search(r"[-–]\s*([^,–-]+?)\s*,\s*([^–-]+?)\s*[-–]\s*([A-Z]{2})", enderecos[0])
        if match:
            dados["bairro"] = match.group(1).strip()
            dados["cidade"] = match.group(2).strip()
            dados["estado"] = match.group(3).strip()

    # Se nao encontrou endereco completo, tentar extrair cidade/estado do texto
    if not dados.get("cidade"):
        # Padrao: "Bairro, Cidade – UF" ou "Cidade - UF"
        match = re.search(r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*([A-Z]{2})\b", texto_completo[:3000])
        if match:
            dados["cidade"] = match.group(1).strip()
            dados["estado"] = match.group(2).strip()

    # Dormitorios - "2 e 3 quartos", "2 quartos", etc.
    dorm_match = re.findall(
        r"\d+\s*(?:e\s*\d+\s*)?(?:quartos?|dorms?\.?|dormitórios?|dormitorios?|suítes?)",
        texto_completo, re.IGNORECASE
    )
    if dorm_match:
        # Deduplicar
        vistos = set()
        unicos = []
        for d in dorm_match:
            d_norm = d.strip().lower()
            if d_norm not in vistos:
                vistos.add(d_norm)
                unicos.append(d.strip())
        dados["dormitorios_descricao"] = " | ".join(unicos[:3])

    # Metragens - "59,72 m²", "44 a 60 m²", etc.
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

        # Extrair min/max numericos
        nums = []
        for m in metragens:
            for n in re.findall(r"(\d+(?:[.,]\d+)?)", m):
                v = float(n.replace(",", "."))
                if 15.0 <= v <= 500.0:
                    nums.append(v)
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)

    # Preco - "R$ 245.000"
    precos = re.findall(r"R\$\s*([\d.,]+)", texto_completo)
    for p in precos:
        try:
            valor = float(p.replace(".", "").replace(",", "."))
            if valor > 50000:
                dados["preco_a_partir"] = valor
                break
        except ValueError:
            continue

    # Ficha tecnica - total unidades, torres, andares, vagas
    # Total de unidades
    uni_match = re.search(r"(\d+)\s*(?:unidades|apartamentos|aptos|apts)", texto_completo, re.IGNORECASE)
    if uni_match:
        dados["total_unidades"] = int(uni_match.group(1))

    # Torres
    torres_match = re.search(r"(\d+)\s*torre", texto_completo, re.IGNORECASE)
    if torres_match:
        dados["numero_torres"] = int(torres_match.group(1))

    # Andares/pavimentos
    andares_match = re.search(r"(\d+)\s*(?:andares?|pavimentos?)", texto_completo, re.IGNORECASE)
    if andares_match:
        dados["numero_andares"] = andares_match.group(0).strip()

    # Vagas
    vagas_match = re.search(r"(\d+)\s*vagas?", texto_completo, re.IGNORECASE)
    if vagas_match:
        dados["numero_vagas"] = vagas_match.group(0).strip()

    # Area do terreno
    terreno_match = re.search(r"(?:terreno|área do terreno)[:\s]*(\d+[.,]?\d*)\s*m[²2]", texto_completo, re.IGNORECASE)
    if terreno_match:
        dados["area_terreno_m2"] = float(terreno_match.group(1).replace(",", "."))

    # Itens de lazer
    itens_lazer = extrair_itens_lazer(texto_completo)
    if itens_lazer:
        dados["itens_lazer"] = " | ".join(itens_lazer)

    # Atributos binarios
    atributos = detectar_atributos_binarios(texto_completo)
    dados.update(atributos)

    return dados


def extrair_itens_lazer(texto):
    """Extrai itens de lazer do texto da pagina."""
    termos = [
        "piscina", "churrasqueira", "fitness", "academia", "playground",
        "brinquedoteca", "salão de festas", "salao de festas", "pet care",
        "pet place", "coworking", "bicicletário", "bicicletario", "quadra",
        "delivery", "horta", "lavanderia", "redário", "redario", "rooftop",
        "sauna", "spa", "sport bar", "cinema", "cine", "mini mercado",
        "market", "espaço beleza", "espaco beleza", "gourmet", "salão de jogos",
        "salao de jogos", "solarium", "solário", "portaria", "hall",
        "mini quadra", "beach tennis", "sala funcional", "guarita",
        "espaço gourmet", "espaco gourmet", "piquenique", "deck",
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
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Direcional")
    parser.add_argument("--atualizar", action="store_true", help="Atualizar empreendimentos existentes")
    parser.add_argument("--limite", type=int, default=0, help="Limitar numero de empreendimentos (0=todos)")
    parser.add_argument("--reset-progresso", action="store_true", help="Resetar progresso")
    parser.add_argument("--sem-imagens", action="store_true", help="Nao baixar imagens")
    args = parser.parse_args()

    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            logger.info("Progresso resetado.")
        return

    progresso = carregar_progresso()

    logger.info("=" * 60)
    logger.info(f"SCRAPER DIRECIONAL ENGENHARIA")
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
                logger.warning(f"  Nome nao encontrado, usando slug")
                nome = slug.replace("-", " ").title()
                dados["nome"] = nome

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
                logger.info(f"  Inserido: {nome} | {dados.get('fase', 'N/A')} | {dados.get('cidade', 'N/A')}-{dados.get('estado', 'N/A')}")

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
    logger.info("RELATORIO FINAL - DIRECIONAL")
    logger.info(f"  Novos: {novos}")
    logger.info(f"  Atualizados: {atualizados}")
    logger.info(f"  Pulados: {pulados}")
    logger.info(f"  Erros: {erros}")
    logger.info(f"  Total no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

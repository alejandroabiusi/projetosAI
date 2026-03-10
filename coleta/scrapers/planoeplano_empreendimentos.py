"""
Scraper de Empreendimentos - Plano&Plano (v3)
===============================================
Coleta dados de todos os empreendimentos do portfolio da Plano&Plano,
incluindo entregues, em construcao e lancamentos.

Funcionalidades:
  - Coleta URLs do portfolio paginado e da pagina de imoveis ativos
  - Extrai ficha tecnica, lazer, metragens, precos, enderecos
  - Detecta atributos binarios (lazer, dormitorios, programas)
  - Baixa imagens categorizadas (perspectivas, plantas, decorado, obra)
  - Execucao incremental: pergunta tamanho do lote, salva progresso,
    retoma de onde parou na proxima execucao

Uso:
    python scrapers/planoeplano_empreendimentos.py
    python scrapers/planoeplano_empreendimentos.py --atualizar
    python scrapers/planoeplano_empreendimentos.py --reset-progresso
"""

import os
import sys
import re
import json
import time
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import PLANOEPLANO, REQUESTS, LOGS_DIR, DOWNLOADS_DIR
from data.database import (
    inserir_empreendimento,
    empreendimento_existe,
    atualizar_empreendimento,
    detectar_atributos_binarios,
    contar_empreendimentos,
    garantir_coluna,
)


EMPRESA = "Plano&Plano"
BASE_URL = "https://www.planoeplano.com.br"
PORTFOLIO_URL = BASE_URL + "/portfolio"
IMOVEIS_URL = BASE_URL + "/imoveis"

# Diretorio para imagens
IMAGENS_DIR = os.path.join(DOWNLOADS_DIR, "planoeplano", "imagens")

# Arquivo de progresso (controle incremental)
PROGRESSO_FILE = os.path.join(LOGS_DIR, "planoeplano_empreendimentos_progresso.json")

# Termos que NAO sao itens de lazer (ruido do site)
RUIDO_LAZER = [
    "priscila", "plano&", "plano &", "planoeplano", "confira", "conheça",
    "saiba mais", "agendar", "simular", "whatsapp", "fale com", "enviar",
    "carregando", "ops!", "obrigad", "voltar", "anterior", "próximo",
    "perspectiva", "ilustrad", "home", "imóveis", "busca", "portfólio",
    "lazer pensado", "ficha técnica", "exibir ficha", "quem assina",
    "imóvel pronto", "lançamento", "em construção", "futuro", "breve",
]


def setup_logger():
    logger = logging.getLogger("scraper.planoeplano_empreendimentos")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    os.makedirs(LOGS_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(LOGS_DIR, "planoeplano_empreendimentos.log"), encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


logger = setup_logger()


def fazer_request(session, url, tentativas=3):
    for t in range(tentativas):
        try:
            resp = session.get(url, timeout=REQUESTS["timeout"])
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            logger.warning(f"Tentativa {t+1}/{tentativas} falhou para {url}: {e}")
            time.sleep(2 * (t + 1))
    logger.error(f"Falha definitiva ao acessar {url}")
    return None


# =========================================================================
# PROGRESSO INCREMENTAL
# =========================================================================

def carregar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"ultimo_indice": 0, "urls_processadas": []}


def salvar_progresso(progresso):
    os.makedirs(os.path.dirname(PROGRESSO_FILE), exist_ok=True)
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(progresso, f, ensure_ascii=False, indent=2)


def resetar_progresso():
    if os.path.exists(PROGRESSO_FILE):
        os.remove(PROGRESSO_FILE)
        logger.info("Progresso resetado.")


# =========================================================================
# FASE 1: COLETAR URLs
# =========================================================================

def _eh_url_empreendimento(href):
    if not href:
        return False
    url_normalizada = href.replace(BASE_URL, "")
    if url_normalizada.startswith("/imoveis/") and url_normalizada.count("/") >= 5:
        return True
    if url_normalizada.startswith("/portfolio/") and url_normalizada.count("/") >= 5:
        return True
    return False


def _nome_da_url(url):
    slug = url.rstrip("/").split("/")[-1]
    return slug.replace("-", " ").replace("_", " ").title()


def coletar_urls_portfolio(session):
    empreendimentos = []
    pagina = 1

    while pagina <= 30:
        url = f"{PORTFOLIO_URL}?filter=todos-os-imoveis&page={pagina}&per-page=12"
        logger.info(f"Portfolio pagina {pagina}: {url}")
        soup = fazer_request(session, url)
        if not soup:
            break

        encontrou = False
        urls_pagina = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not _eh_url_empreendimento(href):
                continue
            if "page=" in href or "filter=" in href:
                continue

            url_completa = BASE_URL + href if href.startswith("/") else href
            if url_completa in urls_pagina:
                continue
            urls_pagina.add(url_completa)

            if any(e["url"] == url_completa for e in empreendimentos):
                continue

            parent = link.find_parent(["div", "article", "li", "section"])
            texto_card = parent.get_text(separator=" ", strip=True) if parent else ""

            nome = ""
            if parent:
                for tag in parent.find_all(["h2", "h3", "h4", "strong", "b"]):
                    t = tag.get_text(strip=True)
                    if t and len(t) > 3 and not any(kw in t.lower() for kw in ["lançamento", "construção", "pronto", "apartamento em", "apartamento no", "apartamento na"]):
                        nome = t
                        break
            if not nome:
                nome = _nome_da_url(url_completa)

            fase = ""
            for f in ["Futuro Lançamento", "Breve Lançamento", "Lançamento", "Em Construção", "Imóvel Pronto"]:
                if f.lower() in texto_card.lower():
                    fase = f
                    break

            bairro = ""
            # Extrai bairro do texto do card (geralmente aparece antes do nome)
            url_parts = url_completa.rstrip("/").split("/")
            if len(url_parts) >= 2:
                bairro = url_parts[-2].replace("-", " ").title()

            empreendimentos.append({"nome": nome, "url": url_completa, "fase": fase, "bairro_card": bairro})
            encontrou = True

        if not encontrou:
            logger.info(f"Nenhum item na pagina {pagina}. Fim.")
            break

        logger.info(f"  Pagina {pagina}: {len(urls_pagina)} itens.")

        tem_proxima = any(f"page={pagina + 1}" in a.get("href", "") for a in soup.find_all("a", href=True))
        if not tem_proxima:
            break

        pagina += 1
        time.sleep(1.5)

    logger.info(f"Total URLs portfolio: {len(empreendimentos)}")
    return empreendimentos


def coletar_urls_imoveis(session):
    empreendimentos = []
    logger.info(f"Acessando imoveis ativos: {IMOVEIS_URL}")
    soup = fazer_request(session, IMOVEIS_URL)
    if not soup:
        return empreendimentos

    urls_vistas = set()
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not _eh_url_empreendimento(href):
            continue

        url_completa = BASE_URL + href if href.startswith("/") else href
        if url_completa in urls_vistas:
            continue
        urls_vistas.add(url_completa)

        parent = link.find_parent(["div", "article", "li", "section"])
        texto_card = parent.get_text(separator=" ", strip=True) if parent else ""

        nome = ""
        if parent:
            for tag in parent.find_all(["h2", "h3", "h4", "strong", "b"]):
                t = tag.get_text(strip=True)
                if t and len(t) > 3 and not any(kw in t.lower() for kw in ["lançamento", "construção", "pronto", "apartamento em", "apartamento no", "apartamento na"]):
                    nome = t
                    break
        if not nome:
            nome = _nome_da_url(url_completa)

        fase = ""
        for f in ["Futuro Lançamento", "Breve Lançamento", "Lançamento", "Em Construção", "Imóvel Pronto"]:
            if f.lower() in texto_card.lower():
                fase = f
                break

        preco = None
        preco_match = re.search(r"R\$\s*([0-9.]+(?:,[0-9]+)?)", texto_card)
        if preco_match:
            try:
                v = float(preco_match.group(1).replace(".", "").replace(",", "."))
                if v > 10000:
                    preco = v
            except ValueError:
                pass

        empreendimentos.append({"nome": nome, "url": url_completa, "fase": fase, "preco_listagem": preco})

    logger.info(f"URLs imoveis ativos: {len(empreendimentos)}")
    return empreendimentos


# =========================================================================
# FASE 2: EXTRAIR DADOS DE CADA EMPREENDIMENTO
# =========================================================================

def _extrair_linhas(soup):
    """Retorna o texto da pagina como lista de linhas limpas."""
    return [line.strip() for line in soup.get_text(separator="\n", strip=True).splitlines() if line.strip()]


def _valor_apos_label(linhas, label_regex, offset=1):
    """Busca um label nas linhas e retorna a(s) linha(s) seguinte(s)."""
    for i, linha in enumerate(linhas):
        if re.search(label_regex, linha, re.IGNORECASE):
            if ":" in linha:
                # Valor na mesma linha apos ":"
                valor = linha.split(":", 1)[1].strip()
                if valor:
                    return valor
            # Valor na proxima linha
            if i + offset < len(linhas):
                return linhas[i + offset]
    return None


def _extrair_numero_de_texto(texto):
    """Extrai primeiro numero inteiro de um texto."""
    if not texto:
        return None
    match = re.search(r"(\d+)", texto.replace(".", ""))
    return int(match.group(1)) if match else None


def _extrair_metragens(linhas):
    """Extrai todas as metragens em m2 das linhas."""
    valores = []
    for linha in linhas:
        for m in re.findall(r"(\d+(?:[,\.]\d+)?)\s*m²", linha):
            try:
                v = float(m.replace(",", "."))
                if 15 < v < 300:
                    valores.append(v)
            except ValueError:
                pass
    return sorted(set(valores))


def _extrair_preco(linhas):
    """Extrai preco a partir de linhas."""
    for linha in linhas:
        if "r$" in linha.lower():
            match = re.search(r"R\$\s*([0-9.]+(?:,[0-9]+)?)", linha)
            if match:
                try:
                    v = float(match.group(1).replace(".", "").replace(",", "."))
                    if v > 50000:
                        return v
                except ValueError:
                    pass
    return None


def _extrair_renda_minima(linhas):
    """Extrai renda minima familiar."""
    for linha in linhas:
        if "renda" in linha.lower():
            match = re.search(r"R\$\s*([0-9.]+(?:,[0-9]+)?)", linha)
            if match:
                try:
                    return float(match.group(1).replace(".", "").replace(",", "."))
                except ValueError:
                    pass
    return None


def _extrair_endereco(linhas):
    """Extrai endereco completo."""
    for i, linha in enumerate(linhas):
        # Procura padrao de endereco antes de "Waze" ou "Google maps"
        if linha in ("Waze", "Google maps") and i > 0:
            candidato = linhas[i - 1]
            if re.search(r"(R\.|Rua|Av\.|Avenida|Al\.|Pça\.|Praça)", candidato):
                return candidato
        # Endereco com padrao direto
        if re.match(r"^(R\.|Rua|Av\.|Avenida|Al\.|Alameda|Pça\.|Praça)\s", linha):
            if " - " in linha and ("SP" in linha or "RJ" in linha or "São Paulo" in linha):
                return linha
    return None


def _extrair_itens_lazer(linhas):
    """
    Extrai itens de lazer da secao estruturada do site.
    Os itens aparecem entre "Lazer pensado..." e "Ficha técnica" OU
    como lista de items entre as perspectivas e a ficha tecnica.
    """
    itens = []
    capturando = False

    for linha in linhas:
        lower = linha.lower()

        # Inicio da secao de lazer
        if "lazer pensado" in lower or (capturando and len(linha) < 40):
            capturando = True
            continue

        # Fim da secao
        if capturando and ("ficha" in lower or "técnica" in lower or "exibir" in lower):
            break

        if capturando and linha:
            # Filtra ruido
            if any(r in lower for r in RUIDO_LAZER):
                continue
            if len(linha) > 2 and len(linha) < 40:
                itens.append(linha)

    # Se nao encontrou secao estruturada, busca nos alt das imagens
    if not itens:
        return []

    return list(dict.fromkeys(itens))  # Remove duplicatas mantendo ordem


def _extrair_ficha_tecnica(linhas):
    """Extrai dados da ficha tecnica a partir das linhas."""
    dados = {}

    # Area do terreno
    for linha in linhas:
        match = re.search(r"[Tt]erreno[:\s]+([0-9.]+(?:,[0-9]+)?)\s*m²", linha)
        if match:
            try:
                dados["area_terreno_m2"] = float(match.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass
            break

    # Numero de torres
    val = _valor_apos_label(linhas, r"[Nn]úmero de [Tt]orres?")
    if val:
        dados["numero_torres"] = _extrair_numero_de_texto(val)

    # Total de unidades
    val = _valor_apos_label(linhas, r"[Tt]otal de [Uu]nidade")
    if val:
        dados["total_unidades"] = _extrair_numero_de_texto(val)

    # Numero de andares (texto livre)
    val = _valor_apos_label(linhas, r"[Nn]úmero de [Aa]ndares?")
    if val and len(val) < 200:
        dados["numero_andares"] = val

    # Unidades por andar
    val = _valor_apos_label(linhas, r"[Uu]nidades por [Aa]ndar")
    if val and len(val) < 200:
        dados["unidades_por_andar"] = val

    # Vagas
    val = _valor_apos_label(linhas, r"[Vv]agas?\s*:")
    if val and len(val) < 200:
        dados["numero_vagas"] = val

    # Evolucao da obra
    for linha in linhas:
        match = re.search(r"(\d+)%", linha)
        if match and "evolu" in linhas[linhas.index(linha) + 1].lower() if linhas.index(linha) + 1 < len(linhas) else False:
            dados["evolucao_obra_pct"] = float(match.group(1))
            break
    # Tentativa alternativa
    if "evolucao_obra_pct" not in dados:
        for i, linha in enumerate(linhas):
            if "evolução da obra" in linha.lower() and i > 0:
                match = re.search(r"(\d+)", linhas[i - 1])
                if match:
                    dados["evolucao_obra_pct"] = float(match.group(1))
                    break

    # Quem assina
    for label_re, campo in [
        (r"[Aa]rquitet[ôo]nico", "arquitetura"),
        (r"[Pp]aisag", "paisagismo"),
        (r"[Dd]ecora[çc]", "decoracao"),
    ]:
        val = _valor_apos_label(linhas, label_re)
        if val and len(val) < 200:
            # Limpa valor (pode ter lixo da proxima secao)
            val_limpo = val.split("Projeto")[0].split("Áreas")[0].strip()
            if val_limpo:
                dados[campo] = val_limpo

    return dados


def _extrair_dormitorios_descricao(linhas):
    """Extrai descricao de dormitorios."""
    for linha in linhas:
        if re.search(r"\d\s*dorm", linha.lower()):
            if len(linha) < 100:
                return linha
    return None


# =========================================================================
# FASE 2b: IMAGENS
# =========================================================================

def _categorizar_imagem(alt_text):
    """Categoriza imagem pelo alt text."""
    alt = alt_text.lower()

    if any(t in alt for t in ["planta", "implantação", "implantacao"]):
        return "plantas"
    if any(t in alt for t in ["fachada", "portaria"]):
        return "fachada"
    if any(t in alt for t in ["decorado", "cozinha", "living", "dormitório",
                               "dormitorio", "banho", "lavanderia", "quarto"]):
        return "decorado"
    if any(t in alt for t in ["área interna", "area interna", "área externa",
                               "area externa", "obra"]):
        return "obra"
    if any(t in alt for t in ["churrasqueira", "fitness", "piscina", "playground",
                               "brinquedoteca", "salão", "salao", "pet", "coworking",
                               "bicicletário", "bicicletario", "quadra", "delivery",
                               "rooftop", "sauna"]):
        return "areas_comuns"

    return "geral"


def _slug_seguro(texto):
    """Converte texto para slug seguro para nomes de arquivo/pasta."""
    slug = texto.lower().strip()
    slug = re.sub(r"[àáâãä]", "a", slug)
    slug = re.sub(r"[èéêë]", "e", slug)
    slug = re.sub(r"[ìíîï]", "i", slug)
    slug = re.sub(r"[òóôõö]", "o", slug)
    slug = re.sub(r"[ùúûü]", "u", slug)
    slug = re.sub(r"[ç]", "c", slug)
    slug = re.sub(r"[&]", "e", slug)
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug[:80]


def extrair_e_baixar_imagens(session, soup, nome_empreendimento):
    """
    Extrai URLs de imagens relevantes e faz download categorizado.
    Retorna dict com URLs por categoria para gravar no banco.
    """
    slug = _slug_seguro(nome_empreendimento)
    pasta_empreendimento = os.path.join(IMAGENS_DIR, slug)

    imagens_por_categoria = {}
    urls_baixadas = set()
    contador = 0

    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        alt = img.get("alt", "").strip()

        # Filtra apenas imagens do dominio estatico da P&P
        if "estatico" not in src:
            continue

        # Ignora thumbnails muito pequenos e placeholders
        if "1x1placeholder" in src or "dot.png" in src:
            continue

        # Ignora icones e logos
        if any(t in src.lower() for t in ["logo", "icon", "governament", "reclame"]):
            continue

        # Ignora imagens sem alt E sem contexto util
        if not alt:
            # Aceita apenas se for imagem grande (provavelmente conteudo)
            # Thumbnails de obra vem sem alt mas com sufixo -thumbnail
            if "-thumbnail" not in src:
                continue

        # Normaliza URL
        if src.startswith("/"):
            src = BASE_URL + src

        if src in urls_baixadas:
            continue
        urls_baixadas.add(src)

        # Categoriza
        categoria = _categorizar_imagem(alt)

        # Monta nome do arquivo
        ext = os.path.splitext(urlparse(src).path)[1] or ".jpg"
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            ext = ".jpg"

        alt_slug = _slug_seguro(alt) if alt else f"img_{contador:03d}"
        nome_arquivo = f"{alt_slug}{ext}"

        # Pasta por categoria
        pasta_cat = os.path.join(pasta_empreendimento, categoria)
        os.makedirs(pasta_cat, exist_ok=True)

        caminho_destino = os.path.join(pasta_cat, nome_arquivo)

        # Evita regravar
        if not os.path.exists(caminho_destino):
            try:
                resp = session.get(src, timeout=30, stream=True)
                resp.raise_for_status()
                with open(caminho_destino, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)
                contador += 1
            except Exception as e:
                logger.warning(f"  Erro ao baixar imagem {src}: {e}")
                continue

        # Acumula URL por categoria
        if categoria not in imagens_por_categoria:
            imagens_por_categoria[categoria] = []
        imagens_por_categoria[categoria].append(src)

    logger.info(f"  Imagens baixadas: {contador} novas, {len(urls_baixadas)} total")
    return imagens_por_categoria


# =========================================================================
# FASE 2c: ORQUESTRACAO DA EXTRACAO
# =========================================================================

def extrair_dados_empreendimento(session, url, dados_basicos, baixar_imagens=True):
    """Acessa pagina do empreendimento e extrai todos os dados."""
    logger.info(f"Extraindo: {dados_basicos.get('nome', 'N/A')} -> {url}")
    soup = fazer_request(session, url)
    if not soup:
        return None

    linhas = _extrair_linhas(soup)

    # Dados basicos
    dados = {
        "empresa": EMPRESA,
        "nome": dados_basicos.get("nome", ""),
        "url_fonte": url,
        "fase": dados_basicos.get("fase", ""),
    }

    # Slug
    dados["slug"] = url.rstrip("/").split("/")[-1]

    # Cidade, estado, bairro da URL
    url_match = re.search(r"/(?:imoveis|portfolio)/(\w+)/([^/]+)/\w+/([^/]+)/", url + "/")
    if url_match:
        dados["estado"] = url_match.group(1).upper()
        dados["cidade"] = url_match.group(2).replace("-", " ").title()
        dados["bairro"] = url_match.group(3).replace("-", " ").title()

    # Endereco
    dados["endereco"] = _extrair_endereco(linhas)

    # Ficha tecnica
    ficha = _extrair_ficha_tecnica(linhas)
    dados.update(ficha)

    # Dormitorios
    dados["dormitorios_descricao"] = _extrair_dormitorios_descricao(linhas)

    # Preco
    preco = _extrair_preco(linhas)
    if not preco and dados_basicos.get("preco_listagem"):
        preco = dados_basicos["preco_listagem"]
    dados["preco_a_partir"] = preco

    # Renda minima
    dados["renda_minima"] = _extrair_renda_minima(linhas)

    # Metragens (filtra area do terreno)
    metragens = _extrair_metragens(linhas)
    terreno = dados.get("area_terreno_m2")
    if terreno and metragens:
        metragens = [m for m in metragens if abs(m - terreno) > 5]
    if metragens:
        dados["area_min_m2"] = min(metragens)
        dados["area_max_m2"] = max(metragens)
        dados["metragens_descricao"] = " | ".join(f"{m}m²" for m in metragens)

    # Itens de lazer
    itens_lazer = _extrair_itens_lazer(linhas)

    # Fallback: buscar lazer nos alt das imagens
    if not itens_lazer:
        for img in soup.find_all("img", alt=True):
            alt = img["alt"].strip()
            if alt and len(alt) > 2 and len(alt) < 80:
                lower = alt.lower()
                if any(t in lower for t in ["churrasqueira", "fitness", "piscina", "playground",
                                             "brinquedoteca", "salão", "pet", "coworking",
                                             "bicicletário", "quadra", "delivery", "rooftop"]):
                    # Remove sufixo " - NomeEmpreendimento" e " - Perspectiva Ilustrada"
                    nome_limpo = re.sub(r"\s*-\s*(?:Plano&.*|Perspectiva\s+Ilustrada.*)$", "", alt, flags=re.IGNORECASE).strip()
                    if nome_limpo and nome_limpo not in itens_lazer:
                        itens_lazer.append(nome_limpo)

    # Limpeza final: remove sufixos comuns de todas as entradas
    itens_limpos = []
    for item in itens_lazer:
        limpo = re.sub(r"\s*-\s*(?:Perspectiva\s+Ilustrada|Foto\s+Ilustrativa|Plano&.*)$", "", item, flags=re.IGNORECASE).strip()
        if limpo and limpo not in itens_limpos:
            itens_limpos.append(limpo)
    itens_lazer = itens_limpos

    if itens_lazer:
        dados["itens_lazer"] = " | ".join(itens_lazer)

    # Deteccao de atributos binarios
    texto_para_deteccao = "\n".join(linhas) + " " + " ".join(itens_lazer)
    atributos = detectar_atributos_binarios(texto_para_deteccao)
    dados.update(atributos)

    # Imagens
    if baixar_imagens:
        imagens = extrair_e_baixar_imagens(session, soup, dados["nome"])
        # Garante colunas de imagem no banco
        for cat in ["fachada", "plantas", "decorado", "obra", "areas_comuns", "geral"]:
            col = f"imagens_{cat}"
            garantir_coluna(col, "TEXT")
            if cat in imagens:
                dados[col] = " | ".join(imagens[cat])

    return dados


# =========================================================================
# MAIN
# =========================================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Plano&Plano")
    parser.add_argument("--atualizar", action="store_true", help="Reatualiza empreendimentos ja existentes")
    parser.add_argument("--reset-progresso", action="store_true", help="Reseta progresso e comeca do zero")
    parser.add_argument("--sem-imagens", action="store_true", help="Pula download de imagens")
    parser.add_argument("--lote", type=int, default=None, help="Tamanho do lote (nao pergunta interativamente)")
    args = parser.parse_args()

    if args.reset_progresso:
        resetar_progresso()

    logger.info("=" * 60)
    logger.info("SCRAPER DE EMPREENDIMENTOS - Plano&Plano (v3)")
    logger.info("=" * 60)

    session = requests.Session()
    session.headers.update({
        "User-Agent": REQUESTS["headers"]["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    })

    # Fase 1: Coleta URLs
    logger.info("\n--- FASE 1: Coletando URLs ---")
    urls_portfolio = coletar_urls_portfolio(session)
    urls_imoveis = coletar_urls_imoveis(session)

    # Unifica e deduplica (por URL)
    urls_por_url = {}
    for item in urls_portfolio + urls_imoveis:
        url = item["url"]
        if url not in urls_por_url:
            urls_por_url[url] = item
        else:
            for k, v in item.items():
                if v and not urls_por_url[url].get(k):
                    urls_por_url[url][k] = v

    lista_completa = list(urls_por_url.values())
    total = len(lista_completa)
    logger.info(f"\nTotal de empreendimentos unicos: {total}")

    # Carrega progresso
    progresso = carregar_progresso()
    inicio = progresso["ultimo_indice"]

    if inicio >= total:
        logger.info(f"Todos os {total} empreendimentos ja foram processados.")
        logger.info("Use --reset-progresso para recomecar.")
        return

    restantes = total - inicio
    logger.info(f"Progresso: {inicio}/{total} processados. Restam {restantes}.")

    # Define tamanho do lote
    if args.lote:
        tamanho_lote = args.lote
    else:
        print(f"\nRestam {restantes} empreendimentos para processar (de {inicio + 1} a {total}).")
        print(f"Com download de imagens, cada um leva ~5-10 segundos.")
        print(f"Voce tem 60 segundos para responder. Padrao: 10 por lote, executando continuamente.")
        try:
            import threading
            resposta_container = [None]

            def ler_input():
                try:
                    resposta_container[0] = input(f"Quantos por lote? [Enter = 10, 0 = todos de uma vez]: ").strip()
                except EOFError:
                    resposta_container[0] = ""

            thread = threading.Thread(target=ler_input, daemon=True)
            thread.start()
            thread.join(timeout=60)

            if thread.is_alive() or resposta_container[0] is None:
                print("\nSem resposta em 60s. Usando lote de 10, execucao continua.")
                tamanho_lote = 10
            elif resposta_container[0] == "":
                tamanho_lote = 10
            elif resposta_container[0] == "0":
                tamanho_lote = restantes
            else:
                try:
                    tamanho_lote = int(resposta_container[0])
                except ValueError:
                    tamanho_lote = 10
                    print(f"Valor invalido. Usando {tamanho_lote}.")
        except Exception:
            tamanho_lote = 10

    # Execucao continua: processa lotes ate acabar
    while inicio < total:
        fim = min(inicio + tamanho_lote, total)
        lote = lista_completa[inicio:fim]

        logger.info(f"\nProcessando lote: {inicio + 1} a {fim} (de {total})")
        logger.info(f"Download de imagens: {'NAO' if args.sem_imagens else 'SIM'}")

        # Fase 2: Extrai dados
        contadores_lote = {"novos": 0, "atualizados": 0, "pulados": 0, "erros": 0}

        for idx, item in enumerate(lote, inicio + 1):
            nome = item["nome"]
            url = item["url"]

            logger.info(f"\n[{idx}/{total}] {nome}")

            ja_existe = empreendimento_existe(EMPRESA, nome)
            if ja_existe and not args.atualizar:
                logger.info(f"  Ja existe, pulando.")
                contadores_lote["pulados"] += 1
                progresso["ultimo_indice"] = idx
                progresso["urls_processadas"].append(url)
                salvar_progresso(progresso)
                continue

            try:
                dados = extrair_dados_empreendimento(
                    session, url, item, baixar_imagens=not args.sem_imagens
                )
                if not dados:
                    contadores_lote["erros"] += 1
                elif ja_existe:
                    atualizar_empreendimento(EMPRESA, nome, dados)
                    contadores_lote["atualizados"] += 1
                    logger.info(f"  Atualizado.")
                else:
                    inserir_empreendimento(dados)
                    contadores_lote["novos"] += 1
                    logger.info(f"  Inserido.")

            except Exception as e:
                logger.error(f"  Erro: {e}", exc_info=True)
                contadores_lote["erros"] += 1

            progresso["ultimo_indice"] = idx
            progresso["urls_processadas"].append(url)
            salvar_progresso(progresso)

            time.sleep(2)

        # Relatorio do lote
        logger.info(f"\n--- Lote {inicio + 1}-{fim} concluido: "
                     f"{contadores_lote['novos']} novos, "
                     f"{contadores_lote['atualizados']} atualizados, "
                     f"{contadores_lote['pulados']} pulados, "
                     f"{contadores_lote['erros']} erros ---")

        inicio = fim

        if inicio >= total:
            break

        logger.info(f"Iniciando proximo lote... (restam {total - inicio})")
        time.sleep(3)

    # Relatorio final
    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL")
    logger.info("=" * 60)
    logger.info(f"Total no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info(f"Todos os empreendimentos foram processados!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

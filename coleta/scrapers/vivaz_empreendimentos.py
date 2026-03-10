"""
Scraper de empreendimentos Vivaz (grupo Cyrela).
=================================================
Usa API REST interna do site meuvivaz.com.br.
O site e uma SPA React que consome endpoints JSON.

Endpoints utilizados:
  POST /imovel/produto-por-estado  -> lista todos os empreendimentos
  POST /imovel/informacoes/        -> detalhes de um empreendimento
  POST /imovel/lazer/              -> itens de lazer

Uso:
    python scrapers/vivaz_empreendimentos.py
    python scrapers/vivaz_empreendimentos.py --limite 3
    python scrapers/vivaz_empreendimentos.py --atualizar
"""

import os
import sys
import re
import time
import json
import logging
import argparse
import requests
from html import unescape
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

# ============================================================
# CONFIGURACAO
# ============================================================

EMPRESA = "Vivaz"
BASE_URL = "https://www.meuvivaz.com.br"
IMOVEL_ORIGEM_ID = 3  # ID interno da Vivaz na API Cyrela
DELAY = 1  # segundos entre requisicoes de detalhe
PROGRESSO_FILE = os.path.join(LOGS_DIR, "vivaz_empreendimentos_progresso.json")

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": f"{BASE_URL}/",
    "Origin": BASE_URL,
}

# ============================================================
# LOGGING
# ============================================================
os.makedirs(LOGS_DIR, exist_ok=True)
logger = logging.getLogger("scraper.vivaz")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOGS_DIR, "vivaz_empreendimentos.log"), encoding="utf-8")
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
# API HELPERS
# ============================================================

def api_post(path, body, timeout=20):
    """Faz POST na API da Vivaz e retorna JSON."""
    url = f"{BASE_URL}{path}"
    try:
        resp = requests.post(url, headers=HEADERS, json=body, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.warning(f"API {path}: status {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Erro API {path}: {e}")
        return None


def listar_empreendimentos():
    """Lista todos os empreendimentos via API."""
    data = api_post("/imovel/produto-por-estado", {"ImovelOrigemId": IMOVEL_ORIGEM_ID}, timeout=30)
    if data and data.get("success"):
        return data.get("Imoveis", [])
    return []


def buscar_detalhes(slug):
    """Busca detalhes de um empreendimento pelo slug."""
    data = api_post("/imovel/informacoes/", {"Url": slug})
    if data and data.get("success"):
        return data.get("imovel", {})
    return {}


def buscar_lazer(slug):
    """Busca itens de lazer de um empreendimento."""
    data = api_post("/imovel/lazer/", {"Url": slug})
    if data and data.get("success"):
        return data.get("Lazer", [])
    return []


# ============================================================
# HELPERS DE TEXTO
# ============================================================

def limpar_html(texto):
    """Remove tags HTML e decodifica entidades."""
    if not texto:
        return ""
    texto = unescape(texto)
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = re.sub(r"&nbsp;", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def mapear_fase(status):
    """Mapeia status da API para fase padrao do banco."""
    if not status:
        return None
    mapa = {
        "lançamento": "Lançamento",
        "lancamento": "Lançamento",
        "em breve": "Breve Lançamento",
        "em obras": "Em Construção",
        "pronto": "Imóvel Pronto",
        "pronto para morar": "Imóvel Pronto",
    }
    return mapa.get(status.lower(), status)


def extrair_renda(renda_str):
    """Converte '2500,00' para float 2500.0"""
    if not renda_str:
        return None
    try:
        return float(renda_str.replace(".", "").replace(",", "."))
    except (ValueError, TypeError):
        return None


def extrair_preco(preco):
    """Converte preco da API para float."""
    if preco is None:
        return None
    if isinstance(preco, (int, float)):
        return float(preco) if preco > 0 else None
    if isinstance(preco, str):
        try:
            return float(preco.replace(".", "").replace(",", "."))
        except (ValueError, TypeError):
            return None
    return None


def formatar_endereco(endereco_dict):
    """Formata endereco a partir do dict da API."""
    if not endereco_dict:
        return None
    rua = endereco_dict.get("Rua", "")
    numero = endereco_dict.get("Numero", "")
    if rua:
        if numero:
            return f"{rua}, {numero}"
        return rua
    return None


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_item_listagem(item):
    """Converte item da listagem para formato do banco."""
    nome = item.get("Nome", "").strip()
    if not nome:
        return None

    slug = item.get("UrlAmigavel", "")
    endereco_dict = item.get("Endereco", {})

    # UF de 2 letras
    uf = (endereco_dict.get("Uf") or "").upper()
    if len(uf) != 2:
        estado_nome = endereco_dict.get("Estado", "")
        uf_map = {"são paulo": "SP", "rio de janeiro": "RJ", "rio grande do sul": "RS"}
        uf = uf_map.get(estado_nome.lower(), uf)

    dados = {
        "empresa": EMPRESA,
        "nome": nome,
        "slug": slug,
        "url_fonte": f"{BASE_URL}/empreendimentos/{slug}" if slug else BASE_URL,
        "cidade": endereco_dict.get("Cidade"),
        "estado": uf if uf else endereco_dict.get("Estado"),
        "bairro": endereco_dict.get("Bairro"),
        "endereco": formatar_endereco(endereco_dict),
        "fase": mapear_fase(item.get("Status")),
        "dormitorios_descricao": item.get("Quartos"),
        "numero_vagas": str(item.get("VagasGaragem", "")) if item.get("VagasGaragem") else None,
    }

    # Preco
    preco = extrair_preco(item.get("PrecoMinimo"))
    if preco and preco > 50000:
        dados["preco_a_partir"] = preco

    # Area
    area = item.get("AreaPrivativaM2")
    if area and area > 0:
        dados["area_min_m2"] = area
        dados["area_max_m2"] = area

    return dados


def enriquecer_com_detalhes(dados, slug):
    """Enriquece dados com informacoes do endpoint de detalhes."""
    detalhe = buscar_detalhes(slug)
    if not detalhe:
        return dados

    # Localizacao mais precisa
    loc = detalhe.get("Localizacao", {})
    if loc:
        if loc.get("Bairro"):
            dados["bairro"] = loc["Bairro"]
        if loc.get("Cidade"):
            dados["cidade"] = loc["Cidade"]
        uf = (loc.get("Uf") or "").upper()
        if len(uf) == 2:
            dados["estado"] = uf

    # Renda minima
    renda = extrair_renda(detalhe.get("RendaAPartirDe"))
    if renda and renda > 0:
        dados["renda_minima"] = renda

    # Preco
    preco = extrair_preco(detalhe.get("PrecoMinimo"))
    if preco and preco > 50000:
        dados["preco_a_partir"] = preco

    # Descricao para texto completo
    descricao = limpar_html(detalhe.get("Descricao", ""))
    meta = limpar_html(detalhe.get("MetaDescription", ""))
    texto_legal = limpar_html(detalhe.get("TextoLegal", ""))

    # Texto completo para deteccao de atributos
    texto_partes = [
        dados.get("nome", ""),
        descricao,
        meta,
        texto_legal,
        dados.get("dormitorios_descricao", "") or "",
        dados.get("fase", "") or "",
    ]
    texto_completo = " ".join(p for p in texto_partes if p)

    # Metragens do slug ou descricao
    metragens = re.findall(r"(\d+(?:[.,]\d+)?)\s*m[²2]", texto_completo)
    if metragens:
        nums = []
        for m in metragens:
            v = float(m.replace(",", "."))
            if 15.0 <= v <= 500.0:
                nums.append(v)
        if nums:
            dados["area_min_m2"] = min(nums)
            dados["area_max_m2"] = max(nums)
            dados["metragens_descricao"] = " | ".join(f"{v}m²" for v in sorted(set(nums)))

    return dados, texto_completo


def enriquecer_com_lazer(dados, slug, texto_completo=""):
    """Enriquece dados com itens de lazer."""
    lazer_items = buscar_lazer(slug)

    # Lazer da API
    itens_lazer = []
    if lazer_items:
        for item in lazer_items:
            if isinstance(item, str):
                itens_lazer.append(item.strip())
            elif isinstance(item, dict):
                nome = item.get("Nome") or item.get("nome") or item.get("Descricao") or ""
                if nome:
                    itens_lazer.append(nome.strip())

    # Se nao veio da API, tentar detectar do texto
    if not itens_lazer and texto_completo:
        termos = [
            "piscina", "churrasqueira", "fitness", "academia", "playground",
            "brinquedoteca", "salão de festas", "pet care", "coworking",
            "bicicletário", "quadra", "delivery", "horta", "lavanderia",
            "rooftop", "sauna", "spa", "gourmet", "salão de jogos",
            "solarium", "cinema", "deck",
        ]
        texto_lower = texto_completo.lower()
        for termo in termos:
            if termo in texto_lower:
                itens_lazer.append(termo.title())

    if itens_lazer:
        dados["itens_lazer"] = " | ".join(sorted(set(itens_lazer)))

    # Atributos binarios
    texto_total = texto_completo + " " + " ".join(itens_lazer)
    atributos = detectar_atributos_binarios(texto_total)
    dados.update(atributos)

    return dados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Vivaz")
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
    logger.info(f"SCRAPER VIVAZ (meuvivaz.com.br)")
    logger.info(f"Empreendimentos no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)

    # Fase 1: Listar todos os empreendimentos
    logger.info("[FASE 1] Listando empreendimentos via API...")
    items = listar_empreendimentos()

    if not items:
        logger.error("Nenhum empreendimento retornado pela API!")
        return

    logger.info(f"Total de empreendimentos na API: {len(items)}")

    if args.limite > 0:
        items = items[:args.limite]
        logger.info(f"Limitado a {args.limite} empreendimentos")

    # Filtrar ja processados
    if not args.atualizar:
        items = [item for item in items if item.get("UrlAmigavel", "") not in progresso["processados"]]
        logger.info(f"Pendentes: {len(items)}")

    if not items:
        logger.info("Todos os empreendimentos ja foram processados.")
        return

    # Fase 2: Processar cada empreendimento
    logger.info(f"[FASE 2] Processando {len(items)} empreendimentos...")
    novos = 0
    atualizados = 0
    erros = 0
    pulados = 0

    for i, item in enumerate(items, 1):
        nome = item.get("Nome", "?")
        slug = item.get("UrlAmigavel", "")
        logger.info(f"\n[{i}/{len(items)}] {nome}")

        try:
            # Dados basicos da listagem
            dados = processar_item_listagem(item)
            if not dados:
                logger.warning(f"  Dados basicos vazios, pulando")
                erros += 1
                continue

            # Enriquecer com detalhes
            resultado = enriquecer_com_detalhes(dados, slug)
            if isinstance(resultado, tuple):
                dados, texto_completo = resultado
            else:
                dados = resultado
                texto_completo = ""

            time.sleep(DELAY)

            # Enriquecer com lazer
            dados = enriquecer_com_lazer(dados, slug, texto_completo)

            # Verificar existencia
            existe = empreendimento_existe(EMPRESA, dados["nome"])

            if existe and not args.atualizar:
                logger.info(f"  Ja existe, pulando.")
                pulados += 1
            elif existe and args.atualizar:
                atualizar_empreendimento(EMPRESA, dados["nome"], dados)
                atualizados += 1
                logger.info(f"  Atualizado.")
            else:
                inserir_empreendimento(dados)
                novos += 1
                logger.info(f"  Inserido: {dados['nome']} | {dados.get('fase', 'N/A')} | {dados.get('bairro', '?')}, {dados.get('cidade', '?')}-{dados.get('estado', '?')}")

            progresso["processados"].append(slug)
            salvar_progresso(progresso)

        except Exception as e:
            logger.error(f"  Erro: {e}")
            erros += 1
            progresso["erros"].append(slug)
            salvar_progresso(progresso)

        time.sleep(DELAY)

    # Relatorio final
    logger.info("\n" + "=" * 60)
    logger.info("RELATORIO FINAL - VIVAZ")
    logger.info(f"  Novos: {novos}")
    logger.info(f"  Atualizados: {atualizados}")
    logger.info(f"  Pulados: {pulados}")
    logger.info(f"  Erros: {erros}")
    logger.info(f"  Total no banco: {contar_empreendimentos(EMPRESA)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

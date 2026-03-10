"""
Scraper MRV - API GraphQL
=========================
A MRV expoe uma API GraphQL interna acessivel via GET simples,
sem autenticacao. Os dados chegam em JSON estruturado, eliminando
a necessidade de Selenium ou parsing de HTML.

Endpoint base:
  https://mrv.com.br/graphql/execute.json/mrv/search-result-card
  ;basePath=/content/dam/mrv/content-fragments/detalhe-empreendimento/{estado}/

Uso:
    python mrv_empreendimentos.py
    python mrv_empreendimentos.py --estado sao-paulo
    python mrv_empreendimentos.py --limite 10
    python mrv_empreendimentos.py --atualizar
    python mrv_empreendimentos.py --testar
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

EMPRESA_NOME = "MRV"
DELAY = 2  # segundos entre requisicoes por estado

# URL base da API GraphQL
API_BASE = (
    "https://mrv.com.br/graphql/execute.json/mrv/search-result-card"
    ";basePath=/content/dam/mrv/content-fragments/detalhe-empreendimento/{estado}/"
)

# Estados onde a MRV atua (slugs conforme padrao da URL)
# 22 estados + DF conforme informado pela propria empresa
ESTADOS = [
    "sao-paulo",
    "minas-gerais",
    "rio-de-janeiro",
    "parana",
    "santa-catarina",
    "rio-grande-do-sul",
    "goias",
    "bahia",
    "pernambuco",
    "ceara",
    "espirito-santo",
    "mato-grosso-do-sul",
    "mato-grosso",
    "para",
    "maranhao",
    "paraiba",
    "rio-grande-do-norte",
    "alagoas",
    "sergipe",
    "amazonas",
    "tocantins",
    "piaui",
    "distrito-federal",
]

HEADERS = {
    "User-Agent": REQUESTS["headers"]["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": "https://mrv.com.br/",
}

# ============================================================
# LOGGING
# ============================================================

os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "mrv.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# HELPERS DE TEXTO
# ============================================================

def limpar_texto(texto):
    """Remove encoding HTML e espacos extras."""
    if not texto:
        return None
    # Decodifica entidades HTML (&atilde; -> ã, &otilde; -> õ, etc.)
    texto = unescape(texto)
    # Decodifica unicode escapado (\u00e3 -> ã)
    texto = texto.encode("utf-8").decode("unicode_escape") if "\\u" in texto else texto
    # Remove hifens usados como separador em slugs (ex: "S~ao-Paulo" -> "São Paulo")
    # Ja tratado pelo unescape acima na maioria dos casos
    texto = texto.replace("-", " ").strip()
    # Normaliza espacos multiplos
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto if texto else None


def limpar_slug_cidade(slug):
    """Converte slug de cidade/bairro para texto legivel."""
    if not slug:
        return None
    # Decodifica HTML entities primeiro
    texto = unescape(slug)
    # Trata ~u00e3 e similares (encoding nao-padrao encontrado na API)
    texto = re.sub(r"~u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), texto)
    # Substitui hifens por espacos
    texto = texto.replace("-", " ")
    # Normaliza espacos
    texto = re.sub(r"\s+", " ", texto).strip()
    # Title case para nomes proprios
    return texto.title() if texto else None


def extrair_preco(preco_str):
    """Converte 'R$ 245.000,00' para float 245000.0"""
    if not preco_str:
        return None
    # Remove 'R$', espacos, pontos de milhar; troca virgula por ponto
    numeros = re.sub(r"[R$\s\.]", "", preco_str).replace(",", ".")
    try:
        return float(numeros)
    except (ValueError, TypeError):
        return None


def mapear_fase(status_api):
    """
    Mapeia statusImovel da API para fases padrao do banco.
    Valores conhecidos: Lançamento, Em Construção, Prontos,
    Breve Lançamento, Pré-Lançamento, Aluguel
    """
    if not status_api:
        return None
    mapa = {
        "lançamento": "Lançamento",
        "lancamento": "Lançamento",
        "em construção": "Em Construção",
        "em construcao": "Em Construção",
        "prontos": "Imóvel Pronto",
        "pronto": "Imóvel Pronto",
        "breve lançamento": "Breve Lançamento",
        "breve lancamento": "Breve Lançamento",
        "pré-lançamento": "Pré-Lançamento",
        "pre-lancamento": "Pré-Lançamento",
        "aluguel": "Aluguel",
    }
    return mapa.get(status_api.lower(), status_api)


def extrair_dormitorios(apresentacao_str):
    """
    Extrai descricao de dormitorios do campo 'apresentacao' da API.
    Ex: '2 dormit&oacute;rios' -> '2 dormitórios'
    """
    if not apresentacao_str:
        return None
    return unescape(apresentacao_str)


# ============================================================
# SCRAPING
# ============================================================

def buscar_estado(estado_slug):
    """
    Faz GET na API GraphQL para um estado e retorna lista de items.
    Retorna None em caso de erro.
    """
    url = API_BASE.format(estado=estado_slug)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 404:
            logger.warning(f"Estado sem dados na API: {estado_slug}")
            return []
        resp.raise_for_status()
        dados = resp.json()
        items = dados.get("data", {}).get("empreendimentosList", {}).get("items", [])
        logger.info(f"Estado {estado_slug}: {len(items)} empreendimentos encontrados")
        return items
    except requests.RequestException as e:
        logger.error(f"Erro ao buscar estado {estado_slug}: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erro ao parsear JSON para estado {estado_slug}: {e}")
        return None


def processar_item(item, estado_slug):
    """
    Converte um item da API GraphQL para o formato do banco.
    """
    # Campos basicos
    nome = item.get("nomeImovel", "").strip()
    if not nome:
        return None

    slug = item.get("_path", "")

    # Localizacao - API retorna com encoding misto
    cidade_raw = item.get("cidade", "")
    bairro_raw = item.get("bairro", "")
    estado_raw = item.get("estado", "")

    cidade = limpar_slug_cidade(cidade_raw)
    bairro = limpar_slug_cidade(bairro_raw)
    estado_nome = limpar_texto(estado_raw) if estado_raw else estado_slug.replace("-", " ").title()

    # Status e tipo
    status = unescape(item.get("statusImovel", "") or "")
    fase = mapear_fase(status)

    tipo_imovel = unescape(item.get("tipoImovel", "") or "")

    # Classificacao (CVA = Casa Verde e Amarela = MCMV)
    classificacao = unescape(item.get("classificacaoImovel", "") or "")
    mcmv = item.get("minhaCasaMinhaVida", False)

    # Dormitorios
    apresentacao = extrair_dormitorios(item.get("apresentacao", ""))

    # Preco
    condicoes = item.get("condicoesPagamento") or {}
    preco_str = condicoes.get("preco", "")
    preco = extrair_preco(preco_str)

    # URL da pagina do empreendimento
    # O _path retorna algo como /content/dam/mrv/.../slug
    # A URL publica segue o padrao mrv.com.br/{estado}/{slug-final}
    path_parts = slug.strip("/").split("/") if slug else []
    slug_final = path_parts[-1] if path_parts else ""
    url_fonte = f"https://mrv.com.br/{estado_slug}/{slug_final}" if slug_final else "https://mrv.com.br"

    # Selos e promocoes para texto de atributos
    # A API pode retornar selos como lista de strings ou lista de dicts
    selos_raw = item.get("selos") or []
    selos = []
    for s in selos_raw:
        if isinstance(s, str):
            selos.append(s)
        elif isinstance(s, dict):
            selos.append(s.get("nome", "") or s.get("value", ""))

    promocoes_raw = item.get("promocoesImovel") or []
    promocoes = []
    for p in promocoes_raw:
        if isinstance(p, str):
            promocoes.append(p)
        elif isinstance(p, dict):
            promocoes.append(p.get("nomeDaPromocao", "") or p.get("nome", ""))

    # Monta texto completo para deteccao de atributos binarios
    texto_completo_partes = [
        nome,
        apresentacao or "",
        tipo_imovel,
        classificacao,
        fase or "",
        " ".join(selos),
        " ".join(promocoes),
        "minha casa minha vida" if mcmv else "",
    ]
    texto_completo = " ".join(p for p in texto_completo_partes if p)

    atributos_binarios = detectar_atributos_binarios(texto_completo)

    # Se classificacao CVA ou flag mcmv, forca prog_mcmv = 1
    if mcmv or "cva" in classificacao.lower():
        atributos_binarios["prog_mcmv"] = 1

    # Monta registro
    registro = {
        "empresa": EMPRESA_NOME,
        "nome": nome,
        "slug": slug_final,
        "url_fonte": url_fonte,
        "cidade": cidade,
        "estado": estado_nome,
        "bairro": bairro,
        "fase": fase,
        "preco_a_partir": preco,
        "dormitorios_descricao": apresentacao,
        "data_coleta": datetime.now().isoformat(),
    }

    # Remove campos None para nao sobrescrever valores existentes com NULL
    registro = {k: v for k, v in registro.items() if v is not None}
    registro.update(atributos_binarios)

    return registro


def scrape_estado(estado_slug, limite=None, atualizar=False):
    """Scrapa todos os empreendimentos de um estado."""
    items = buscar_estado(estado_slug)
    if items is None:
        return 0, 0

    inseridos = 0
    atualizados = 0
    ignorados = 0

    if limite:
        items = items[:limite]

    for item in items:
        registro = processar_item(item, estado_slug)
        if not registro:
            continue

        nome = registro["nome"]
        existe = empreendimento_existe(EMPRESA_NOME, nome)

        if existe:
            if atualizar:
                atualizar_empreendimento(EMPRESA_NOME, nome, registro)
                atualizados += 1
            else:
                ignorados += 1
        else:
            inserir_empreendimento(registro)
            inseridos += 1

    logger.info(
        f"Estado {estado_slug}: {inseridos} inseridos, "
        f"{atualizados} atualizados, {ignorados} ignorados"
    )
    return inseridos, atualizados


def scrape_todos(estados=None, limite=None, atualizar=False):
    """Scrapa todos os estados configurados."""
    estados = estados or ESTADOS
    total_inseridos = 0
    total_atualizados = 0

    inicio = datetime.now()
    logger.info(f"Iniciando scraping MRV | {len(estados)} estados | {inicio.strftime('%H:%M:%S')}")

    for i, estado in enumerate(estados, 1):
        logger.info(f"[{i}/{len(estados)}] Processando: {estado}")
        ins, atu = scrape_estado(estado, limite=limite, atualizar=atualizar)
        total_inseridos += ins
        total_atualizados += atu

        if i < len(estados):
            time.sleep(DELAY)

    duracao = (datetime.now() - inicio).seconds
    total_banco = contar_empreendimentos(EMPRESA_NOME)

    logger.info(
        f"\n=== MRV CONCLUIDO ==="
        f"\n  Inseridos: {total_inseridos}"
        f"\n  Atualizados: {total_atualizados}"
        f"\n  Total no banco: {total_banco}"
        f"\n  Duracao: {duracao}s"
    )

    return total_inseridos, total_atualizados


# ============================================================
# TESTE
# ============================================================

def testar():
    """Busca 3 empreendimentos de Sao Paulo e exibe o resultado."""
    logger.info("=== MODO TESTE: 3 empreendimentos de sao-paulo ===")
    items = buscar_estado("sao-paulo")
    if not items:
        logger.error("Sem dados retornados. Verifique a URL da API.")
        return

    for item in items[:3]:
        registro = processar_item(item, "sao-paulo")
        if registro:
            print(json.dumps(
                {k: v for k, v in registro.items() if not k.startswith("lazer_") and not k.startswith("apto_") and not k.startswith("prog_")},
                ensure_ascii=False,
                indent=2
            ))
            print(f"  prog_mcmv: {registro.get('prog_mcmv', 0)}")
            print(f"  apto_1_dorm: {registro.get('apto_1_dorm', 0)}, apto_2_dorms: {registro.get('apto_2_dorms', 0)}")
            print("---")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper MRV via API GraphQL")
    parser.add_argument("--estado", help="Processar apenas este estado (slug, ex: sao-paulo)")
    parser.add_argument("--limite", type=int, help="Limite de empreendimentos por estado (para testes)")
    parser.add_argument("--atualizar", action="store_true", help="Atualizar registros existentes")
    parser.add_argument("--testar", action="store_true", help="Modo teste: 3 empreendimentos de SP")
    args = parser.parse_args()

    if args.testar:
        testar()
    elif args.estado:
        scrape_estado(args.estado, limite=args.limite, atualizar=args.atualizar)
    else:
        scrape_todos(limite=args.limite, atualizar=args.atualizar)

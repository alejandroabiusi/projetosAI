"""
Requalificacao de empreendimentos de empresas NOVAS.
====================================================
Revisita URLs e re-extrai dados com validacao rigorosa.
PROTEGE dados de 77 empresas originais (nunca altera).

Uso:
    python scripts/requalificar_completo.py --limite 3000
    python scripts/requalificar_completo.py --empresa "Nome"
    python scripts/requalificar_completo.py --dry-run --limite 10
"""
import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

import os
import sqlite3
import requests
import re
import json
import time
import argparse
import logging
import unicodedata
from datetime import datetime
from collections import defaultdict
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("requalificar")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
DIVERGENCIAS_PATH = os.path.join(PROJECT_ROOT, "build_logs", "divergencias.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# 77 empresas originais — NUNCA alterar
EMPRESAS_ORIGINAIS = {
    "Tenda", "MRV", "Cury", "VIC Engenharia", "Plano&Plano",
    "Vitta Residencial", "Magik JC", "Direcional", "Metrocasa", "Vivaz",
    "EBM", "Trisul", "HM Engenharia", "Grafico", "Pacaembu",
    "Econ Construtora", "Vibra Residencial", "CAC", "Kazzas", "Conx",
    "MGF", "Vega", "Canopus Construcoes", "Novolar", "Graal Engenharia",
    "Árbore", "Viva Benx", "Quartzo", "Vasco Construtora", "Mundo Apto",
    "Vinx", "SR Engenharia", "Canopus", "Riformato", "AP Ponto",
    "SUGOI", "ACLF", "Exata", "Somos", "Emccamp",
    "BM7", "FYP Engenharia", "Stanza", "Sousa Araujo", "Vila Brasil",
    "Smart Construtora", "EPH", "Cosbat", "Belmais", "Jotanunes",
    "Cavazani", "Victa Engenharia", "SOL Construtora", "Morana", "Lotus",
    "House Inc", "ART Construtora", "Rev3", "Carrilho", "BP8",
    "ACL Incorporadora", "Maccris", "Ampla", "Tenório Simões",
    "Grupo Delta", "Domma", "Dimensional", "Mirantes", "Torreão Villarim",
    "Bora", "Novvo", "Você", "M.Lar", "Construtora Open",
    "Ún1ca", "Versati", "Sertenge",
}

FASES_CANONICAS = [
    "Breve Lançamento", "Lançamento", "Em Construção",
    "Pronto para Morar", "100% Vendido",
]

# Mapeamento de lazer
LAZER_MAP = {
    "lazer_piscina": ["piscina"],
    "lazer_churrasqueira": ["churrasqueira", "churrasq"],
    "lazer_fitness": ["fitness", "academia", "sala funcional", "ginástica", "ginastica"],
    "lazer_playground": ["playground"],
    "lazer_brinquedoteca": ["brinquedoteca"],
    "lazer_salao_festas": ["salão de festas", "salao de festas", "salão festas", "salao festas"],
    "lazer_pet_care": ["pet care", "pet place", "pet"],
    "lazer_coworking": ["coworking", "co-working"],
    "lazer_bicicletario": ["bicicletário", "bicicletario"],
    "lazer_quadra": ["quadra", "mini quadra", "beach tennis"],
    "lazer_delivery": ["delivery", "sala delivery"],
    "lazer_horta": ["horta"],
    "lazer_lavanderia": ["lavanderia"],
    "lazer_redario": ["redário", "redario"],
    "lazer_rooftop": ["rooftop"],
    "lazer_sauna": ["sauna"],
    "lazer_spa": ["spa"],
    "lazer_piquenique": ["piquenique", "picnic"],
    "lazer_sport_bar": ["sport bar", "sportbar"],
    "lazer_cine": ["cinema", "cine open", "cine "],
    "lazer_easy_market": ["easy market", "mini market", "mini mercado"],
    "lazer_espaco_beleza": ["espaço beleza", "espaco beleza", "beleza"],
    "lazer_sala_estudos": ["sala de estudos", "estudos"],
    "lazer_espaco_gourmet": ["gourmet", "espaço gourmet"],
    "lazer_praca": ["praça", "praca"],
    "lazer_solarium": ["solarium", "solário"],
    "lazer_sala_jogos": ["sala de jogos", "jogos", "game"],
    "lazer_varanda": ["varanda gourmet"],
}

# UFs brasileiras validas
UFS_BRASIL = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}

# Bairros conhecidos (nao sao cidades)
BAIRROS_CONHECIDOS = {
    "butanta", "mooca", "tatupe", "vila prudente", "penha",
    "pinheiros", "itaim bibi", "moema", "lapa", "perdizes",
    "santana", "tucuruvi", "mandaqui", "jacana",
    "copacabana", "ipanema", "leblon", "botafogo", "flamengo",
    "tijuca", "graja", "barra da tijuca", "recreio", "jacarepagua",
    "meier", "campo grande", "santa cruz", "bangu", "realengo",
    "brooklin", "vila olimpia", "consolacao", "bela vista",
    "higienopolis", "republica", "santa cecilia", "vila mariana",
}


def _sem_acento(s):
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def _parse_numero_br(s):
    """Parseia numero no formato brasileiro: 1.234,56 -> 1234.56, 43,35 -> 43.35."""
    if not s:
        return None
    s = s.strip()
    # Se tem virgula, eh decimal brasileiro
    if "," in s:
        # Remove pontos (separador milhar) e troca virgula por ponto
        s = s.replace(".", "").replace(",", ".")
    else:
        # Sem virgula: se tem ponto, pode ser milhar ou decimal
        # Se padrao X.XXX (3 digitos apos ponto), eh milhar
        if re.match(r'^\d+\.\d{3}$', s):
            s = s.replace(".", "")
        # Senao, ponto eh decimal (43.35)
    try:
        return float(s)
    except ValueError:
        return None


# ============================================================
# EXTRACAO DE DADOS
# ============================================================

def _limpar_html(soup):
    """Remove nav, footer, header, aside, menu ANTES de extrair.
    Re-parseia o HTML para evitar efeitos colaterais do decompose.
    """
    from bs4 import BeautifulSoup as _BS
    # Re-parsear para nao afetar o soup original
    clean = _BS(str(soup), "html.parser")
    for tag in clean.find_all(["nav", "footer", "header", "aside"]):
        tag.decompose()
    # Remover elementos de menu/sidebar, mas SOMENTE tags menores (nao body/main/section)
    tags_safe_to_remove = {"div", "ul", "ol", "li", "a", "span", "p"}
    for cls in ["sidebar", "breadcrumb", "cookie-notice", "popup"]:
        for el in clean.find_all(class_=lambda c: c and cls in str(c).lower()):
            if el.name in tags_safe_to_remove:
                el.decompose()
    return clean


def _validar_cidade(cidade):
    """Valida se uma cidade e plausivel. Retorna cidade limpa ou None."""
    if not cidade or not cidade.strip():
        return None
    cidade = cidade.strip()

    # Comprimento
    if len(cidade) < 3 or len(cidade) > 40:
        return None

    # Texto juridico/lixo
    lower = cidade.lower()
    lixo = ["cartório", "cartorio", "ofício", "oficio", "registro", "comarca",
            "simulação", "simulacao", "facebook", "instagram", "http", "www.",
            ".com", "whatsapp", "cookie", "privacidade", "lgpd", "newsletter",
            "apartamento", "construtora", "incorporadora", "empreendimento",
            "condomínio", "condominio", "residencial", "@"]
    for l in lixo:
        if l in lower:
            return None

    # Quebra de linha
    if "\n" in cidade or "\r" in cidade:
        return None

    # Bairro como cidade
    if _sem_acento(cidade) in BAIRROS_CONHECIDOS:
        return None

    # Parece numero / CEP
    if re.match(r'^\d+', cidade):
        return None

    return cidade


def _validar_uf(uf):
    """Valida UF."""
    if not uf:
        return None
    uf = uf.strip().upper()
    if uf in UFS_BRASIL:
        return uf
    return None


def _extrair_endereco_cidade(soup, url):
    """Extrai endereco, cidade, estado, bairro de multiplas fontes."""
    resultado = {}

    # 1. Meta tags OG
    for meta_name, campo in [
        ("og:locality", "cidade"),
        ("og:region", "estado"),
        ("og:street-address", "endereco"),
    ]:
        tag = soup.find("meta", property=meta_name)
        if tag and tag.get("content"):
            val = tag["content"].strip()
            if campo == "cidade":
                val = _validar_cidade(val)
            elif campo == "estado":
                val = _validar_uf(val)
            if val:
                resultado[campo] = val

    # 2. Schema.org JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    _extrair_schema_address(item, resultado)
            elif isinstance(data, dict):
                _extrair_schema_address(data, resultado)
        except (json.JSONDecodeError, TypeError):
            pass

    # 3. Texto estruturado
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)

    if "cidade" not in resultado or "endereco" not in resultado:
        # Padrao "Bairro - Cidade/UF" ou "Bairro, Cidade - UF"
        patterns = [
            r'(?:Endereço|Localização|Local)\s*[:]\s*(.+)',
            r'([A-Z][a-záàãâéêíóôõúç]+(?:\s+[A-Za-záàãâéêíóôõúç]+)*)\s*[-–]\s*([A-Z][a-záàãâéêíóôõúç\s]+?)\s*[/\-]\s*([A-Z]{2})\b',
        ]
        for pat in patterns:
            m = re.search(pat, texto)
            if m:
                if m.lastindex == 1:
                    # Endereco completo em uma linha
                    end_line = m.group(1).strip()
                    if len(end_line) < 200 and "endereco" not in resultado:
                        resultado["endereco"] = end_line
                elif m.lastindex >= 3:
                    bairro_cand = m.group(1).strip()
                    cidade_cand = _validar_cidade(m.group(2).strip())
                    uf_cand = _validar_uf(m.group(3).strip())
                    if cidade_cand and "cidade" not in resultado:
                        resultado["cidade"] = cidade_cand
                    if uf_cand and "estado" not in resultado:
                        resultado["estado"] = uf_cand
                    if bairro_cand and len(bairro_cand) < 50 and "bairro" not in resultado:
                        resultado["bairro"] = bairro_cand

        # Tentar extrair cidade do title: "Nome - Cidade/UF" ou "Nome em Cidade"
        title_tag = soup.find("title")
        if title_tag and "cidade" not in resultado:
            title = title_tag.get_text(strip=True)
            # Padrao "... em Cidade - UF" ou "... | Cidade/UF"
            m = re.search(r'(?:em|[-|])\s+([A-Z][a-záàãâéêíóôõúç\s]+?)\s*[-/]\s*([A-Z]{2})\b', title)
            if m:
                cidade_cand = _validar_cidade(m.group(1).strip())
                uf_cand = _validar_uf(m.group(2).strip())
                if cidade_cand:
                    resultado["cidade"] = cidade_cand
                if uf_cand and "estado" not in resultado:
                    resultado["estado"] = uf_cand

        # 4. Extrair cidade/UF do breadcrumb
        if "cidade" not in resultado:
            for el in soup.find_all(class_=lambda c: c and "breadcrumb" in str(c).lower()):
                btxt = el.get_text(separator=" > ", strip=True)
                # "Home > Cidade > Empreendimento" or similar
                m = re.search(r'([A-Z][a-záàãâéêíóôõúç\s]+?)\s*[-/]\s*([A-Z]{2})\b', btxt)
                if m:
                    cidade_cand = _validar_cidade(m.group(1).strip())
                    uf_cand = _validar_uf(m.group(2).strip())
                    if cidade_cand:
                        resultado["cidade"] = cidade_cand
                    if uf_cand and "estado" not in resultado:
                        resultado["estado"] = uf_cand
                    break

        # 5. Extrair cidade da URL: /imoveis/cidade/nome ou /empreendimentos/cidade/nome
        if "cidade" not in resultado:
            try:
                parsed = urlparse(url)
                path_parts = [p for p in parsed.path.strip("/").split("/") if p]
                # Patterns like /imoveis/cidade/slug or /empreendimentos/uf/cidade/slug
                if len(path_parts) >= 2:
                    for idx in range(len(path_parts) - 1):
                        segment = path_parts[idx]
                        # Skip known non-city segments
                        if segment.lower() in {"imoveis", "empreendimentos", "produtos", "empreendimento",
                                                "lancamentos", "imovel", "casas", "apartamentos", "lotes",
                                                "sp", "rj", "mg", "pr", "rs", "sc", "ba", "ce", "pe", "go",
                                                "df", "es", "mt", "ms", "pa", "am", "ma", "al", "se", "rn",
                                                "pb", "pi", "to", "ro", "ac", "rr", "ap"}:
                            continue
                        # Convert slug to city name
                        city_from_url = segment.replace("-", " ").title()
                        city_valid = _validar_cidade(city_from_url)
                        if city_valid and len(city_valid) > 3:
                            resultado["cidade"] = city_valid
                            break
            except Exception:
                pass

        # 6. Extrair de texto com padrao "Cidade - UF" ou "Cidade/UF" no corpo limpo
        if "cidade" not in resultado:
            # Buscar padrao solto no texto (linhas curtas com Cidade - UF)
            for linha in texto.split("\n"):
                linha = linha.strip()
                if len(linha) > 60 or len(linha) < 4:
                    continue
                m = re.match(r'^([A-Z][a-záàãâéêíóôõúç\s]+?)\s*[-/]\s*([A-Z]{2})$', linha)
                if m:
                    cidade_cand = _validar_cidade(m.group(1).strip())
                    uf_cand = _validar_uf(m.group(2).strip())
                    if cidade_cand:
                        resultado["cidade"] = cidade_cand
                    if uf_cand and "estado" not in resultado:
                        resultado["estado"] = uf_cand
                    break

    return resultado


def _extrair_schema_address(data, resultado):
    """Extrai endereco de JSON-LD Schema.org."""
    if not isinstance(data, dict):
        return

    # Buscar address diretamente ou dentro de @graph
    candidates = [data]
    if "@graph" in data:
        candidates.extend(data["@graph"] if isinstance(data["@graph"], list) else [data["@graph"]])

    for item in candidates:
        if not isinstance(item, dict):
            continue
        addr = item.get("address") or (item if item.get("@type") == "PostalAddress" else None)
        if isinstance(addr, dict):
            cidade = _validar_cidade(addr.get("addressLocality", ""))
            uf = _validar_uf(addr.get("addressRegion", ""))
            rua = addr.get("streetAddress", "")
            bairro = addr.get("addressNeighborhood") or addr.get("neighborhood", "")

            if cidade and "cidade" not in resultado:
                resultado["cidade"] = cidade
            if uf and "estado" not in resultado:
                resultado["estado"] = uf
            if rua and len(rua) < 200 and "endereco" not in resultado:
                resultado["endereco"] = rua.strip()
            if bairro and len(bairro) < 50 and "bairro" not in resultado:
                resultado["bairro"] = bairro.strip()

        # geo
        geo = item.get("geo")
        if isinstance(geo, dict):
            lat = geo.get("latitude")
            lon = geo.get("longitude")
            if lat and lon:
                try:
                    lat, lon = float(lat), float(lon)
                    if -35 <= lat <= 6 and -75 <= lon <= -30:
                        if "latitude" not in resultado:
                            resultado["latitude"] = str(lat)
                            resultado["longitude"] = str(lon)
                except (ValueError, TypeError):
                    pass


def _extrair_fase(soup):
    """Extrai fase do empreendimento."""
    clean = _limpar_html(soup)

    # Buscar badges/labels com classe status/fase
    for cls_kw in ["status", "fase", "badge", "label", "tag"]:
        for el in clean.find_all(class_=lambda c: c and cls_kw in str(c).lower()):
            txt = el.get_text(strip=True)
            fase = _mapear_fase(txt)
            if fase:
                return fase

    # Buscar no texto geral
    texto = clean.get_text(separator="\n", strip=True)
    for linha in texto.split("\n"):
        linha = linha.strip()
        if len(linha) > 100:
            continue
        fase = _mapear_fase(linha)
        if fase:
            return fase

    return None


def _mapear_fase(texto):
    """Mapeia texto para fase canonica."""
    if not texto:
        return None
    lower = texto.lower().strip()

    mapa = {
        "breve lançamento": "Breve Lançamento",
        "breve lançamento": "Breve Lançamento",
        "breve lancamento": "Breve Lançamento",
        "em breve": "Breve Lançamento",
        "futuro lançamento": "Breve Lançamento",
        "lançamento": "Lançamento",
        "lancamento": "Lançamento",
        "em construção": "Em Construção",
        "em construcao": "Em Construção",
        "em obras": "Em Construção",
        "obra em andamento": "Em Construção",
        "pronto": "Pronto para Morar",
        "pronto para morar": "Pronto para Morar",
        "pronta entrega": "Pronto para Morar",
        "entrega imediata": "Pronto para Morar",
        "100% vendido": "100% Vendido",
        "esgotado": "100% Vendido",
        "vendido": "100% Vendido",
    }

    for chave, valor in mapa.items():
        if chave in lower:
            return valor

    return None


def _extrair_secao_planta(soup):
    """Tenta encontrar secao de planta/tipologia."""
    clean = _limpar_html(soup)
    kws = ["planta", "tipolog", "ficha", "metragem", "apartamento", "unidade"]
    for kw in kws:
        for tag in clean.find_all(["section", "div"], id=lambda x: x and kw in str(x).lower()):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 20:
                return text
        for tag in clean.find_all(["section", "div"],
                                   class_=lambda x: x and any(kw in str(c).lower() for c in (x if isinstance(x, list) else [x]))):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 20:
                return text
    return None


def _extrair_tipologias(soup):
    """Extrai dormitorios e flags de tipologia de secoes de planta."""
    secao = _extrair_secao_planta(soup)

    # Se a secao nao menciona dormitorios, tentar fallback
    if secao and not re.search(r'(?:dorm|quarto|suíte|suite|studio)', secao, re.IGNORECASE):
        secao = None  # forcar fallback

    if not secao:
        # Fallback: buscar linhas com padrao "X dorms" no texto limpo (mais restrito)
        clean = _limpar_html(soup)
        texto = clean.get_text(separator="\n", strip=True)
        linhas_relevantes = []
        for linha in texto.split("\n"):
            linha = linha.strip()
            if len(linha) > 120:
                continue
            if re.search(r'\d\s*(?:dorm|quarto|suíte|suite)', linha, re.IGNORECASE):
                linhas_relevantes.append(linha)
        if linhas_relevantes:
            secao = "\n".join(linhas_relevantes)
        else:
            return None, {}

    texto_lower = secao.lower()
    flags = {}
    descricao_parts = []

    # Studio — so em contexto de planta/metragem
    if re.search(r'\bstudios?\b', texto_lower):
        flags["apto_studio"] = 1
        descricao_parts.append("Studio")

    # Dormitorios
    # Ranges "2 a 4 dorms"
    m_range = re.search(r'(\d)\s*a\s*(\d)\s*(?:dorm|quarto|suíte|suite)', texto_lower)
    if m_range:
        d_min, d_max = int(m_range.group(1)), int(m_range.group(2))
        for d in range(d_min, d_max + 1):
            if d == 1:
                flags["apto_1_dorm"] = 1
                descricao_parts.append("1 dorm")
            elif d == 2:
                flags["apto_2_dorms"] = 1
                descricao_parts.append("2 dorms")
            elif d == 3:
                flags["apto_3_dorms"] = 1
                descricao_parts.append("3 dorms")
            elif d == 4:
                flags["apto_4_dorms"] = 1
                descricao_parts.append("4 dorms")
    else:
        # Individual
        if re.search(r'\b0?1\s*(?:dorm|quarto)', texto_lower):
            flags["apto_1_dorm"] = 1
            descricao_parts.append("1 dorm")
        if re.search(r'\b0?2\s*(?:dorm|quarto)', texto_lower):
            flags["apto_2_dorms"] = 1
            descricao_parts.append("2 dorms")
        if re.search(r'\b0?3\s*(?:dorm|quarto|suíte|suite)', texto_lower):
            flags["apto_3_dorms"] = 1
            descricao_parts.append("3 dorms")
        if re.search(r'\b0?4\s*(?:dorm|quarto|suíte|suite)', texto_lower):
            flags["apto_4_dorms"] = 1
            descricao_parts.append("4 dorms")

    # Suite
    if re.search(r'su[ií]te', texto_lower):
        flags["apto_suite"] = 1
        descricao_parts.append("c/suíte")

    # Cobertura, duplex, giardino
    if re.search(r'\bcobertura\b', texto_lower):
        flags["apto_cobertura"] = 1
    if re.search(r'\bduplex\b', texto_lower):
        flags["apto_duplex"] = 1
    if re.search(r'\b(?:giardino|garden)\b', texto_lower):
        flags["apto_giardino"] = 1

    descricao = " e ".join(descricao_parts) if descricao_parts else None
    return descricao, flags


def _extrair_metragens(soup):
    """Extrai area_min e area_max de secoes de planta/tipologia."""

    def _parse_areas(text):
        """Extrai areas validas de um texto."""
        pattern = r'(\d[\d.]*(?:,\d+)?)\s*(?:a\s*(\d[\d.]*(?:,\d+)?)\s*)?m[²2]'
        matches = re.findall(pattern, text, re.IGNORECASE)
        areas = []
        for m in matches:
            try:
                v1 = _parse_numero_br(m[0])
                if v1 is not None:
                    areas.append(v1)
                if m[1]:
                    v2 = _parse_numero_br(m[1])
                    if v2 is not None:
                        areas.append(v2)
            except ValueError:
                continue
        return [a for a in areas if 10 <= a <= 500]

    # Tentar secao de planta primeiro
    secao = _extrair_secao_planta(soup)
    if secao:
        areas = _parse_areas(secao)
        if areas:
            return min(areas), max(areas)

    # Fallback: linhas com m2 no texto limpo (excluindo terreno/garagem)
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)
    linhas_m2 = []
    for linha in texto.split("\n"):
        linha = linha.strip()
        if len(linha) > 150:
            continue
        if re.search(r'\d+(?:[.,]\d+)?\s*m[²2]', linha, re.IGNORECASE):
            if not re.search(r'(?:terreno|lote|garagem|vaga|estacionamento|constru[ií]da|total)', linha, re.IGNORECASE):
                linhas_m2.append(linha)
    if linhas_m2:
        secao_fb = "\n".join(linhas_m2)
        areas = _parse_areas(secao_fb)
        if areas:
            return min(areas), max(areas)

    return None, None


def _extrair_secao_lazer(soup):
    """Encontra secao especifica de lazer/amenidades."""
    clean = _limpar_html(soup)
    keywords = ["lazer", "amenidad", "comodidad", "areas-comuns", "áreas comuns",
                 "areas_comuns", "leisure", "infraestrutura", "diferenciais",
                 "condominio", "condomínio"]

    for kw in keywords:
        for tag in clean.find_all(["section", "div"], id=lambda x: x and kw in x.lower()):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return text
        for tag in clean.find_all(["section", "div"],
                                   class_=lambda x: x and any(kw in str(c).lower() for c in (x if isinstance(x, list) else [x]))):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return text

    # Heading com keyword
    for heading in clean.find_all(["h2", "h3", "h4"]):
        htxt = heading.get_text(strip=True).lower()
        if any(kw in htxt for kw in keywords):
            parts = [heading.get_text(strip=True)]
            for sib in heading.find_next_siblings():
                if sib.name in ["h2", "h3", "h4"]:
                    break
                parts.append(sib.get_text(separator="\n", strip=True))
            text = "\n".join(parts)
            if len(text) > 20:
                return text

    return None


def _extrair_lazer(soup):
    """Extrai itens de lazer APENAS de secao especifica."""
    secao = _extrair_secao_lazer(soup)
    if not secao:
        # Sem secao especifica -> NULL (nao buscar no body todo)
        return None, {}

    texto_lower = secao.lower()
    itens = []
    flags = {}

    for coluna, termos in LAZER_MAP.items():
        for termo in termos:
            if termo.lower() in texto_lower:
                flags[coluna] = 1
                item_name = termo.strip().title()
                if item_name not in itens:
                    itens.append(item_name)
                break

    itens_str = " | ".join(sorted(set(itens))) if itens else None
    return itens_str, flags


def _extrair_preco(soup):
    """Extrai preco_a_partir com validacao."""
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)

    patterns = [
        r'(?:a\s*partir\s*(?:de\s*)?)?R\$\s*([\d.,]+)',
        r'(?:desde|por)\s*R\$\s*([\d.,]+)',
    ]
    for pat in patterns:
        for m in re.finditer(pat, texto, re.IGNORECASE):
            try:
                val_str = m.group(1).replace(".", "").replace(",", ".")
                val = float(val_str)
                # Validar: R$50k - R$20M
                if 50_000 <= val <= 20_000_000:
                    return val
            except ValueError:
                continue
    return None


def _extrair_coordenadas(soup):
    """Extrai coordenadas de Google Maps ou JSON-LD."""
    # 1. Google Maps iframe
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src") or iframe.get("data-src") or ""
        m = re.search(r'(?:q=|@|center=|ll=)(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)', src)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)

    # 2. Scripts com lat/lng
    for script in soup.find_all("script"):
        txt = script.string or ""
        m = re.search(r'"lat(?:itude)?"\s*:\s*(-?\d+\.\d+).*?"lng|lon(?:gitude)?"\s*:\s*(-?\d+\.\d+)', txt, re.DOTALL)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)
        m = re.search(r'LatLng\((-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\)', txt)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)

    return None, None


def extrair_dados_pagina(url, html):
    """Extrai todos os dados possiveis com validacao rigorosa."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    dados = {}

    # 1. Endereco/Cidade/Estado/Bairro
    try:
        loc = _extrair_endereco_cidade(soup, url)
        dados.update(loc)
    except Exception as e:
        logger.debug(f"  Erro endereco: {e}")

    # 2. Fase
    try:
        fase = _extrair_fase(soup)
        if fase:
            dados["fase"] = fase
    except Exception as e:
        logger.debug(f"  Erro fase: {e}")

    # 3. Tipologias (APENAS de secao de planta)
    try:
        dorm_desc, flags_dorm = _extrair_tipologias(soup)
        if dorm_desc:
            dados["dormitorios_descricao"] = dorm_desc
        if flags_dorm:
            dados.update(flags_dorm)
    except Exception as e:
        logger.debug(f"  Erro tipologias: {e}")

    # 4. Metragens (APENAS de secao de planta)
    try:
        area_min, area_max = _extrair_metragens(soup)
        if area_min is not None:
            dados["area_min_m2"] = area_min
        if area_max is not None:
            dados["area_max_m2"] = area_max
    except Exception as e:
        logger.debug(f"  Erro metragens: {e}")

    # 5. Lazer (APENAS de secao especifica)
    try:
        itens_lazer, flags_lazer = _extrair_lazer(soup)
        if itens_lazer:
            dados["itens_lazer"] = itens_lazer
        if flags_lazer:
            dados.update(flags_lazer)
    except Exception as e:
        logger.debug(f"  Erro lazer: {e}")

    # 6. Preco
    try:
        preco = _extrair_preco(soup)
        if preco is not None:
            dados["preco_a_partir"] = preco
    except Exception as e:
        logger.debug(f"  Erro preco: {e}")

    # 7. Coordenadas
    try:
        lat, lon = _extrair_coordenadas(soup)
        if lat is not None:
            dados["latitude"] = lat
            dados["longitude"] = lon
    except Exception as e:
        logger.debug(f"  Erro coordenadas: {e}")

    return dados


def main():
    parser = argparse.ArgumentParser(description="Requalificacao de empreendimentos novos")
    parser.add_argument("--limite", type=int, default=100, help="Limite de URLs")
    parser.add_argument("--empresa", type=str, help="Filtrar por empresa")
    parser.add_argument("--dry-run", action="store_true", help="Nao gravar no banco")
    parser.add_argument("--somente-novos", action="store_true", default=True,
                        help="So processar empresas novas (default)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay entre requests (s)")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout do request (s)")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(DIVERGENCIAS_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Construir WHERE
    where_parts = ["url_fonte IS NOT NULL", "url_fonte != ''"]
    params = []

    if args.somente_novos:
        placeholders = ",".join(["?" for _ in EMPRESAS_ORIGINAIS])
        where_parts.append(f"empresa NOT IN ({placeholders})")
        params.extend(sorted(EMPRESAS_ORIGINAIS))

    if args.empresa:
        where_parts.append("empresa = ?")
        params.append(args.empresa)

    where = " AND ".join(where_parts)
    query = f"""
        SELECT id, empresa, nome, url_fonte, prog_mcmv,
               cidade, estado, bairro, endereco, fase,
               dormitorios_descricao, area_min_m2, area_max_m2,
               preco_a_partir, itens_lazer, latitude, longitude
        FROM empreendimentos
        WHERE {where}
        ORDER BY empresa, nome
        LIMIT ?
    """
    params.append(args.limite)

    cur.execute(query, params)
    registros = [dict(r) for r in cur.fetchall()]

    logger.info(f"=== Requalificacao: {len(registros)} empreendimentos de empresas NOVAS ===")
    if not registros:
        logger.info("Nenhum empreendimento para processar.")
        conn.close()
        return

    # Verificar que nenhum e original (safety check)
    for reg in registros:
        if reg["empresa"] in EMPRESAS_ORIGINAIS:
            logger.error(f"ERRO CRITICO: empresa original '{reg['empresa']}' no lote! Abortando.")
            conn.close()
            return

    session = requests.Session()
    session.headers.update(HEADERS)

    # Stats
    total_processados = 0
    total_erros = 0
    total_campos_preenchidos = 0
    total_emps_atualizados = 0
    total_divergencias = 0
    campos_contador = defaultdict(int)
    erros_por_tipo = defaultdict(int)
    divergencias = []

    # Contagem de fases por empresa (para detectar contaminacao)
    fases_empresa = defaultdict(lambda: defaultdict(int))
    for reg in registros:
        if reg["fase"]:
            fases_empresa[reg["empresa"]][reg["fase"]] += 1

    for i, reg in enumerate(registros, 1):
        url = reg["url_fonte"]
        emp_id = reg["id"]
        nome = reg["nome"] or "(sem nome)"
        empresa = reg["empresa"]

        # Progresso a cada 25
        if i % 25 == 0 or i == 1:
            logger.info(f"--- Progresso: {i}/{len(registros)} | processados={total_processados} erros={total_erros} campos={total_campos_preenchidos} ---")

        # Fetch
        try:
            resp = session.get(url, timeout=args.timeout, allow_redirects=True)
            if resp.status_code != 200:
                erros_por_tipo[f"HTTP_{resp.status_code}"] += 1
                total_erros += 1
                time.sleep(args.delay)
                continue

            html = resp.text
            if len(html) < 500:
                erros_por_tipo["pagina_curta"] += 1
                total_erros += 1
                time.sleep(args.delay)
                continue

            # Detectar SPA: pagina com HTML mas sem conteudo renderizado
            from bs4 import BeautifulSoup as _BS
            _quick = _BS(html, "html.parser")
            for _t in _quick.find_all(["nav", "footer", "header", "aside", "script", "style"]):
                _t.decompose()
            _clean_text = _quick.get_text(separator=" ", strip=True)
            if len(_clean_text) < 200:
                erros_por_tipo["SPA_sem_conteudo"] += 1
                total_erros += 1
                time.sleep(args.delay)
                continue

        except requests.exceptions.Timeout:
            erros_por_tipo["timeout"] += 1
            total_erros += 1
            time.sleep(args.delay)
            continue
        except requests.RequestException as e:
            erros_por_tipo[type(e).__name__] += 1
            total_erros += 1
            time.sleep(args.delay)
            continue

        total_processados += 1

        # Extrair dados
        dados = extrair_dados_pagina(url, html)
        if not dados:
            time.sleep(args.delay)
            continue

        # Aplicar regras de gravacao
        campos_para_atualizar = {}

        # Campos de texto/numerico
        campos_check = [
            "cidade", "estado", "bairro", "endereco", "fase",
            "dormitorios_descricao", "area_min_m2", "area_max_m2",
            "preco_a_partir", "itens_lazer", "latitude", "longitude",
        ]

        for campo in campos_check:
            if campo not in dados or dados[campo] is None:
                continue

            novo_val = dados[campo]
            val_atual = reg.get(campo)

            # Caso 1: campo atual NULL/vazio -> gravar
            if val_atual is None or val_atual == "" or val_atual == 0:
                campos_para_atualizar[campo] = novo_val
            else:
                # Caso 2: campo atual tem valor e novo e DIFERENTE -> divergencia
                # Comparar como string para uniformidade
                str_atual = str(val_atual).strip()
                str_novo = str(novo_val).strip()
                if str_atual.lower() != str_novo.lower():
                    divergencias.append({
                        "id": emp_id,
                        "empresa": empresa,
                        "nome": nome,
                        "campo": campo,
                        "valor_atual": str_atual,
                        "valor_novo": str_novo,
                        "url": url,
                    })
                    total_divergencias += 1
                # Caso 3: valores iguais -> nada a fazer

        # Flags binarias de lazer/dorm: so setar para 1 se ainda for 0/NULL
        campos_binarios = [k for k in dados if k.startswith("lazer_") or k.startswith("apto_")]
        for campo in campos_binarios:
            if campo in campos_check:
                continue  # ja tratado acima
            if dados[campo] == 1:
                val_atual = reg.get(campo, 0) or 0
                if not val_atual:
                    campos_para_atualizar[campo] = 1

        if not campos_para_atualizar:
            time.sleep(args.delay)
            continue

        # Validacao adicional de fase: nao aceitar se > 90% da empresa tem a mesma fase
        if "fase" in campos_para_atualizar:
            fase_nova = campos_para_atualizar["fase"]
            total_emp = sum(fases_empresa[empresa].values())
            if total_emp >= 5:
                cnt_fase = fases_empresa[empresa].get(fase_nova, 0)
                if total_emp > 0 and cnt_fase / total_emp > 0.9:
                    # Contaminacao provavel — nao gravar
                    del campos_para_atualizar["fase"]

        if not campos_para_atualizar:
            time.sleep(args.delay)
            continue

        # Gravar
        if not args.dry_run:
            update_cur = conn.cursor()
            for campo, valor in campos_para_atualizar.items():
                update_cur.execute(
                    f"UPDATE empreendimentos SET {campo}=?, data_atualizacao=? WHERE id=?",
                    (valor, datetime.now().isoformat(), emp_id)
                )
                campos_contador[campo] += 1

        total_campos_preenchidos += len(campos_para_atualizar)
        total_emps_atualizados += 1

        # Log detalhado para primeiros e a cada 25
        if i <= 5 or i % 25 == 0:
            campos_lista = list(campos_para_atualizar.keys())
            logger.info(f"  [{i}] {empresa} | {nome}: {len(campos_lista)} campos -> {campos_lista[:6]}")

        # Commit a cada 50
        if not args.dry_run and total_processados % 50 == 0:
            conn.commit()
            logger.info(f"  >> Commit batch {total_processados // 50}")

            # Mini-auditoria do batch
            batch_start = max(1, total_processados - 49)
            logger.info(f"     Mini-auditoria batch {batch_start}-{total_processados}: "
                        f"campos={total_campos_preenchidos}, divergencias={total_divergencias}")

        time.sleep(args.delay)

    # Commit final
    if not args.dry_run:
        conn.commit()

    conn.close()

    # Salvar divergencias
    if divergencias:
        with open(DIVERGENCIAS_PATH, "w", encoding="utf-8") as f:
            json.dump(divergencias, f, ensure_ascii=False, indent=2)
        logger.info(f"Divergencias salvas em: {DIVERGENCIAS_PATH}")

    # Resumo
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO DA REQUALIFICACAO")
    logger.info("=" * 60)
    logger.info(f"URLs processadas:              {total_processados}")
    logger.info(f"Erros (HTTP/timeout):           {total_erros}")
    logger.info(f"Empreendimentos atualizados:    {total_emps_atualizados}")
    logger.info(f"Total campos preenchidos:       {total_campos_preenchidos}")
    logger.info(f"Divergencias encontradas:       {total_divergencias}")
    logger.info("")

    if erros_por_tipo:
        logger.info("Erros por tipo:")
        for tipo, cnt in sorted(erros_por_tipo.items(), key=lambda x: -x[1]):
            logger.info(f"  {tipo:30s}: {cnt}")
        logger.info("")

    if campos_contador:
        logger.info("Campos preenchidos por tipo:")
        for campo, cnt in sorted(campos_contador.items(), key=lambda x: -x[1]):
            logger.info(f"  {campo:30s}: {cnt}")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()

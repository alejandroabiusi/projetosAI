"""
Enriquecimento profundo de empreendimentos novos.
==================================================
Revisita url_fonte de empreendimentos com dados incompletos e extrai:
  - itens_lazer + atributos binarios de lazer
  - area_min_m2, area_max_m2
  - dormitorios_descricao + flags apto_*
  - total_unidades, numero_torres, numero_andares, numero_vagas
  - preco_a_partir
  - evolucao_obra_pct
  - imagem_url
  - coordenadas (Google Maps embed)

Uso:
    python scripts/enriquecer_profundo_novos.py
    python scripts/enriquecer_profundo_novos.py --limite 50
    python scripts/enriquecer_profundo_novos.py --empresa "ACL Incorporadora"
"""
import sys
sys.stdout.reconfigure(errors="replace")
sys.stdout.reconfigure(line_buffering=True)

import os
import sqlite3
import requests
import re
import time
import argparse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("enriquecer_profundo")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "empreendimentos.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# Mapeamento de lazer: coluna -> termos de deteccao
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


def _limpar_html(soup):
    """Remove nav, footer, header, menus para evitar falso positivo."""
    from copy import copy
    # Trabalhar numa copia para nao alterar o soup original
    clean = copy(soup)
    for tag in clean.find_all(["nav", "footer", "header"]):
        tag.decompose()
    # Remover menus e sidebars
    for cls in ["menu", "sidebar", "nav-", "navbar", "footer-", "breadcrumb"]:
        for el in clean.find_all(class_=lambda c: c and cls in str(c).lower()):
            el.decompose()
    return clean


def _extrair_secao_lazer(soup):
    """Tenta encontrar a secao de lazer/amenidades da pagina."""
    # Buscar secoes com id/class relevante
    keywords = ["lazer", "amenidad", "comodidad", "areas-comuns", "áreas comuns",
                 "areas_comuns", "leisure", "infraestrutura", "diferenciais",
                 "condominio", "condomínio"]

    # Primeiro: tentar por secao (section, div) com id/class que contenha keyword
    for kw in keywords:
        for tag in soup.find_all(["section", "div"], id=lambda x: x and kw in x.lower()):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return text
        for tag in soup.find_all(["section", "div"], class_=lambda x: x and any(kw in str(c).lower() for c in (x if isinstance(x, list) else [x]))):
            text = tag.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return text

    # Segundo: buscar h2/h3 com keyword e pegar texto ate proximo heading
    for heading in soup.find_all(["h2", "h3", "h4"]):
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
    """Extrai itens de lazer e flags binarias."""
    clean = _limpar_html(soup)
    secao = _extrair_secao_lazer(clean)

    if not secao:
        # Fallback: texto inteiro sem nav/footer (menos confiavel)
        secao = clean.get_text(separator="\n", strip=True)

    texto_lower = secao.lower()
    itens = []
    flags = {}

    for coluna, termos in LAZER_MAP.items():
        for termo in termos:
            if termo.lower() in texto_lower:
                flags[coluna] = 1
                # Adicionar item legivel (capitalizado)
                item_name = termo.strip().title()
                if item_name not in itens:
                    itens.append(item_name)
                break

    itens_str = " | ".join(sorted(set(itens))) if itens else None
    return itens_str, flags


def _extrair_metragens(soup, is_mcmv=False):
    """Extrai area_min e area_max das secoes de planta/tipologia."""
    clean = _limpar_html(soup)

    # Tentar secao especifica de plantas/tipologias
    secao_text = None
    kws = ["planta", "tipolog", "ficha", "metragem", "apartamento"]
    for kw in kws:
        for tag in clean.find_all(["section", "div"],
                                   id=lambda x: x and kw in str(x).lower()):
            secao_text = tag.get_text(separator="\n", strip=True)
            break
        if secao_text:
            break
        for tag in clean.find_all(["section", "div"],
                                   class_=lambda x: x and any(kw in str(c).lower() for c in (x if isinstance(x, list) else [x]))):
            secao_text = tag.get_text(separator="\n", strip=True)
            break
        if secao_text:
            break

    if not secao_text:
        secao_text = clean.get_text(separator="\n", strip=True)

    # Regex para metragens
    pattern = r'(\d+(?:[.,]\d+)?)\s*(?:a\s*(\d+(?:[.,]\d+)?)\s*)?m[²2]'
    matches = re.findall(pattern, secao_text, re.IGNORECASE)

    areas = []
    for m in matches:
        try:
            v1 = float(m[0].replace(",", "."))
            areas.append(v1)
            if m[1]:
                v2 = float(m[1].replace(",", "."))
                areas.append(v2)
        except ValueError:
            continue

    # Filtrar areas razoaveis
    if is_mcmv:
        areas = [a for a in areas if 15 <= a <= 80]
    else:
        areas = [a for a in areas if 10 <= a <= 500]

    if not areas:
        return None, None

    return min(areas), max(areas)


def _extrair_dormitorios(soup):
    """Extrai dormitorios_descricao e flags de tipologia."""
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)
    texto_lower = texto.lower()

    flags = {}
    descricao_parts = []

    # Studio
    if re.search(r'\bstudios?\b', texto_lower):
        flags["apto_studio"] = 1
        descricao_parts.append("Studio")

    # 1 dorm
    if re.search(r'\b1\s*(?:dorm|quarto)', texto_lower):
        flags["apto_1_dorm"] = 1
        descricao_parts.append("1 dorm")

    # 2 dorms
    if re.search(r'\b2\s*(?:dorm|quarto)', texto_lower):
        flags["apto_2_dorms"] = 1
        descricao_parts.append("2 dorms")

    # 3 dorms
    if re.search(r'\b3\s*(?:dorm|quarto)', texto_lower):
        flags["apto_3_dorms"] = 1
        descricao_parts.append("3 dorms")

    # 4 dorms
    if re.search(r'\b4\s*(?:dorm|quarto)', texto_lower):
        flags["apto_4_dorms"] = 1
        descricao_parts.append("4 dorms")

    # Suite
    if re.search(r'su[ií]te', texto_lower):
        flags["apto_suite"] = 1
        descricao_parts.append("c/suíte")

    # Cobertura
    if re.search(r'\bcobertura\b', texto_lower):
        flags["apto_cobertura"] = 1

    # Duplex
    if re.search(r'\bduplex\b', texto_lower):
        flags["apto_duplex"] = 1

    # Giardino/garden
    if re.search(r'\b(?:giardino|garden)\b', texto_lower):
        flags["apto_giardino"] = 1

    descricao = " e ".join(descricao_parts) if descricao_parts else None
    return descricao, flags


def _extrair_unidades_torres(soup):
    """Extrai total_unidades, numero_torres, numero_andares, numero_vagas."""
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)

    dados = {}

    # Total de unidades
    patterns_un = [
        r'(?<!\d[\-./])(\d[\d.]*)\s*(?:unidades?\s*(?:residenciais?|habitacionais?)?|UHs?)',
        r'(?:total\s*(?:de\s*)?)?(\d[\d.]*)\s*apartamentos?',
        r'(\d[\d.]*)\s*(?:casas|sobrados)',
        r'(?:unidades|apartamentos)\s*[:=]\s*(\d[\d.]*)',
    ]
    for pat in patterns_un:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            val = m.group(1).replace(".", "")
            try:
                n = int(val)
                if 20 <= n <= 5000:
                    # Verificar que nao e telefone
                    start = max(0, m.start() - 10)
                    prefix = texto[start:m.start()]
                    if re.search(r'[\d\-()]{4,}', prefix):
                        continue
                    # Verificar que nao e um ano (2020-2030)
                    if 2020 <= n <= 2030:
                        continue
                    dados["total_unidades"] = n
                    break
            except ValueError:
                pass

    # Torres
    m = re.search(r'(\d+)\s*torres?', texto, re.IGNORECASE)
    if m:
        try:
            n = int(m.group(1))
            if 1 <= n <= 30:
                dados["numero_torres"] = n
        except ValueError:
            pass

    # Andares
    m = re.search(r'(\d+)\s*(?:andares|pavimentos)', texto, re.IGNORECASE)
    if m:
        try:
            n = int(m.group(1))
            if 1 <= n <= 60:
                dados["numero_andares"] = str(n)
        except ValueError:
            pass

    # Vagas
    patterns_vagas = [
        r'(\d+)\s*vagas?\s*(?:de\s*)?(?:garagem|estacionamento)',
        r'vagas?\s*[:=]\s*(\d+)',
        r'(\d+)\s*vagas?',
    ]
    for pat in patterns_vagas:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            try:
                n = int(m.group(1))
                if 0 < n <= 5:
                    dados["numero_vagas"] = str(n)
                    break
            except ValueError:
                pass

    return dados


def _extrair_preco(soup):
    """Extrai preco_a_partir."""
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
                # Preco razoavel: entre 100k e 2M
                if 100_000 <= val <= 2_000_000:
                    return val
            except ValueError:
                continue
    return None


def _extrair_obra(soup):
    """Extrai evolucao_obra_pct."""
    clean = _limpar_html(soup)
    texto = clean.get_text(separator="\n", strip=True)

    patterns = [
        r'(\d+(?:[.,]\d+)?)\s*%\s*(?:conclu[ií]d|executad|pront|evolução|evolucao|obra)',
        r'(?:evolução|evolucao|obra|execução)\s*[:=]?\s*(\d+(?:[.,]\d+)?)\s*%',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", "."))
                if 0 <= val <= 100:
                    return val
            except ValueError:
                pass
    return None


def _extrair_imagem(soup, url):
    """Extrai imagem_url (fachada/hero)."""
    # Tentar og:image primeiro
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        img_url = og["content"]
        if img_url.startswith("http"):
            return img_url

    # Hero image: primeira img grande dentro de section/div principal
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        if not src or src.startswith("data:"):
            continue
        # Ignorar logos e icones
        src_lower = src.lower()
        if any(x in src_lower for x in ["logo", "icon", "favicon", "sprite", "placeholder"]):
            continue
        # Resolver URL relativa
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(url)
            src = f"{parsed.scheme}://{parsed.netloc}{src}"
        if src.startswith("http"):
            return src

    return None


def _extrair_coordenadas(soup):
    """Extrai latitude/longitude de iframes Google Maps ou scripts."""
    # 1. Google Maps iframe
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src") or iframe.get("data-src") or ""
        # Padrao: q=lat,lon ou @lat,lon
        m = re.search(r'(?:q=|@|center=|ll=)(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)', src)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            # Validar coordenadas Brasil
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)

    # 2. JSON-LD ou scripts com lat/lng
    for script in soup.find_all("script"):
        txt = script.string or ""
        # Padrao: "latitude": -23.55, "longitude": -46.63
        m = re.search(r'"lat(?:itude)?"\s*:\s*(-?\d+\.\d+).*?"lng|lon(?:gitude)?"\s*:\s*(-?\d+\.\d+)', txt, re.DOTALL)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)
        # google.maps.LatLng(-23.55, -46.63)
        m = re.search(r'LatLng\((-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\)', txt)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -35 <= lat <= 6 and -75 <= lon <= -30:
                return str(lat), str(lon)

    return None, None


def extrair_dados_pagina(url, html, is_mcmv=False):
    """Extrai todos os dados possiveis da pagina."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    dados = {}

    # 1. Lazer
    try:
        itens_lazer, flags_lazer = _extrair_lazer(soup)
        if itens_lazer:
            dados["itens_lazer"] = itens_lazer
        dados.update(flags_lazer)
    except Exception as e:
        logger.debug(f"  Erro lazer: {e}")

    # 2. Metragens
    try:
        area_min, area_max = _extrair_metragens(soup, is_mcmv)
        if area_min is not None:
            dados["area_min_m2"] = area_min
        if area_max is not None:
            dados["area_max_m2"] = area_max
    except Exception as e:
        logger.debug(f"  Erro metragens: {e}")

    # 3. Dormitorios
    try:
        dorm_desc, flags_dorm = _extrair_dormitorios(soup)
        if dorm_desc:
            dados["dormitorios_descricao"] = dorm_desc
        dados.update(flags_dorm)
    except Exception as e:
        logger.debug(f"  Erro dormitorios: {e}")

    # 4. Unidades/torres/andares/vagas
    try:
        dados_ut = _extrair_unidades_torres(soup)
        dados.update(dados_ut)
    except Exception as e:
        logger.debug(f"  Erro unidades: {e}")

    # 5. Preco
    try:
        preco = _extrair_preco(soup)
        if preco is not None:
            dados["preco_a_partir"] = preco
    except Exception as e:
        logger.debug(f"  Erro preco: {e}")

    # 6. Evolucao obra
    try:
        obra = _extrair_obra(soup)
        if obra is not None:
            dados["evolucao_obra_pct"] = obra
    except Exception as e:
        logger.debug(f"  Erro obra: {e}")

    # 7. Imagem
    try:
        img = _extrair_imagem(soup, url)
        if img:
            dados["imagem_url"] = img
    except Exception as e:
        logger.debug(f"  Erro imagem: {e}")

    # 8. Coordenadas
    try:
        lat, lon = _extrair_coordenadas(soup)
        if lat is not None:
            dados["latitude"] = lat
            dados["longitude"] = lon
    except Exception as e:
        logger.debug(f"  Erro coordenadas: {e}")

    return dados


def main():
    parser = argparse.ArgumentParser(description="Enriquecimento profundo de empreendimentos novos")
    parser.add_argument("--limite", type=int, default=200, help="Limite de URLs a processar")
    parser.add_argument("--empresa", type=str, help="Filtrar por empresa")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay entre requests (s)")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout do request (s)")
    parser.add_argument("--dry-run", action="store_true", help="Nao gravar no banco")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Query base
    where_extra = ""
    params = []
    if args.empresa:
        where_extra = " AND empresa = ?"
        params.append(args.empresa)

    query = f"""
        SELECT id, empresa, nome, url_fonte, prog_mcmv,
               itens_lazer, area_min_m2, area_max_m2, total_unidades,
               dormitorios_descricao, numero_torres, numero_andares, numero_vagas,
               preco_a_partir, evolucao_obra_pct, imagem_url, latitude, longitude
        FROM empreendimentos
        WHERE url_fonte IS NOT NULL AND url_fonte != ''
        AND (itens_lazer IS NULL OR itens_lazer = ''
             OR area_min_m2 IS NULL
             OR total_unidades IS NULL)
        {where_extra}
        ORDER BY empresa, nome
        LIMIT ?
    """
    params.append(args.limite)

    cur.execute(query, params)
    registros = cur.fetchall()

    logger.info(f"=== Enriquecimento profundo: {len(registros)} empreendimentos ===")
    if not registros:
        logger.info("Nenhum empreendimento para processar.")
        conn.close()
        return

    session = requests.Session()
    session.headers.update(HEADERS)

    total_processados = 0
    total_erros = 0
    total_campos_atualizados = 0
    total_emps_atualizados = 0
    campos_contador = {}  # campo -> quantas vezes atualizado

    for i, reg in enumerate(registros, 1):
        url = reg["url_fonte"]
        emp_id = reg["id"]
        nome = reg["nome"]
        empresa = reg["empresa"]
        is_mcmv = bool(reg["prog_mcmv"])

        # Progresso a cada 25
        if i % 25 == 0 or i == 1:
            logger.info(f"--- Progresso: {i}/{len(registros)} (erros: {total_erros}, campos: {total_campos_atualizados}) ---")

        try:
            resp = session.get(url, timeout=args.timeout, allow_redirects=True)
            if resp.status_code != 200:
                logger.info(f"  [{i}] {empresa} | {nome}: HTTP {resp.status_code}")
                total_erros += 1
                time.sleep(args.delay)
                continue

            html = resp.text
            if len(html) < 500:
                logger.info(f"  [{i}] {empresa} | {nome}: pagina muito curta ({len(html)} chars)")
                total_erros += 1
                time.sleep(args.delay)
                continue

        except requests.RequestException as e:
            logger.info(f"  [{i}] {empresa} | {nome}: {type(e).__name__}")
            total_erros += 1
            time.sleep(args.delay)
            continue

        total_processados += 1

        # Extrair dados
        dados = extrair_dados_pagina(url, html, is_mcmv)

        if not dados:
            time.sleep(args.delay)
            continue

        # Filtrar: so atualizar campos que estao NULL/vazio no banco
        campos_para_atualizar = {}

        # Campos texto/numerico: so preencher se vazio
        campos_check = [
            "itens_lazer", "area_min_m2", "area_max_m2", "total_unidades",
            "dormitorios_descricao", "numero_torres", "numero_andares", "numero_vagas",
            "preco_a_partir", "evolucao_obra_pct", "imagem_url", "latitude", "longitude",
        ]

        for campo in campos_check:
            if campo in dados and dados[campo] is not None:
                val_atual = reg[campo] if campo in reg.keys() else None
                if val_atual is None or val_atual == "" or val_atual == 0:
                    campos_para_atualizar[campo] = dados[campo]

        # Flags binarias de lazer/dorm: so setar para 1 se ainda for 0
        campos_binarios = [k for k in dados if k.startswith("lazer_") or k.startswith("apto_")]
        for campo in campos_binarios:
            if dados[campo] == 1:
                try:
                    val_atual = reg[campo] if campo in reg.keys() else 0
                except (IndexError, KeyError):
                    val_atual = 0
                if not val_atual:
                    campos_para_atualizar[campo] = 1

        if not campos_para_atualizar:
            time.sleep(args.delay)
            continue

        # Gravar no banco
        if not args.dry_run:
            update_cur = conn.cursor()
            for campo, valor in campos_para_atualizar.items():
                update_cur.execute(
                    f"UPDATE empreendimentos SET {campo}=?, data_atualizacao=? WHERE id=?",
                    (valor, datetime.now().isoformat(), emp_id)
                )
                campos_contador[campo] = campos_contador.get(campo, 0) + 1

        total_campos_atualizados += len(campos_para_atualizar)
        total_emps_atualizados += 1

        # Log detalhado para primeiros e a cada 25
        if i <= 5 or i % 25 == 0:
            campos_lista = list(campos_para_atualizar.keys())
            logger.info(f"  [{i}] {empresa} | {nome}: {len(campos_lista)} campos -> {campos_lista[:6]}")

        # Commit a cada 50
        if not args.dry_run and total_processados % 50 == 0:
            conn.commit()
            logger.info(f"  >> Commit (batch {total_processados // 50})")

        time.sleep(args.delay)

    # Commit final
    if not args.dry_run:
        conn.commit()

    conn.close()

    # Resumo
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO")
    logger.info("=" * 60)
    logger.info(f"URLs processadas:       {total_processados}")
    logger.info(f"Erros (HTTP/timeout):    {total_erros}")
    logger.info(f"Empreendimentos atualizados: {total_emps_atualizados}")
    logger.info(f"Total campos preenchidos:    {total_campos_atualizados}")
    logger.info("")
    logger.info("Campos preenchidos por tipo:")
    for campo, cnt in sorted(campos_contador.items(), key=lambda x: -x[1]):
        logger.info(f"  {campo:30s}: {cnt}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

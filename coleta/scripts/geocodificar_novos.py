"""
Geocodifica empreendimentos sem coordenadas usando estratégia em camadas:
1. CEP direto na base local
2. Endereço (logradouro) + cidade na base local
3. Bairro + cidade na base local
4. Revisitar URL e extrair coords de Google Maps embeds / JS / data-attrs
5. Se nada funcionar: NULL (nunca coordenada genérica)

Uso: python scripts/geocodificar_novos.py [--dry-run] [--limite N]
"""

import sqlite3
import requests
import re
import json
import time
import unicodedata
import argparse
import os
import sys

import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "empreendimentos.db")
CEPS_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ceps_brasil.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Stats
stats = {"cep": 0, "endereco": 0, "bairro": 0, "url": 0, "sem_match": 0, "total": 0}


def _sem_acento(s):
    """Remove acentos para busca normalizada."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def _validar_coords_brasil(lat, lng):
    """Valida se coordenadas estão dentro do bounding box do Brasil."""
    return -35 <= lat <= 6 and -75 <= lng <= -30


def _cep_db_disponivel():
    """Verifica se a base de CEPs tem dados."""
    if not os.path.exists(CEPS_DB) or os.path.getsize(CEPS_DB) == 0:
        return False
    try:
        conn = sqlite3.connect(CEPS_DB)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ceps")
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


# ================================================================
# Camada 1: CEP direto
# ================================================================

def geocodificar_por_cep(cep):
    """Busca coordenadas pelo CEP na base local."""
    if not cep:
        return None, None
    cep_limpo = re.sub(r"\D", "", cep)
    if len(cep_limpo) != 8:
        return None, None
    try:
        conn = sqlite3.connect(CEPS_DB)
        cur = conn.cursor()
        cur.execute("SELECT latitude, longitude FROM ceps WHERE cep = ?", (cep_limpo,))
        row = cur.fetchone()
        conn.close()
        if row and row[0] and row[1]:
            lat, lng = float(row[0]), float(row[1])
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)
    except Exception:
        pass
    return None, None


# ================================================================
# Camada 2: Endereço (logradouro) + cidade
# ================================================================

def geocodificar_por_endereco(endereco, cidade, estado=None):
    """Busca por logradouro + cidade na base de CEPs."""
    if not endereco or not cidade:
        return None, None

    # Extrair nome da rua (sem prefixo e sem número)
    m = re.match(
        r"(?:Rua|R\.|Av\.|Avenida|Al\.|Alameda|Estr\.|Estrada|Rod\.|Rodovia|Travessa|Tv\.|"
        r"Pça\.|Praça|Largo|Viela|Beco|Servidão)\s+(.+)",
        endereco, re.IGNORECASE,
    )
    if not m:
        # Tentar sem prefixo — usar o endereço inteiro cortando no número
        nome_rua = re.split(r",\s*(?:n[ºo°]?\s*)?\d|\s+n[ºo°]\s*\d|\s*-\s*\d|\s+\d{2,}", endereco)[0].strip()
    else:
        nome_rua = m.group(1).strip()
        nome_rua = re.split(r",\s*(?:n[ºo°]?\s*)?\d|\s*-\s*\d|\s+\d{2,}", nome_rua)[0].strip()

    if len(nome_rua) < 3:
        return None, None

    nome_rua_norm = _sem_acento(nome_rua)
    cidade_norm = _sem_acento(cidade)

    try:
        conn = sqlite3.connect(CEPS_DB)
        cur = conn.cursor()

        # Buscar por logradouro + cidade
        for nr, cid in [(nome_rua, cidade), (nome_rua_norm, cidade_norm)]:
            cur.execute(
                "SELECT latitude, longitude FROM ceps WHERE logradouro LIKE ? AND nome_municipio LIKE ? LIMIT 1",
                (f"%{nr}%", cid),
            )
            row = cur.fetchone()
            if row and row[0] and row[1]:
                lat, lng = float(row[0]), float(row[1])
                if _validar_coords_brasil(lat, lng):
                    conn.close()
                    return str(lat), str(lng)

        # Fallback: logradouro + UF
        if estado:
            for nr in [nome_rua, nome_rua_norm]:
                cur.execute(
                    "SELECT latitude, longitude FROM ceps WHERE logradouro LIKE ? AND sigla_uf = ? LIMIT 1",
                    (f"%{nr}%", estado),
                )
                row = cur.fetchone()
                if row and row[0] and row[1]:
                    lat, lng = float(row[0]), float(row[1])
                    if _validar_coords_brasil(lat, lng):
                        conn.close()
                        return str(lat), str(lng)

        conn.close()
    except Exception:
        pass
    return None, None


# ================================================================
# Camada 3: Bairro + cidade
# ================================================================

def geocodificar_por_bairro(bairro, cidade, estado=None):
    """Busca qualquer CEP do mesmo bairro+cidade como aproximação."""
    if not bairro or not cidade:
        return None, None

    bairro_norm = _sem_acento(bairro)
    cidade_norm = _sem_acento(cidade)

    try:
        conn = sqlite3.connect(CEPS_DB)
        cur = conn.cursor()

        for b, c in [(bairro, cidade), (bairro_norm, cidade_norm)]:
            cur.execute(
                "SELECT latitude, longitude FROM ceps WHERE bairro LIKE ? AND nome_municipio LIKE ? LIMIT 1",
                (b, c),
            )
            row = cur.fetchone()
            if row and row[0] and row[1]:
                lat, lng = float(row[0]), float(row[1])
                if _validar_coords_brasil(lat, lng):
                    conn.close()
                    return str(lat), str(lng)

        # Busca parcial no bairro
        for b, c in [(bairro_norm, cidade_norm)]:
            cur.execute(
                "SELECT latitude, longitude FROM ceps WHERE bairro LIKE ? AND nome_municipio LIKE ? LIMIT 1",
                (f"%{b}%", c),
            )
            row = cur.fetchone()
            if row and row[0] and row[1]:
                lat, lng = float(row[0]), float(row[1])
                if _validar_coords_brasil(lat, lng):
                    conn.close()
                    return str(lat), str(lng)

        conn.close()
    except Exception:
        pass
    return None, None


# ================================================================
# Camada 4: Revisitar URL e extrair coords de embeds
# ================================================================

def extrair_coordenadas_de_html(html_raw):
    """Extrai lat/lng de iframes Google Maps, JS vars, data-attrs, JSON-LD, links."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_raw, "html.parser")

    # 1. Google Maps iframe (src, data-src, data-lazy-src)
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src") or iframe.get("data-src") or iframe.get("data-lazy-src") or ""
        if not src or src == "about:blank":
            continue
        if "google" in src and "map" in src.lower():
            # q=LAT,LNG
            m = re.search(r"[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # center=LAT,LNG
            m = re.search(r"center=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # @LAT,LNG
            m = re.search(r"@(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", src)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            # !2dLNG!3dLAT (invertido)
            m = re.search(r"!2d(-?\d+\.?\d+)!3d(-?\d+\.?\d+)", src)
            if m:
                lng, lat = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)

    # 2. JS variables
    for script in soup.find_all("script"):
        text = script.string or ""
        if not text or len(text) < 10:
            continue

        # Pular scripts que mencionam "construtora" no contexto do mapa (coord genérica do escritório)
        if "construtora" in text.lower() and "map" in text.lower():
            continue

        # LatLng(LAT, LNG)
        m = re.search(r"LatLng\(\s*(-?\d+\.?\d+)\s*,\s*(-?\d+\.?\d+)\s*\)", text)
        if m:
            lat, lng = float(m.group(1)), float(m.group(2))
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)

        # lat: VAL, lng: VAL — mas não se é fallback de ternário (typeof ... ? ... : DEFAULT)
        lat_m = re.search(r"""(?:lat|latitude)\s*[:=]\s*['"]?(-?\d+\.?\d+)['"]?""", text)
        lng_m = re.search(r"""(?:lng|longitude|lon)\s*[:=]\s*['"]?(-?\d+\.?\d+)['"]?""", text)
        if lat_m and lng_m:
            # Verificar se o match é um fallback de ternário (ex: "? latitude : -27.32")
            lat_ctx = text[max(0, lat_m.start()-30):lat_m.start()]
            if "?" in lat_ctx and ":" in lat_ctx:
                continue  # Provavelmente fallback genérico
            lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)

    # 3. Data attributes
    for attr_lat, attr_lng in [("data-lat", "data-lng"), ("data-latitude", "data-longitude")]:
        elem = soup.find(True, attrs={attr_lat: True, attr_lng: True})
        if elem:
            try:
                lat, lng = float(elem[attr_lat]), float(elem[attr_lng])
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)
            except (ValueError, TypeError):
                pass

    # 4. JSON-LD geo
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                geo = item.get("geo", {})
                if isinstance(geo, dict) and geo.get("latitude") and geo.get("longitude"):
                    lat, lng = float(geo["latitude"]), float(geo["longitude"])
                    if _validar_coords_brasil(lat, lng):
                        return str(lat), str(lng)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # 5. Hidden inputs com latitude/longitude
    for input_tag in soup.find_all("input", type="hidden"):
        name = (input_tag.get("name") or "").lower()
        if "latitude" in name:
            lat_val = input_tag.get("value", "")
            for sib in soup.find_all("input", type="hidden"):
                sib_name = (sib.get("name") or "").lower()
                if "longitude" in sib_name:
                    lng_val = sib.get("value", "")
                    try:
                        lat, lng = float(lat_val), float(lng_val)
                        if _validar_coords_brasil(lat, lng):
                            return str(lat), str(lng)
                    except (ValueError, TypeError):
                        pass
            break

    # 6. Links Waze com ll=LAT%2CLNG
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "waze.com" in href:
            m = re.search(r"ll=(-?\d+\.?\d+)%2C(-?\d+\.?\d+)", href)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)

    # 7. Links Google Maps <a> com query=LAT,LNG
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "google.com/maps" in href:
            m = re.search(r"query=(-?\d+\.?\d+),(-?\d+\.?\d+)", href)
            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _validar_coords_brasil(lat, lng):
                    return str(lat), str(lng)

    # 8. Raw HTML regex for coordinates in Google Maps URLs (lazy-loaded iframes, etc.)
    patterns = [
        r"maps\.google\.com[^\"']*[?&]q=(-?\d+\.?\d+),\s*(-?\d+\.?\d+)",
        r"google\.com/maps[^\"']*[?&]q=(-?\d+\.?\d+),\s*(-?\d+\.?\d+)",
        r"google\.com/maps[^\"']*@(-?\d+\.?\d+),\s*(-?\d+\.?\d+)",
        r"google\.com/maps[^\"']*query=(-?\d+\.?\d+),\s*(-?\d+\.?\d+)",
        r"!2d(-?\d+\.?\d+)!3d(-?\d+\.?\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, html_raw)
        if m:
            if "!2d" in pat:
                lng, lat = float(m.group(1)), float(m.group(2))
            else:
                lat, lng = float(m.group(1)), float(m.group(2))
            if _validar_coords_brasil(lat, lng):
                return str(lat), str(lng)

    return None, None


def geocodificar_por_url(url):
    """Visita a URL do empreendimento e tenta extrair coordenadas."""
    if not url:
        return None, None
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code == 200:
            return extrair_coordenadas_de_html(resp.text)
    except Exception:
        pass
    return None, None


# ================================================================
# Processamento principal
# ================================================================

def processar(dry_run=False, limite=None):
    """Processa todos os empreendimentos sem coordenadas."""

    cep_db_ok = _cep_db_disponivel()
    if not cep_db_ok:
        print("AVISO: Base de CEPs (ceps_brasil.db) vazia ou inexistente. Camadas 1-3 desabilitadas.")
        print("       Apenas camada 4 (revisitar URL) será usada.\n")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = "SELECT id, empresa, nome, cep, endereco, bairro, cidade, estado, url_fonte FROM empreendimentos WHERE latitude IS NULL"
    if limite:
        query += f" LIMIT {limite}"
    cur.execute(query)
    rows = cur.fetchall()
    total = len(rows)
    stats["total"] = total

    print(f"Empreendimentos sem coordenadas: {total}")
    if cep_db_ok:
        print("Base de CEPs: OK")
    print(f"Dry run: {dry_run}\n")

    batch_size = 100
    atualizados = 0
    # Rastrear coords por empresa para detectar coords genéricas (todas iguais)
    coords_por_empresa = {}  # empresa -> {(lat,lng): [list of ids]}
    updates = []  # (id, lat, lng, metodo, nome, empresa) para aplicar

    for i, row in enumerate(rows):
        eid = row["id"]
        nome = row["nome"]
        cep = row["cep"]
        endereco = row["endereco"]
        bairro = row["bairro"]
        cidade = row["cidade"]
        estado = row["estado"]
        url = row["url_fonte"]
        empresa = row["empresa"]

        lat, lng = None, None
        metodo = None

        # Camada 1: CEP direto
        if cep_db_ok and cep:
            lat, lng = geocodificar_por_cep(cep)
            if lat:
                metodo = "cep"

        # Camada 2: Endereço + cidade
        if not lat and cep_db_ok and endereco and cidade:
            lat, lng = geocodificar_por_endereco(endereco, cidade, estado)
            if lat:
                metodo = "endereco"

        # Camada 3: Bairro + cidade
        if not lat and cep_db_ok and bairro and cidade:
            lat, lng = geocodificar_por_bairro(bairro, cidade, estado)
            if lat:
                metodo = "bairro"

        # Camada 4: Revisitar URL
        if not lat and url:
            lat, lng = geocodificar_por_url(url)
            if lat:
                metodo = "url"
            time.sleep(1)  # Rate limit entre requests

        # Registrar resultado
        if metodo:
            stats[metodo] += 1
            atualizados += 1
            updates.append((eid, lat, lng, metodo, nome, empresa))
            # Rastrear por empresa
            if empresa not in coords_por_empresa:
                coords_por_empresa[empresa] = {}
            coord_key = (lat, lng)
            if coord_key not in coords_por_empresa[empresa]:
                coords_por_empresa[empresa][coord_key] = []
            coords_por_empresa[empresa][coord_key].append(eid)
            print(f"  [{i+1}/{total}] {nome} ({empresa}) -> {metodo}: {lat}, {lng}", flush=True)
        else:
            stats["sem_match"] += 1
            if (i + 1) % 50 == 0 or i < 5:
                print(f"  [{i+1}/{total}] {nome} ({empresa}) -> sem match", flush=True)

        # Progresso a cada batch
        if (i + 1) % batch_size == 0:
            print(f"\n--- Progresso {i+1}/{total}: {atualizados} encontrados até agora ---\n", flush=True)

    # Detectar e filtrar coordenadas genéricas (mesma coord para 3+ empreendimentos da mesma empresa)
    ids_genericos = set()
    for empresa, coord_dict in coords_por_empresa.items():
        for (lat, lng), ids in coord_dict.items():
            if len(ids) >= 3:
                print(f"\n  AVISO: {empresa} tem {len(ids)} empreendimentos com coords idênticas ({lat}, {lng}) -> DESCARTANDO (provável coord genérica)", flush=True)
                ids_genericos.update(ids)

    # Aplicar updates (excluindo genéricos)
    aplicados = 0
    descartados = 0
    if not dry_run:
        for eid, lat, lng, metodo, nome, empresa in updates:
            if eid in ids_genericos:
                descartados += 1
                continue
            cur.execute(
                "UPDATE empreendimentos SET latitude = ?, longitude = ? WHERE id = ?",
                (lat, lng, eid),
            )
            aplicados += 1
        conn.commit()
    else:
        aplicados = atualizados - len(ids_genericos)
        descartados = len(ids_genericos)

    # Stats finais
    print("\n" + "=" * 60)
    print("RESULTADO DA GEOCODIFICAÇÃO")
    print("=" * 60)
    print(f"Total processados:    {stats['total']}")
    print(f"  Por CEP:            {stats['cep']}")
    print(f"  Por endereço:       {stats['endereco']}")
    print(f"  Por bairro:         {stats['bairro']}")
    print(f"  Por URL (scraping): {stats['url']}")
    print(f"  Sem match:          {stats['sem_match']}")
    print(f"  Encontrados:        {atualizados}")
    print(f"  Descartados (genérico): {descartados}")
    print(f"  TOTAL aplicados:    {aplicados}")
    print()

    # Stats gerais do banco
    cur.execute("SELECT COUNT(*), COUNT(latitude) FROM empreendimentos")
    total_geral, total_com_coords = cur.fetchone()
    pct = (total_com_coords / total_geral * 100) if total_geral else 0
    print(f"Estado do banco: {total_com_coords}/{total_geral} com coordenadas ({pct:.1f}%)")
    if dry_run:
        print("(DRY RUN — nenhuma alteração foi salva)")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geocodifica empreendimentos sem coordenadas")
    parser.add_argument("--dry-run", action="store_true", help="Simular sem salvar")
    parser.add_argument("--limite", type=int, help="Limitar quantidade processada")
    args = parser.parse_args()

    processar(dry_run=args.dry_run, limite=args.limite)

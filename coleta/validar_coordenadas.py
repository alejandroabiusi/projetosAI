"""
Valida coordenadas dos empreendimentos comparando lat/lng com a cidade cadastrada.
Detecta pins no mapa que estão em locais inconsistentes com a cidade informada.

Modos:
  relatorio  - Lista inconsistências sem alterar nada
  limpar     - Limpa lat/lng dos suspeitos (para re-geocodificar depois)
  corrigir   - Tenta extrair cidade real do endereço + limpa coords suspeitas
"""
import sqlite3
import json
import math
import re
import time
import argparse
import os
import sys

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(errors="replace")
    sys.stderr.reconfigure(errors="replace")
from urllib.request import urlopen, Request
from urllib.parse import quote

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "empreendimentos.db")
CACHE_FILE = os.path.join(os.path.dirname(__file__), "data", "cache_cidades.json")

# Thresholds em km
THRESHOLD_METRO = 80   # capitais e regiões metropolitanas
THRESHOLD_OUTROS = 50  # demais cidades

CAPITAIS = {
    "São Paulo", "Sao Paulo", "Rio de Janeiro", "Belo Horizonte",
    "Brasília", "Brasilia", "Curitiba", "Porto Alegre", "Salvador",
    "Recife", "Fortaleza", "Goiânia", "Goiania", "Belém", "Belem",
    "Manaus", "Campinas", "Guarulhos", "São Bernardo do Campo",
}

# Padrões regex para extrair cidade do endereço
PADROES_CIDADE = [
    # "Cidade - UF" ou "Cidade -UF" no final
    r',\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+(?:do|da|de|dos|das|e)\s+)?[A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*[A-Z]{2}\s*$',
    # "Cidade / UF"
    r',?\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+(?:do|da|de|dos|das|e)\s+)?[A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*/\s*[A-Z]{2}',
    # "Cidade - UF, CEP" ou no meio do texto
    r'[-–,]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+(?:do|da|de|dos|das|e)\s+)?[A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–]\s*[A-Z]{2}\b',
]

# UFs brasileiras
UFS = {"AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA",
       "PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"}


def haversine(lat1, lon1, lat2, lon2):
    """Distância em km entre dois pontos (lat/lon em graus)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def carregar_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def salvar_cache(cache):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


_ultimo_req = 0

def geocodificar_cidade(cidade, estado, cache):
    """Obtém lat/lng do centro de uma cidade via Nominatim (com cache)."""
    global _ultimo_req
    chave = f"{cidade}|{estado}"
    if chave in cache:
        return cache[chave]

    # Rate limit
    agora = time.time()
    espera = max(0, 1.2 - (agora - _ultimo_req))
    if espera > 0:
        time.sleep(espera)

    query = f"{cidade}, {estado}, Brasil" if estado else f"{cidade}, Brasil"
    url = f"https://nominatim.openstreetmap.org/search?q={quote(query)}&format=json&limit=1&countrycodes=br"
    req = Request(url, headers={"User-Agent": "Coleta-Validacao/1.0 (educational-project)"})

    try:
        with urlopen(req, timeout=10) as resp:
            _ultimo_req = time.time()
            data = json.loads(resp.read().decode())
            if data:
                result = (float(data[0]["lat"]), float(data[0]["lon"]))
                cache[chave] = result
                return result
    except Exception as e:
        print(f"  [WARN] Erro geocodificando '{cidade}, {estado}': {e}")

    cache[chave] = None
    return None


def extrair_cidade_do_endereco(endereco):
    """Tenta extrair nome da cidade do campo endereço."""
    if not endereco:
        return None, None

    # Padrão: "..., Cidade - UF" ou "... Cidade / UF"
    for padrao in PADROES_CIDADE:
        m = re.search(padrao, endereco)
        if m:
            cidade = m.group(1).strip()
            # Encontrar UF próximo
            uf_match = re.search(r'[-–/]\s*([A-Z]{2})\b', endereco[m.start():])
            uf = uf_match.group(1) if uf_match and uf_match.group(1) in UFS else None
            # Filtrar bairros comuns
            bairros = {"Centro", "Barra Funda", "Mooca", "Penha", "Itaquera",
                       "Sacomã", "Butantã", "Jaçanã", "Pirituba", "Vila Ema",
                       "Santo Amaro", "Belenzinho", "Alphaville"}
            if cidade in bairros:
                continue
            return cidade, uf

    # Padrão alternativo: "cidade/UF" em qualquer posição
    m = re.search(r'([A-ZÀ-Ú][a-zà-ú]+(?:\s+(?:do|da|de|dos|das)\s+[A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)?)\s*/\s*([A-Z]{2})\b', endereco)
    if m and m.group(2) in UFS:
        return m.group(1).strip(), m.group(2)

    # Nome da cidade no nome do empreendimento
    return None, None


def extrair_cidade_do_nome(nome):
    """Tenta extrair cidade do nome do empreendimento (ex: 'Breve Lançamento Catanduva')."""
    if not nome:
        return None
    # Padrões comuns em nomes
    m = re.search(r'(?:Lançamento|Lancamento)\s+(.+)', nome)
    if m:
        return m.group(1).strip()
    return None


def validar_todos(db_path, verbose=False):
    """Valida coordenadas de todos os empreendimentos. Retorna lista de suspeitos."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Buscar todos com coordenadas
    cur.execute("""
        SELECT id, empresa, nome, cidade, estado, bairro, endereco, latitude, longitude
        FROM empreendimentos
        WHERE latitude IS NOT NULL AND latitude != ''
          AND longitude IS NOT NULL AND longitude != ''
    """)
    registros = cur.fetchall()
    conn.close()

    print(f"Total de registros com coordenadas: {len(registros)}")

    # Coletar cidades únicas
    cidades_unicas = set()
    for r in registros:
        if r["cidade"]:
            cidades_unicas.add((r["cidade"], r["estado"]))

    print(f"Cidades únicas a geocodificar: {len(cidades_unicas)}")

    # Geocodificar centros das cidades
    cache = carregar_cache()
    cache_inicial = len(cache)
    centros = {}

    for cidade, estado in sorted(cidades_unicas):
        coords = geocodificar_cidade(cidade, estado, cache)
        if coords:
            centros[(cidade, estado)] = coords
            if verbose:
                print(f"  {cidade}/{estado}: {coords[0]:.4f}, {coords[1]:.4f}")
        else:
            print(f"  [WARN] Não consegui geocodificar centro de '{cidade}/{estado}'")

    if len(cache) > cache_inicial:
        salvar_cache(cache)
        print(f"Cache atualizado: {cache_inicial} -> {len(cache)} cidades")

    # Validar cada registro
    suspeitos = []
    ok_count = 0

    for r in registros:
        cidade = r["cidade"]
        estado = r["estado"]
        lat = float(r["latitude"])
        lon = float(r["longitude"])

        if not cidade:
            continue

        centro = centros.get((cidade, estado))
        if not centro:
            continue

        dist = haversine(lat, lon, centro[0], centro[1])
        threshold = THRESHOLD_METRO if cidade in CAPITAIS else THRESHOLD_OUTROS

        if dist > threshold:
            # Tentar extrair cidade real
            cidade_extraida, uf_extraida = extrair_cidade_do_endereco(r["endereco"])
            if not cidade_extraida:
                cidade_nome = extrair_cidade_do_nome(r["nome"])
                if cidade_nome:
                    cidade_extraida = cidade_nome

            suspeitos.append({
                "id": r["id"],
                "empresa": r["empresa"],
                "nome": r["nome"],
                "cidade_cadastrada": cidade,
                "estado": estado,
                "endereco": r["endereco"],
                "lat": lat,
                "lon": lon,
                "distancia_km": round(dist, 1),
                "threshold": threshold,
                "cidade_extraida": cidade_extraida,
                "uf_extraida": uf_extraida,
            })
        else:
            ok_count += 1

    print(f"\nResultado: {ok_count} OK, {len(suspeitos)} suspeitos")
    return suspeitos


def modo_relatorio(suspeitos):
    """Exibe relatório agrupado por empresa."""
    if not suspeitos:
        print("\nNenhuma inconsistência encontrada!")
        return

    # Agrupar por empresa
    por_empresa = {}
    for s in suspeitos:
        por_empresa.setdefault(s["empresa"], []).append(s)

    print(f"\n{'='*80}")
    print(f"RELATÓRIO DE COORDENADAS SUSPEITAS ({len(suspeitos)} total)")
    print(f"{'='*80}")

    for empresa in sorted(por_empresa.keys()):
        items = por_empresa[empresa]
        print(f"\n--- {empresa} ({len(items)} suspeitos) ---")
        for s in items:
            cidade_info = f"cadastrada: {s['cidade_cadastrada']}"
            if s["cidade_extraida"]:
                cidade_info += f" -> extraída: {s['cidade_extraida']}"
                if s["uf_extraida"]:
                    cidade_info += f"/{s['uf_extraida']}"
            print(f"  [{s['id']}] {s['nome']}")
            print(f"       {cidade_info}")
            print(f"       distância: {s['distancia_km']}km (threshold: {s['threshold']}km)")
            if s["endereco"]:
                end_trunc = s["endereco"][:80] + "..." if len(s["endereco"] or "") > 80 else s["endereco"]
                print(f"       endereço: {end_trunc}")


def modo_limpar(db_path, suspeitos):
    """Limpa lat/lng dos empreendimentos suspeitos."""
    if not suspeitos:
        print("Nenhum suspeito para limpar.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    count = 0

    for s in suspeitos:
        cur.execute("UPDATE empreendimentos SET latitude=NULL, longitude=NULL WHERE id=?", (s["id"],))
        count += cur.rowcount

    conn.commit()
    conn.close()
    print(f"\nCoordenadas limpas: {count} registros")


def modo_corrigir(db_path, suspeitos):
    """Corrige cidade e limpa coordenadas dos suspeitos."""
    if not suspeitos:
        print("Nenhum suspeito para corrigir.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    corrigidos_cidade = 0
    coords_limpas = 0

    for s in suspeitos:
        updates = ["latitude=NULL", "longitude=NULL"]
        params = []

        if s["cidade_extraida"]:
            updates.append("cidade=?")
            params.append(s["cidade_extraida"])
            if s["uf_extraida"]:
                updates.append("estado=?")
                params.append(s["uf_extraida"])
            corrigidos_cidade += 1

        params.append(s["id"])
        cur.execute(f"UPDATE empreendimentos SET {', '.join(updates)} WHERE id=?", params)
        coords_limpas += cur.rowcount

    conn.commit()
    conn.close()
    print(f"\nCidades corrigidas: {corrigidos_cidade}")
    print(f"Coordenadas limpas: {coords_limpas}")
    print("Execute enriquecer_dados.py para re-geocodificar.")


def main():
    parser = argparse.ArgumentParser(description="Validação de coordenadas de empreendimentos")
    parser.add_argument("modo", choices=["relatorio", "limpar", "corrigir"],
                       help="Modo: relatorio (read-only), limpar (limpa coords), corrigir (corrige cidade + limpa coords)")
    parser.add_argument("--db", default=DB_PATH, help="Caminho do banco de dados")
    parser.add_argument("--verbose", "-v", action="store_true", help="Modo verboso")
    args = parser.parse_args()

    suspeitos = validar_todos(args.db, verbose=args.verbose)

    if args.modo == "relatorio":
        modo_relatorio(suspeitos)
    elif args.modo == "limpar":
        modo_relatorio(suspeitos)
        print("\n" + "-"*40)
        resp = input(f"Limpar coordenadas de {len(suspeitos)} registros? (s/n): ")
        if resp.lower() == "s":
            modo_limpar(args.db, suspeitos)
    elif args.modo == "corrigir":
        modo_relatorio(suspeitos)
        print("\n" + "-"*40)
        resp = input(f"Corrigir {len(suspeitos)} registros? (s/n): ")
        if resp.lower() == "s":
            modo_corrigir(args.db, suspeitos)


if __name__ == "__main__":
    main()

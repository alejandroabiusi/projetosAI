"""
Pre-calcula os centros de zonas de oportunidade para todas as cidades e raios.
Salva em data/vendas.db na tabela 'zonas_precalc'.
Deve ser rodado sempre que a base de vendas for atualizada.

Algoritmo: greedy set cover
  1. Cada empreendimento e candidato a centro de zona
  2. Conta vendas dentro do raio para cada candidato
  3. Seleciona o candidato que cobre mais vendas ainda nao cobertas
  4. Marca essas vendas como cobertas
  5. Repete ate cobrir todas as vendas (ou restar menos que min_samples)
"""

import sqlite3
import numpy as np
import pandas as pd

RAIOS = [2, 3, 5, 7, 10]
MIN_VENDAS_ZONA = 3  # minimo de vendas nao-cobertas para criar nova zona


def _haversine_matrix(lats1, lons1, lats2, lons2):
    """Matriz de distancias (km) entre dois conjuntos de pontos."""
    lat1 = np.radians(lats1)[:, None]
    lon1 = np.radians(lons1)[:, None]
    lat2 = np.radians(lats2)[None, :]
    lon2 = np.radians(lons2)[None, :]
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def _greedy_zones(df_emprs: pd.DataFrame, df_vendas: pd.DataFrame, raio_km: float) -> list[dict]:
    """
    Greedy set cover: posiciona zonas para maximizar cobertura de vendas.
    Cada empreendimento e um candidato a centro. Em cada iteracao, escolhe
    o candidato que cobre mais vendas ainda nao cobertas.
    """
    lats_e = df_emprs["lat"].values.astype(float)
    lons_e = df_emprs["lon"].values.astype(float)
    lats_v = df_vendas["latitude"].values.astype(float)
    lons_v = df_vendas["longitude"].values.astype(float)
    n_vendas = len(lats_v)

    # Matriz de distancias: candidatos (emprs) x vendas
    dist_matrix = _haversine_matrix(lats_e, lons_e, lats_v, lons_v)

    # Quais vendas cada candidato cobre (dentro do raio)
    cobertura = dist_matrix <= raio_km  # shape: (n_emprs, n_vendas)

    coberto = np.zeros(n_vendas, dtype=bool)
    zonas = []

    # Distancias entre candidatos (para eliminar vizinhos apos selecao)
    dist_emprs = _haversine_matrix(lats_e, lons_e, lats_e, lons_e)
    candidato_ativo = np.ones(len(lats_e), dtype=bool)

    while True:
        # Para cada candidato ativo, conta vendas NAO cobertas
        nao_coberto = ~coberto
        contagens = np.where(
            candidato_ativo,
            (cobertura & nao_coberto[None, :]).sum(axis=1),
            0,
        )

        melhor = int(contagens.argmax())
        if contagens[melhor] < MIN_VENDAS_ZONA:
            break

        # Vendas cobertas por este candidato
        novas = cobertura[melhor] & nao_coberto
        coberto |= novas

        # Centro da zona = centroide das vendas dentro do raio deste candidato
        vendas_na_zona = cobertura[melhor]
        center_lat = float(lats_v[vendas_na_zona].mean())
        center_lon = float(lons_v[vendas_na_zona].mean())
        zonas.append({"lat": center_lat, "lon": center_lon})

        # Desativa candidatos a menos de 1 raio do escolhido (evita sobreposicao)
        candidato_ativo[dist_emprs[melhor] < raio_km] = False

    return zonas


def main():
    conn = sqlite3.connect("data/vendas.db", timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Criar tabela de zonas pre-calculadas
    conn.execute("DROP TABLE IF EXISTS zonas_precalc")
    conn.execute("""
        CREATE TABLE zonas_precalc (
            cidade TEXT,
            estado TEXT,
            raio_km INTEGER,
            zona_id INTEGER,
            latitude REAL,
            longitude REAL
        )
    """)
    conn.execute("CREATE INDEX idx_zonas_cidade ON zonas_precalc(cidade, estado, raio_km)")

    # Listar cidades com vendas geocodificadas
    df_cidades = pd.read_sql("""
        SELECT cidade, estado, COUNT(*) as vendas
        FROM vendas
        WHERE latitude IS NOT NULL AND cidade IS NOT NULL
        GROUP BY cidade, estado
        HAVING vendas >= 10
        ORDER BY vendas DESC
    """, conn)

    print(f"Cidades para processar: {len(df_cidades)}")
    print(f"Raios: {RAIOS}")
    total = len(df_cidades) * len(RAIOS)
    done = 0

    for _, row_cidade in df_cidades.iterrows():
        cidade = row_cidade["cidade"]
        estado = row_cidade["estado"]

        # Carregar vendas da cidade
        df_vendas = pd.read_sql("""
            SELECT latitude, longitude, empreendimento
            FROM vendas
            WHERE cidade = ? AND estado = ? AND latitude IS NOT NULL
        """, conn, params=[cidade, estado])

        if df_vendas.empty:
            done += len(RAIOS)
            continue

        for col in ["latitude", "longitude"]:
            df_vendas[col] = pd.to_numeric(df_vendas[col], errors="coerce")
        df_vendas = df_vendas.dropna(subset=["latitude", "longitude"])

        # Empreendimentos agrupados
        df_emprs = df_vendas.groupby("empreendimento").agg(
            lat=("latitude", "mean"),
            lon=("longitude", "mean"),
        ).reset_index()

        for raio in RAIOS:
            zonas = _greedy_zones(df_emprs, df_vendas, raio)

            # Inserir no banco
            for i, zona in enumerate(zonas):
                conn.execute(
                    "INSERT INTO zonas_precalc (cidade, estado, raio_km, zona_id, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)",
                    (cidade, estado, raio, i + 1, zona["lat"], zona["lon"])
                )

            done += 1
            if done % 50 == 0:
                conn.commit()
                pct = 100 * done / total
                print(f"  [{done}/{total} ({pct:.0f}%)] {cidade}/{estado} raio={raio}km -> {len(zonas)} zonas")

    conn.commit()

    # Stats
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM zonas_precalc")
    total_zonas = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT cidade || estado) FROM zonas_precalc")
    total_cidades = cur.fetchone()[0]
    conn.close()

    print(f"\nFinalizado!")
    print(f"Total zonas pre-calculadas: {total_zonas}")
    print(f"Cidades cobertas: {total_cidades}")
    print(f"Raios: {RAIOS}")


if __name__ == "__main__":
    main()

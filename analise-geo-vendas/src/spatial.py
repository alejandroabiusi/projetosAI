"""
Módulo espacial — filtro por raio usando fórmula de Haversine.
"""

import math

import pandas as pd
import numpy as np


RAIO_TERRA_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula distância em km entre dois pontos usando Haversine."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return RAIO_TERRA_KM * 2 * math.asin(math.sqrt(a))


def haversine_np(lat_centro, lon_centro, lats, lons):
    """Calcula distâncias em km (vetorizado com numpy)."""
    lat1 = np.radians(lat_centro)
    lon1 = np.radians(lon_centro)
    lat2 = np.radians(lats)
    lon2 = np.radians(lons)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return RAIO_TERRA_KM * 2 * np.arcsin(np.sqrt(a))


def filtrar_por_raio(
    df: pd.DataFrame,
    lat_centro: float,
    lon_centro: float,
    raio_km: float,
) -> pd.DataFrame:
    """
    Filtra DataFrame por raio em km a partir de um ponto central.
    Usa vetorização numpy para performance com 100k+ linhas.

    Retorna DataFrame filtrado com coluna 'distancia_km' adicionada.
    """
    df_valid = df.dropna(subset=["latitude", "longitude"]).copy()

    if df_valid.empty:
        df_valid["distancia_km"] = pd.Series(dtype=float)
        return df_valid

    distancias = haversine_np(
        lat_centro, lon_centro,
        df_valid["latitude"].astype(float).values,
        df_valid["longitude"].astype(float).values,
    )

    df_valid["distancia_km"] = distancias
    return df_valid[df_valid["distancia_km"] <= raio_km].copy()


def filtrar_por_periodo(
    df: pd.DataFrame,
    data_inicio: pd.Timestamp | None = None,
    data_fim: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Filtra DataFrame por período de data_venda."""
    if "data_venda" not in df.columns:
        return df

    df_filtrado = df.copy()

    if data_inicio is not None:
        df_filtrado = df_filtrado[df_filtrado["data_venda"] >= pd.Timestamp(data_inicio)]

    if data_fim is not None:
        df_filtrado = df_filtrado[df_filtrado["data_venda"] <= pd.Timestamp(data_fim)]

    return df_filtrado


def gerar_circulo_raio(lat: float, lon: float, raio_km: float, n_pontos: int = 64) -> list[dict]:
    """
    Gera lista de pontos (lat, lon) formando um círculo no mapa.
    Usado para desenhar o raio no pydeck.
    """
    pontos = []
    for i in range(n_pontos + 1):
        angulo = 2 * math.pi * i / n_pontos
        # Deslocamento aproximado em graus
        dlat = (raio_km / RAIO_TERRA_KM) * math.cos(angulo)
        dlon = (raio_km / (RAIO_TERRA_KM * math.cos(math.radians(lat)))) * math.sin(angulo)
        pontos.append({
            "lat": lat + math.degrees(dlat),
            "lon": lon + math.degrees(dlon),
        })
    return pontos

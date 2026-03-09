"""
Módulo de geocodificação — converte endereços em coordenadas lat/long.
"""

import os
import time

import pandas as pd
from dotenv import load_dotenv
from geopy.geocoders import GoogleV3, Nominatim
from geopy.extra.rate_limiter import RateLimiter

load_dotenv()


def _get_geocoder():
    """Retorna geocoder configurado (Google Maps se disponível, senão Nominatim)."""
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if api_key:
        return GoogleV3(api_key=api_key)
    return Nominatim(user_agent="analise-geo-vendas-tenda")


def geocodificar_endereco(endereco: str) -> tuple[float | None, float | None]:
    """
    Geocodifica um endereço textual.
    Retorna (latitude, longitude) ou (None, None) se não encontrar.
    """
    geocoder = _get_geocoder()
    try:
        location = geocoder.geocode(endereco, timeout=10)
        if location:
            return location.latitude, location.longitude
    except Exception:
        pass
    return None, None


def geocodificar_dataframe(
    df: pd.DataFrame,
    col_endereco: str = "endereco_empreendimento",
    col_cidade: str = "cidade",
    col_estado: str = "estado",
    delay: float = 1.0,
) -> pd.DataFrame:
    """
    Geocodifica linhas do DataFrame que não têm lat/long.
    Adiciona/atualiza colunas 'latitude' e 'longitude'.

    Args:
        df: DataFrame com endereços
        col_endereco: nome da coluna de endereço
        col_cidade: nome da coluna de cidade
        col_estado: nome da coluna de estado
        delay: segundos entre requests (respeitar rate limit)
    """
    geocoder = _get_geocoder()
    geocode = RateLimiter(geocoder.geocode, min_delay_seconds=delay)

    if "latitude" not in df.columns:
        df["latitude"] = None
    if "longitude" not in df.columns:
        df["longitude"] = None

    # Só geocodifica linhas sem coordenadas
    mask = df["latitude"].isna() | df["longitude"].isna()
    total = mask.sum()

    if total == 0:
        return df

    for idx in df[mask].index:
        partes = []
        if col_endereco in df.columns and pd.notna(df.at[idx, col_endereco]):
            partes.append(str(df.at[idx, col_endereco]))
        if col_cidade in df.columns and pd.notna(df.at[idx, col_cidade]):
            partes.append(str(df.at[idx, col_cidade]))
        if col_estado in df.columns and pd.notna(df.at[idx, col_estado]):
            partes.append(str(df.at[idx, col_estado]))
        partes.append("Brasil")

        endereco_completo = ", ".join(partes)

        try:
            location = geocode(endereco_completo, timeout=10)
            if location:
                df.at[idx, "latitude"] = location.latitude
                df.at[idx, "longitude"] = location.longitude
        except Exception:
            continue

    return df

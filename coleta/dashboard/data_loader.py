"""
Camada de dados do Dashboard — carrega e filtra dados do SQLite.
"""
import sqlite3
import pandas as pd
import streamlit as st
from dashboard.config import DB_PATH

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.regionais import classificar_regional, nome_regional


@st.cache_data(ttl=300)
def carregar_dados_completos():
    """Carrega todos os empreendimentos do banco. Cache de 5 min."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM empreendimentos", conn)
    conn.close()

    # Adicionar coluna regional (tratando NaN/float)
    df["regional"] = df.apply(
        lambda r: classificar_regional(
            str(r["cidade"]) if pd.notna(r.get("cidade")) else "",
            str(r["estado"]) if pd.notna(r.get("estado")) else ""
        ), axis=1
    )
    df["regional_nome"] = df["regional"].apply(nome_regional)

    # Converter coordenadas para float
    for col in ["latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Converter preço para float
    df["preco_a_partir"] = pd.to_numeric(df["preco_a_partir"], errors="coerce")
    df["area_min_m2"] = pd.to_numeric(df["area_min_m2"], errors="coerce")
    df["area_max_m2"] = pd.to_numeric(df["area_max_m2"], errors="coerce")
    df["total_unidades"] = pd.to_numeric(df["total_unidades"], errors="coerce")

    return df


def aplicar_filtros(df, filtros):
    """Aplica filtros globais ao DataFrame."""
    resultado = df.copy()

    if filtros.get("estados"):
        resultado = resultado[resultado["estado"].isin(filtros["estados"])]
    if filtros.get("cidades"):
        resultado = resultado[resultado["cidade"].isin(filtros["cidades"])]
    if filtros.get("empresas"):
        resultado = resultado[resultado["empresa"].isin(filtros["empresas"])]
    if filtros.get("fases"):
        resultado = resultado[resultado["fase"].isin(filtros["fases"])]
    if filtros.get("regionais"):
        resultado = resultado[resultado["regional"].isin(filtros["regionais"])]
    if filtros.get("regionais_nome"):
        resultado = resultado[resultado["regional_nome"].isin(filtros["regionais_nome"])]
    if filtros.get("clusters"):
        resultado = resultado[resultado["cluster_mpr"].isin(filtros["clusters"])]

    return resultado


def opcoes_unicas(df, campo):
    """Retorna valores únicos não-nulos de um campo, ordenados."""
    vals = df[campo].dropna().unique().tolist()
    return sorted([str(v) for v in vals if v and str(v).strip()])

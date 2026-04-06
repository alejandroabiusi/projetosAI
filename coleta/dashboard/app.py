"""
Dashboard de Inteligência Competitiva — Empreendimentos Imobiliários
====================================================================
streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.data_loader import carregar_dados_completos, aplicar_filtros, opcoes_unicas
from dashboard.config import FASES_ORDEM

st.set_page_config(
    page_title="Radar IM — Inteligência de Mercado",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

df_completo = carregar_dados_completos()

# CSS para header sticky
st.markdown("""
<style>
    [data-testid="stHeader"] { display: none; }
    .block-container { padding-top: 1rem; }
    [data-testid="stExpander"] {
        position: sticky;
        top: 0;
        z-index: 999;
        background: white;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# SIDEBAR — APENAS NAVEGAÇÃO
# ============================================================
with st.sidebar:
    st.title("🏗️ Radar IM")
    st.divider()
    pagina = st.radio(
        "Módulo",
        ["Mapa", "Visão Geral", "Tipologias", "Lazer", "Preços", "Lançamentos"],
    )

# ============================================================
# HEADER — FILTROS (compacto, retrátil, sticky)
# ============================================================
with st.expander("Filtros", expanded=True):
    col_regional, col_uf, col_cidade, col_empresa, col_fase, col_flags = st.columns([2, 1.5, 2, 2, 1.5, 1])

    with col_regional:
        from config.regionais import REGIONAIS, ORDEM_REGIONAIS
        ordem_nomes = [REGIONAIS[k]["nome"] for k in ORDEM_REGIONAIS if k in REGIONAIS]
        outros = sorted(set(df_completo["regional_nome"].dropna().unique()) - set(ordem_nomes))
        regionais_disponiveis = ordem_nomes + outros
        regionais = st.multiselect("Regional", regionais_disponiveis, default=[], key="f_regional")

    with col_uf:
        df_f0 = df_completo[df_completo["regional_nome"].isin(regionais)] if regionais else df_completo
        estados_disponiveis = opcoes_unicas(df_f0, "estado")
        estados = st.multiselect("UF", estados_disponiveis, default=[], key="f_estado")

    with col_cidade:
        df_f1 = df_f0[df_f0["estado"].isin(estados)] if estados else df_f0
        cidades_disponiveis = opcoes_unicas(df_f1, "cidade")
        cidades = st.multiselect("Cidade", cidades_disponiveis, default=[], key="f_cidade")

    with col_empresa:
        df_f2 = df_f1[df_f1["cidade"].isin(cidades)] if cidades else df_f1
        empresas_disponiveis = opcoes_unicas(df_f2, "empresa")
        empresas = st.multiselect("Empresa", empresas_disponiveis, default=[], key="f_empresa")

    with col_fase:
        fases_disponiveis = [f for f in FASES_ORDEM if f in df_completo["fase"].values]
        fases = st.multiselect("Fase", fases_disponiveis, default=[], key="f_fase")

    with col_flags:
        apenas_mcmv = st.checkbox("MCMV", value=True, key="f_mcmv")
        incluir_tenda = st.checkbox("Tenda", value=False, key="f_tenda")

# Aplicar filtros
filtros = {}
if regionais:
    filtros["regionais_nome"] = regionais
if estados:
    filtros["estados"] = estados
if cidades:
    filtros["cidades"] = cidades
if empresas:
    filtros["empresas"] = empresas
if fases:
    filtros["fases"] = fases

df = aplicar_filtros(df_completo, filtros)

if apenas_mcmv:
    df = df[df["prog_mcmv"] != 0]

if not incluir_tenda:
    df = df[df["empresa"] != "Tenda"]

n_total = len(df)
n_empresas = df["empresa"].nunique()
n_cidades = df["cidade"].nunique()
st.markdown(
    f"<div style='margin:-10px 0 -5px 0;padding:0;font-size:13px;color:#888'>"
    f"{n_total:,} empreendimentos · {n_empresas} empresas · {n_cidades} cidades</div>"
    f"<hr style='margin:4px 0 8px 0;border:none;border-top:1px solid #eee'>",
    unsafe_allow_html=True,
)

# ============================================================
# SESSION STATE
# ============================================================
st.session_state["df"] = df
st.session_state["df_completo"] = df_completo
st.session_state["filtros"] = filtros

# ============================================================
# ROUTING
# ============================================================
if pagina == "Mapa":
    from dashboard.modules import pg_mapa
    pg_mapa.render(df, filtros)
elif pagina == "Visão Geral":
    from dashboard.modules import pg_visao_geral
    pg_visao_geral.render(df, df_completo)
elif pagina == "Tipologias":
    from dashboard.modules import pg_tipologias
    pg_tipologias.render(df)
elif pagina == "Lazer":
    from dashboard.modules import pg_lazer
    pg_lazer.render(df)
elif pagina == "Preços":
    from dashboard.modules import pg_precos
    pg_precos.render(df)
elif pagina == "Lançamentos":
    from dashboard.modules import pg_lancamentos
    pg_lancamentos.render(df)

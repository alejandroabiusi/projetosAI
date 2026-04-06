"""Módulo Tipologias — Análise de dormitórios e metragens."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dashboard.config import APTO_COLUNAS, APTO_NOMES


def render(df):
    st.header("Análise de Tipologias")

    if df.empty:
        st.warning("Nenhum dado com os filtros selecionados.")
        return

    # Tabela: % por empresa que tem cada tipologia
    st.subheader("Tipologias por Empresa")

    # Conta empreendimentos COM dados de dormitórios por empresa
    empresas_dados = []
    for empresa, grupo in df.groupby("empresa"):
        com_dorms = grupo["dormitorios_descricao"].notna().sum()
        if com_dorms < 2:
            continue
        total = len(grupo)
        row = {"Empresa": empresa, "Total": total, "C/ dados": com_dorms}
        for col in APTO_COLUNAS:
            nome = APTO_NOMES.get(col, col)
            com = (grupo[col] == 1).sum() if col in grupo.columns else 0
            row[nome] = f"{100*com/total:.0f}%"
        empresas_dados.append(row)

    if empresas_dados:
        df_tip = pd.DataFrame(empresas_dados).sort_values("C/ dados", ascending=False)
        st.dataframe(df_tip, use_container_width=True, hide_index=True, height=500)

    st.divider()

    # Heatmap
    st.subheader("Heatmap: Empresas × Tipologias")

    heatmap_data = []
    for empresa, grupo in df.groupby("empresa"):
        com_dorms = grupo["dormitorios_descricao"].notna().sum()
        total = len(grupo)
        if com_dorms < 5:
            continue
        row = {"Empresa": empresa}
        for col in APTO_COLUNAS[:6]:  # 1D, 2D, 3D, 4D, Suite, Terraço
            nome = APTO_NOMES.get(col, col)
            com = (grupo[col] == 1).sum() if col in grupo.columns else 0
            row[nome] = round(100 * com / total)
        heatmap_data.append(row)

    if heatmap_data:
        df_heat = pd.DataFrame(heatmap_data).set_index("Empresa").sort_index()
        fig = px.imshow(
            df_heat.values,
            labels=dict(x="Tipologia", y="Empresa", color="% Empreend."),
            x=df_heat.columns.tolist(),
            y=df_heat.index.tolist(),
            color_continuous_scale="YlOrRd",
            aspect="auto",
            height=max(400, len(df_heat) * 20),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Distribuição de metragens
    st.subheader("Distribuição de Metragens")

    col1, col2 = st.columns(2)
    with col1:
        df_area = df[df["area_min_m2"].notna() & (df["area_min_m2"] > 10) & (df["area_min_m2"] < 200)]
        if not df_area.empty:
            fig = px.histogram(
                df_area, x="area_min_m2", nbins=30,
                title="Área Mínima (m²)",
                labels={"area_min_m2": "Área mín. (m²)"},
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        df_area = df[df["area_max_m2"].notna() & (df["area_max_m2"] > 10) & (df["area_max_m2"] < 200)]
        if not df_area.empty:
            fig = px.histogram(
                df_area, x="area_max_m2", nbins=30,
                title="Área Máxima (m²)",
                labels={"area_max_m2": "Área máx. (m²)"},
            )
            st.plotly_chart(fig, use_container_width=True)

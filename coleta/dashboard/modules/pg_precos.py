"""Módulo Preços — Comparativo de preços.
Exibe apenas empresas com maior volume de dados de preço nos filtros aplicados."""
import streamlit as st
import plotly.express as px
import pandas as pd
from dashboard.config import CORES


def render(df):
    st.header("Análise de Preços")

    df_preco = df[df["preco_a_partir"].notna() & (df["preco_a_partir"] > 50000)].copy()

    if df_preco.empty:
        st.warning("Nenhum empreendimento com preço nos filtros selecionados.")
        st.info(f"Apenas {(df['preco_a_partir'].notna() & (df['preco_a_partir'] > 50000)).sum()}/{len(df)} registros têm preço.")
        return

    st.caption(f"{len(df_preco)} empreendimentos com preço informado")

    # Ranking de empresas por volume de dados de preço (para todos os gráficos)
    empresa_rank = df_preco["empresa"].value_counts()
    top_empresas = empresa_rank.head(15).index.tolist()

    # KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Preço médio", f"R$ {df_preco['preco_a_partir'].mean():,.0f}")
    with col2:
        st.metric("Preço mediano", f"R$ {df_preco['preco_a_partir'].median():,.0f}")
    with col3:
        st.metric("Faixa", f"R$ {df_preco['preco_a_partir'].min():,.0f} — R$ {df_preco['preco_a_partir'].max():,.0f}")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(f"Preço por Empresa (Top {len(top_empresas)} com mais dados)")
        df_top = df_preco[df_preco["empresa"].isin(top_empresas)]

        # Ordenar pelo volume de dados (mais dados primeiro)
        ordem = empresa_rank.loc[top_empresas].index.tolist()

        fig = px.box(
            df_top, x="preco_a_partir", y="empresa",
            orientation="h", height=max(400, len(top_empresas) * 35),
            labels={"preco_a_partir": "Preço (R$)", "empresa": "Empresa"},
            color="empresa", color_discrete_map=CORES,
            category_orders={"empresa": ordem},
        )
        fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Preço por Regional")
        fig2 = px.box(
            df_preco, x="preco_a_partir", y="regional_nome",
            orientation="h", height=400,
            labels={"preco_a_partir": "Preço (R$)", "regional_nome": "Regional"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Scatter: preço × área (apenas top empresas)
    st.subheader("Preço × Área")
    df_scatter = df_preco[
        df_preco["area_min_m2"].notna() &
        (df_preco["area_min_m2"] > 10) &
        df_preco["empresa"].isin(top_empresas)
    ]
    if not df_scatter.empty:
        fig3 = px.scatter(
            df_scatter, x="area_min_m2", y="preco_a_partir",
            color="empresa", hover_name="nome",
            color_discrete_map=CORES,
            labels={"area_min_m2": "Área mín. (m²)", "preco_a_partir": "Preço (R$)"},
            height=500,
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Tabela resumo (todas as empresas com preço, ordenadas por volume)
    st.divider()
    st.subheader("Preço médio por Empresa")
    resumo = (
        df_preco.groupby("empresa")["preco_a_partir"]
        .agg(["count", "mean", "median", "min", "max"])
        .round(0)
        .sort_values("count", ascending=False)
        .rename(columns={"count": "Qtd c/ preço", "mean": "Média", "median": "Mediana", "min": "Mín", "max": "Máx"})
    )
    resumo = resumo.map(lambda x: f"R$ {x:,.0f}" if isinstance(x, (int, float)) and x > 100 else x)
    st.dataframe(resumo, use_container_width=True)

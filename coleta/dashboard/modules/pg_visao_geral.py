"""Módulo Visão Geral — KPIs e overview do banco."""
import streamlit as st
import plotly.express as px
import pandas as pd
from dashboard.config import CORES, FASE_CORES, FASES_ORDEM


def render(df, df_completo):
    st.header("Visão Geral")

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Empreendimentos", f"{len(df):,}")
    with col2:
        st.metric("Empresas", df["empresa"].nunique())
    with col3:
        st.metric("Cidades", df["cidade"].nunique())
    with col4:
        com_coords = df["latitude"].notna().sum()
        st.metric("Com coordenadas", f"{100*com_coords/len(df):.0f}%")
    with col5:
        unidades = df["total_unidades"].sum()
        st.metric("Total unidades", f"{int(unidades):,}" if pd.notna(unidades) else "N/A")

    st.divider()

    # Gráficos
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Empreendimentos por Empresa (Top 20)")
        top = df["empresa"].value_counts().head(20).reset_index()
        top.columns = ["Empresa", "Count"]
        top["Cor"] = top["Empresa"].map(CORES).fillna("#999")

        fig = px.bar(
            top, x="Count", y="Empresa", orientation="h",
            color="Empresa", color_discrete_map=CORES,
            height=500,
        )
        fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Distribuição por Fase")
        fase_counts = df["fase"].value_counts().reset_index()
        fase_counts.columns = ["Fase", "Count"]

        fig2 = px.pie(
            fase_counts, values="Count", names="Fase",
            color="Fase", color_discrete_map=FASE_CORES,
            height=350,
        )
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Por Regional")
        reg_counts = df["regional_nome"].value_counts().reset_index()
        reg_counts.columns = ["Regional", "Count"]
        fig3 = px.bar(reg_counts, x="Regional", y="Count", color="Regional", height=300)
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    # Completude
    st.divider()
    st.subheader("Completude dos Dados")

    campos = ["latitude", "cidade", "fase", "dormitorios_descricao", "endereco",
              "preco_a_partir", "total_unidades", "itens_lazer", "area_min_m2"]

    completude = []
    for campo in campos:
        com = df[campo].notna().sum()
        if campo in ("latitude", "longitude"):
            com = ((df[campo].notna()) & (df[campo] != 0)).sum()
        pct = 100 * com / len(df) if len(df) > 0 else 0
        completude.append({"Campo": campo, "Preenchidos": com, "Total": len(df), "%": f"{pct:.1f}%"})

    st.dataframe(pd.DataFrame(completude), use_container_width=True, hide_index=True)

"""Módulo Lazer — Análise de amenidades."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dashboard.config import LAZER_COLUNAS, LAZER_NOMES


def render(df):
    st.header("Análise de Amenidades de Lazer")

    if df.empty:
        st.warning("Nenhum dado com os filtros selecionados.")
        return

    # Top amenidades
    st.subheader("Amenidades mais comuns")

    amenidade_counts = []
    for col in LAZER_COLUNAS:
        if col in df.columns:
            com = (df[col] == 1).sum()
            pct = 100 * com / len(df) if len(df) > 0 else 0
            amenidade_counts.append({
                "Amenidade": LAZER_NOMES.get(col, col),
                "Empreendimentos": com,
                "%": round(pct, 1),
            })

    df_amen = pd.DataFrame(amenidade_counts).sort_values("%", ascending=False)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_amen.head(15), x="%", y="Amenidade", orientation="h",
            title="Top 15 Amenidades (%)",
            height=450,
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(df_amen, use_container_width=True, hide_index=True, height=450)

    st.divider()

    # Comparativo por empresa
    st.subheader("Amenidades por Empresa")

    empresa_sel = st.selectbox(
        "Selecione uma empresa para radar",
        sorted(df["empresa"].unique()),
    )

    if empresa_sel:
        grupo = df[df["empresa"] == empresa_sel]
        total = len(grupo)

        if total > 0:
            valores = []
            nomes = []
            for col in LAZER_COLUNAS:
                if col in grupo.columns:
                    com = (grupo[col] == 1).sum()
                    pct = 100 * com / total
                    if pct > 0:  # Só mostra amenidades presentes
                        valores.append(round(pct))
                        nomes.append(LAZER_NOMES.get(col, col))

            if valores:
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(
                    r=valores, theta=nomes, fill="toself",
                    name=empresa_sel,
                ))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    title=f"{empresa_sel} — Amenidades ({total} empreendimentos)",
                    height=500,
                )
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Ranking de empresas por "índice de lazer"
    st.subheader("Ranking: Empresas com mais amenidades (média)")

    ranking = []
    for empresa, grupo in df.groupby("empresa"):
        lazer_cols = [c for c in LAZER_COLUNAS if c in grupo.columns]
        com_lazer = (grupo["itens_lazer"].notna() & (grupo["itens_lazer"] != "")).sum()
        if com_lazer < 3:
            continue
        media = grupo[lazer_cols].sum(axis=1).mean()
        ranking.append({"Empresa": empresa, "Empreendimentos": len(grupo), "C/ dados lazer": com_lazer, "Amenidades (média)": round(media, 1)})

    df_rank = pd.DataFrame(ranking).sort_values("C/ dados lazer", ascending=False)
    st.dataframe(df_rank, use_container_width=True, hide_index=True)

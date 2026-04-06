"""Módulo Lançamentos — Radar de novos empreendimentos."""
import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta


def render(df):
    st.header("Radar de Lançamentos")

    # Empreendimentos por data de coleta
    df["data_coleta_dt"] = pd.to_datetime(df["data_coleta"], errors="coerce")

    st.subheader("Empreendimentos coletados por período")

    col1, col2 = st.columns(2)
    with col1:
        # Últimos 7 dias
        cutoff_7d = datetime.now() - timedelta(days=7)
        novos_7d = df[df["data_coleta_dt"] >= cutoff_7d]
        st.metric("Últimos 7 dias", len(novos_7d))

    with col2:
        # Últimos 30 dias
        cutoff_30d = datetime.now() - timedelta(days=30)
        novos_30d = df[df["data_coleta_dt"] >= cutoff_30d]
        st.metric("Últimos 30 dias", len(novos_30d))

    # Timeline
    df_timeline = df[df["data_coleta_dt"].notna()].copy()
    df_timeline["mes_coleta"] = df_timeline["data_coleta_dt"].dt.to_period("M").astype(str)

    if not df_timeline.empty:
        por_mes = df_timeline.groupby("mes_coleta").size().reset_index(name="Novos")
        fig = px.bar(por_mes, x="mes_coleta", y="Novos", title="Empreendimentos coletados por mês")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Novos empreendimentos recentes (tabela)
    st.subheader("Empreendimentos mais recentes")

    if not novos_30d.empty:
        tabela = novos_30d[["empresa", "nome", "cidade", "estado", "fase", "data_coleta"]].copy()
        tabela = tabela.sort_values("data_coleta", ascending=False).head(50)
        st.dataframe(tabela, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum empreendimento coletado nos últimos 30 dias.")

    st.divider()

    # Distribuição por fase
    st.subheader("Distribuição de Fases")

    fase_empresa = df.groupby(["empresa", "fase"]).size().reset_index(name="count")

    # Alertas: empresas com muitos Breve Lançamento
    st.subheader("Alertas: Concentração de Breve Lançamento")

    alertas = []
    for empresa, grupo in df.groupby("empresa"):
        total = len(grupo)
        if total < 5:
            continue
        breves = (grupo["fase"] == "Breve Lançamento").sum()
        pct = 100 * breves / total
        if pct > 30:
            alertas.append({
                "Empresa": empresa,
                "Total": total,
                "Breve Lançamento": breves,
                "%": f"{pct:.0f}%",
                "Status": "⚠️ Possível erro" if pct > 50 else "⚡ Atenção",
            })

    if alertas:
        st.dataframe(pd.DataFrame(alertas), use_container_width=True, hide_index=True)
    else:
        st.success("Nenhuma concentração anormal de Breve Lançamento detectada.")

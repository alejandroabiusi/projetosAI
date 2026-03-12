"""
Dashboard de Oportunidades — Onde o mercado vende e a Tenda não está.
Analisa zonas geográficas (círculos de raio configurável) para identificar
regiões com alta demanda e baixa penetração da Tenda.
"""

import math

import numpy as np
import pandas as pd
import pydeck as pdk
import plotly.graph_objects as go
import streamlit as st

from src.database import get_sqlite_connection, sqlite_existe
from src.spatial import haversine_np, gerar_circulo_raio

# ──────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="Oportunidades de Mercado",
    page_icon="🎯",
    layout="wide",
)

TENDA_NOME = "TENDA (728)"

# ──────────────────────────────────────────────
# Funções de dados
# ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def carregar_ranking_cidades(data_inicio, data_fim) -> pd.DataFrame:
    """Ranking de cidades por oportunidade: mercado grande + Tenda pequena."""
    conn = get_sqlite_connection()
    df = pd.read_sql("""
        SELECT
            cidade, estado,
            COUNT(*) as vendas_mercado,
            SUM(CASE WHEN incorporadora = ? THEN 1 ELSE 0 END) as vendas_tenda,
            COUNT(DISTINCT incorporadora) as n_incorporadoras,
            COUNT(DISTINCT empreendimento) as n_empreendimentos,
            ROUND(AVG(CASE WHEN preco IS NOT NULL THEN preco END)) as ticket_medio,
            ROUND(AVG(CASE WHEN renda_cliente IS NOT NULL THEN renda_cliente END)) as renda_media
        FROM vendas
        WHERE latitude IS NOT NULL
          AND cidade IS NOT NULL
          AND data_venda >= ? AND data_venda <= ?
        GROUP BY cidade, estado
        HAVING vendas_mercado >= 50
        ORDER BY vendas_mercado DESC
    """, conn, params=[TENDA_NOME, str(data_inicio), str(data_fim)])
    conn.close()

    if not df.empty:
        df["vendas_concorrencia"] = df["vendas_mercado"] - df["vendas_tenda"]
        df["pct_tenda"] = (df["vendas_tenda"] / df["vendas_mercado"] * 100).round(1)
        max_mercado = df["vendas_mercado"].max()
        df["score"] = ((df["vendas_concorrencia"] / max_mercado) * (100 - df["pct_tenda"])).round(1)
        df = df.sort_values("score", ascending=False)

    return df


@st.cache_data(ttl=300)
def carregar_pontos_cidade(cidade, estado, data_inicio, data_fim) -> pd.DataFrame:
    """Carrega pontos geocodificados de uma cidade."""
    conn = get_sqlite_connection()
    df = pd.read_sql("""
        SELECT latitude, longitude, incorporadora, empreendimento,
               preco, tipologia
        FROM vendas
        WHERE cidade = ? AND estado = ?
          AND latitude IS NOT NULL
          AND data_venda >= ? AND data_venda <= ?
    """, conn, params=[cidade, estado, str(data_inicio), str(data_fim)])
    conn.close()

    for col in ["latitude", "longitude", "preco"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=600)
def _carregar_zonas_precalc(cidade, estado, raio_km) -> list[dict]:
    """Le centros de zona pre-calculados do banco."""
    conn = get_sqlite_connection()
    df = pd.read_sql(
        "SELECT zona_id, latitude, longitude FROM zonas_precalc WHERE cidade = ? AND estado = ? AND raio_km = ? ORDER BY zona_id",
        conn, params=[cidade, estado, raio_km],
    )
    conn.close()
    return [{"lat": r["latitude"], "lon": r["longitude"]} for _, r in df.iterrows()]


@st.cache_data(ttl=300)
def analisar_zonas(cidade, estado, data_inicio, data_fim, raio_km, min_vendas_zona) -> pd.DataFrame:
    """
    Para cada zona (circulo de raio_km), conta vendas do mercado vs Tenda.
    Usa centros de zona pre-calculados (tabela zonas_precalc).
    """
    df_pontos = carregar_pontos_cidade(cidade, estado, data_inicio, data_fim)

    if df_pontos.empty:
        return pd.DataFrame()

    zonas = _carregar_zonas_precalc(cidade, estado, raio_km)

    if not zonas:
        return pd.DataFrame()

    lats = df_pontos["latitude"].values.astype(float)
    lons = df_pontos["longitude"].values.astype(float)
    is_tenda = (df_pontos["incorporadora"] == TENDA_NOME).values

    resultados = []
    zonas_usadas = []

    for i, zona in enumerate(zonas):
        dists = haversine_np(zona["lat"], zona["lon"], lats, lons)
        dentro = dists <= raio_km

        total = int(dentro.sum())
        if total < min_vendas_zona:
            continue

        zonas_usadas.append(zona)

        tenda = int((dentro & is_tenda).sum())
        concorrencia = total - tenda
        pct_tenda = round(tenda / total * 100, 1) if total > 0 else 0

        # Incorporadoras na zona
        mask_dentro = dentro
        incorps_zona = df_pontos.loc[mask_dentro, "incorporadora"].nunique()

        # Ticket médio na zona
        precos_zona = df_pontos.loc[mask_dentro, "preco"].dropna()
        ticket = round(precos_zona.mean()) if not precos_zona.empty else 0

        # Tipologias dominantes
        tipos_zona = df_pontos.loc[mask_dentro, "tipologia"].value_counts()
        tipo_top = tipos_zona.index[0] if not tipos_zona.empty else "N/D"

        # Top empreendimentos da concorrência nesta zona
        emprs_conc = df_pontos.loc[mask_dentro & ~is_tenda, "empreendimento"].value_counts().head(3)
        top_emprs = ", ".join(emprs_conc.index.tolist()) if not emprs_conc.empty else ""

        # Empreendimentos da Tenda nesta zona
        emprs_tenda = df_pontos.loc[mask_dentro & is_tenda, "empreendimento"].value_counts().head(3)
        top_tenda = ", ".join(emprs_tenda.index.tolist()) if not emprs_tenda.empty else "—"

        resultados.append({
            "zona": i + 1,
            "lat": zona["lat"],
            "lon": zona["lon"],
            "vendas_mercado": total,
            "vendas_tenda": tenda,
            "vendas_concorrencia": concorrencia,
            "pct_tenda": pct_tenda,
            "n_incorporadoras": incorps_zona,
            "ticket_medio": ticket,
            "tipologia_dominante": tipo_top,
            "top_empreendimentos": top_emprs,
            "empreendimentos_tenda": top_tenda,
        })

    if not resultados:
        return pd.DataFrame()

    # Calcula cobertura: quantas vendas estão dentro de pelo menos uma zona
    coberto = np.zeros(len(df_pontos), dtype=bool)
    for zona in zonas_usadas:
        dists = haversine_np(zona["lat"], zona["lon"], lats, lons)
        coberto |= (dists <= raio_km)

    df_zonas = pd.DataFrame(resultados)
    df_zonas.attrs["total_vendas"] = len(df_pontos)
    df_zonas.attrs["vendas_cobertas"] = int(coberto.sum())
    df_zonas.attrs["pct_cobertura"] = round(coberto.sum() / len(df_pontos) * 100, 1) if len(df_pontos) > 0 else 0

    # Score de oportunidade: mercado alto * (1 - presença Tenda)
    max_mercado = df_zonas["vendas_mercado"].max()
    df_zonas["score"] = (
        (df_zonas["vendas_concorrencia"] / max_mercado) * (100 - df_zonas["pct_tenda"])
    ).round(1)
    df_zonas = df_zonas.sort_values("score", ascending=False).reset_index(drop=True)
    df_zonas["ranking"] = range(1, len(df_zonas) + 1)

    return df_zonas


@st.cache_data(ttl=300)
def carregar_perfil_oportunidade(cidade, estado, data_inicio, data_fim) -> dict:
    """Perfil detalhado da oportunidade numa cidade."""
    conn = get_sqlite_connection()

    df_tipo = pd.read_sql("""
        SELECT tipologia,
               COUNT(*) as total,
               SUM(CASE WHEN incorporadora = ? THEN 1 ELSE 0 END) as tenda,
               SUM(CASE WHEN incorporadora != ? THEN 1 ELSE 0 END) as concorrencia
        FROM vendas
        WHERE cidade = ? AND estado = ?
          AND latitude IS NOT NULL AND tipologia IS NOT NULL
          AND data_venda >= ? AND data_venda <= ?
        GROUP BY tipologia ORDER BY total DESC
    """, conn, params=[TENDA_NOME, TENDA_NOME, cidade, estado, str(data_inicio), str(data_fim)])

    df_preco = pd.read_sql("""
        SELECT
            CASE
                WHEN preco < 150000 THEN 'Até 150k'
                WHEN preco < 200000 THEN '150k-200k'
                WHEN preco < 250000 THEN '200k-250k'
                WHEN preco < 300000 THEN '250k-300k'
                WHEN preco < 350000 THEN '300k-350k'
                ELSE 'Acima 350k'
            END as faixa_preco,
            COUNT(*) as total,
            SUM(CASE WHEN incorporadora = ? THEN 1 ELSE 0 END) as tenda,
            SUM(CASE WHEN incorporadora != ? THEN 1 ELSE 0 END) as concorrencia
        FROM vendas
        WHERE cidade = ? AND estado = ?
          AND latitude IS NOT NULL AND preco IS NOT NULL
          AND data_venda >= ? AND data_venda <= ?
        GROUP BY faixa_preco ORDER BY MIN(preco)
    """, conn, params=[TENDA_NOME, TENDA_NOME, cidade, estado, str(data_inicio), str(data_fim)])

    df_incorp = pd.read_sql("""
        SELECT incorporadora, COUNT(*) as vendas,
               ROUND(AVG(preco)) as ticket_medio
        FROM vendas
        WHERE cidade = ? AND estado = ?
          AND latitude IS NOT NULL
          AND data_venda >= ? AND data_venda <= ?
        GROUP BY incorporadora ORDER BY vendas DESC LIMIT 15
    """, conn, params=[cidade, estado, str(data_inicio), str(data_fim)])

    df_emprs = pd.read_sql("""
        SELECT empreendimento, incorporadora, COUNT(*) as vendas,
               ROUND(AVG(preco)) as ticket_medio,
               MIN(tipologia) as tipologia
        FROM vendas
        WHERE cidade = ? AND estado = ?
          AND latitude IS NOT NULL AND incorporadora != ?
          AND data_venda >= ? AND data_venda <= ?
        GROUP BY empreendimento ORDER BY vendas DESC LIMIT 15
    """, conn, params=[cidade, estado, TENDA_NOME, str(data_inicio), str(data_fim)])

    conn.close()
    return {
        "tipologias": df_tipo,
        "faixas_preco": df_preco,
        "incorporadoras": df_incorp,
        "empreendimentos": df_emprs,
    }


# ──────────────────────────────────────────────
# Interface
# ──────────────────────────────────────────────

st.title("Oportunidades de Mercado")
st.caption("Onde o mercado vende e a Tenda ainda não está")

if not sqlite_existe():
    st.warning("Nenhum dado carregado. Importe um arquivo na página principal.")
    st.stop()

# Sidebar — filtros
with st.sidebar:
    st.subheader("Período de Análise")
    col1, col2 = st.columns(2)
    with col1:
        data_inicio = st.date_input("De", value=pd.Timestamp("2024-01-01").date(),
                                     format="DD/MM/YYYY", key="oport_dt_ini")
    with col2:
        data_fim = st.date_input("Até", value=pd.Timestamp.now().date(),
                                  format="DD/MM/YYYY", key="oport_dt_fim")

    # Placeholder para filtro UF/Cidade (preenchido após carregar ranking)
    _sidebar_cidade_container = st.container()

    st.divider()
    min_vendas = st.slider("Mínimo de vendas na cidade", 50, 5000, 200, step=50)

    st.divider()
    st.subheader("Análise por Zonas")
    raio_zona = st.select_slider("Raio das zonas (km)",
                                  options=[2, 3, 5, 7, 10], value=5,
                                  key="oport_raio_zona")
    min_vendas_zona = st.slider("Mínimo de vendas por zona", 10, 500, 30, step=10,
                                 key="oport_min_zona")

# ── Ranking de Oportunidades ──
ranking = carregar_ranking_cidades(data_inicio, data_fim)

if ranking.empty:
    st.warning("Sem dados suficientes no período selecionado.")
    st.stop()

ranking_filtrado = ranking[ranking["vendas_mercado"] >= min_vendas].copy()

# Métricas gerais
col_m1, col_m2, col_m3, col_m4 = st.columns(4)
cidades_sem_tenda = len(ranking_filtrado[ranking_filtrado["vendas_tenda"] == 0])
with col_m1:
    st.metric("Cidades analisadas", len(ranking_filtrado))
with col_m2:
    st.metric("Cidades sem Tenda", cidades_sem_tenda)
with col_m3:
    vendas_oport = ranking_filtrado[ranking_filtrado["vendas_tenda"] == 0]["vendas_mercado"].sum()
    st.metric("Vendas em cidades sem Tenda", f"{vendas_oport:,}")
with col_m4:
    media_pct = ranking_filtrado["pct_tenda"].mean()
    st.metric("Market share médio Tenda", f"{media_pct:.1f}%")

st.markdown("---")

# ── Filtro UF + Cidade (sidebar, no container reservado acima) ──
_ufs_disponiveis = sorted(ranking_filtrado["estado"].dropna().unique().tolist())

with _sidebar_cidade_container:
    st.divider()
    st.subheader("Filtro de Cidade")
    _uf_sel = st.selectbox("UF", ["Todas"] + _ufs_disponiveis, key="oport_uf")

    if _uf_sel != "Todas":
        _ranking_uf = ranking_filtrado[ranking_filtrado["estado"] == _uf_sel]
    else:
        _ranking_uf = ranking_filtrado

    # Ordena alfabeticamente e inclui quantidade no label
    _ranking_uf_sorted = _ranking_uf.sort_values("cidade", ascending=True)
    _cidades_opcoes = [
        f"{row['cidade']} ({int(row['vendas_mercado']):,} vendas)"
        for _, row in _ranking_uf_sorted.iterrows()
    ]
    _cidades_keys = [
        f"{row['cidade']}/{row['estado']}"
        for _, row in _ranking_uf_sorted.iterrows()
    ]
    _cidade_map = dict(zip(_cidades_opcoes, _cidades_keys))

    _cidade_label = st.selectbox(
        "Cidade", _cidades_opcoes, index=None,
        placeholder="Selecione uma cidade...",
        key="oport_cidade_global",
    ) if _cidades_opcoes else None
    _cidade_sel_global = _cidade_map.get(_cidade_label) if _cidade_label else None

tab_ranking, tab_zonas, tab_perfil = st.tabs([
    "Ranking de Cidades",
    "Mapa de Zonas de Oportunidade",
    "Perfil da Oportunidade",
])

# ── Tab 1: Ranking de Cidades ──
with tab_ranking:
    st.subheader("Cidades com maior potencial")

    top20 = ranking_filtrado.head(20).copy()
    top20["label"] = top20["cidade"] + "/" + top20["estado"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=top20["label"], x=top20["vendas_concorrencia"],
        name="Concorrência", orientation="h", marker_color="#EF5350",
    ))
    fig.add_trace(go.Bar(
        y=top20["label"], x=top20["vendas_tenda"],
        name="Tenda", orientation="h", marker_color="#26A69A",
    ))
    fig.update_layout(
        barmode="stack",
        title="Top 20 Cidades — Vendas Mercado vs Tenda",
        xaxis_title="Vendas",
        yaxis=dict(autorange="reversed"),
        height=600, template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True, key="chart_ranking_oport")

    # Tabela completa
    st.subheader("Tabela Completa")
    df_tabela = ranking_filtrado[[
        "cidade", "estado", "vendas_mercado", "vendas_tenda",
        "vendas_concorrencia", "pct_tenda", "n_incorporadoras",
        "n_empreendimentos", "ticket_medio", "renda_media", "score",
    ]].copy()
    df_tabela.columns = [
        "Cidade", "UF", "Vendas Mercado", "Vendas Tenda",
        "Vendas Concorrência", "% Tenda", "Incorporadoras",
        "Empreendimentos", "Ticket Médio", "Renda Média", "Score",
    ]
    st.dataframe(df_tabela, use_container_width=True, height=500)


# ── Tab 2: Mapa de Zonas de Oportunidade ──
with tab_zonas:
    st.subheader(f"Zonas de Oportunidade (raio {raio_zona} km)")

    if _cidade_sel_global:
        _cidade, _estado = _cidade_sel_global.rsplit("/", 1)

        with st.spinner("Analisando zonas geográficas..."):
            df_zonas = analisar_zonas(_cidade, _estado, data_inicio, data_fim,
                                      raio_zona, min_vendas_zona)

        if df_zonas.empty:
            st.warning("Sem dados suficientes para criar zonas nesta cidade.")
        else:
            # Métricas da cidade
            info_cidade = ranking_filtrado[
                (ranking_filtrado["cidade"] == _cidade) & (ranking_filtrado["estado"] == _estado)
            ].iloc[0]

            pct_cob = df_zonas.attrs.get("pct_cobertura", 0)
            vendas_cob = df_zonas.attrs.get("vendas_cobertas", 0)
            total_cid = df_zonas.attrs.get("total_vendas", 0)

            col_i1, col_i2, col_i3, col_i4, col_i5 = st.columns(5)
            with col_i1:
                st.metric("Vendas do Mercado", f"{int(info_cidade['vendas_mercado']):,}")
            with col_i2:
                st.metric("Vendas da Tenda", f"{int(info_cidade['vendas_tenda']):,}")
            with col_i3:
                st.metric("Zonas mapeadas", len(df_zonas))
            with col_i4:
                zonas_sem_tenda = len(df_zonas[df_zonas["vendas_tenda"] == 0])
                st.metric("Zonas sem Tenda", zonas_sem_tenda)
            with col_i5:
                st.metric("Cobertura", f"{pct_cob}%",
                          help=f"{vendas_cob:,} de {total_cid:,} vendas estão dentro de alguma zona")

            # ── Mapa com círculos coloridos por score ──
            layers = []

            # Normaliza score para cor (vermelho = mais oportunidade, verde = Tenda presente)
            max_score = df_zonas["score"].max()
            min_score = df_zonas["score"].min()

            df_zonas_mapa = df_zonas.copy()

            # Gera círculos para cada zona
            for _, zona in df_zonas_mapa.iterrows():
                # Cor: vermelho intenso = alta oportunidade, verde = Tenda presente
                if max_score > min_score:
                    t = (zona["score"] - min_score) / (max_score - min_score)
                else:
                    t = 0.5
                r = int(220 * t + 40 * (1 - t))
                g = int(40 * t + 180 * (1 - t))
                b = int(40 * t + 80 * (1 - t))
                cor = [r, g, b, 80]

                circulo = gerar_circulo_raio(zona["lat"], zona["lon"], raio_zona, n_pontos=48)
                path = [[p["lon"], p["lat"]] for p in circulo]

                # Círculo preenchido (polygon)
                _zona_info = (f"<b>Zona #{int(zona['ranking'])}</b><br/>"
                              f"Score: {zona['score']}<br/>"
                              f"Vendas mercado: {int(zona['vendas_mercado'])}<br/>"
                              f"Vendas Tenda: {int(zona['vendas_tenda'])}<br/>"
                              f"% Tenda: {zona['pct_tenda']}%")
                layers.append(pdk.Layer(
                    "PolygonLayer",
                    data=[{"polygon": path, "info": _zona_info}],
                    get_polygon="polygon",
                    get_fill_color=cor,
                    get_line_color=[r, g, b, 180],
                    line_width_min_pixels=2,
                    pickable=True,
                    auto_highlight=True,
                ))

            # Pins de empreendimentos da cidade (agrupados)
            df_pontos = carregar_pontos_cidade(_cidade, _estado, data_inicio, data_fim)
            if not df_pontos.empty:
                emprs_mapa = df_pontos.groupby("empreendimento").agg(
                    latitude=("latitude", "mean"),
                    longitude=("longitude", "mean"),
                    incorporadora=("incorporadora", "first"),
                    vendas=("empreendimento", "size"),
                    tipologia=("tipologia", "first"),
                    ticket=("preco", "mean"),
                ).reset_index()
                emprs_mapa["ticket"] = emprs_mapa["ticket"].round(0).fillna(0).astype(int)

                # Cores por incorporadora
                incorps = emprs_mapa["incorporadora"].dropna().unique()
                paleta = [
                    [38, 166, 154], [66, 165, 245], [255, 112, 67], [171, 71, 188],
                    [102, 187, 106], [255, 202, 40], [239, 83, 80], [141, 110, 99],
                    [120, 144, 156], [236, 64, 122],
                ]
                cor_incorp = {inc: paleta[i % len(paleta)] for i, inc in enumerate(incorps)}
                emprs_mapa["cor"] = emprs_mapa["incorporadora"].map(
                    lambda x: cor_incorp.get(x, [158, 158, 158]) + [220]
                )
                # Formata tooltip do empreendimento
                emprs_mapa["info"] = emprs_mapa.apply(
                    lambda r: f"<b>{r['empreendimento']}</b><br/>"
                              f"{r['incorporadora']}<br/>"
                              f"{r['vendas']} vendas | {r['tipologia']}<br/>"
                              f"Ticket: R$ {r['ticket']:,}",
                    axis=1
                )

                layers.append(pdk.Layer(
                    "ScatterplotLayer",
                    data=emprs_mapa,
                    get_position=["longitude", "latitude"],
                    get_color="cor",
                    get_radius=120,
                    pickable=True,
                    opacity=0.9,
                    auto_highlight=True,
                ))

            # Pin central de cada zona (com número do ranking)
            df_pins = df_zonas_mapa.head(10).copy()  # só top 10 com labels
            df_pins["label"] = df_pins["ranking"].astype(str)
            df_pins["cor_pin"] = df_pins.apply(
                lambda r: [220, 40, 40, 230] if r["pct_tenda"] < 5
                else [255, 165, 0, 200] if r["pct_tenda"] < 20
                else [40, 180, 80, 180], axis=1
            )

            layers.append(pdk.Layer(
                "TextLayer",
                data=df_pins,
                get_position=["lon", "lat"],
                get_text="label",
                get_size=18,
                get_color=[255, 255, 255],
                get_angle=0,
                background=True,
                get_background_color=[0, 0, 0, 160],
                background_padding=[4, 2],
                get_text_anchor="'middle'",
                get_alignment_baseline="'center'",
            ))

            center_lat = df_zonas_mapa["lat"].mean()
            center_lon = df_zonas_mapa["lon"].mean()

            lat_range = df_zonas_mapa["lat"].max() - df_zonas_mapa["lat"].min()
            lon_range = df_zonas_mapa["lon"].max() - df_zonas_mapa["lon"].min()
            spread = max(lat_range, lon_range, 0.01)
            if spread < 0.05:
                zoom = 13
            elif spread < 0.1:
                zoom = 12
            elif spread < 0.3:
                zoom = 11
            elif spread < 0.5:
                zoom = 10
            else:
                zoom = 9

            deck = pdk.Deck(
                layers=layers,
                initial_view_state=pdk.ViewState(
                    latitude=center_lat, longitude=center_lon,
                    zoom=zoom, pitch=0,
                ),
                tooltip={
                    "html": "{info}",
                    "style": {"backgroundColor": "#1a1a2e", "color": "white"},
                },
            )
            st.pydeck_chart(deck, use_container_width=True, height=550, key="mapa_zonas")

            # Legenda
            st.markdown(
                '<span style="color: rgb(220,40,40); font-size:16px;">●</span> Alta oportunidade (mercado forte, Tenda ausente) &nbsp;&nbsp; '
                '<span style="color: rgb(255,165,0); font-size:16px;">●</span> Oportunidade moderada &nbsp;&nbsp; '
                '<span style="color: rgb(40,180,80); font-size:16px;">●</span> Tenda já presente',
                unsafe_allow_html=True,
            )

            # ── Tabela de zonas rankeadas ──
            st.subheader("Ranking das Zonas")
            df_tabela_zonas = df_zonas[[
                "ranking", "vendas_mercado", "vendas_tenda", "vendas_concorrencia",
                "pct_tenda", "n_incorporadoras", "ticket_medio",
                "tipologia_dominante", "top_empreendimentos", "empreendimentos_tenda", "score",
            ]].copy()
            df_tabela_zonas.columns = [
                "#", "Vendas Mercado", "Vendas Tenda", "Vendas Concorrência",
                "% Tenda", "Incorporadoras", "Ticket Médio",
                "Tipologia", "Top Concorrentes", "Tenda na zona", "Score",
            ]
            st.dataframe(df_tabela_zonas, use_container_width=True, height=400)
    else:
        st.info("Selecione uma cidade na barra lateral para analisar as zonas de oportunidade.")


# ── Tab 3: Perfil da Oportunidade ──
with tab_perfil:
    st.subheader("Perfil da Oportunidade")

    if _cidade_sel_global:
        _cidade_p, _estado_p = _cidade_sel_global.rsplit("/", 1)
        perfil = carregar_perfil_oportunidade(_cidade_p, _estado_p, data_inicio, data_fim)

        col_p1, col_p2 = st.columns(2)

        with col_p1:
            df_tipo = perfil["tipologias"]
            if not df_tipo.empty:
                fig_tipo = go.Figure()
                fig_tipo.add_trace(go.Bar(
                    x=df_tipo["tipologia"], y=df_tipo["concorrencia"],
                    name="Concorrência", marker_color="#EF5350",
                ))
                fig_tipo.add_trace(go.Bar(
                    x=df_tipo["tipologia"], y=df_tipo["tenda"],
                    name="Tenda", marker_color="#26A69A",
                ))
                fig_tipo.update_layout(
                    title="Tipologias — Concorrência vs Tenda",
                    barmode="group", template="plotly_white", height=350,
                )
                st.plotly_chart(fig_tipo, use_container_width=True, key="chart_tipo_oport")

        with col_p2:
            df_preco = perfil["faixas_preco"]
            if not df_preco.empty:
                fig_preco = go.Figure()
                fig_preco.add_trace(go.Bar(
                    x=df_preco["faixa_preco"], y=df_preco["concorrencia"],
                    name="Concorrência", marker_color="#EF5350",
                ))
                fig_preco.add_trace(go.Bar(
                    x=df_preco["faixa_preco"], y=df_preco["tenda"],
                    name="Tenda", marker_color="#26A69A",
                ))
                fig_preco.update_layout(
                    title="Faixas de Preço — Concorrência vs Tenda",
                    barmode="group", template="plotly_white", height=350,
                )
                st.plotly_chart(fig_preco, use_container_width=True, key="chart_preco_oport")

        st.subheader("Quem domina o mercado nesta cidade")
        df_incorp = perfil["incorporadoras"]
        if not df_incorp.empty:
            df_incorp["destaque"] = df_incorp["incorporadora"].apply(
                lambda x: "TENDA" if x == TENDA_NOME else ""
            )
            fig_inc = go.Figure()
            for _, row in df_incorp.iterrows():
                cor = "#26A69A" if row["incorporadora"] == TENDA_NOME else "#EF5350"
                fig_inc.add_trace(go.Bar(
                    y=[row["incorporadora"]], x=[row["vendas"]],
                    orientation="h", marker_color=cor,
                    showlegend=False,
                ))
            fig_inc.update_layout(
                title="Top Incorporadoras",
                template="plotly_white",
                yaxis=dict(autorange="reversed"),
                height=400,
            )
            st.plotly_chart(fig_inc, use_container_width=True, key="chart_incorp_oport")

        st.subheader("Empreendimentos da concorrência com mais vendas")
        df_emprs = perfil["empreendimentos"]
        if not df_emprs.empty:
            df_emprs.columns = ["Empreendimento", "Incorporadora", "Vendas", "Ticket Médio", "Tipologia"]
            st.dataframe(df_emprs, use_container_width=True)
    else:
        st.info("Selecione uma cidade na barra lateral para ver o perfil da oportunidade.")

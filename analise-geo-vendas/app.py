"""
Análise de Vendas por Geolocalização — App Streamlit
Ferramenta para análise de vendas imobiliárias da concorrência por raio geográfico.
"""

import io
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

from src.database import (
    carregar_dados_sqlite,
    contar_registros,
    importar_arquivo,
    sqlite_existe,
)
from src.geocoding import geocodificar_endereco
from src.spatial import filtrar_por_periodo, filtrar_por_raio, gerar_circulo_raio
from src.charts import (
    grafico_absorcao_mensal,
    grafico_absorcao_por_incorporadora,
    grafico_absorcao_por_faixa_preco,
    grafico_comprometimento_renda,
    grafico_comprometimento_por_incorporadora,
    grafico_distribuicao_idade,
    grafico_distribuicao_renda,
    grafico_evolucao_por_incorporadora,
    grafico_evolucao_vendas,
    grafico_faixa_metragem,
    grafico_faixa_preco,
    grafico_financiamento_vs_proprio,
    grafico_matriz_regiao_renda,
    grafico_origem_clientes_cidade,
    grafico_preco_m2_por_empreendimento,
    grafico_preco_m2_por_incorporadora,
    grafico_renda_vs_preco,
    grafico_tipologias,
    grafico_top_profissoes,
    grafico_vendas_por_incorporadora,
    tabela_comparativa_incorporadoras,
)

# ──────────────────────────────────────────────
# Configuração da página
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="Análise Geolocalizada de Vendas",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS customizado
css_path = Path(__file__).parent / "assets" / "style.css"
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Cache de dados
# ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def carregar_dados() -> pd.DataFrame:
    """Carrega dados do SQLite com cache de 5 minutos."""
    return carregar_dados_sqlite()


@st.cache_data(ttl=300)
def aplicar_filtros(
    _df: pd.DataFrame,
    data_inicio,
    data_fim,
    incorporadoras: tuple,
    cidades: tuple,
    empreendimentos: tuple,
    usar_raio: bool,
    lat_centro: float | None,
    lon_centro: float | None,
    raio_km: float | None,
    renda_min: float | None = None,
    renda_max: float | None = None,
    preco_min: float | None = None,
    preco_max: float | None = None,
    tipologias: tuple = (),
) -> pd.DataFrame:
    """Aplica todos os filtros com cache para evitar recálculo."""
    df = _df.copy()

    # Período
    df = filtrar_por_periodo(df, data_inicio, data_fim)

    # Incorporadoras
    if incorporadoras and "incorporadora" in df.columns:
        df = df[df["incorporadora"].isin(incorporadoras)]

    # Geográfico
    if usar_raio and lat_centro is not None and lon_centro is not None:
        df = filtrar_por_raio(df, lat_centro, lon_centro, raio_km)
    else:
        if cidades:
            df = df[df["cidade"].isin(cidades)]
        if empreendimentos:
            df = df[df["empreendimento"].isin(empreendimentos)]

    # Faixa de renda
    if renda_min is not None and "renda_cliente" in df.columns:
        df = df[df["renda_cliente"] >= renda_min]
    if renda_max is not None and "renda_cliente" in df.columns:
        df = df[df["renda_cliente"] <= renda_max]

    # Faixa de preço
    if preco_min is not None and "preco" in df.columns:
        df = df[df["preco"] >= preco_min]
    if preco_max is not None and "preco" in df.columns:
        df = df[df["preco"] <= preco_max]

    # Tipologia
    if tipologias and "tipologia" in df.columns:
        df = df[df["tipologia"].isin(tipologias)]

    return df


# ──────────────────────────────────────────────
# Sidebar — Configuração e Filtros
# ──────────────────────────────────────────────

with st.sidebar:
    st.title("Configuração")

    # ── Importação de dados ──
    with st.expander("Importar Dados", expanded=not sqlite_existe()):
        uploaded = st.file_uploader(
            "Upload arquivo de vendas",
            type=["csv", "xlsx", "xls"],
        )
        if uploaded is not None:
            tmp_path = Path("data") / uploaded.name
            tmp_path.parent.mkdir(exist_ok=True)
            tmp_path.write_bytes(uploaded.getvalue())

            if st.button("Importar para banco local"):
                with st.spinner("Importando..."):
                    n = importar_arquivo(str(tmp_path))
                    st.success(f"{n:,} registros importados!")
                    st.cache_data.clear()
                    st.rerun()

    # Verifica se há dados
    if not sqlite_existe():
        st.warning("Nenhum dado carregado. Importe um arquivo para começar.")
        st.stop()

    total = contar_registros()
    st.caption(f"Base: {total:,} registros")

    st.divider()

    # Carrega dados para preencher filtros
    df_all = carregar_dados()

    # ── Período ──
    st.subheader("Período")

    if "data_venda" in df_all.columns:
        datas_validas = df_all["data_venda"].dropna()
        if not datas_validas.empty:
            data_min = datas_validas.min().date()
            data_max = datas_validas.max().date()
        else:
            data_min = pd.Timestamp("2020-01-01").date()
            data_max = pd.Timestamp.now().date()
    else:
        data_min = pd.Timestamp("2020-01-01").date()
        data_max = pd.Timestamp.now().date()

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        data_inicio = st.date_input("De", value=data_min, min_value=data_min, max_value=data_max, format="DD/MM/YYYY")
    with col_d2:
        data_fim = st.date_input("Até", value=data_max, min_value=data_min, max_value=data_max, format="DD/MM/YYYY")

    st.divider()

    # ── Filtro por Cidade / Empreendimento ──
    st.subheader("Localização")

    cidades_sel = []
    emprs_sel = []

    if "cidade" in df_all.columns:
        cidades = sorted(df_all["cidade"].dropna().unique())
        cidades_sel = st.multiselect(
            "Cidades",
            options=cidades,
            default=[],
            help="Selecione uma ou mais cidades",
        )

    if "empreendimento" in df_all.columns:
        if cidades_sel:
            emprs_disponiveis = sorted(
                df_all[df_all["cidade"].isin(cidades_sel)]["empreendimento"].dropna().unique()
            )
        else:
            emprs_disponiveis = sorted(df_all["empreendimento"].dropna().unique())

        emprs_sel = st.multiselect(
            "Empreendimentos",
            options=emprs_disponiveis,
            default=[],
            help="Opcional — filtra empreendimentos específicos",
        )

    st.divider()

    # ── Raio geográfico (opcional) ──
    tem_coords = (
        "latitude" in df_all.columns
        and df_all["latitude"].notna().any()
    )

    lat_centro, lon_centro, raio_km = None, None, None
    usar_raio = False

    if tem_coords:
        st.subheader("Filtro por Raio")
        usar_raio = st.checkbox("Ativar filtro por raio", value=False)

        if usar_raio:
            # Se tem empreendimento selecionado (sidebar ou mapa), usa como centro
            empr_como_centro = False
            _empr_click = st.session_state.get("empr_click")
            _empr_centro_nome = None

            if _empr_click:
                df_empr = df_all[df_all["empreendimento"] == _empr_click]
                empr_lat = df_empr["latitude"].dropna().mean()
                empr_lon = df_empr["longitude"].dropna().mean()
                if pd.notna(empr_lat) and pd.notna(empr_lon):
                    empr_como_centro = True
                    _empr_centro_nome = _empr_click
            elif len(emprs_sel) == 1:
                df_empr = df_all[df_all["empreendimento"] == emprs_sel[0]]
                empr_lat = df_empr["latitude"].dropna().mean()
                empr_lon = df_empr["longitude"].dropna().mean()
                if pd.notna(empr_lat) and pd.notna(empr_lon):
                    empr_como_centro = True
                    _empr_centro_nome = emprs_sel[0]

            if empr_como_centro:
                st.info(f"Centro: {_empr_centro_nome}")
                lat_centro = empr_lat
                lon_centro = empr_lon
            else:
                modo_loc = st.radio(
                    "Centro do raio",
                    ["Endereço", "Coordenadas"],
                    horizontal=True,
                )

                if modo_loc == "Endereço":
                    endereco = st.text_input(
                        "Endereço",
                        placeholder="Rua, número, cidade, estado...",
                    )
                    if endereco:
                        if st.button("Buscar coordenadas"):
                            with st.spinner("Geocodificando..."):
                                lat, lon = geocodificar_endereco(endereco)
                                if lat and lon:
                                    st.session_state["lat_centro"] = lat
                                    st.session_state["lon_centro"] = lon
                                    st.success(f"Encontrado: {lat:.6f}, {lon:.6f}")
                                else:
                                    st.error("Endereço não encontrado.")

                    lat_centro = st.session_state.get("lat_centro")
                    lon_centro = st.session_state.get("lon_centro")
                else:
                    lat_centro = st.number_input("Latitude", value=-23.5505, format="%.6f")
                    lon_centro = st.number_input("Longitude", value=-46.6333, format="%.6f")

            raio_km = st.select_slider(
                "Raio (km)",
                options=[0.5, 1, 2, 3, 5, 10, 15, 20, 30, 50],
                value=5,
            )

        st.divider()

    # ── Filtro de incorporadoras ──
    if "incorporadora" in df_all.columns:
        incorporadoras = sorted(df_all["incorporadora"].dropna().unique())
        incorporadoras_sel = st.multiselect(
            "Incorporadoras",
            options=incorporadoras,
            default=incorporadoras,
        )
    else:
        incorporadoras_sel = []

    st.divider()

    # ── Filtros adicionais ──
    st.subheader("Filtros Adicionais")

    # Faixa de renda
    renda_min_val, renda_max_val = None, None
    if "renda_cliente" in df_all.columns:
        renda_vals = df_all["renda_cliente"].dropna()
        if not renda_vals.empty:
            r_min = int(renda_vals.quantile(0.01))
            r_max = int(renda_vals.quantile(0.99))
            renda_range = st.slider(
                "Faixa de Renda (R$)",
                min_value=r_min,
                max_value=r_max,
                value=(r_min, r_max),
                step=500,
                format="R$ %d",
            )
            renda_min_val, renda_max_val = renda_range

    # Faixa de preço
    preco_min_val, preco_max_val = None, None
    if "preco" in df_all.columns:
        preco_vals = df_all["preco"].dropna()
        if not preco_vals.empty:
            p_min = int(preco_vals.quantile(0.01))
            p_max = int(preco_vals.quantile(0.99))
            preco_range = st.slider(
                "Faixa de Preço (R$)",
                min_value=p_min,
                max_value=p_max,
                value=(p_min, p_max),
                step=10000,
                format="R$ %d",
            )
            preco_min_val, preco_max_val = preco_range

    # Tipologia
    tipologias_sel = ()
    if "tipologia" in df_all.columns:
        tips_disponiveis = sorted(df_all["tipologia"].dropna().unique())
        tipologias_sel = st.multiselect(
            "Tipologia",
            options=tips_disponiveis,
            default=tips_disponiveis,
        )


# ──────────────────────────────────────────────
# Área Principal
# ──────────────────────────────────────────────

# Limpa flag do dialog após rerun
st.session_state.pop("_dialog_aplicando", None)

st.title("Análise de Vendas por Geolocalização")

# ── Filtragem dos dados (cacheada) ──
if usar_raio and lat_centro is None:
    st.info("Selecione um empreendimento ou informe coordenadas para ativar o filtro por raio.")
    st.stop()

# Empreendimento selecionado via mapa
empr_click = st.session_state.get("empr_click")

# Se tem empr_click + raio ativado → raio centrado no empreendimento (empr_click vira centro, não filtro)
# Se tem empr_click sem raio → filtra só por aquele empreendimento
if empr_click and usar_raio:
    usar_raio_efetivo = True
    emprs_efetivos = tuple(emprs_sel)  # raio cuida da filtragem geográfica
elif empr_click:
    usar_raio_efetivo = False
    emprs_efetivos = (empr_click,)
else:
    usar_raio_efetivo = usar_raio
    emprs_efetivos = tuple(emprs_sel)

with st.spinner("Aplicando filtros e gerando análises..."):
    df_filtrado = aplicar_filtros(
        carregar_dados(),
        data_inicio,
        data_fim,
        tuple(incorporadoras_sel),
        tuple(cidades_sel),
        emprs_efetivos,
        usar_raio_efetivo,
        lat_centro,
        lon_centro,
        raio_km,
        renda_min_val,
        renda_max_val,
        preco_min_val,
        preco_max_val,
        tuple(tipologias_sel),
    )

# Banner de filtro por clique
if empr_click:
    col_aviso, col_limpar = st.columns([4, 1])
    with col_aviso:
        st.success(f"Filtrado por: **{empr_click}** (selecionado no mapa)")
    with col_limpar:
        if st.button("Limpar filtro do mapa"):
            del st.session_state["empr_click"]
            st.rerun()

# Monta descrição do filtro
partes_filtro = []
if cidades_sel:
    partes_filtro.append(", ".join(cidades_sel))
if emprs_sel:
    partes_filtro.append(f"{len(emprs_sel)} empreendimento(s)")
if usar_raio and raio_km:
    partes_filtro.append(f"Raio {raio_km} km")
titulo_filtro = " | ".join(partes_filtro) if partes_filtro else "Toda a base"

if df_filtrado.empty:
    st.warning("Nenhuma venda encontrada com os filtros selecionados.")
    st.stop()

# ── Métricas resumo ──
st.caption(f"Filtro: {titulo_filtro} | {data_inicio} a {data_fim}")
st.markdown("---")
cols_metricas = st.columns(5)

with cols_metricas[0]:
    st.metric("Vendas", f"{len(df_filtrado):,}")

with cols_metricas[1]:
    n_incorp = df_filtrado["incorporadora"].nunique() if "incorporadora" in df_filtrado.columns else 0
    st.metric("Incorporadoras", n_incorp)

with cols_metricas[2]:
    if "preco" in df_filtrado.columns and not df_filtrado["preco"].dropna().empty:
        st.metric("Ticket Médio", f"R$ {df_filtrado['preco'].mean():,.0f}")
    else:
        st.metric("Ticket Médio", "-")

with cols_metricas[3]:
    if "metragem" in df_filtrado.columns and not df_filtrado["metragem"].dropna().empty:
        st.metric("Metragem Média", f"{df_filtrado['metragem'].mean():.0f} m²")
    else:
        st.metric("Metragem Média", "-")

with cols_metricas[4]:
    if "renda_cliente" in df_filtrado.columns and not df_filtrado["renda_cliente"].dropna().empty:
        st.metric("Renda Média", f"R$ {df_filtrado['renda_cliente'].mean():,.0f}")
    else:
        st.metric("Renda Média", "-")

n_emprs = df_filtrado["empreendimento"].nunique() if "empreendimento" in df_filtrado.columns else 0
cols_metricas2 = st.columns(5)
with cols_metricas2[0]:
    st.metric("Empreendimentos", n_emprs)
with cols_metricas2[1]:
    if "valor_financiado" in df_filtrado.columns and not df_filtrado["valor_financiado"].dropna().empty:
        st.metric("Financiamento Médio", f"R$ {df_filtrado['valor_financiado'].mean():,.0f}")
    else:
        st.metric("Financiamento Médio", "-")
with cols_metricas2[2]:
    if "recursos_proprios" in df_filtrado.columns and not df_filtrado["recursos_proprios"].dropna().empty:
        st.metric("Entrada Média", f"R$ {df_filtrado['recursos_proprios'].mean():,.0f}")
    else:
        st.metric("Entrada Média", "-")
with cols_metricas2[3]:
    if "encargo_mensal" in df_filtrado.columns and not df_filtrado["encargo_mensal"].dropna().empty:
        st.metric("Encargo Mensal Médio", f"R$ {df_filtrado['encargo_mensal'].mean():,.0f}")
    else:
        st.metric("Encargo Mensal Médio", "-")
with cols_metricas2[4]:
    if "prazo_meses" in df_filtrado.columns and not df_filtrado["prazo_meses"].dropna().empty:
        st.metric("Prazo Médio", f"{df_filtrado['prazo_meses'].mean():.0f} meses")
    else:
        st.metric("Prazo Médio", "-")

st.markdown("---")

# ──────────────────────────────────────────────
# Mapa Interativo
# ──────────────────────────────────────────────

# Só mostra mapa se há coordenadas
df_com_coords = df_filtrado.dropna(subset=["latitude", "longitude"]) if "latitude" in df_filtrado.columns else pd.DataFrame()

if not df_com_coords.empty:
    st.subheader("Mapa de Vendas")

    # Atribui cores por incorporadora
    if "incorporadora" in df_com_coords.columns:
        incorporadoras_unicas = df_com_coords["incorporadora"].dropna().unique()
        paleta = [
            [38, 166, 154], [66, 165, 245], [255, 112, 67], [171, 71, 188],
            [102, 187, 106], [255, 202, 40], [239, 83, 80], [141, 110, 99],
            [120, 144, 156], [236, 64, 122],
        ]
        cor_map = {inc: paleta[i % len(paleta)] for i, inc in enumerate(incorporadoras_unicas)}
        df_com_coords = df_com_coords.copy()
        df_com_coords["cor"] = df_com_coords["incorporadora"].map(cor_map)
        df_com_coords["cor"] = df_com_coords["cor"].apply(
            lambda x: x if isinstance(x, list) else [158, 158, 158]
        )
    else:
        df_com_coords = df_com_coords.copy()
        df_com_coords["cor"] = [[38, 166, 154]] * len(df_com_coords)

    layers = []

    # Pontos de vendas
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=df_com_coords,
        get_position=["longitude", "latitude"],
        get_color="cor",
        get_radius=80,
        pickable=True,
        opacity=0.8,
        auto_highlight=True,
    ))

    # Centro e raio (só se filtro por raio ativado)
    if usar_raio and lat_centro and lon_centro:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame([{"lat": lat_centro, "lon": lon_centro}]),
            get_position=["lon", "lat"],
            get_color=[220, 20, 60, 200],
            get_radius=150,
            pickable=False,
        ))

        circulo_pontos = gerar_circulo_raio(lat_centro, lon_centro, raio_km)
        layers.append(pdk.Layer(
            "PathLayer",
            data=[{"path": [[p["lon"], p["lat"]] for p in circulo_pontos]}],
            get_path="path",
            get_color=[220, 20, 60, 100],
            width_min_pixels=2,
            get_width=3,
        ))

        view_lat, view_lon = lat_centro, lon_centro
        view_zoom = max(10, 14 - raio_km * 0.3)
    else:
        view_lat = df_com_coords["latitude"].astype(float).mean()
        view_lon = df_com_coords["longitude"].astype(float).mean()
        # Zoom automático baseado na dispersão dos pontos
        lat_range = df_com_coords["latitude"].astype(float).max() - df_com_coords["latitude"].astype(float).min()
        lon_range = df_com_coords["longitude"].astype(float).max() - df_com_coords["longitude"].astype(float).min()
        spread = max(lat_range, lon_range, 0.001)
        if spread < 0.01:
            view_zoom = 14
        elif spread < 0.05:
            view_zoom = 13
        elif spread < 0.1:
            view_zoom = 12
        elif spread < 0.3:
            view_zoom = 11
        elif spread < 0.5:
            view_zoom = 10
        elif spread < 1:
            view_zoom = 9
        elif spread < 3:
            view_zoom = 8
        else:
            view_zoom = 6

    # Tooltip
    tooltip_html = ""
    if "incorporadora" in df_com_coords.columns:
        tooltip_html += "<b>{incorporadora}</b>"
    if "empreendimento" in df_com_coords.columns:
        tooltip_html += "<br/>{empreendimento}"
    if "cidade" in df_com_coords.columns:
        tooltip_html += "<br/>{cidade}/{estado}"
    if "preco" in df_com_coords.columns:
        tooltip_html += "<br/>R$ {preco}"
    if "tipologia" in df_com_coords.columns:
        tooltip_html += "<br/>{tipologia}"

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=view_lat,
            longitude=view_lon,
            zoom=view_zoom,
            pitch=0,
        ),
        tooltip={"html": tooltip_html, "style": {"backgroundColor": "#1a1a2e", "color": "white"}},
    )

    evento = st.pydeck_chart(deck, use_container_width=True, height=500,
                             key="mapa_vendas", selection_mode="single-object",
                             on_select="rerun")

    # Detecta clique em ponto do mapa
    if evento and evento.selection and evento.selection.objects:
        for _layer_objs in evento.selection.objects.values():
            if _layer_objs:
                _obj = _layer_objs[0]
                if "empreendimento" in _obj:
                    _nome = _obj["empreendimento"]
                    if _nome != st.session_state.get("_mapa_sel_processado"):
                        st.session_state["_mapa_sel_processado"] = _nome
                        st.session_state["empr_pendente"] = _nome
                break

    # Dialog de confirmação (abre automaticamente quando empr_pendente é definido)
    @st.dialog("Filtrar por empreendimento")
    def _dialog_filtrar(nome_empr):
        st.write(f"Deseja filtrar os dados pelo empreendimento **{nome_empr}**?")
        col_sim, col_nao = st.columns(2)
        with col_sim:
            if st.button("Sim, filtrar", type="primary", use_container_width=True,
                         disabled=st.session_state.get("_dialog_aplicando", False)):
                st.session_state["_dialog_aplicando"] = True
                st.session_state["empr_click"] = nome_empr
                st.session_state.pop("empr_pendente", None)
                st.session_state.pop("_mapa_sel_processado", None)
                st.rerun()
        with col_nao:
            if st.button("Cancelar", use_container_width=True,
                         disabled=st.session_state.get("_dialog_aplicando", False)):
                st.session_state.pop("empr_pendente", None)
                st.session_state.pop("_mapa_sel_processado", None)
                st.rerun()
        if st.session_state.get("_dialog_aplicando"):
            st.info("Aplicando filtro, aguarde...")

    if "empr_pendente" in st.session_state:
        _dialog_filtrar(st.session_state["empr_pendente"])

    # Legenda
    if "incorporadora" in df_com_coords.columns and len(incorporadoras_unicas) > 0:
        with st.expander("Legenda do Mapa"):
            n_cols = min(5, len(incorporadoras_unicas))
            legend_cols = st.columns(n_cols)
            for i, inc in enumerate(incorporadoras_unicas):
                cor = cor_map[inc]
                with legend_cols[i % n_cols]:
                    st.markdown(
                        f'<span style="color: rgb({cor[0]},{cor[1]},{cor[2]}); font-size: 20px;">●</span> {inc}',
                        unsafe_allow_html=True,
                    )

    st.markdown("---")

# ──────────────────────────────────────────────
# Análises em Abas
# ──────────────────────────────────────────────

tab_volume, tab_comprador, tab_produto, tab_financeiro, tab_velocidade, tab_origem = st.tabs([
    "Volume de Vendas",
    "Perfil do Comprador",
    "Perfil do Produto",
    "Análise Financeira",
    "Velocidade de Vendas",
    "Origem dos Clientes",
])

# ── Volume de Vendas ──
with tab_volume:
    col1, col2 = st.columns(2)
    with col1:
        fig = grafico_vendas_por_incorporadora(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_vendas_incorp")
    with col2:
        freq = st.radio("Frequência", ["Mensal", "Trimestral"], horizontal=True, key="freq_vol")
        freq_code = "M" if freq == "Mensal" else "Q"
        fig = grafico_evolucao_vendas(df_filtrado, freq=freq_code)
        st.plotly_chart(fig, use_container_width=True, key="chart_evolucao_vendas")

    st.subheader("Evolução por Incorporadora")
    fig = grafico_evolucao_por_incorporadora(df_filtrado, freq=freq_code)
    st.plotly_chart(fig, use_container_width=True, key="chart_evolucao_incorp")

# ── Perfil do Comprador ──
with tab_comprador:
    col1, col2 = st.columns(2)
    with col1:
        fig = grafico_distribuicao_renda(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_dist_renda")
    with col2:
        fig = grafico_distribuicao_idade(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_dist_idade")

    fig = grafico_top_profissoes(df_filtrado)
    st.plotly_chart(fig, use_container_width=True, key="chart_profissoes")

    st.subheader("Comprometimento de Renda")
    col3, col4 = st.columns(2)
    with col3:
        fig = grafico_comprometimento_renda(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_comprom_renda")
    with col4:
        fig = grafico_comprometimento_por_incorporadora(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_comprom_incorp")

# ── Perfil do Produto ──
with tab_produto:
    col1, col2 = st.columns(2)
    with col1:
        fig = grafico_tipologias(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_tipologias")
    with col2:
        fig = grafico_faixa_preco(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_faixa_preco")

    col3, col4 = st.columns(2)
    with col3:
        fig = grafico_faixa_metragem(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_faixa_metragem")
    with col4:
        fig = grafico_preco_m2_por_incorporadora(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_pm2_incorp")

    st.subheader("Preço por m² — Empreendimentos")
    fig = grafico_preco_m2_por_empreendimento(df_filtrado)
    st.plotly_chart(fig, use_container_width=True, key="chart_pm2_empr")

    st.subheader("Comparativo por Incorporadora")
    tabela = tabela_comparativa_incorporadoras(df_filtrado)
    if not tabela.empty:
        st.dataframe(tabela, use_container_width=True)
    else:
        st.info("Dados insuficientes para tabela comparativa.")

# ── Análise Financeira ──
with tab_financeiro:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        if "preco" in df_filtrado.columns:
            st.metric("Ticket Médio", f"R$ {df_filtrado['preco'].mean():,.0f}")
    with col_f2:
        if "valor_financiado" in df_filtrado.columns and "preco" in df_filtrado.columns:
            preco_medio = df_filtrado["preco"].mean()
            if preco_medio > 0:
                pct_fin = df_filtrado["valor_financiado"].mean() / preco_medio * 100
                st.metric("% Financiamento", f"{pct_fin:.0f}%")
    with col_f3:
        if "recursos_proprios" in df_filtrado.columns and "preco" in df_filtrado.columns:
            preco_medio = df_filtrado["preco"].mean()
            if preco_medio > 0:
                pct_rp = df_filtrado["recursos_proprios"].mean() / preco_medio * 100
                st.metric("% Recursos Próprios", f"{pct_rp:.0f}%")
    with col_f4:
        if "fgts" in df_filtrado.columns and not df_filtrado["fgts"].dropna().empty:
            fgts_medio = df_filtrado["fgts"].mean()
            st.metric("FGTS Médio", f"R$ {fgts_medio:,.0f}")

    col1, col2 = st.columns(2)
    with col1:
        fig = grafico_financiamento_vs_proprio(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_fin_vs_proprio")
    with col2:
        fig = grafico_renda_vs_preco(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_renda_vs_preco")

# ── Velocidade de Vendas ──
with tab_velocidade:
    fig = grafico_absorcao_mensal(df_filtrado)
    st.plotly_chart(fig, use_container_width=True, key="chart_absorcao_mensal")

    fig = grafico_absorcao_por_incorporadora(df_filtrado)
    st.plotly_chart(fig, use_container_width=True, key="chart_absorcao_incorp")

    st.subheader("Absorção por Faixa de Preço")
    fig = grafico_absorcao_por_faixa_preco(df_filtrado)
    st.plotly_chart(fig, use_container_width=True, key="chart_absorcao_faixa")

# ── Origem dos Clientes ──
with tab_origem:
    col1, col2 = st.columns(2)
    with col1:
        fig = grafico_origem_clientes_cidade(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_origem_clientes")
    with col2:
        fig = grafico_matriz_regiao_renda(df_filtrado)
        st.plotly_chart(fig, use_container_width=True, key="chart_matriz_regiao")

# ──────────────────────────────────────────────
# Exportação
# ──────────────────────────────────────────────

st.markdown("---")
st.subheader("Exportar Dados")

# Remove coluna de cor interna se existir
df_export = df_filtrado.drop(columns=["cor"], errors="ignore")

col_exp1, col_exp2 = st.columns(2)

with col_exp1:
    buffer_xlsx = io.BytesIO()
    with pd.ExcelWriter(buffer_xlsx, engine="openpyxl") as writer:
        df_export.to_excel(writer, sheet_name="Vendas", index=False)
    st.download_button(
        label="Baixar Excel",
        data=buffer_xlsx.getvalue(),
        file_name="vendas_filtradas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with col_exp2:
    csv_data = df_export.to_csv(index=False)
    st.download_button(
        label="Baixar CSV",
        data=csv_data,
        file_name="vendas_filtradas.csv",
        mime="text/csv",
    )

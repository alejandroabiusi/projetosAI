"""
Módulo de gráficos — visualizações Plotly para análise de vendas.
"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

_CEP_BAIRROS_PATH = Path(__file__).parent.parent / "data" / "cep_bairros.json"
_cep_cache: dict | None = None


def _carregar_cep_bairros() -> dict:
    """Carrega mapeamento CEP → bairro/cidade do cache JSON."""
    global _cep_cache
    if _cep_cache is None:
        if _CEP_BAIRROS_PATH.exists():
            _cep_cache = json.loads(_CEP_BAIRROS_PATH.read_text(encoding="utf-8"))
        else:
            _cep_cache = {}
    return _cep_cache


def _cep_para_bairro(cep: str) -> str:
    """Traduz CEP para 'Bairro - Cidade' usando o cache."""
    cache = _carregar_cep_bairros()
    cep_limpo = re.sub(r"\D", "", str(cep)).strip()
    info = cache.get(cep_limpo, {})
    bairro = info.get("bairro", "")
    cidade = info.get("cidade", "")
    if bairro and cidade:
        return f"{bairro} - {cidade}"
    if bairro:
        return bairro
    if cidade:
        return cidade
    return cep_limpo[:5]  # fallback: prefixo CEP

# Paleta profissional para apresentações
CORES = px.colors.qualitative.Set2
TEMPLATE = "plotly_white"


def _layout_base(fig: go.Figure, titulo: str = "") -> go.Figure:
    """Aplica layout padrão profissional."""
    fig.update_layout(
        title=dict(text=titulo, font=dict(size=16, color="#333")),
        template=TEMPLATE,
        font=dict(family="Segoe UI, Arial", size=12, color="#555"),
        margin=dict(l=40, r=20, t=50, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(font=dict(size=11)),
    )
    return fig


# ──────────────────────────────────────────────
# VOLUME DE VENDAS
# ──────────────────────────────────────────────

def grafico_vendas_por_incorporadora(df: pd.DataFrame) -> go.Figure:
    """Ranking de unidades vendidas por incorporadora (barras horizontais)."""
    contagem = (
        df.groupby("incorporadora")
        .size()
        .reset_index(name="unidades")
        .sort_values("unidades", ascending=True)
    )
    fig = px.bar(
        contagem, x="unidades", y="incorporadora",
        orientation="h", color="unidades",
        color_continuous_scale="Teal",
    )
    fig.update_coloraxes(showscale=False)
    return _layout_base(fig, "Unidades Vendidas por Incorporadora")


def grafico_evolucao_vendas(df: pd.DataFrame, freq: str = "M") -> go.Figure:
    """Evolução de vendas ao longo do tempo (mensal ou trimestral)."""
    if "data_venda" not in df.columns:
        return go.Figure()

    df_temp = df.dropna(subset=["data_venda"]).copy()
    df_temp["periodo"] = df_temp["data_venda"].dt.to_period(freq).dt.to_timestamp()

    evolucao = df_temp.groupby("periodo").size().reset_index(name="unidades")

    fig = px.line(
        evolucao, x="periodo", y="unidades",
        markers=True,
    )
    fig.update_traces(line=dict(color="#2196F3", width=3), marker=dict(size=8))
    label_freq = "Mês" if freq == "M" else "Trimestre"
    return _layout_base(fig, f"Evolução de Vendas por {label_freq}")


def grafico_evolucao_por_incorporadora(df: pd.DataFrame, freq: str = "M") -> go.Figure:
    """Evolução de vendas por incorporadora."""
    if "data_venda" not in df.columns:
        return go.Figure()

    df_temp = df.dropna(subset=["data_venda"]).copy()
    df_temp["periodo"] = df_temp["data_venda"].dt.to_period(freq).dt.to_timestamp()

    evolucao = (
        df_temp.groupby(["periodo", "incorporadora"])
        .size()
        .reset_index(name="unidades")
    )

    fig = px.line(
        evolucao, x="periodo", y="unidades",
        color="incorporadora", markers=True,
    )
    return _layout_base(fig, "Vendas por Incorporadora ao Longo do Tempo")


# ──────────────────────────────────────────────
# PERFIL DO COMPRADOR
# ──────────────────────────────────────────────

def grafico_distribuicao_idade(df: pd.DataFrame) -> go.Figure:
    """Histograma de idade dos compradores."""
    if "idade_cliente" not in df.columns:
        return go.Figure()

    df_valid = df.dropna(subset=["idade_cliente"])
    fig = px.histogram(
        df_valid, x="idade_cliente", nbins=20,
        color_discrete_sequence=["#26A69A"],
    )
    fig.update_xaxes(title="Idade")
    fig.update_yaxes(title="Quantidade")
    return _layout_base(fig, "Distribuição de Idade dos Compradores")


def grafico_top_profissoes(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """Top profissões dos compradores (barras horizontais)."""
    if "profissao_cliente" not in df.columns:
        return go.Figure()

    prof = df["profissao_cliente"].dropna()
    prof = prof[prof.str.strip().ne("") & prof.ne("0")]
    contagem = (
        prof
        .value_counts()
        .head(top_n)
        .sort_values(ascending=True)
        .reset_index()
    )
    contagem.columns = ["profissao", "quantidade"]

    fig = px.bar(
        contagem, x="quantidade", y="profissao",
        orientation="h",
        color_discrete_sequence=["#42A5F5"],
    )
    return _layout_base(fig, f"Top {top_n} Profissões dos Compradores")


def grafico_distribuicao_renda(df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras de renda dos compradores por faixas de R$ 500."""
    if "renda_cliente" not in df.columns:
        return go.Figure()

    df_valid = df[df["renda_cliente"].notna() & (df["renda_cliente"] > 0)].copy()

    # Faixas de renda
    labels = ["Menor de 2.000"]
    bins = [0, 2000]
    for inicio in range(2000, 8000, 500):
        fim = inicio + 500
        labels.append(f"{inicio + 1:,.0f} a {fim:,.0f}".replace(",", "."))
        bins.append(fim)
    labels.append("Acima de 8.000")
    bins.append(float("inf"))

    df_valid["faixa_renda"] = pd.cut(
        df_valid["renda_cliente"], bins=bins, labels=labels, right=True,
    )

    contagem = df_valid["faixa_renda"].value_counts().reindex(labels).fillna(0).reset_index()
    contagem.columns = ["faixa", "quantidade"]

    fig = px.bar(
        contagem, x="faixa", y="quantidade",
        color_discrete_sequence=["#66BB6A"],
    )
    fig.update_xaxes(title="Faixa de Renda (R$)", tickangle=-45)
    fig.update_yaxes(title="Quantidade")
    return _layout_base(fig, "Distribuição de Renda dos Compradores")


# ──────────────────────────────────────────────
# PERFIL DO PRODUTO
# ──────────────────────────────────────────────

def grafico_tipologias(df: pd.DataFrame) -> go.Figure:
    """Distribuição de tipologias (pizza)."""
    if "tipologia" not in df.columns:
        return go.Figure()

    contagem = df["tipologia"].dropna().value_counts().reset_index()
    contagem.columns = ["tipologia", "quantidade"]

    fig = px.pie(
        contagem, names="tipologia", values="quantidade",
        color_discrete_sequence=CORES,
        hole=0.4,
    )
    fig.update_traces(textinfo="label+percent", textfont_size=12)
    return _layout_base(fig, "Tipologias Vendidas")


def grafico_faixa_preco(df: pd.DataFrame) -> go.Figure:
    """Histograma de preços."""
    if "preco" not in df.columns:
        return go.Figure()

    df_valid = df[df["preco"].notna() & (df["preco"] > 0)]
    fig = px.histogram(
        df_valid, x="preco", nbins=25,
        color_discrete_sequence=["#FF7043"],
    )
    fig.update_xaxes(title="Preço (R$)")
    fig.update_yaxes(title="Quantidade")
    return _layout_base(fig, "Distribuição de Preço")


def grafico_faixa_metragem(df: pd.DataFrame) -> go.Figure:
    """Histograma de metragem."""
    if "metragem" not in df.columns:
        return go.Figure()

    df_valid = df[df["metragem"].notna() & (df["metragem"] > 0)]
    fig = px.histogram(
        df_valid, x="metragem", nbins=20,
        color_discrete_sequence=["#AB47BC"],
    )
    fig.update_xaxes(title="Metragem (m²)")
    fig.update_yaxes(title="Quantidade")
    return _layout_base(fig, "Distribuição de Metragem")


def tabela_comparativa_incorporadoras(df: pd.DataFrame) -> pd.DataFrame:
    """Tabela comparativa com métricas por incorporadora."""
    colunas_agg = {}

    if "preco" in df.columns:
        colunas_agg["preco"] = ["mean", "median", "min", "max"]
    if "metragem" in df.columns:
        colunas_agg["metragem"] = "mean"

    if not colunas_agg:
        return pd.DataFrame()

    tabela = df.groupby("incorporadora").agg(
        unidades=("incorporadora", "size"),
        **{f"preco_{fn}": ("preco", fn) for fn in ["mean", "median", "min", "max"] if "preco" in df.columns},
        **({"metragem_media": ("metragem", "mean")} if "metragem" in df.columns else {}),
    ).sort_values("unidades", ascending=False)

    # Formata valores monetários
    for col in tabela.columns:
        if "preco" in col:
            tabela[col] = tabela[col].apply(lambda x: f"R$ {x:,.0f}" if pd.notna(x) else "-")
        if "metragem" in col:
            tabela[col] = tabela[col].apply(lambda x: f"{x:.1f} m²" if pd.notna(x) else "-")

    tabela.columns = [
        c.replace("preco_mean", "Preço Médio")
        .replace("preco_median", "Preço Mediano")
        .replace("preco_min", "Preço Mín.")
        .replace("preco_max", "Preço Máx.")
        .replace("metragem_media", "Metragem Média")
        .replace("unidades", "Unidades")
        for c in tabela.columns
    ]
    return tabela


# ──────────────────────────────────────────────
# ANÁLISE FINANCEIRA
# ──────────────────────────────────────────────

def grafico_financiamento_vs_proprio(df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras empilhadas: financiamento vs recursos próprios."""
    cols_necessarias = {"valor_financiado", "recursos_proprios", "incorporadora"}
    if not cols_necessarias.issubset(df.columns):
        return go.Figure()

    medias = df.groupby("incorporadora").agg(
        financiado=("valor_financiado", "mean"),
        proprio=("recursos_proprios", "mean"),
    ).sort_values("financiado", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=medias.index, x=medias["proprio"],
        name="Recursos Próprios", orientation="h",
        marker_color="#66BB6A",
    ))
    fig.add_trace(go.Bar(
        y=medias.index, x=medias["financiado"],
        name="Financiamento", orientation="h",
        marker_color="#42A5F5",
    ))
    fig.update_layout(barmode="stack")
    return _layout_base(fig, "Financiamento vs Recursos Próprios (Média)")


def grafico_renda_vs_preco(df: pd.DataFrame) -> go.Figure:
    """Scatter plot: renda do cliente vs preço do imóvel."""
    if "renda_cliente" not in df.columns or "preco" not in df.columns:
        return go.Figure()

    df_valid = df.dropna(subset=["renda_cliente", "preco"])
    df_valid = df_valid[(df_valid["renda_cliente"] > 0) & (df_valid["preco"] > 0)]

    fig = px.scatter(
        df_valid, x="renda_cliente", y="preco",
        color="incorporadora" if "incorporadora" in df_valid.columns else None,
        opacity=0.5,
        color_discrete_sequence=CORES,
    )
    fig.update_xaxes(title="Renda do Cliente (R$)")
    fig.update_yaxes(title="Preço do Imóvel (R$)")
    return _layout_base(fig, "Renda x Preço do Imóvel")


# ──────────────────────────────────────────────
# VELOCIDADE DE VENDAS
# ──────────────────────────────────────────────

def grafico_absorcao_mensal(df: pd.DataFrame) -> go.Figure:
    """Unidades vendidas por mês (absorção geral)."""
    if "data_venda" not in df.columns:
        return go.Figure()

    df_temp = df.dropna(subset=["data_venda"]).copy()
    df_temp["mes"] = df_temp["data_venda"].dt.to_period("M").dt.to_timestamp()

    absorcao = df_temp.groupby("mes").size().reset_index(name="unidades")

    fig = px.bar(
        absorcao, x="mes", y="unidades",
        color_discrete_sequence=["#26A69A"],
    )
    fig.update_xaxes(title="Mês")
    fig.update_yaxes(title="Unidades Vendidas")
    return _layout_base(fig, "Absorção Mensal")


def grafico_absorcao_por_incorporadora(df: pd.DataFrame) -> go.Figure:
    """Absorção mensal comparativa entre incorporadoras."""
    if "data_venda" not in df.columns:
        return go.Figure()

    df_temp = df.dropna(subset=["data_venda"]).copy()
    df_temp["mes"] = df_temp["data_venda"].dt.to_period("M").dt.to_timestamp()

    absorcao = (
        df_temp.groupby(["mes", "incorporadora"])
        .size()
        .reset_index(name="unidades")
    )

    fig = px.bar(
        absorcao, x="mes", y="unidades",
        color="incorporadora", barmode="group",
        color_discrete_sequence=CORES,
    )
    fig.update_xaxes(title="Mês")
    fig.update_yaxes(title="Unidades Vendidas")
    return _layout_base(fig, "Absorção Mensal por Incorporadora")


# ──────────────────────────────────────────────
# ORIGEM DOS CLIENTES
# ──────────────────────────────────────────────

def grafico_origem_clientes_cidade(
    df: pd.DataFrame,
    lat_coords: dict | None = None,
    lon_coords: dict | None = None,
) -> go.Figure:
    """Top bairros/cidades de origem dos clientes (a partir do CEP)."""
    if "cep_cliente" not in df.columns:
        return go.Figure()

    df_valid = df[df["cep_cliente"].notna()].copy()
    df_valid["bairro_origem"] = df_valid["cep_cliente"].apply(_cep_para_bairro)

    contagem = (
        df_valid["bairro_origem"]
        .value_counts()
        .head(20)
        .sort_values(ascending=True)
        .reset_index()
    )
    contagem.columns = ["bairro", "quantidade"]

    fig = px.bar(
        contagem, x="quantidade", y="bairro",
        orientation="h",
        color_discrete_sequence=["#5C6BC0"],
    )
    fig.update_yaxes(title="Bairro / Cidade")
    fig.update_xaxes(title="Quantidade de Compradores")
    return _layout_base(fig, "Top 20 Bairros de Origem dos Compradores")


def grafico_matriz_regiao_renda(df: pd.DataFrame) -> go.Figure:
    """Heatmap: bairro de origem do cliente x faixa de renda."""
    if "cep_cliente" not in df.columns or "renda_cliente" not in df.columns:
        return go.Figure()

    df_valid = df[df["cep_cliente"].notna() & df["renda_cliente"].notna()].copy()
    df_valid["bairro_origem"] = df_valid["cep_cliente"].apply(_cep_para_bairro)

    # Faixas de renda simplificadas
    bins = [0, 2000, 3000, 4000, 5000, 6000, 8000, float("inf")]
    labels = ["<2k", "2-3k", "3-4k", "4-5k", "5-6k", "6-8k", ">8k"]
    df_valid["faixa_renda"] = pd.cut(df_valid["renda_cliente"], bins=bins, labels=labels)

    # Top 15 bairros
    top_bairros = df_valid["bairro_origem"].value_counts().head(15).index
    df_valid = df_valid[df_valid["bairro_origem"].isin(top_bairros)]

    matriz = pd.crosstab(df_valid["bairro_origem"], df_valid["faixa_renda"])
    matriz = matriz.reindex(columns=labels, fill_value=0)

    fig = px.imshow(
        matriz.values,
        x=labels,
        y=matriz.index.tolist(),
        color_continuous_scale="Blues",
        labels=dict(x="Faixa de Renda", y="Bairro / Cidade", color="Vendas"),
        text_auto=True,
    )
    return _layout_base(fig, "Matriz Bairro de Origem x Faixa de Renda")


# ──────────────────────────────────────────────
# PREÇO POR M²
# ──────────────────────────────────────────────

def grafico_preco_m2_por_empreendimento(df: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """Preço por m² comparativo entre empreendimentos."""
    if "preco" not in df.columns or "metragem" not in df.columns:
        return go.Figure()

    df_valid = df[
        df["preco"].notna() & df["metragem"].notna()
        & (df["preco"] > 0) & (df["metragem"] > 0)
    ].copy()

    if df_valid.empty:
        return go.Figure()

    df_valid["preco_m2"] = df_valid["preco"] / df_valid["metragem"]

    medias = (
        df_valid.groupby("empreendimento")
        .agg(preco_m2=("preco_m2", "mean"), vendas=("preco_m2", "size"))
        .sort_values("preco_m2", ascending=True)
        .tail(top_n)
    )

    fig = px.bar(
        medias.reset_index(), x="preco_m2", y="empreendimento",
        orientation="h",
        color="preco_m2",
        color_continuous_scale="Oranges",
        hover_data={"vendas": True},
    )
    fig.update_coloraxes(showscale=False)
    fig.update_xaxes(title="R$/m²")
    return _layout_base(fig, f"Preço por m² — Top {top_n} Empreendimentos")


def grafico_preco_m2_por_incorporadora(df: pd.DataFrame) -> go.Figure:
    """Box plot de preço por m² por incorporadora."""
    if "preco" not in df.columns or "metragem" not in df.columns:
        return go.Figure()

    df_valid = df[
        df["preco"].notna() & df["metragem"].notna()
        & (df["preco"] > 0) & (df["metragem"] > 0)
    ].copy()

    if df_valid.empty:
        return go.Figure()

    df_valid["preco_m2"] = df_valid["preco"] / df_valid["metragem"]

    fig = px.box(
        df_valid, x="incorporadora", y="preco_m2",
        color="incorporadora",
        color_discrete_sequence=CORES,
    )
    fig.update_xaxes(title="")
    fig.update_yaxes(title="R$/m²")
    return _layout_base(fig, "Distribuição de Preço/m² por Incorporadora")


# ──────────────────────────────────────────────
# COMPROMETIMENTO DE RENDA
# ──────────────────────────────────────────────

def grafico_comprometimento_renda(df: pd.DataFrame) -> go.Figure:
    """Histograma de % comprometimento da renda (encargo/renda)."""
    if "encargo_mensal" not in df.columns or "renda_cliente" not in df.columns:
        return go.Figure()

    df_valid = df[
        df["encargo_mensal"].notna() & df["renda_cliente"].notna()
        & (df["renda_cliente"] > 0) & (df["encargo_mensal"] > 0)
    ].copy()

    df_valid["pct_comprometimento"] = (df_valid["encargo_mensal"] / df_valid["renda_cliente"]) * 100

    # Limita a 100% para visualização
    df_valid = df_valid[df_valid["pct_comprometimento"] <= 100]

    fig = px.histogram(
        df_valid, x="pct_comprometimento", nbins=20,
        color_discrete_sequence=["#EF5350"],
    )
    # Linha de referência em 30%
    fig.add_vline(x=30, line_dash="dash", line_color="red",
                  annotation_text="30%", annotation_position="top right")
    fig.update_xaxes(title="% Comprometimento da Renda")
    fig.update_yaxes(title="Quantidade")
    return _layout_base(fig, "Comprometimento de Renda (Encargo / Renda)")


def grafico_comprometimento_por_incorporadora(df: pd.DataFrame) -> go.Figure:
    """Box plot de comprometimento de renda por incorporadora."""
    if "encargo_mensal" not in df.columns or "renda_cliente" not in df.columns:
        return go.Figure()

    df_valid = df[
        df["encargo_mensal"].notna() & df["renda_cliente"].notna()
        & (df["renda_cliente"] > 0) & (df["encargo_mensal"] > 0)
    ].copy()

    df_valid["pct_comprometimento"] = (df_valid["encargo_mensal"] / df_valid["renda_cliente"]) * 100
    df_valid = df_valid[df_valid["pct_comprometimento"] <= 100]

    fig = px.box(
        df_valid, x="incorporadora", y="pct_comprometimento",
        color="incorporadora",
        color_discrete_sequence=CORES,
    )
    fig.add_hline(y=30, line_dash="dash", line_color="red",
                  annotation_text="Limite 30%")
    fig.update_xaxes(title="")
    fig.update_yaxes(title="% Comprometimento")
    return _layout_base(fig, "Comprometimento de Renda por Incorporadora")


# ──────────────────────────────────────────────
# ABSORÇÃO POR FAIXA DE PREÇO
# ──────────────────────────────────────────────

def grafico_absorcao_por_faixa_preco(df: pd.DataFrame) -> go.Figure:
    """Evolução mensal de vendas por faixa de preço."""
    if "data_venda" not in df.columns or "preco" not in df.columns:
        return go.Figure()

    df_valid = df[df["data_venda"].notna() & df["preco"].notna() & (df["preco"] > 0)].copy()

    # Faixas de preço via quantis (sempre monotônicas)
    try:
        df_valid["faixa_preco"], bins = pd.qcut(
            df_valid["preco"], q=5, retbins=True, duplicates="drop",
        )
        df_valid["faixa_preco"] = df_valid["faixa_preco"].apply(
            lambda x: f"{x.left/1000:.0f}-{x.right/1000:.0f}k" if pd.notna(x) else None
        )
    except ValueError:
        return go.Figure()

    df_valid["mes"] = df_valid["data_venda"].dt.to_period("M").dt.to_timestamp()

    absorcao = (
        df_valid.groupby(["mes", "faixa_preco"])
        .size()
        .reset_index(name="unidades")
    )

    fig = px.area(
        absorcao, x="mes", y="unidades",
        color="faixa_preco",
        color_discrete_sequence=CORES,
    )
    fig.update_xaxes(title="Mês")
    fig.update_yaxes(title="Unidades Vendidas")
    return _layout_base(fig, "Absorção por Faixa de Preço")

"""Tool: gerador de gráficos Plotly — interface simplificada para LLMs."""

import json
import plotly.graph_objects as go

DARK_LAYOUT = dict(
    paper_bgcolor="#1a1a2e",
    plot_bgcolor="#16213e",
    font=dict(color="#e0e0e0", family="Segoe UI"),
    xaxis=dict(gridcolor="#2a2a4a", zerolinecolor="#2a2a4a"),
    yaxis=dict(gridcolor="#2a2a4a", zerolinecolor="#2a2a4a"),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    colorway=[
        "#4ecdc4", "#ff6b6b", "#45b7d1", "#f9ca24",
        "#a29bfe", "#fd79a8", "#6c5ce7", "#00b894",
    ],
)


def execute(chart_type: str, title: str, labels: list, values: list,
            series_name: str = "Dados", x_label: str = "", y_label: str = "",
            labels2: list = None, values2: list = None, series_name2: str = None) -> dict:
    """Cria gráfico Plotly com parâmetros planos (sem objetos aninhados)."""
    try:
        fig = go.Figure()
        fig.update_layout(
            title=dict(text=title, font=dict(size=16, color="#ffffff")),
            xaxis_title=x_label,
            yaxis_title=y_label,
            **DARK_LAYOUT,
        )

        if chart_type == "bar":
            fig.add_trace(go.Bar(name=series_name, x=labels, y=values))
            if values2 and labels2:
                fig.add_trace(go.Bar(name=series_name2 or "Série 2", x=labels2, y=values2))
                fig.update_layout(barmode="group")

        elif chart_type == "line":
            fig.add_trace(go.Scatter(name=series_name, x=labels, y=values, mode="lines+markers"))
            if values2 and labels2:
                fig.add_trace(go.Scatter(name=series_name2 or "Série 2", x=labels2, y=values2, mode="lines+markers"))

        elif chart_type == "pie":
            fig = go.Figure(go.Pie(labels=labels, values=values, hole=0.35))
            fig.update_layout(
                title=dict(text=title, font=dict(size=16, color="#ffffff")),
                **DARK_LAYOUT,
            )

        elif chart_type == "waterfall":
            fig = go.Figure(go.Waterfall(
                name=series_name, x=labels, y=values,
                connector={"line": {"color": "#4ecdc4"}},
            ))
            fig.update_layout(
                title=dict(text=title, font=dict(size=16, color="#ffffff")),
                **DARK_LAYOUT,
            )

        else:
            return {"error": f"Tipo '{chart_type}' não suportado. Use: bar, line, pie, waterfall."}

        fig.update_layout(height=450, margin=dict(l=50, r=30, t=60, b=50))

        return {
            "title": title,
            "chart_type": chart_type,
            "plotly_json": fig.to_json(),
        }

    except Exception as e:
        return {"error": f"Erro ao criar gráfico: {str(e)}"}

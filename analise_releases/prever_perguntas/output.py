"""Geracao do markdown final de previsoes."""
from __future__ import annotations

from datetime import datetime

from .pool import CandidatoBanco
from .themes import AnaliseTemas


def gerar_output_markdown(
    empresa: str,
    periodo: str,
    resultados: dict,
    analise: AnaliseTemas | None = None,
    pool: list[CandidatoBanco] | None = None,
) -> str:
    """`resultados` no formato {analista: {banco, num_calls_historico, periodos, previsao}}."""
    lines: list[str] = [
        f"# Previsao de Perguntas - {empresa.upper()} {periodo}",
        f"_Gerado em {datetime.now().strftime('%Y-%m-%d %H:%M')}_",
        "",
    ]

    if analise is not None:
        lines.extend([
            "## Sumario da analise do release",
            f"- Surpresa negativa forte: "
            f"{'**SIM**' if analise.surpresa_negativa_forte else 'nao'}",
            f"- Tema dominante: **{analise.tema_dominante or '(nenhum)'}**",
            f"- Temas obrigatorios: {', '.join(analise.temas_obrigatorios) or '(nenhum)'}",
            f"- Total de flags detectados: {len(analise.flags)}",
            "",
        ])

    if pool:
        lines.extend(["## Pool de analistas considerado", ""])
        for c in pool:
            extras: list[str] = []
            if c.alternativos:
                extras.append(f"alt: {', '.join(c.alternativos)}")
            if not c.tem_historico_empresa:
                extras.append("sem historico empresa")
            extra_txt = f" ({'; '.join(extras)})" if extras else ""
            lines.append(f"- **{c.banco}** — {c.primario}{extra_txt}")
        lines.append("")

    lines.append("---\n")

    for analista, data in resultados.items():
        banco = data.get("banco", "?")
        n = data.get("num_calls_historico", 0)
        periodos = ", ".join(data.get("periodos", [])[-4:])
        lines.append(f"## {analista} ({banco})")
        lines.append(f"_Historico: {n} calls ({periodos})_")
        lines.append("")
        lines.append(data.get("previsao", ""))
        lines.append("\n---\n")

    return "\n".join(lines)

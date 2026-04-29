"""Testes do markdown final."""
from __future__ import annotations

from prever_perguntas.output import gerar_output_markdown
from prever_perguntas.pool import CandidatoBanco
from prever_perguntas.themes import AnaliseTemas, FlagTema


def test_markdown_basico_sem_analise():
    md = gerar_output_markdown(
        "tenda", "4T2025",
        resultados={
            "Gustavo Cambauva": {
                "banco": "BTG Pactual",
                "num_calls_historico": 4,
                "periodos": ["1T2025", "2T2025", "3T2025", "4T2024"],
                "previsao": "### Pergunta 1\n**Pergunta:** ...",
            }
        },
    )
    assert "# Previsao de Perguntas - TENDA 4T2025" in md
    assert "Gustavo Cambauva (BTG Pactual)" in md
    assert "Pergunta 1" in md


def test_markdown_inclui_sumario_quando_ha_analise():
    analise = AnaliseTemas(
        flags=[FlagTema("Desvio de custo", "trecho", "alta", "desvio_custo")],
        tema_dominante="Desvio de custo",
        surpresa_negativa_forte=True,
        temas_obrigatorios=["Desvio de custo"],
    )
    md = gerar_output_markdown(
        "tenda", "4T2025", resultados={}, analise=analise,
    )
    assert "Sumario da analise" in md
    assert "**SIM**" in md
    assert "Desvio de custo" in md


def test_markdown_inclui_pool_quando_passado():
    pool = [
        CandidatoBanco(banco="BTG", primario="Gustavo", alternativos=["Pedro"]),
        CandidatoBanco(banco="?", primario="Jorel",
                       tem_historico_empresa=False, n_calls_empresa=3),
    ]
    md = gerar_output_markdown("tenda", "4T2025", resultados={}, pool=pool)
    assert "Pool de analistas" in md
    assert "Gustavo" in md
    assert "alt: Pedro" in md
    assert "sem historico empresa" in md

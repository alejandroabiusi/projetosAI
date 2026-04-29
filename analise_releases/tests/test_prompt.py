"""Testes de montagem de prompt."""
from __future__ import annotations

from prever_perguntas.pool import CandidatoBanco
from prever_perguntas.prompt import (
    SYSTEM_PROMPT,
    montar_bloco_analista,
    montar_bloco_compartilhado,
)
from prever_perguntas.themes import analisar


def test_system_prompt_menciona_5_licoes():
    # As 5 licoes devem estar mencionadas no system prompt
    s = SYSTEM_PROMPT.lower()
    assert "tema dominante" in s
    assert "banco" in s
    assert "curto prazo" in s
    assert "cross-setor" in s
    assert "obrigat" in s  # obrigatorios / obrigatorias


def test_bloco_compartilhado_inclui_release_e_analise(release_com_surpresa):
    analise = analisar(release_com_surpresa)
    pool = [
        CandidatoBanco(banco="BTG Pactual", primario="Gustavo Cambauva", n_calls_empresa=4),
        CandidatoBanco(banco="?", primario="Jorel Guilloty",
                       tem_historico_empresa=False, n_calls_empresa=3),
    ]
    bloco = montar_bloco_compartilhado("tenda", "4T2025", release_com_surpresa, analise, pool)
    assert "TENDA" in bloco
    assert "4T2025" in bloco
    assert "Surpresa negativa forte detectada: SIM" in bloco
    assert "Gustavo Cambauva" in bloco
    assert "Jorel Guilloty" in bloco
    assert "SEM historico" in bloco  # marca cross-setor


def test_bloco_analista_marca_alternativo_e_sem_hist():
    bloco = montar_bloco_analista(
        "tenda", "4T2025", "Tainan Costa", "UBS",
        historico_empresa=[],
        historico_setor=[],
        alternativos=["Victor Tapia"],
    )
    assert "Victor Tapia" in bloco
    assert "alternativo" in bloco.lower()


def test_bloco_analista_alerta_quando_sem_historico():
    bloco = montar_bloco_analista(
        "tenda", "4T2025", "Jorel Guilloty", "Goldman Sachs",
        historico_empresa=[],
        historico_setor=[{"empresa": "cury", "periodo": "3T2025", "texto": "..."}],
        sem_historico_empresa=True,
    )
    assert "NAO tem historico" in bloco
    assert "cross-setor" in bloco.lower()


def test_bloco_analista_pede_marca_obrigatorio():
    bloco = montar_bloco_analista(
        "tenda", "4T2025", "Gustavo Cambauva", "BTG Pactual",
        historico_empresa=[{"periodo": "3T2025", "texto": "..."}],
        historico_setor=[],
    )
    assert "[OBRIGATORIO]" in bloco

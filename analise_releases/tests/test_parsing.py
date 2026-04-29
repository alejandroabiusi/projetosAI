"""Testes de parsing: SPEAKER_PATTERN, ANALYST_PATTERN, extrair_perguntas_analistas."""
from __future__ import annotations

from prever_perguntas.parsing import (
    extrair_perguntas_analistas,
    parse_speaker_tag,
)


def test_parse_speaker_tag_com_banco():
    assert parse_speaker_tag("Gustavo Cambauva - Analista (BTG Pactual)") == (
        "Gustavo Cambauva",
        "BTG Pactual",
    )


def test_parse_speaker_tag_sem_banco():
    assert parse_speaker_tag("Joao Silva - Analista") == ("Joao Silva", "?")


def test_parse_speaker_tag_nao_e_analista():
    assert parse_speaker_tag("CEO - Empresa") is None


def test_extrair_perguntas_pega_so_analistas(texto_processado_exemplo):
    perguntas = extrair_perguntas_analistas(texto_processado_exemplo)
    nomes = {k.split("|")[0] for k in perguntas}
    assert nomes == {"Gustavo Cambauva", "Rafael Rehder", "Andre Mazini"}
    # CEO nao e analista, nao deve entrar
    assert not any("CEO" in k for k in perguntas)


def test_extrair_perguntas_concatena_falas_repetidas(texto_processado_exemplo):
    perguntas = extrair_perguntas_analistas(texto_processado_exemplo)
    gustavo = perguntas["Gustavo Cambauva|BTG Pactual"]
    # Tem 2 falas concatenadas — deve aparecer "Alea" e "REF" no mesmo blob
    assert "Alea" in gustavo
    assert "REF" in gustavo
    assert "\n\n" in gustavo  # juntou com separador


def test_extrair_perguntas_texto_vazio():
    assert extrair_perguntas_analistas("") == {}

"""Testes do detector de temas (licoes 1, 3, 5)."""
from __future__ import annotations

from prever_perguntas.themes import (
    analisar,
    detectar_divergencias_guidance,
    detectar_flags,
)


def test_release_com_desvio_custo_dispara_surpresa_forte(release_com_surpresa):
    analise = analisar(release_com_surpresa)
    assert analise.surpresa_negativa_forte is True
    # Tema dominante deve ser desvio de custo OU provisao (ambos foram detectados)
    assert analise.tema_dominante is not None
    assert any(f.tipo == "desvio_custo" for f in analise.flags)
    assert any(f.tipo == "provisao" for f in analise.flags)


def test_release_normal_nao_dispara_surpresa(release_normal):
    analise = analisar(release_normal)
    assert analise.surpresa_negativa_forte is False
    # Sem flags de severidade alta, tema dominante nao identificado
    altas = [f for f in analise.flags if f.severidade == "alta"]
    assert len(altas) == 0


def test_detectar_flags_isoladamente(release_com_surpresa):
    flags = detectar_flags(release_com_surpresa)
    tipos = {f.tipo for f in flags}
    assert "desvio_custo" in tipos
    assert "provisao" in tipos


def test_temas_obrigatorios_inclui_dominante(release_com_surpresa):
    analise = analisar(release_com_surpresa)
    assert analise.tema_dominante in analise.temas_obrigatorios


def test_divergencia_guidance_detecta_fora_do_intervalo():
    texto = "A margem bruta consolidada foi de 25,5% no trimestre."
    flags = detectar_divergencias_guidance(
        texto, guidance_referencia={"margem bruta": (30.0, 33.0)}
    )
    assert len(flags) == 1
    assert flags[0].tipo == "divergencia_guidance"


def test_divergencia_guidance_nao_dispara_dentro_do_intervalo():
    texto = "A margem bruta consolidada foi de 31,5% no trimestre."
    flags = detectar_divergencias_guidance(
        texto, guidance_referencia={"margem bruta": (30.0, 33.0)}
    )
    assert flags == []


def test_divergencia_guidance_sem_referencia_retorna_vazio(release_normal):
    flags = detectar_divergencias_guidance(release_normal, guidance_referencia=None)
    assert flags == []


def test_analise_propaga_divergencias_para_obrigatorios():
    texto = "Reportamos margem bruta de 22% no trimestre, abaixo do guidance."
    analise = analisar(
        texto, guidance_referencia={"margem bruta": (30.0, 33.0)}
    )
    # divergencia + revisao_negativa = surpresa forte
    assert analise.surpresa_negativa_forte is True
    rotulos_obrigatorios = " ".join(analise.temas_obrigatorios)
    assert "margem bruta" in rotulos_obrigatorios.lower()

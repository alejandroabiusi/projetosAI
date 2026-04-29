"""Testes de history: queries no DB e ranking de analistas."""
from __future__ import annotations

from prever_perguntas.history import (
    buscar_historico_empresa,
    buscar_historico_setor,
    identificar_analistas_provaveis,
)


def test_buscar_historico_empresa_pega_so_empresa_alvo(db_com_calls):
    historico = buscar_historico_empresa(db_com_calls, "tenda")
    # Tres calls da Tenda × 3 analistas = 3 entradas no historico
    assert set(historico.keys()) == {"Gustavo Cambauva", "Rafael Rehder", "Andre Mazini"}
    # Cada analista tem 3 intervencoes (uma por trimestre)
    assert all(len(v["interventions"]) == 3 for v in historico.values())
    assert historico["Gustavo Cambauva"]["banco"] == "BTG Pactual"


def test_buscar_historico_empresa_ignora_outras_empresas(db_com_calls):
    historico = buscar_historico_empresa(db_com_calls, "tenda")
    # MRV existe no DB mas nao deve influenciar o resultado
    for data in historico.values():
        for it in data["interventions"]:
            assert it["periodo"] in {"1T2025", "2T2025", "3T2025"}


def test_buscar_historico_setor_traz_outras_empresas(db_com_calls):
    cross = buscar_historico_setor(db_com_calls, "tenda", ["Gustavo Cambauva"])
    assert "Gustavo Cambauva" in cross
    # MRV e outra empresa do setor
    assert any(it["empresa"] == "mrv" for it in cross["Gustavo Cambauva"])


def test_identificar_analistas_provaveis_ordena_por_score(db_com_calls):
    historico = buscar_historico_empresa(db_com_calls, "tenda")
    top = identificar_analistas_provaveis(historico, top_n=5)
    # Todos tem mesmo numero de calls — deve retornar todos
    assert set(top) == {"Gustavo Cambauva", "Rafael Rehder", "Andre Mazini"}


def test_identificar_analistas_provaveis_top_n():
    historico = {
        "A": {"banco": "X", "interventions": [{"periodo": "1T", "texto": ""}]},
        "B": {"banco": "Y", "interventions": [
            {"periodo": "1T", "texto": ""},
            {"periodo": "2T", "texto": ""},
            {"periodo": "3T", "texto": ""},
        ]},
        "C": {"banco": "Z", "interventions": [
            {"periodo": "1T", "texto": ""},
            {"periodo": "2T", "texto": ""},
        ]},
    }
    top = identificar_analistas_provaveis(historico, top_n=2)
    assert len(top) == 2
    # B tem mais peso (3 intervencoes, ultima recente vale 4)
    assert top[0] == "B"

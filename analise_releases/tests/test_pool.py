"""Testes de pool: agrupamento por banco + cross-setor (licoes 2, 4)."""
from __future__ import annotations

from prever_perguntas.pool import (
    agrupar_por_banco,
    candidatos_cross_setor,
    consolidar_pool,
    montar_candidatos_por_banco,
)


def _historico_minimo() -> dict:
    return {
        "Gustavo Cambauva": {
            "banco": "BTG Pactual",
            "interventions": [{"periodo": f"{i}T2024", "texto": ""} for i in range(1, 5)],
        },
        "Pedro Lobato": {
            "banco": "BTG Pactual",
            "interventions": [{"periodo": "4T2024", "texto": ""}],
        },
        "Rafael Rehder": {
            "banco": "Safra",
            "interventions": [{"periodo": f"{i}T2024", "texto": ""} for i in range(1, 4)],
        },
    }


def test_agrupar_por_banco():
    grupos = agrupar_por_banco(_historico_minimo())
    assert set(grupos.keys()) == {"BTG Pactual", "Safra"}
    # BTG tem 2 analistas, ordenados por num de calls desc
    assert grupos["BTG Pactual"][0][0] == "Gustavo Cambauva"
    assert grupos["BTG Pactual"][1][0] == "Pedro Lobato"


def test_candidatos_por_banco_pega_alternativos():
    candidatos = montar_candidatos_por_banco(_historico_minimo(), max_alternativos=1)
    btg = next(c for c in candidatos if c.banco == "BTG Pactual")
    assert btg.primario == "Gustavo Cambauva"
    assert btg.alternativos == ["Pedro Lobato"]


def test_cross_setor_usa_dados_reais(cross_company_analysts):
    """Usa o JSON real de data/. Pedro Lobato cobre 5+ empresas."""
    historico_vazio: dict = {}  # nenhum analista cobre nossa empresa
    cross = candidatos_cross_setor(
        historico_vazio, cross_company_analysts, min_empresas_cobertas=2
    )
    nomes = [c.primario for c in cross]
    # Pedro Lobato e um dos analistas mais cross-setor segundo o JSON
    assert "Pedro Lobato" in nomes
    # Quem ja cobre a empresa nao entra em cross-setor
    assert all(not c.tem_historico_empresa for c in cross)


def test_cross_setor_exclui_quem_ja_cobre_empresa(cross_company_analysts):
    # Simulamos que Pedro Lobato ja cobre a empresa alvo
    historico = {
        "Pedro Lobato": {"banco": "Bradesco BBI", "interventions": [
            {"periodo": "1T", "texto": ""}
        ]}
    }
    cross = candidatos_cross_setor(historico, cross_company_analysts)
    assert all(c.primario != "Pedro Lobato" for c in cross)


def test_consolidar_pool_combina_ambos(cross_company_analysts):
    pool = consolidar_pool(
        _historico_minimo(),
        cross_company_data=cross_company_analysts,
        top_n_bancos=5,
        top_n_cross=3,
    )
    # Tem candidatos com historico (BTG, Safra) e candidatos sem (cross-setor)
    com_hist = [c for c in pool if c.tem_historico_empresa]
    sem_hist = [c for c in pool if not c.tem_historico_empresa]
    assert len(com_hist) > 0
    assert len(sem_hist) > 0
    assert len(pool) <= 5 + 3

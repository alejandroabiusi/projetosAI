"""Pool de analistas: agrupamento por banco e candidatos cross-setor.

Implementa as licoes 2 e 4 do backtest Tenda 4T25:
2. Prever por banco, nao so por pessoa: bancos rotacionam analistas (Tainan->Victor
   na UBS, Marcelo->Jonatas no JP Morgan). Listar 1-2 analistas por banco.
4. Pool cross-setor: analistas que cobrem outras empresas mas nunca apareceram
   na empresa alvo (Goldman Sachs, Tivio Capital no caso Tenda 4T25) sao candidatos
   reais e devem ser considerados.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CandidatoBanco:
    """Bloco de candidatos de um banco."""

    banco: str
    primario: str  # nome do analista mais provavel
    alternativos: list[str] = field(default_factory=list)
    tem_historico_empresa: bool = True
    n_calls_empresa: int = 0


def agrupar_por_banco(historico_empresa: dict) -> dict[str, list[tuple[str, int]]]:
    """Agrupa o historico por banco. Retorna {banco: [(nome, n_calls), ...]}.

    `historico_empresa` no formato de history.buscar_historico_empresa.
    """
    por_banco: dict[str, list[tuple[str, int]]] = {}
    for nome, data in historico_empresa.items():
        banco = data.get("banco", "?")
        n = len(data.get("interventions", []))
        por_banco.setdefault(banco, []).append((nome, n))
    # ordena cada banco por num de calls desc
    for banco in por_banco:
        por_banco[banco].sort(key=lambda x: x[1], reverse=True)
    return por_banco


def montar_candidatos_por_banco(
    historico_empresa: dict,
    max_alternativos: int = 1,
) -> list[CandidatoBanco]:
    """Para cada banco que ja apareceu, monta CandidatoBanco com primario+alternativos."""
    grupos = agrupar_por_banco(historico_empresa)
    candidatos: list[CandidatoBanco] = []
    for banco, lista in grupos.items():
        if not lista:
            continue
        primario, n_primario = lista[0]
        alternativos = [nome for nome, _ in lista[1: 1 + max_alternativos]]
        candidatos.append(CandidatoBanco(
            banco=banco,
            primario=primario,
            alternativos=alternativos,
            tem_historico_empresa=True,
            n_calls_empresa=n_primario,
        ))
    candidatos.sort(key=lambda c: c.n_calls_empresa, reverse=True)
    return candidatos


def candidatos_cross_setor(
    historico_empresa: dict,
    cross_company_data: dict,
    min_empresas_cobertas: int = 2,
) -> list[CandidatoBanco]:
    """Analistas com historico em >=N empresas do setor mas SEM historico na empresa alvo.

    `cross_company_data` no formato de data/cross_company_analysts.json:
    {nome_analista: [{empresa, periodo, texto}, ...]}.

    Bancos novos (Goldman, Tivio) entram aqui. Retorna lista ordenada por
    cobertura (numero de empresas distintas).
    """
    nomes_ja_cobertos = set(historico_empresa.keys())
    novos: list[CandidatoBanco] = []
    for nome, intervencoes in cross_company_data.items():
        if nome in nomes_ja_cobertos:
            continue
        empresas_cobertas = {it["empresa"] for it in intervencoes if it.get("empresa")}
        if len(empresas_cobertas) < min_empresas_cobertas:
            continue
        # banco nao consta em cross_company_analysts.json — usar "?" como placeholder
        novos.append(CandidatoBanco(
            banco="?",
            primario=nome,
            alternativos=[],
            tem_historico_empresa=False,
            n_calls_empresa=len(empresas_cobertas),
        ))
    novos.sort(key=lambda c: c.n_calls_empresa, reverse=True)
    return novos


def consolidar_pool(
    historico_empresa: dict,
    cross_company_data: dict | None = None,
    top_n_bancos: int = 8,
    top_n_cross: int = 4,
) -> list[CandidatoBanco]:
    """Pool final: bancos com historico + analistas cross-setor sem historico.

    Limita a top_n_bancos com historico + top_n_cross cross-setor (licao 4).
    """
    do_banco = montar_candidatos_por_banco(historico_empresa)[:top_n_bancos]
    pool = list(do_banco)
    if cross_company_data:
        cross = candidatos_cross_setor(historico_empresa, cross_company_data)
        pool.extend(cross[:top_n_cross])
    return pool

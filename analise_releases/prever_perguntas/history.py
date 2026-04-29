"""Queries no DB de transcricoes e ranking de analistas."""
from __future__ import annotations

import sqlite3
from typing import Iterable

from .parsing import extrair_perguntas_analistas

EMPRESAS_SETOR = (
    "tenda", "cury", "mrv", "direcional", "planoeplano", "mouradubeux", "cyrela",
)


def buscar_historico_empresa(conn: sqlite3.Connection, empresa: str) -> dict:
    """Historico de perguntas dos analistas em calls da empresa.

    {nome: {"banco": str, "interventions": [{"periodo": str, "texto": str}]}}
    """
    cur = conn.cursor()
    cur.execute(
        """SELECT periodo, texto_processado
           FROM transcricoes
           WHERE empresa = ?
             AND texto_processado IS NOT NULL
           ORDER BY ano, trimestre""",
        (empresa,),
    )
    historico: dict[str, dict] = {}
    for periodo, texto in cur.fetchall():
        if not texto:
            continue
        for key, content in extrair_perguntas_analistas(texto).items():
            nome, banco = key.split("|", 1)
            slot = historico.setdefault(nome, {"banco": banco, "interventions": []})
            slot["interventions"].append({"periodo": periodo, "texto": content})
    return historico


def buscar_historico_setor(
    conn: sqlite3.Connection,
    empresa_atual: str,
    analistas_alvo: Iterable[str],
    periodos_recentes: int = 3,
) -> dict:
    """Perguntas dos analistas-alvo nas N calls mais recentes de OUTRAS empresas."""
    alvo = set(analistas_alvo)
    cur = conn.cursor()
    cur.execute(
        """SELECT empresa, periodo, texto_processado
           FROM transcricoes
           WHERE empresa != ?
             AND texto_processado IS NOT NULL
           ORDER BY ano DESC, trimestre DESC""",
        (empresa_atual,),
    )
    por_empresa: dict[str, list[tuple[str, str]]] = {}
    for emp, periodo, texto in cur.fetchall():
        por_empresa.setdefault(emp, []).append((periodo, texto))

    cross: dict[str, list[dict]] = {}
    for emp, calls in por_empresa.items():
        for periodo, texto in calls[:periodos_recentes]:
            if not texto:
                continue
            for key, content in extrair_perguntas_analistas(texto).items():
                nome, _banco = key.split("|", 1)
                if nome not in alvo:
                    continue
                cross.setdefault(nome, []).append({
                    "empresa": emp,
                    "periodo": periodo,
                    "texto": content[:1500],
                })
    return cross


def identificar_analistas_provaveis(historico: dict, top_n: int = 10) -> list[str]:
    """Ranking por frequencia ponderada por recencia (4/2/1)."""
    scores: dict[str, int] = {}
    for nome, data in historico.items():
        n = len(data["interventions"])
        score = 0
        for idx in range(n):
            if idx >= n - 1:
                score += 4
            elif idx >= n - 2:
                score += 2
            else:
                score += 1
        scores[nome] = score
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [nome for nome, _ in ranked[:top_n]]

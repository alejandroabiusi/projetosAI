"""Fixtures compartilhadas: JSONs reais de data/ + DB in-memory."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA = ROOT / "data"


@pytest.fixture(scope="session")
def cross_company_analysts() -> dict:
    """data/cross_company_analysts.json — analistas que aparecem em >1 empresa."""
    return json.loads((DATA / "cross_company_analysts.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def tenda_analyst_questions() -> dict:
    """data/tenda_analyst_questions.json — historico de perguntas em calls da Tenda."""
    return json.loads((DATA / "tenda_analyst_questions.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def analyst_full_data() -> dict:
    """data/analyst_full_data.json — fixture mais rica (cobre cross-empresa + perfis)."""
    return json.loads((DATA / "analyst_full_data.json").read_text(encoding="utf-8"))


@pytest.fixture
def texto_processado_exemplo() -> str:
    """Trecho de texto_processado no formato gerado por pos_processar_transcricoes.

    Mantido aqui (em vez de em arquivo) pra que o teste seja autocontido e
    facil de inspecionar.
    """
    return (
        "**[Operadora]:** Bom dia, vamos comecar. Nossa primeira pergunta vem do "
        "Gustavo Cambauva.\n\n"
        "**[Gustavo Cambauva - Analista (BTG Pactual)]:** Bom dia, pessoal. "
        "Tenho duas perguntas sobre Alea: o desvio de custo e a credibilidade do "
        "guidance 2026.\n\n"
        "**[CEO - Empresa]:** Obrigado Gustavo. Vamos por partes.\n\n"
        "**[Gustavo Cambauva - Analista (BTG Pactual)]:** Complementando, "
        "queria entender a margem bruta REF.\n\n"
        "**[Rafael Rehder - Analista (Safra)]:** Bom dia. Sobre geracao de "
        "caixa operacional vs lucro, queria entender a divergencia.\n\n"
        "**[Andre Mazini - Analista (Citi)]:** Pessoal, sobre verticalizacao "
        "e modelo construtivo.\n"
    )


@pytest.fixture
def db_in_memory():
    """SQLite in-memory com schema minimo de transcricoes."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE transcricoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empresa TEXT NOT NULL,
            periodo TEXT NOT NULL,
            ano INTEGER,
            trimestre INTEGER,
            texto_processado TEXT
        );
        """
    )
    yield conn
    conn.close()


@pytest.fixture
def db_com_calls(db_in_memory, texto_processado_exemplo):
    """DB populado com 3 calls da Tenda (1T/2T/3T 2025) + 1 da MRV."""
    conn = db_in_memory
    conn.executemany(
        "INSERT INTO transcricoes (empresa, periodo, ano, trimestre, texto_processado) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("tenda", "1T2025", 2025, 1, texto_processado_exemplo),
            ("tenda", "2T2025", 2025, 2, texto_processado_exemplo),
            ("tenda", "3T2025", 2025, 3, texto_processado_exemplo),
            ("mrv", "3T2025", 2025, 3, texto_processado_exemplo),
        ],
    )
    conn.commit()
    return conn


@pytest.fixture
def release_com_surpresa() -> str:
    """Release com surpresa negativa forte: desvio de custo Alea + provisao."""
    return (
        "Resultados 4T2025 - Tenda. Receita liquida cresceu 18% vs 4T24. "
        "Margem bruta consolidada de 31,2%. "
        "No segmento Alea reconhecemos um desvio de custo de R$ 99 MM neste "
        "trimestre, refletindo materiais e mao de obra. "
        "A provisao de custos foi elevada de 11,2% para 13,9% para refletir "
        "maior conservadorismo. "
        "Geracao de caixa operacional de R$ 25,6 MM no trimestre. "
        "Mantemos o guidance de lucro liquido 2026 de R$ 600-700 MM."
    )


@pytest.fixture
def release_normal() -> str:
    """Release sem surpresas — bom resultado."""
    return (
        "Resultados 3T2025. Receita de R$ 1,2 bi, alta de 12%. "
        "Margem bruta de 32%. Lucro liquido de R$ 180 MM. "
        "Tudo dentro do esperado, mantendo o guidance."
    )

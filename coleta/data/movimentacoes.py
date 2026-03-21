"""
Banco de dados de movimentacoes de empreendimentos.
====================================================
Registra o historico completo do ciclo de vida de cada produto:
  - novo:           produto apareceu pela primeira vez
  - removido:       produto sumiu do site (fase="Removido")
  - fase_mudou:     produto trocou de status (ex: Lancamento -> Em Construcao)
  - preco_mudou:    preco a partir mudou
  - campo_mudou:    outro campo rastreado mudou
  - renomeado:      URL mudou mas e o mesmo produto (redirect ou match por nome)
  - relancado:      produto novo no mesmo local (<200m)
  - cancelado:      produto sumiu sem substituto

Banco separado: data/movimentacoes.db
Nao mistura com empreendimentos.db para manter historico limpo e exportavel.
"""

import os
import sqlite3
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
MOV_DB_PATH = os.path.join(DATA_DIR, "movimentacoes.db")


def _get_connection():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(MOV_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _criar_tabela():
    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movimentacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            empresa TEXT NOT NULL,
            nome TEXT NOT NULL,
            url_fonte TEXT,
            tipo TEXT NOT NULL,
            campo TEXT,
            valor_anterior TEXT,
            valor_novo TEXT,
            observacao TEXT,
            origem TEXT
        )
    """)
    # Indice para consultas por empresa e por data
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mov_empresa
        ON movimentacoes (empresa, data)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mov_tipo
        ON movimentacoes (tipo, data)
    """)
    conn.commit()
    conn.close()


# Inicializa ao importar
_criar_tabela()


def registrar_movimentacao(empresa, nome, tipo, url_fonte=None,
                           campo=None, valor_anterior=None, valor_novo=None,
                           observacao=None, origem=None):
    """
    Registra uma movimentacao no historico.

    Tipos validos:
        novo, removido, fase_mudou, preco_mudou, campo_mudou,
        renomeado, relancado, cancelado
    """
    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO movimentacoes "
        "(data, empresa, nome, url_fonte, tipo, campo, valor_anterior, valor_novo, observacao, origem) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            datetime.now().isoformat(),
            empresa,
            nome,
            url_fonte,
            tipo,
            campo,
            str(valor_anterior) if valor_anterior is not None else None,
            str(valor_novo) if valor_novo is not None else None,
            observacao,
            origem,
        ),
    )
    conn.commit()
    conn.close()


def consultar_movimentacoes(empresa=None, tipo=None, desde=None, limit=500):
    """
    Consulta movimentacoes com filtros opcionais.

    Args:
        empresa: filtrar por empresa
        tipo: filtrar por tipo (novo, removido, fase_mudou, etc.)
        desde: data ISO minima (ex: "2026-03-01")
        limit: maximo de resultados
    """
    conn = _get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM movimentacoes WHERE 1=1"
    params = []

    if empresa:
        query += " AND empresa = ?"
        params.append(empresa)
    if tipo:
        query += " AND tipo = ?"
        params.append(tipo)
    if desde:
        query += " AND data >= ?"
        params.append(desde)

    query += " ORDER BY data DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def resumo_movimentacoes(desde=None):
    """
    Retorna contadores por tipo de movimentacao.
    Util para relatorios.
    """
    conn = _get_connection()
    cursor = conn.cursor()

    query = "SELECT tipo, COUNT(*) as total FROM movimentacoes"
    params = []
    if desde:
        query += " WHERE data >= ?"
        params.append(desde)
    query += " GROUP BY tipo ORDER BY total DESC"

    cursor.execute(query, params)
    rows = {row["tipo"]: row["total"] for row in cursor.fetchall()}
    conn.close()
    return rows


def historico_empreendimento(empresa, nome):
    """Retorna todas as movimentacoes de um empreendimento especifico."""
    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM movimentacoes WHERE empresa = ? AND nome = ? ORDER BY data",
        (empresa, nome),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows

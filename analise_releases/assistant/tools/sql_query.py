"""Tool: consulta SQL read-only ao banco dados_financeiros.db."""

import sqlite3
import re
from config import DB_PATH

TOOL_DEFINITION = {
    "name": "query_database",
    "description": (
        "Executa uma consulta SQL SELECT no banco de dados de indicadores financeiros "
        "trimestrais de incorporadoras brasileiras. Tabela principal: dados_trimestrais. "
        "Colunas-chave: empresa, segmento, periodo, ano, trimestre, e ~130 métricas "
        "financeiras e operacionais. Use segmento='Consolidado' para comparar empresas."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": (
                    "Consulta SQL SELECT. Apenas SELECT é permitido. "
                    "Exemplos: "
                    "SELECT empresa, receita_liquida FROM dados_trimestrais WHERE periodo='3T2024' AND segmento='Consolidado'; "
                    "SELECT periodo, margem_bruta FROM dados_trimestrais WHERE empresa='Cury' AND ano>=2022 ORDER BY ano, trimestre"
                ),
            }
        },
        "required": ["sql"],
    },
}


def _validate_sql(sql: str) -> str | None:
    """Retorna mensagem de erro se SQL não for seguro, None se OK."""
    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip().rstrip(";").strip()

    if not cleaned.upper().startswith("SELECT"):
        return "Apenas consultas SELECT são permitidas."

    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|PRAGMA|VACUUM)\b"
    if re.search(forbidden, cleaned, re.IGNORECASE):
        return "Comando SQL não permitido. Apenas SELECT é aceito."

    return None


def execute(sql: str) -> dict:
    """Executa query e retorna resultado estruturado."""
    error = _validate_sql(sql)
    if error:
        return {"error": error}

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        data = [dict(row) for row in rows]
        conn.close()

        return {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
        }
    except Exception as e:
        return {"error": f"Erro SQL: {str(e)}"}

"""Tool: leitor de planilhas Excel das incorporadoras."""

import pandas as pd
from config import PLANILHAS_DIR, PLANILHAS_MAP

TOOL_DEFINITION = {
    "name": "read_spreadsheet",
    "description": (
        "Lê planilhas Excel das incorporadoras. Ações disponíveis: "
        "'list_spreadsheets' (lista planilhas disponíveis), "
        "'list_sheets' (lista abas de uma planilha), "
        "'read_sheet' (lê dados de uma aba específica). "
        "Planilhas disponíveis: Cury, Cyrela_Operacionais, Cyrela_DFs, "
        "Cyrela_Lancamentos, Cyrela_Indicadores, Direcional, MouraDubeux, "
        "MRV, PlanoePlano, Tenda."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["list_spreadsheets", "list_sheets", "read_sheet"],
                "description": "Ação a executar.",
            },
            "spreadsheet": {
                "type": "string",
                "description": "Nome da planilha (ex: 'Cury', 'MRV', 'Cyrela_DFs'). Necessário para list_sheets e read_sheet.",
            },
            "sheet_name": {
                "type": "string",
                "description": "Nome da aba. Necessário para read_sheet.",
            },
            "max_rows": {
                "type": "integer",
                "description": "Máximo de linhas a retornar (padrão: 50).",
            },
        },
        "required": ["action"],
    },
}


def execute(action: str, spreadsheet: str = None, sheet_name: str = None, max_rows: int = 50) -> dict:
    if action == "list_spreadsheets":
        return {"spreadsheets": list(PLANILHAS_MAP.keys())}

    if spreadsheet not in PLANILHAS_MAP:
        return {"error": f"Planilha '{spreadsheet}' não encontrada. Disponíveis: {list(PLANILHAS_MAP.keys())}"}

    filepath = PLANILHAS_DIR / PLANILHAS_MAP[spreadsheet]
    if not filepath.exists():
        return {"error": f"Arquivo não encontrado: {filepath.name}"}

    if action == "list_sheets":
        try:
            xl = pd.ExcelFile(filepath)
            return {"spreadsheet": spreadsheet, "sheets": xl.sheet_names}
        except Exception as e:
            return {"error": str(e)}

    if action == "read_sheet":
        if not sheet_name:
            return {"error": "sheet_name é obrigatório para action='read_sheet'."}
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=0)
            df = df.head(max_rows)
            # Converter para formato limpo
            df = df.fillna("")
            records = df.to_dict(orient="records")
            return {
                "spreadsheet": spreadsheet,
                "sheet": sheet_name,
                "columns": list(df.columns),
                "rows": records,
                "row_count": len(records),
                "total_rows_in_sheet": len(pd.read_excel(filepath, sheet_name=sheet_name)),
            }
        except Exception as e:
            return {"error": str(e)}

    return {"error": f"Ação '{action}' não reconhecida."}

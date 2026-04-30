import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ASSISTANT_DIR = Path(__file__).parent

DB_PATH = BASE_DIR / "dados_financeiros.db"
PLANILHAS_DIR = BASE_DIR / "planilhas"
RELEASES_BASE = BASE_DIR / "downloads"
CHROMA_DIR = ASSISTANT_DIR / "chroma_db"

MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
MAX_TOKENS = 4096

EMPRESAS_RELEASES = ["cury", "cyrela", "direcional", "mrv", "planoeplano", "tenda"]

PLANILHAS_MAP = {
    "Cury": "Cury_Planilha_Fundamentos_2026-03.xlsx",
    "Cyrela_Operacionais": "Cyrela_Dados_Operacionais_2026-03.xlsx",
    "Cyrela_DFs": "Cyrela_Demonstracoes_Financeiras_2026-03.xlsx",
    "Cyrela_Lancamentos": "Cyrela_Lancamentos_2026-03.xlsx",
    "Cyrela_Indicadores": "Cyrela_Principais_Indicadores_2026-03.xlsx",
    "Direcional": "Direcional_Planilha_Interativa_2026-03.xlsx",
    "MouraDubeux": "MouraDubeux_Planilha_Fundamentos_2026-03.xlsx",
    "MRV": "MRV_Base_Dados_Operacionais_Financeiros_2026-03.xlsx",
    "PlanoePlano": "PlanoePlano_Planilha_Interativa_2026-03.xlsx",
    "Tenda": "Tenda_Planilha_Fundamentos_2026-03.xlsx",
}

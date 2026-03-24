"""
Configuracoes centralizadas do projeto de analise TOTVS.
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DOWNLOADS_DIR = os.path.join(PROJECT_ROOT, "downloads")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# TOTVS
# ============================================================

TOTVS = {
    "nome": "TOTVS",
    "ticker": "TOTS3",
    "ri_url": "https://ri.totvs.com/informacoes-financeiras/central-de-resultados/",
    "ri_planilhas_url": "https://ri.totvs.com/informacoes-financeiras/planilhas-interativas/",
    "site_comercial": "https://www.totvs.com/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "totvs", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "totvs", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "totvs", "demonstracoes"),
    "downloads_apresentacoes": os.path.join(DOWNLOADS_DIR, "totvs", "apresentacoes"),
    "downloads_planilhas": os.path.join(DOWNLOADS_DIR, "totvs", "planilhas"),
    "downloads_audios": os.path.join(DOWNLOADS_DIR, "totvs", "audios"),
    "downloads_transcricoes_ri": os.path.join(DOWNLOADS_DIR, "totvs", "transcricoes_ri"),
}

# ============================================================
# PARAMETROS DO SELENIUM
# ============================================================
SELENIUM = {
    "headless": True,
    "timeout": 30,
    "wait_between_requests": 3,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}

# ============================================================
# PARAMETROS DO REQUESTS (para downloads de PDF)
# ============================================================
REQUESTS = {
    "timeout": 60,
    "headers": {
        "User-Agent": SELENIUM["user_agent"],
        "Accept": "application/pdf,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    },
}

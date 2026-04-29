"""
Configuracoes centralizadas do projeto analise_releases.

==================================================================
AVISO: Este arquivo e uma COPIA INDEPENDENTE de coleta/config/settings.py.

Os dois projetos compartilham o mesmo formato de empresa-config historicamente,
mas vivem em pacotes Python separados. Quando alterar algo aqui que tambem
deva valer no projeto de coleta de empreendimentos (ou vice-versa), atualize
manualmente o outro arquivo.

Campos relevantes para este projeto: ri_url, ri_base_url, ri_anos,
downloads_releases, downloads_itr_dfp, downloads_demonstracoes, ticker.
==================================================================
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DOWNLOADS_DIR = os.path.join(PROJECT_ROOT, "downloads")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# EMPRESAS MONITORADAS
# Todas usam plataforma MZ Group com mesma logica de navegacao.
# ============================================================

PLANOEPLANO = {
    "nome": "Plano&Plano",
    "ticker": "PLPL3",
    "ri_url": "https://ri.planoeplano.com.br/informacoes-financeiras/central-de-resultados/",
    "site_comercial": "https://www.planoeplano.com.br/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "planoeplano", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "planoeplano", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "planoeplano", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "planoeplano", "lancamentos"),
}

CURY = {
    "nome": "Cury",
    "ticker": "CURY3",
    "ri_url": "https://ri.cury.net/informacoes-aos-investidores/central-de-resultados/",
    "site_comercial": "https://cury.net/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "cury", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "cury", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "cury", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "cury", "lancamentos"),
}

MRV = {
    "nome": "MRV",
    "ticker": "MRVE3",
    "ri_url": "https://ri.mrv.com.br/informacoes-financeiras/central-de-resultados/",
    "site_comercial": "https://www.mrv.com.br/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "mrv", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "mrv", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "mrv", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "mrv", "lancamentos"),
}

DIRECIONAL = {
    "nome": "Direcional",
    "ticker": "DIRR3",
    "ri_url": "https://ri.direcional.com.br/informacoes-financeiras/central-de-resultados/",
    "site_comercial": "https://www.direcional.com.br/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "direcional", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "direcional", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "direcional", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "direcional", "lancamentos"),
}

CYRELA = {
    "nome": "Cyrela",
    "ticker": "CYRE3",
    "ri_url": "https://ri.cyrela.com.br/informacoes-financeiras/central-de-resultados/",
    "site_comercial": "https://www.cyrela.com.br/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "cyrela", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "cyrela", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "cyrela", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "cyrela", "lancamentos"),
}

MOURADUBEUX = {
    "nome": "MouraDubeux",
    "ticker": "MDNE3",
    "ri_url": "https://ri.mouradubeux.com.br/informacoes-financeiras/central-de-resultados/",
    "site_comercial": "https://www.mouradubeux.com.br/",
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "mouradubeux", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "mouradubeux", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "mouradubeux", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "mouradubeux", "lancamentos"),
}

TENDA = {
    "nome": "Tenda",
    "ticker": "TEND3",
    "ri_url": "https://ri.tenda.com/informacoes-financeiras/central-de-resultados",
    "ri_base_url": "https://ri.tenda.com",
    "ri_anos": list(range(2016, 2027)),  # 2016 a 2026
    "site_comercial": "https://www.tenda.com/",
    "plataforma": "sumaq",  # Nao e MZ Group
    "downloads_releases": os.path.join(DOWNLOADS_DIR, "tenda", "releases"),
    "downloads_itr_dfp": os.path.join(DOWNLOADS_DIR, "tenda", "itr_dfp"),
    "downloads_demonstracoes": os.path.join(DOWNLOADS_DIR, "tenda", "demonstracoes"),
    "downloads_lancamentos": os.path.join(DOWNLOADS_DIR, "tenda", "lancamentos"),
}

# Lista de todas as empresas MZ Group (util para iteracao no runner)
EMPRESAS_MZ = [PLANOEPLANO, CURY, MRV, DIRECIONAL, CYRELA, MOURADUBEUX]

# Todas as empresas incluindo Tenda
TODAS_EMPRESAS = EMPRESAS_MZ + [TENDA]

# ============================================================
# ATUALIZACAO RECORRENTE
# ============================================================
ATUALIZACAO = {
    "backup_antes": True,
    "max_backups": 10,
    "timeout_por_scraper": 1800,  # 30 min
    "retry_falhas": True,
    "campos_rastreados": ["fase", "preco_a_partir", "total_unidades",
                          "evolucao_obra_pct", "area_min_m2", "area_max_m2"],
}

# ============================================================
# EMAIL DE NOTIFICACAO
# ============================================================
EMAIL = {
    "metodo": "outlook",  # ou "smtp"
    "destinatarios": [],  # preencher com emails do time IM
    "assunto_prefixo": "[Coleta IM]",
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
        "Accept": "application/pdf,*/*",
    },
}

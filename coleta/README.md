# Web Scraper de Inteligencia Competitiva

Monitoramento automatizado de lancamentos e produtos da concorrencia.

## Estrutura do Projeto

```
scraper_inteligencia/
├── config/
│   ├── __init__.py
│   └── settings.py            # Configuracoes gerais (URLs, paths, timeouts)
├── scrapers/
│   ├── __init__.py
│   ├── base_scraper.py        # Classe base com logica compartilhada
│   ├── planoeplano_ri.py      # Scraper de releases - Plano&Plano
│   └── cury_ri.py             # Scraper de releases - Cury (a construir)
├── downloads/
│   ├── planoeplano/
│   │   ├── releases/          # PDFs de resultados trimestrais
│   │   └── lancamentos/       # Dados de empreendimentos (futuro)
│   └── cury/
│       ├── releases/
│       └── lancamentos/
├── logs/                      # Logs de execucao
├── data/                      # CSVs e bases consolidadas (futuro)
├── run_releases.py            # Runner para todos os scrapers de releases
├── requirements.txt
└── README.md
```

## Instalacao

A unica dependencia que nao estava no guia de instalacao original e o
webdriver-manager, que gerencia automaticamente o ChromeDriver:

```
pip install webdriver-manager
```

As demais dependencias (selenium, beautifulsoup4, requests) ja foram
instaladas na Etapa 3 do Guia de Instalacao.

## Execucao

Abra o terminal na pasta raiz do projeto (scraper_inteligencia):

```
python scrapers/planoeplano_ri.py
python scrapers/planoeplano_ri.py 2025 2024 2023
python run_releases.py
python run_releases.py plano
```

## Empresas Monitoradas

| Empresa       | RI                        | Site Comercial     | Releases | Lancamentos |
|---------------|---------------------------|--------------------|----------|-------------|
| Plano&Plano   | ri.planoeplano.com.br     | planoeplano.com.br | Pronto   | Fase 3      |
| Cury          | A confirmar               | cury.net           | Fase 2   | Fase 4      |

## Configuracao

URLs, paths e parametros estao centralizados em `config/settings.py`.
Para debug visual (ver o navegador funcionando), altere `headless` para `False`.

## Notas Tecnicas

O site de RI da Plano&Plano usa a plataforma MZ Group, que carrega o conteudo
da Central de Resultados via JavaScript. Por isso o Selenium e obrigatorio.
Os PDFs sao servidos pela API da MZ em `api.mziq.com`.

O scraper respeita intervalos entre requisicoes (configuravel em settings.py)
para nao sobrecarregar os servidores.

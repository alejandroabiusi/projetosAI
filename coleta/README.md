# Projeto Coleta — Empreendimentos Imobiliários

Scrapers e pipeline de enriquecimento de dados de **empreendimentos imobiliários** de incorporadoras brasileiras. Mantém uma base SQLite consolidada (`data/empreendimentos.db`) com ~3.500 empreendimentos de ~78 empresas, geocodificada via base local de CEPs, com mapa interativo HTML e dashboard Streamlit.

> **NOTA**: A parte de coleta e análise de releases / RI / earnings calls foi movida para `../analise_releases/`. Este projeto cobre exclusivamente empreendimentos.

## Estrutura

```
coleta/
├── config/                     # Settings, regionais, catálogo de sites
├── scrapers/                   # Scrapers de empreendimentos (38 arquivos)
│   ├── base_scraper.py         # Classe base (Selenium, logging, downloads)
│   ├── {mrv,cury,direcional,planoeplano,tenda,vivabenx,vivaz,metrocasa}_empreendimentos.py
│   ├── generico_empreendimentos.py     # Scraper genérico (parametrizável por empresa)
│   ├── wpapi_empreendimentos.py        # WordPress REST API
│   ├── generico_novas_empresas_*.py    # 17 batches de empresas menores
│   ├── verificar_status.py             # Re-detecta fases, reconcilia URLs mortas
│   └── ...
├── scripts/                    # Scripts utilitários (extrações geográficas, requalificação)
├── data/                       # empreendimentos.db, ceps_brasil.db, movimentacoes.db, KMLs
├── downloads/                  # Imagens de empreendimentos (não versionado)
├── dashboard/                  # Dashboard Streamlit
├── docs/                       # Mapeamentos técnicos, relatórios de sessão
├── build_logs/                 # Logs de execução (não versionado)
├── enriquecer_*.py             # Pipeline de enriquecimento
├── qualificar_produto.py       # Qualificação profunda (tipologias, lazer, vagas)
├── gerar_mapa.py               # Mapa HTML interativo
├── gerar_relatorio_pptx.py     # Relatório executivo PPTX
├── run_atualizacao.py          # Atualização recorrente com diff/changelog
└── CLAUDE.md                   # Documentação detalhada (ler antes de mexer)
```

## Pipeline padrão

```bash
# 1. Coletar empreendimentos (todas as empresas ou específicas)
python run_coleta.py
python run_coleta.py mrv cury

# 2. Enriquecer (geocodificação, áreas, RI, unidades)
python run_enriquecimento_completo.py

# 3. Qualificar (tipologias detalhadas, lazer, vagas)
python qualificar_produto.py

# 4. Gerar mapa e dashboard
python gerar_mapa.py
streamlit run dashboard/app.py

# 5. Atualização recorrente (com detecção de mudanças)
python run_atualizacao.py
```

## Banco de Dados

- **`data/empreendimentos.db`** — tabela principal com ~120 colunas, ~3.500 registros, ~98% com coordenadas
- **`data/ceps_brasil.db`** — 905k CEPs com lat/lon (basedosdados.org)
- **`data/movimentacoes.db`** — histórico de ciclo de vida (novo, removido, fase_mudou, preco_mudou, renomeado, relancado, cancelado)
- Tabelas `runs`, `changelog`, `reconciliacao` em `empreendimentos.db` para change tracking

## Empresas cobertas

77 incorporadoras. Top 15 por número de produtos: Tenda (469), MRV (441), Cury (272), VIC (185), Plano&Plano (157), Vitta (155), Magik JC (112), Direcional (111), Metrocasa (109), Vivaz (106), EBM (81), Trisul (65), HM (62), Grafico (60), Pacaembu (54). Lista completa em `CLAUDE.md`.

## Empresas adicionadas via genéricos

Cada empresa nova requer registro em `EMPRESA_CONFIG` do `enriquecer_dados.py` e configuração no `generico_empreendimentos.py` (ou `wpapi_empreendimentos.py` se for WP REST API). Cada site tem seus próprios elementos HTML — nunca usar solução totalmente genérica sem testar.

## Documentação completa

Ver `CLAUDE.md` na raiz deste diretório para:
- Descrição detalhada de cada scraper e enriquecedor
- Hierarquia de geocodificação (4 níveis)
- Detector de fase e regressões conhecidas
- Pipeline completo de qualificação
- Convenções e gotchas por empresa

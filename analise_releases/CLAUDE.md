# Projeto Analise Releases — Dados Financeiros de Incorporadoras

## Objetivo
Extrair dados financeiros trimestrais de releases, planilhas interativas e ITRs de incorporadoras brasileiras de capital aberto, populando um banco SQLite para analise comparativa e assistente financeiro.

## Banco de Dados
- **Arquivo**: `dados_financeiros.db` (SQLite, ~230KB)
- **Tabela principal**: `dados_trimestrais` (136 colunas, 279 registros)
- **Tabela auxiliar**: `empresas` (6 registros)
- **Periodo**: 1T2020 a 4T2024 (banco precisa ser atualizado com dados 2025)
- **Empresas**: Tenda, MRV, Direcional, Cury, PlanoePlano, Cyrela
- **Segmentos**: Consolidado, MRV Incorporacao, Luggo, Resia, Urba, Alea, Riva, Tenda

## Estrutura de Pastas

```
analise_releases/
├── dados_financeiros.db          # Banco SQLite principal
├── CLAUDE.md                     # Este arquivo
├── INSTRUCOES_PROJETO_RELEASES.md
│
├── downloads/                    # PDFs e docs de RI (antes em coleta/downloads/)
│   ├── cury/
│   │   ├── releases/             # Cury_Release_YYYY_QT.pdf
│   │   ├── apresentacoes/
│   │   ├── audios/
│   │   ├── demonstracoes/
│   │   ├── itr_dfp/
│   │   ├── transcricoes/
│   │   └── transcricoes_ri/
│   ├── cyrela/                   # Mesma estrutura
│   ├── direcional/
│   ├── mouradubeux/
│   ├── mrv/
│   ├── planoeplano/
│   └── tenda/
│
├── planilhas/                    # Planilhas interativas XLSX
│   ├── Tenda_Planilha_Fundamentos_2026-03.xlsx
│   ├── Cury_Planilha_Fundamentos_2026-03.xlsx
│   ├── MRV_Base_Dados_Operacionais_Financeiros_2026-03.xlsx
│   ├── Direcional_Planilha_Interativa_2026-03.xlsx
│   ├── PlanoePlano_Planilha_Interativa_2026-03.xlsx
│   ├── Cyrela_Dados_Operacionais_2026-03.xlsx
│   ├── Cyrela_Demonstracoes_Financeiras_2026-03.xlsx
│   ├── Cyrela_Lancamentos_2026-03.xlsx
│   ├── Cyrela_Principais_Indicadores_2026-03.xlsx
│   └── MouraDubeux_Planilha_Fundamentos_2026-03.xlsx
│
├── scrapers/                     # Scrapers de sites de RI
│   ├── base_scraper.py           # Classe base (Selenium, logging, download)
│   ├── mzgroup_ri.py             # Scraper generico MZ Group (Cury, MRV, Direcional, Cyrela, PlanoePlano, MouraDubeux)
│   ├── tenda_ri.py               # Scraper Tenda (Next.js/Sumaq, requests)
│   └── planoeplano_ri.py         # Scraper PlanoePlano (variante especifica)
│
├── config/
│   └── settings.py               # URLs de RI, paths de downloads, config Selenium/requests
│
├── output/                       # Analises geradas (briefings, quantitativos)
│   ├── briefing_*.md
│   ├── quanti_*.md
│   ├── analise_investimento.md
│   └── analise_tenda_lens.md
│
├── assistant/                    # Assistente Chainlit/Gradio com RAG
│   ├── app.py                    # Chainlit app
│   ├── app_gradio.py             # Gradio app
│   ├── config.py
│   └── rag/                      # RAG pipeline
│
├── logs/                         # Logs dos scrapers
│
├── criar_banco_v2.py             # Criacao do DB via extracao label-based das planilhas
├── populate_itr_batch*.py        # Scripts de populacao incremental (batch 2-14)
├── ingerir_releases.py           # Ingestao de releases PDF no banco
├── run_all_steps.py              # Pipeline original de 5 etapas
├── gerar_analise_incorporadoras.py  # Geracao de analises comparativas
├── extract_itr_batch.py          # Extracao batch de ITRs/DFPs
├── update_schema.py              # Atualizacao do schema do banco
└── scan_itr.py                   # Scanner de documentos ITR
```

## Downloads de RI — Nomenclatura padrao

Formato: `Empresa_Tipo_YYYY_QT.ext`

Exemplos:
- `MRV_Release_2025_4T.pdf`
- `Cyrela_ITR_2024_3T.pdf`
- `Tenda_Apresentacao_2025_1T.pdf`

## Cobertura dos releases PDF (em downloads/{empresa}/releases/)
- **Tenda**: 3T2016 a 4T2025
- **MRV**: 1T2012 a 4T2025
- **Direcional**: 4T2007 a 4T2025
- **Cury**: 3T2020 a 4T2025
- **PlanoePlano**: 3T2020 a 4T2025
- **Cyrela**: 3T2005 a 4T2025
- **MouraDubeux**: 4T2019 a 4T2025

## Campos principais do banco (136 colunas em `dados_trimestrais`)

### Identificacao
- empresa, segmento, periodo, ano, trimestre, fonte

### Lancamentos e vendas
- vgv_lancado, empreendimentos_lancados, unidades_lancadas, preco_medio_lancamento
- vendas_brutas_vgv, vendas_brutas_unidades, vendas_liquidas_vgv, vendas_liquidas_unidades
- distratos_vgv, distratos_unidades, vso_liquido

### DRE
- receita_liquida, custo_mercadorias_vendidas, lucro_bruto, margem_bruta
- despesas_comerciais, despesas_gerais_administrativas, sga_total
- resultado_financeiro, lucro_liquido, margem_liquida, ebitda, margem_ebitda

### Balanco e endividamento
- divida_bruta, caixa_equivalentes, divida_liquida, patrimonio_liquido
- divida_liquida_patrimonio, roe

### Operacional
- estoque_unidades, estoque_vgv, landbank_vgv, landbank_unidades
- obras_entregues_unidades, velocidade_vendas

## Scrapers de RI

### mzgroup_ri.py — Scraper generico MZ Group
Todas as incorporadoras (exceto Tenda) usam plataforma MZ Group. Selenium navega a central de resultados, seleciona ano, filtra por tipo e baixa PDFs.

```bash
cd analise_releases
python scrapers/mzgroup_ri.py cury                    # Releases de todos os anos
python scrapers/mzgroup_ri.py mrv 2025 2024            # Apenas anos especificos
python scrapers/mzgroup_ri.py cyrela --tipo itr_dfp    # ITRs/DFPs
python scrapers/mzgroup_ri.py direcional --tipo demonstracoes
```

### Pipeline financeiro (DB `dados_financeiros.db`)
| Script | Função |
|--------|--------|
| `criar_banco_v2.py` | Criação do DB e população via extração label-based das planilhas |
| `populate_itr_batch*.py` | Scripts de população incremental por empresa/período (batch 2-14) |
| `update_schema.py` | Atualização do schema (adição de campos SG&A/VSO) |
| `extract_itr_batch.py` | Extração batch de ITRs/DFPs da CVM |
| `scan_itr.py` | Scanner de documentos ITR |
| `run_all_steps.py` | Pipeline original de 5 etapas |

### Coleta de documentos de RI (migrado de `coleta/` em 2026-04-29)
| Script | Função |
|--------|--------|
| `run_releases.py` | Runner para baixar releases trimestrais (PDFs) de todas as empresas |
| `run_audios.py` | Runner para baixar áudios de teleconferências |
| `run_apresentacoes.py` | Runner para baixar apresentações de resultados |
| `run_itr_dfp.py` | Runner para ITR/DFP e Demonstrações Financeiras (CVM) |
| `scrapers/mzgroup_ri.py` | Scraper genérico para empresas em plataforma MZ Group |
| `scrapers/tenda_ri.py` | Scraper Tenda (Sumaq, formato próprio) |
| `scrapers/planoeplano_ri.py` | Scraper Plano&Plano |

### Pipeline de transcrição e previsão de perguntas (DB `data/transcricoes.db`)
| Script | Função |
|--------|--------|
| `transcrever_audios.py` | Transcrição via faster-whisper (large-v3) |
| `pos_processar_transcricoes.py` | Correção de nomes, detecção de speakers, normalização de períodos |
| `importar_transcricoes_mrv.py` | Importa transcrições oficiais MRV (PDFs do RI) |
| `prever_perguntas.py` | Prevê 2-3 perguntas por analista no próximo call (release PDF + histórico) |
| `scripts/extract_analyst_full_data.py` | Extrai histórico cross-empresa de analistas |

**Empresas no escopo de releases/calls:** planoeplano, cury, mrv, direcional, cyrela, mouradubeux, tenda.

**Áudios e `data/transcricoes.db` ficam fora do git** (`.gitignore` ignora `**/downloads/` e `*.db` exceto principais). O usuário mantém os áudios localmente.

## Notas técnicas
- Todos os valores monetários normalizados para **R$ milhões**
- Campos percentuais (margem_*, vso_*, pct_*, roe) **NÃO** são multiplicados pelo multiplicador
- Cyrela DRE usa formato Economatica com prefixos +/-/= (stripped na extração)
- Cyrela labels **sem acentos** (e.g., "liquida" não "líquida")
- Cury tem unidades mistas: dados operacionais em R$ mil, landbank em R$ milhões
- MRV tem labels repetidos entre seções → usa extração row-based
- Low fill rates esperados: divida_liquida/geracao_caixa (~16%) — só em algumas planilhas

## Como rodar

```bash
# --- Pipeline financeiro ---
python criar_banco_v2.py                       # Criar/recriar DB do zero
python populate_itr_batch14_planoeplano.py     # Adicionar dados incrementais
python update_schema.py                         # Atualizar schema

# --- Coleta de documentos de RI ---
python run_releases.py                          # Releases trimestrais (todas)
python run_releases.py tenda cury               # Empresas específicas
python run_audios.py                            # Áudios de calls
python run_itr_dfp.py --tipo demonstracoes      # ITR/DFP/Demonstrações

# --- Pipeline transcrição + previsão ---
python transcrever_audios.py tenda              # Transcreve áudios da Tenda
python pos_processar_transcricoes.py            # Corrige nomes + detecta speakers
python prever_perguntas.py tenda downloads/tenda/releases/Tenda_Release_4T2025.pdf
```

## Notas estruturais
- `config/settings.py` e `scrapers/base_scraper.py` são **cópias independentes** das respectivas versões em `coleta/`. Quando alterar algo que valha pros dois projetos, atualize manualmente os dois arquivos. Cabeçalhos de aviso estão nos arquivos.

# Projeto Análise Releases — Dados Financeiros de Incorporadoras

## Objetivo
Extrair dados financeiros trimestrais de releases e planilhas de incorporadoras brasileiras, populando um banco SQLite que servirá como referência para um assistente de análises financeiras.

## Banco de Dados
- **Arquivo**: `dados_financeiros.db` (SQLite, ~230KB)
- **Tabela principal**: `dados_trimestrais` (136 colunas, 279 registros)
- **Tabela auxiliar**: `empresas` (6 registros)
- **Período**: 1T2020 a 4T2024
- **Empresas**: Tenda, MRV, Direcional, Cury, PlanoePlano, Cyrela
- **Segmentos**: Consolidado, MRV Incorporação, Luggo, Resia, Urba, Alea, Riva, Tenda

## Campos principais (136 colunas em `dados_trimestrais`)

### Identificação
- empresa, segmento, periodo, ano, trimestre, fonte

### Lançamentos e vendas
- vgv_lancado, empreendimentos_lancados, unidades_lancadas, preco_medio_lancamento
- vendas_brutas_vgv, vendas_brutas_unidades, vendas_liquidas_vgv, vendas_liquidas_unidades
- distratos_vgv, distratos_unidades, vso_liquido

### DRE (Demonstração de Resultados)
- receita_liquida, custo_mercadorias_vendidas, lucro_bruto, margem_bruta
- despesas_comerciais, despesas_gerais_administrativas, sga_total
- resultado_financeiro, lucro_liquido, margem_liquida, ebitda, margem_ebitda

### Balanço e endividamento
- divida_bruta, caixa_equivalentes, divida_liquida, patrimonio_liquido
- divida_liquida_patrimonio, roe

### Operacional
- estoque_unidades, estoque_vgv, landbank_vgv, landbank_unidades
- obras_entregues_unidades, velocidade_vendas

## Planilhas fonte (em `planilhas/`)
- `Cury_Planilha_Fundamentos_2026-03.xlsx`
- `Cyrela_Dados_Operacionais_2026-03.xlsx`, `Cyrela_Demonstracoes_Financeiras_2026-03.xlsx`, etc.
- `Direcional_Planilha_Interativa_2026-03.xlsx`
- `MRV_Base_Dados_Operacionais_Financeiros_2026-03.xlsx`
- `PlanoePlano_Planilha_Interativa_2026-03.xlsx`
- `Tenda_Planilha_Fundamentos_2026-03.xlsx`
- `MouraDubeux_Planilha_Fundamentos_2026-03.xlsx` (excluída do DB por decisão do projeto)

## Schema de referência
- `schema_sugestao_v2.xlsx` — 100 campos organizados em 6 categorias

## Scripts principais

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

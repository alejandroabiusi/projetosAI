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

| Script | Função |
|--------|--------|
| `criar_banco_v2.py` | Criação do DB e população via extração label-based das planilhas |
| `populate_itr_batch*.py` | Scripts de população incremental por empresa/período (batch 2-14) |
| `update_schema.py` | Atualização do schema (adição de campos SG&A/VSO) |
| `extract_itr_batch.py` | Extração batch de ITRs/DFPs da CVM |
| `scan_itr.py` | Scanner de documentos ITR |
| `run_all_steps.py` | Pipeline original de 5 etapas |

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
# Criar/recriar banco do zero
python criar_banco_v2.py

# Adicionar dados incrementais
python populate_itr_batch14_planoeplano.py  # (exemplo)

# Atualizar schema
python update_schema.py
```

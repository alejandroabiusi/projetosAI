"""Schema completo do banco dados_financeiros.db para injeção no system prompt."""

DB_SCHEMA = """
## Banco de Dados: dados_financeiros.db (SQLite)

### Tabela: empresas
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| id | INTEGER PK | ID da empresa |
| nome | TEXT UNIQUE | Nome (Cury, Cyrela, Direcional, MRV, PlanoePlano, Tenda) |
| sigla | TEXT | Ticker B3 (CURY3, CYRE3, DIRR3, MRVE3, PLPL3, TEND3) |
| segmentos | TEXT | Segmentos disponíveis separados por vírgula |

### Tabela: dados_trimestrais (279 registros, 1T2020 a 3T2025)
Chave única: (empresa, segmento, periodo)

**Identificação:**
- empresa TEXT — Nome da empresa
- segmento TEXT — 'Consolidado', 'MRV Incorporação', 'Resia', 'Urba', 'Luggo', 'Riva', 'Alea', 'Tenda'
- periodo TEXT — Formato 'xTAAAA' (ex: '3T2024')
- ano INTEGER, trimestre INTEGER
- fonte TEXT — 'planilha'

**Lançamentos:**
- vgv_lancado — VGV lançado (R$ milhões)
- empreendimentos_lancados — Qtd empreendimentos
- unidades_lancadas — Qtd unidades
- preco_medio_lancamento — Preço médio (R$ mil)
- tamanho_medio_empreendimentos — Tamanho médio

**Vendas:**
- vendas_brutas_vgv — VGV vendas brutas (R$ milhões)
- vendas_brutas_unidades — Qtd unidades brutas
- distratos_vgv — VGV distratos (R$ milhões)
- distratos_unidades — Qtd distratos
- vendas_liquidas_vgv — VGV vendas líquidas (R$ milhões)
- vendas_liquidas_unidades — Qtd unidades líquidas
- vso_bruta_trimestral — VSO bruta trimestral (%)
- vso_liquida_trimestral — VSO líquida trimestral (%)
- vso_bruta_12m — VSO bruta 12 meses (%)
- vso_liquida_12m — VSO líquida 12 meses (%)
- preco_medio_vendas — Preço médio vendas (R$ mil)

**Repasses/Entregas:**
- vgv_repassado — VGV repassado (R$ milhões)
- unidades_repassadas — Qtd unidades repassadas
- unidades_entregues — Qtd unidades entregues
- obras_em_andamento — Qtd obras em andamento

**Estoque:**
- estoque_vgv — VGV em estoque (R$ milhões)
- estoque_unidades — Qtd unidades em estoque
- preco_medio_estoque — Preço médio estoque (R$ mil)
- pct_estoque_pronto — % estoque pronto
- giro_estoque_meses — Giro do estoque (meses)

**Landbank:**
- landbank_vgv — VGV do banco de terrenos (R$ milhões)
- landbank_empreendimentos — Qtd empreendimentos landbank
- landbank_unidades — Qtd unidades landbank
- landbank_preco_medio — Preço médio landbank (R$ mil)
- pct_permuta_total — % permuta total

**Backlog:**
- receitas_apropriar — Receitas a apropriar (R$ milhões)
- custo_apropriar — Custo a apropriar (R$ milhões)
- resultado_apropriar — Resultado a apropriar (R$ milhões)
- margem_apropriar — Margem a apropriar (%)

**DRE:**
- receita_bruta — Receita bruta (R$ milhões)
- receita_liquida — Receita líquida (R$ milhões)
- custo_imoveis_vendidos — Custo imóveis vendidos (R$ milhões)
- lucro_bruto — Lucro bruto (R$ milhões)
- margem_bruta — Margem bruta (%)
- lucro_bruto_ajustado — Lucro bruto ajustado (R$ milhões)
- margem_bruta_ajustada — Margem bruta ajustada (%)
- ebitda — EBITDA (R$ milhões)
- ebitda_ajustado — EBITDA ajustado (R$ milhões)
- margem_ebitda — Margem EBITDA (%)
- margem_ebitda_ajustada — Margem EBITDA ajustada (%)
- resultado_financeiro — Resultado financeiro (R$ milhões)
- receitas_financeiras — Receitas financeiras (R$ milhões)
- despesas_financeiras — Despesas financeiras (R$ milhões)
- lucro_liquido — Lucro líquido (R$ milhões)
- margem_liquida — Margem líquida (%)
- roe — ROE (%)

**SG&A:**
- despesas_comerciais — Despesas comerciais (R$ milhões)
- despesas_ga — Despesas G&A (R$ milhões)
- honorarios_administracao — Honorários administração (R$ milhões)
- outras_receitas_despesas_op — Outras receitas/despesas op (R$ milhões)
- equivalencia_patrimonial — Equivalência patrimonial (R$ milhões)
- total_despesas_operacionais — Total despesas operacionais (R$ milhões)
- pct_comerciais_receita_liquida — Desp. comerciais / RL (%)
- pct_ga_receita_liquida — G&A / RL (%)
- pct_sga_receita_liquida — SG&A / RL (%)

**Endividamento:**
- divida_bruta — Dívida bruta (R$ milhões)
- caixa_aplicacoes — Caixa e aplicações (R$ milhões)
- divida_liquida — Dívida líquida (R$ milhões)
- divida_liquida_pl — Dívida líquida / PL (%)
- patrimonio_liquido — Patrimônio líquido (R$ milhões)

**Geração de Caixa:**
- geracao_caixa — Geração de caixa (R$ milhões)

**Recebíveis:**
- carteira_recebiveis_total — Carteira total de recebíveis (R$ milhões)
- carteira_pre_chaves — Recebíveis pré-chaves (R$ milhões)
- carteira_pos_chaves — Recebíveis pós-chaves (R$ milhões)
- pdd_provisao — PDD provisão (R$ milhões)
- pdd_cobertura_pct — Cobertura PDD (%)
- inadimplencia_total_pct — Inadimplência total (%)

**Detalhamento Recebíveis/Dívida (campos adicionais):**
- aging_adimplente_pct, aging_vencido_90d_pct, aging_vencido_360d_pct, aging_vencido_360d_mais_pct
- cessao_recebiveis_trimestre, saldo_cessao_recebiveis
- carteira_pct_pl, carteira_pos_chaves_pct
- recebiveis_unidades_concluidas, recebiveis_unidades_construcao
- avp_recebiveis, taxa_avp_aa, provisao_distratos
- pro_soluto_saldo, pro_soluto_pct_carteira
- recebiveis_circulante, recebiveis_nao_circulante
- aging_vencido_90d, aging_vencido_180d, aging_vencido_180d_mais
- aging_a_vencer_12m, aging_a_vencer_24m, aging_a_vencer_36m, aging_a_vencer_36m_mais
- pecld_adicoes, pecld_reversoes, pecld_baixas, pecld_pct_receita_bruta
- cessao_passivo_total, cessao_pro_soluto, cessao_financ_direto, cessao_fundo_reserva
- cessao_taxa_media, cessao_num_operacoes
- divida_debentures, divida_cri, divida_sfh_producao
- divida_custo_medio_aa, divida_duration_meses
- divida_venc_12m, divida_venc_24m, divida_venc_24m_mais
- encargos_capitalizados
- receita_incorporacao_concluidas, receita_incorporacao_construcao
- receita_distratos, receita_pecld_deducao, poc_medio
- provisao_garantia_obra, provisao_riscos_civeis, provisao_riscos_trabalhistas, provisao_riscos_total
- receita_fin_aplicacoes, receita_fin_recebiveis
- despesa_fin_juros_divida, despesa_fin_cessao, despesa_fin_derivativos

### Tabela: log_ingestao
Metadados de ingestão (empresa, planilha, aba, registros_novos, registros_atualizados, timestamp).

### Observações importantes:
- Valores monetários em R$ milhões (exceto preço médio em R$ mil)
- Campos de % (margem_*, vso_*, pct_*, roe) já estão em formato percentual (ex: 35.2 = 35.2%)
- Segmento 'Consolidado' existe para todas as empresas
- Nem todos os campos estão preenchidos para todas as empresas/períodos
- Para comparações entre empresas, use segmento = 'Consolidado'
"""

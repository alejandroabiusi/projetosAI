import os
import pdfplumber

_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
pdf = pdfplumber.open(os.path.join(_BASE, 'cyrela', 'itr_dfp', 'Cyrela_ITR_1T2025.pdf'))

# ============================================================
# 1T2025 (31/03/2025) and 4T2024 (31/12/2024) data
# All values in R$ mil
# ============================================================

data_1t2025 = {}
data_4t2024 = {}

# ===========================================
# 1. RECEBIVEIS (from Balance Sheet pages 12-13 and Note 5 page 32/34)
# ===========================================

# From balance sheet (page 12) - Consolidado
data_1t2025['recebiveis_circulante'] = 3_770_590
data_4t2024['recebiveis_circulante'] = 3_700_652

data_1t2025['recebiveis_nao_circulante'] = 1_121_591
data_4t2024['recebiveis_nao_circulante'] = 1_076_047

# From Note 5 (page 32/34) - Consolidado
data_1t2025['recebiveis_unidades_concluidas'] = 978_133
data_4t2024['recebiveis_unidades_concluidas'] = 1_190_631

data_1t2025['recebiveis_unidades_construcao'] = 4_552_870
data_4t2024['recebiveis_unidades_construcao'] = 4_129_348

data_1t2025['avp_recebiveis'] = -251_416
data_4t2024['avp_recebiveis'] = -217_156

data_1t2025['pdd_provisao'] = -64_282
data_4t2024['pdd_provisao'] = -60_651

data_1t2025['provisao_distratos'] = -599_516
data_4t2024['provisao_distratos'] = -486_183

# ===========================================
# 2. AGING (Cronograma) - Note 5 page 35
# Consolidado - full receivable schedule (recognized + to recognize)
# ===========================================

data_1t2025['aging_a_vencer_12m'] = 5_322_991
data_4t2024['aging_a_vencer_12m'] = 5_356_965

data_1t2025['aging_a_vencer_24m'] = 3_808_769
data_4t2024['aging_a_vencer_24m'] = 3_268_255

data_1t2025['aging_a_vencer_36m'] = 3_129_550
data_4t2024['aging_a_vencer_36m'] = 2_827_208

# 48M + >48M combined
data_1t2025['aging_a_vencer_36m_mais'] = 1_376_461 + 625_672  # = 2_002_133
data_4t2024['aging_a_vencer_36m_mais'] = 1_587_443 + 483_985  # = 2_071_428

# ===========================================
# 3. PECLD Movement (1T2025) - Note 5 page 33
# Cyrela reports additions as POSITIVE
# ===========================================

data_1t2025['pecld_adicoes'] = 9_709
data_1t2025['pecld_reversoes'] = -5_708
data_1t2025['pecld_baixas'] = -370

# ===========================================
# 4. DIVIDA - from Balance Sheet pages 14-15 and Notes 10/11/12
# ===========================================

# Emprestimos (Note 10): 2.445.513
emprestimos_1t = 2_445_513
# Debentures (Note 11): 199.589
debentures_1t = 199_589
# CRI (Note 12 page 67): 3.372.356
cri_1t = 3_372_356

data_1t2025['divida_bruta'] = emprestimos_1t + debentures_1t + cri_1t  # = 6.017.458
data_1t2025['divida_debentures'] = debentures_1t
data_1t2025['divida_cri'] = cri_1t
data_1t2025['divida_sfh_producao'] = emprestimos_1t

# From BS: Circulante = 1.003.400, Nao Circulante = 5.014.058
data_1t2025['divida_venc_12m'] = 1_003_400
data_1t2025['divida_venc_24m_mais'] = 5_014_058

# 4T2024
emprestimos_4t = 2_342_780
debentures_4t = 205_434
cri_4t = 3_426_366
data_4t2024['divida_bruta'] = emprestimos_4t + debentures_4t + cri_4t
data_4t2024['divida_venc_12m'] = 1_005_064
data_4t2024['divida_venc_24m_mais'] = 4_969_516

# ===========================================
# 5. CAIXA = caixa_equivalentes + TVM (todas as aplicacoes financeiras)
# From BS page 12
# ===========================================

# 1T2025: Caixa=145.454 + Aplic CP=2.866.950 + Aplic LP(25.328+2.325.729+139.243=2.490.300)
data_1t2025['caixa_aplicacoes'] = 145_454 + 2_866_950 + 25_328 + 2_325_729 + 139_243  # = 5.502.704

# 4T2024: Caixa=531.729 + Aplic CP=2.520.865 + Aplic LP(25.004+2.108.990+122.468=2.256.462)
data_4t2024['caixa_aplicacoes'] = 531_729 + 2_520_865 + 25_004 + 2_108_990 + 122_468  # = 5.309.056

# ===========================================
# 6. PROVISOES - Note 17 (page 75) and Note 19 (page 77/79)
# ===========================================

# Note 17 - Provisao garantia obra (page 75)
# 1T2025: provisao garantia obra = 200.230, demais = 9.081, total = 209.311
# Circ=78.585, Nao Circ=130.726
data_1t2025['provisao_garantia_obra'] = 209_311
# 4T2024: provisao garantia = 191.964 + demais 13.383 = 205.347
# Circ=81.138, Nao Circ=124.209
data_4t2024['provisao_garantia_obra'] = 205_347

# Note 19 - Provisoes riscos (page 77/79)
data_1t2025['provisao_riscos_civeis'] = 134_030
data_4t2024['provisao_riscos_civeis'] = 128_518

data_1t2025['provisao_riscos_trabalhistas'] = 58_638
data_4t2024['provisao_riscos_trabalhistas'] = 64_613

data_1t2025['provisao_riscos_total'] = 200_600
data_4t2024['provisao_riscos_total'] = 199_057

# ===========================================
# 7. RESULTADO FINANCEIRO - TRIMESTRAL (01/01 a 31/03/2025)
# From Note 27 page 100 - Consolidado
# ===========================================

data_1t2025['receitas_financeiras'] = 211_618
data_1t2025['despesas_financeiras'] = -152_617
data_1t2025['resultado_financeiro'] = 59_001

data_1t2025['receita_fin_aplicacoes'] = 192_619

# Juros divida: SFH (-59.466) + emprestimos nacionais (-121.847) = -181.313
data_1t2025['despesa_fin_juros_divida'] = -181_313

data_1t2025['despesa_fin_derivativos'] = -6_099  # Perdas operacionais SWAP

data_1t2025['encargos_capitalizados'] = 41_655

# ===========================================
# OUTPUT
# ===========================================

print("=" * 70)
print("CYRELA ITR 1T2025 - DADOS EXTRAIDOS (CONSOLIDADO)")
print("Todos os valores em R$ mil")
print("=" * 70)

print("\n### 1T2025 (31/03/2025) ###\n")
for k, v in data_1t2025.items():
    print(f"  {k:40s} = {v:>15,}")

print("\n### 4T2024 (31/12/2024) - Comparativo ###\n")
for k, v in data_4t2024.items():
    print(f"  {k:40s} = {v:>15,}")

# Cross-checks
print("\n### VERIFICACOES ###")
rec_total = data_1t2025['recebiveis_circulante'] + data_1t2025['recebiveis_nao_circulante']
print(f"  Recebiveis total (circ+nao_circ) 1T2025: {rec_total:,}")
bruto = data_1t2025['recebiveis_unidades_concluidas'] + data_1t2025['recebiveis_unidades_construcao']
ajustes = data_1t2025['avp_recebiveis'] + data_1t2025['pdd_provisao'] + data_1t2025['provisao_distratos']
prestacao = 24_976
print(f"  Recebiveis bruto (concl+constr): {bruto:,}")
print(f"  {bruto:,} + ({ajustes:,}) + {prestacao:,} (prest.serv) = {bruto + ajustes + prestacao:,}")
print(f"  BS total recebiveis: {rec_total:,}")

divida_total_bs = data_1t2025['divida_venc_12m'] + data_1t2025['divida_venc_24m_mais']
print(f"  Divida bruta 1T2025: {data_1t2025['divida_bruta']:,}")
print(f"  Divida bruta = circ + nao_circ (BS): {divida_total_bs:,}")
print(f"  Caixa total 1T2025: {data_1t2025['caixa_aplicacoes']:,}")
print(f"  Divida liquida 1T2025: {data_1t2025['divida_bruta'] - data_1t2025['caixa_aplicacoes']:,}")

aging_total = (data_1t2025['aging_a_vencer_12m'] + data_1t2025['aging_a_vencer_24m'] +
               data_1t2025['aging_a_vencer_36m'] + data_1t2025['aging_a_vencer_36m_mais'])
print(f"  Aging total 1T2025: {aging_total:,}")

pdf.close()

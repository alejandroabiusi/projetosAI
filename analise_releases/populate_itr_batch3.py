"""
Batch 3 - ITR data population from 3T2023 and 3T2025 ITR PDFs.
All values from ITR are in R$ mil. Database stores R$ milhoes.
Conversion: value_in_mil / 1000 = value_in_milhoes
E.g., 1.348.006 R$ mil = 1348.006 R$ milhoes
"""
import sqlite3

conn = sqlite3.connect('dados_financeiros.db')
cur = conn.cursor()

def update(empresa, periodo, segmento, **fields):
    sets = ", ".join(f"{k} = ?" for k in fields.keys())
    vals = list(fields.values())
    vals.extend([empresa, periodo, segmento])
    cur.execute(
        f"UPDATE dados_trimestrais SET {sets} WHERE empresa=? AND periodo=? AND segmento=?",
        vals
    )
    return cur.rowcount

count = 0

# ============================================================
# DIRECIONAL 3T2025 (from ITR 3T2025, balance date 30/09/2025)
# ============================================================
n = update("Direcional", "3T2025", "Consolidado",
    recebiveis_circulante=1348.006,
    recebiveis_nao_circulante=1271.939,
    recebiveis_unidades_concluidas=761.421,
    recebiveis_unidades_construcao=2006.728,
    avp_recebiveis=-103.410,
    # PECLD
    pdd_provisao=-97.558,  # also stored in pdd_provisao for backward compat
    pecld_adicoes=-56.376,   # 9M2025 constituicao
    pecld_reversoes=16.460,  # 9M2025 reversao
    # Aging circulante
    aging_a_vencer_12m=1322.162,  # total a vencer circulante
    aging_vencido_90d=29.477,    # vencidos ate 1m + 1-2m + 2-3m = 12645+9630+7202
    aging_vencido_180d=16.797,   # vencidos 3-4m + 4-6m = 6142+10655
    aging_vencido_180d_mais=77.910,
    # Aging nao circulante
    aging_a_vencer_24m=609.515,  # entre 1 e 2 anos
    aging_a_vencer_36m=261.287,  # entre 2 e 3 anos
    aging_a_vencer_36m_mais=503.765,  # 139201+364564 (entre 3-4 + apos 4)
    # Divida
    divida_cri=2127.858,
    divida_sfh_producao=513.331,
    divida_bruta=2642.140,
    divida_venc_12m=238.164,     # curto prazo
    divida_venc_24m_mais=2341.473,  # longo prazo total
    # Caixa already populated (1575.523)
    # Provisoes
    provisao_garantia_obra=48.971,
    provisao_riscos_civeis=36.049,
    provisao_riscos_trabalhistas=3.660,
    provisao_riscos_total=39.713,  # tributario+trabalhista+civeis
    # Resultado financeiro (3T2025 trimestral)
    receitas_financeiras=119.310,
    despesas_financeiras=-92.014,
    resultado_financeiro=27.296,
    receita_fin_aplicacoes=73.294,  # rendimento aplicacoes
    despesa_fin_cessao=-8.286,     # correcao passivo cessao
    despesa_fin_derivativos=-25.967,
    encargos_capitalizados=-23.277,  # juros capitalizados 3T2025
)
print(f"Direcional 3T2025: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 4T2024 (comparative from ITR 3T2025, date 31/12/2024)
# ============================================================
n = update("Direcional", "4T2024", "Consolidado",
    recebiveis_circulante=992.821,
    recebiveis_nao_circulante=846.706,
    recebiveis_unidades_concluidas=406.704,
    recebiveis_unidades_construcao=1540.726,
    avp_recebiveis=-74.346,
    pdd_provisao=-54.627,
    # Aging circulante
    aging_a_vencer_12m=970.134,
    aging_vencido_90d=29.691,    # 19737+5096+4858
    aging_vencido_180d=9.817,    # 3778+6039
    aging_vencido_180d_mais=47.585,
    # Aging nao circulante
    aging_a_vencer_24m=382.339,
    aging_a_vencer_36m=191.628,
    aging_a_vencer_36m_mais=337.306,  # 96671+240635
    # Divida
    divida_cri=1175.926,
    divida_sfh_producao=400.994,
    divida_bruta=1578.127,
    divida_venc_12m=202.562,
    divida_venc_24m_mais=1347.099,
    # Provisoes
    provisao_garantia_obra=43.036,
    provisao_riscos_civeis=32.074,
    provisao_riscos_trabalhistas=2.242,
    provisao_riscos_total=34.319,
    # PECLD movement for FY2024
    pecld_adicoes=-35.108,
    pecld_reversoes=16.530,
)
print(f"Direcional 4T2024: {n} row(s)")
count += n

# ============================================================
# TENDA 3T2023 (from ITR 3T2023, balance date 30/09/2023)
# ============================================================
n = update("Tenda", "3T2023", "Consolidado",
    recebiveis_circulante=529.185,
    recebiveis_nao_circulante=668.444,
    avp_recebiveis=-95.825,
    pdd_provisao=-318.022,
    # Aging - Tenda uses different buckets
    aging_vencido_90d=30.972,      # vencidas ate 90 dias
    aging_vencido_180d=12.720,     # vencidas 91-180 dias
    aging_vencido_180d_mais=112.427,  # vencidas acima 180 dias
    aging_a_vencer_12m=709.999,    # a vencer 1 ano
    aging_a_vencer_24m=443.899,    # a vencer 2 anos
    aging_a_vencer_36m=112.410,    # a vencer 3 anos
    aging_a_vencer_36m_mais=195.725,  # 61122+134603 (4+5+ anos)
    # PECLD movement (9M2023)
    pecld_adicoes=-59.303,
    pecld_reversoes=13.646,
    pecld_baixas=0,
    # Divida
    divida_debentures=763.794,  # 133611+630183
    divida_venc_12m=441.570,   # curto prazo total
    divida_venc_24m_mais=759.378,  # longo prazo total
    divida_bruta=1200.948,     # 441570+759378
    # Caixa
    caixa_aplicacoes=748.158,  # 64165 caixa + 683993 TVM
    # Provisoes
    provisao_riscos_civeis=105.699,
    provisao_riscos_trabalhistas=16.830,
    provisao_riscos_total=145.384,
    # Cessao
    cessao_passivo_total=251.627,  # 45899+205728
    # Resultado financeiro (3T2023 trimestral)
    receitas_financeiras=19.282,
    despesas_financeiras=-47.196,
    resultado_financeiro=-27.914,
    receita_fin_aplicacoes=16.796,
    encargos_capitalizados=18.839,
)
print(f"Tenda 3T2023: {n} row(s)")
count += n

# Tenda 4T2022 comparative - already populated, but add missing fields
# Check first what's already there
cur.execute("SELECT cessao_passivo_total FROM dados_trimestrais WHERE empresa='Tenda' AND periodo='4T2022' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Tenda", "4T2022", "Consolidado",
        # Aging from 3T2023 ITR comparative (31/12/2022)
        aging_vencido_90d=22.120,
        aging_vencido_180d=8.821,
        aging_vencido_180d_mais=112.221,
        aging_a_vencer_12m=729.391,
        aging_a_vencer_24m=287.388,
        aging_a_vencer_36m=99.049,
        aging_a_vencer_36m_mais=135.006,  # 41811+93195
        cessao_passivo_total=0,
    )
    print(f"Tenda 4T2022 (aging update): {n} row(s)")
    count += n

# ============================================================
# CYRELA 3T2023 (from ITR 3T2023, balance date 30/09/2023)
# ============================================================
n = update("Cyrela", "3T2023", "Consolidado",
    recebiveis_circulante=2569.020,
    recebiveis_nao_circulante=629.874,
    recebiveis_unidades_concluidas=917.097,
    recebiveis_unidades_construcao=2743.610,
    avp_recebiveis=-80.437,
    pdd_provisao=-55.913,
    provisao_distratos=-333.304,
    # PECLD movement (9M2023)
    pecld_adicoes=29.207,
    pecld_reversoes=-21.521,
    pecld_baixas=-4.097,
    # Divida
    divida_debentures=989.332,   # debentures total
    divida_cri=1983.257,         # CRI total
    divida_bruta=4926.016,       # emprestimos+financ+debentures+CRI
    divida_venc_12m=1098.553,    # curto prazo
    divida_venc_24m_mais=3827.463,  # longo prazo
    # Caixa
    caixa_aplicacoes=4553.797,   # 256623 caixa + 4297174 aplicacoes
    # Provisoes
    provisao_garantia_obra=129.562,
    provisao_riscos_civeis=141.745,
    provisao_riscos_trabalhistas=77.223,
    provisao_riscos_total=234.083,
    # Resultado financeiro (3T2023 trimestral)
    receitas_financeiras=156.108,
    despesas_financeiras=-128.027,
    resultado_financeiro=28.081,
    receita_fin_aplicacoes=148.544,
    despesa_fin_juros_divida=-140.033,  # juros SFH + emprestimos nac. (41368+98665)
    despesa_fin_derivativos=-9.254,     # perdas SWAP
    encargos_capitalizados=31.929,
    # Aging - only partial: vencidas >90d
    aging_vencido_180d_mais=151.422,  # parcelas vencidas >90 dias (approximate)
)
print(f"Cyrela 3T2023: {n} row(s)")
count += n

# CYRELA 4T2022 comparative - update missing fields
cur.execute("SELECT provisao_garantia_obra FROM dados_trimestrais WHERE empresa='Cyrela' AND periodo='4T2022' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cyrela", "4T2022", "Consolidado",
        recebiveis_unidades_concluidas=831.133,
        recebiveis_unidades_construcao=2348.192,
        avp_recebiveis=-80.492,
        provisao_distratos=-343.440,
        divida_debentures=1070.246,
        divida_cri=1949.484,
        divida_bruta=4879.009,
        divida_venc_12m=1518.586,
        divida_venc_24m_mais=3360.423,
        caixa_aplicacoes=4615.752,  # 129013+4486739
        provisao_garantia_obra=111.906,
        provisao_riscos_civeis=136.508,
        provisao_riscos_trabalhistas=86.581,
        provisao_riscos_total=237.512,
    )
    print(f"Cyrela 4T2022 (extra fields): {n} row(s)")
    count += n

# ============================================================
# MRV 3T2023 (from ITR 3T2023, balance date 30/09/2023)
# ============================================================
n = update("MRV", "3T2023", "Consolidado",
    recebiveis_circulante=2500.086,
    recebiveis_nao_circulante=2203.547,
    avp_recebiveis=-210.933,
    pdd_provisao=-419.672,
    # PECLD movement (9M2023) - adicoes/reversoes nao segregadas
    pecld_adicoes=-317.906,  # liquido adicoes/reversoes
    pecld_baixas=103.339,
    # Divida
    divida_bruta=8040.984,
    divida_venc_12m=1363.024,     # curto prazo (excl. mantidos venda)
    divida_venc_24m_mais=6334.038,  # longo prazo
    # Caixa
    caixa_aplicacoes=740.097,
    # Provisoes
    provisao_garantia_obra=268.840,
    provisao_riscos_civeis=51.124,
    provisao_riscos_trabalhistas=43.219,
    provisao_riscos_total=94.790,
    # Cessao
    cessao_passivo_total=1431.822,  # 385697+1046125
    # Resultado financeiro (3T2023 trimestral)
    receitas_financeiras=61.235,
    despesas_financeiras=-211.327,
    resultado_financeiro=-128.788,  # inclui receita clientes incorp
    receita_fin_aplicacoes=49.819,
    receita_fin_recebiveis=21.304,  # receita proveniente clientes incorp
    despesa_fin_juros_divida=-72.127,
    despesa_fin_cessao=-38.738,
    despesa_fin_derivativos=-80.667,
    encargos_capitalizados=204.642,  # total capitalizado (imoveis+PPI)
    # Aging - expectativa recebimento
    aging_a_vencer_12m=3441.465,
    aging_a_vencer_24m=1621.264,
    aging_a_vencer_36m=701.445,
    aging_a_vencer_36m_mais=1223.814,  # 424968+798846 (37-48m + >48m)
)
print(f"MRV 3T2023: {n} row(s)")
count += n

# MRV 4T2022 comparative - update missing fields
cur.execute("SELECT provisao_garantia_obra FROM dados_trimestrais WHERE empresa='MRV' AND periodo='4T2022' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("MRV", "4T2022", "Consolidado",
        avp_recebiveis=-196.104,
        divida_bruta=7429.176,
        divida_venc_12m=1148.232,
        divida_venc_24m_mais=6280.944,
        caixa_aplicacoes=733.748,
        provisao_garantia_obra=243.841,
        provisao_riscos_civeis=33.088,
        provisao_riscos_trabalhistas=39.348,
        provisao_riscos_total=72.829,
        cessao_passivo_total=357.606,
        aging_a_vencer_12m=3260.293,
        aging_a_vencer_24m=1250.572,
        aging_a_vencer_36m=418.394,
        aging_a_vencer_36m_mais=512.375,  # 275004+237371
    )
    print(f"MRV 4T2022 (extra fields): {n} row(s)")
    count += n

# ============================================================
# CURY 3T2023 (from ITR 3T2023, balance date 30/09/2023)
# ============================================================
n = update("Cury", "3T2023", "Consolidado",
    recebiveis_circulante=489.221,
    recebiveis_nao_circulante=591.170,
    recebiveis_unidades_concluidas=257.403,
    recebiveis_unidades_construcao=955.930,
    avp_recebiveis=-4.963,
    pdd_provisao=-88.029,
    # PECLD movement (9M2023)
    pecld_adicoes=-26.494,
    pecld_reversoes=0.682,
    pecld_baixas=12.838,
    # Aging - Cury uses different faixas
    aging_a_vencer_12m=514.348,     # 1-360 dias (closest to 12m)
    aging_a_vencer_24m=212.927,     # 361-720 dias
    aging_a_vencer_36m_mais=378.243,  # >721 dias
    aging_vencido_90d=18.593,       # 1-30d + 31-90d = 8871+9722
    aging_vencido_180d_mais=90.645, # >90 dias
    # Divida
    divida_venc_12m=134.940,   # curto prazo
    divida_venc_24m_mais=510.779,  # longo prazo
    divida_bruta=645.719,      # 134940+510779
    # Caixa
    caixa_aplicacoes=1109.075,  # 730072+379003
    # Provisoes
    provisao_garantia_obra=30.803,
    provisao_riscos_civeis=5.285,
    provisao_riscos_trabalhistas=15.052,
    provisao_riscos_total=20.337,
    # Resultado financeiro (3T2023 trimestral)
    receitas_financeiras=19.668,
    despesas_financeiras=-23.214,
    resultado_financeiro=-3.546,
    receita_fin_aplicacoes=17.410,
    despesa_fin_juros_divida=-15.492,
    despesa_fin_derivativos=-0.616,
    encargos_capitalizados=-1.489,
)
print(f"Cury 3T2023: {n} row(s)")
count += n

# CURY 4T2022 comparative - update missing fields
cur.execute("SELECT provisao_garantia_obra FROM dados_trimestrais WHERE empresa='Cury' AND periodo='4T2022' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cury", "4T2022", "Consolidado",
        recebiveis_unidades_concluidas=235.844,
        recebiveis_unidades_construcao=1010.426,
        avp_recebiveis=-3.551,
        aging_a_vencer_12m=667.725,
        aging_a_vencer_24m=204.519,
        aging_a_vencer_36m_mais=293.207,
        aging_vencido_90d=49.458,  # 34347+15111
        aging_vencido_180d_mais=83.715,
        divida_venc_12m=120.906,
        divida_venc_24m_mais=360.082,
        divida_bruta=480.988,
        caixa_aplicacoes=789.426,
        provisao_garantia_obra=21.389,
        provisao_riscos_civeis=4.560,
        provisao_riscos_trabalhistas=14.309,
        provisao_riscos_total=18.868,
    )
    print(f"Cury 4T2022 (extra fields): {n} row(s)")
    count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

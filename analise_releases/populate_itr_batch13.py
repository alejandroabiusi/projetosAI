"""
Batch 13 - ITR data from 1T2020 (Cyrela, Direcional), 1T2025 (all 5), 2T2025 (all 5), Cyrela 4T2024.
All values in R$ mil = R$ milhoes as float (agent R$ mil integers / 1000).
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
# CYRELA 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("Cyrela", "1T2020", "Consolidado",
    recebiveis_circulante=1329.078,
    recebiveis_nao_circulante=735.266,
    recebiveis_unidades_concluidas=933.180,
    recebiveis_unidades_construcao=1490.806,
    avp_recebiveis=-35.875,
    pdd_provisao=-25.546,
    provisao_distratos=-307.098,
    pecld_adicoes=6.014,
    pecld_reversoes=-0.666,
    pecld_baixas=-0.269,
    aging_a_vencer_12m=2159.017,
    aging_a_vencer_24m=1290.532,
    aging_a_vencer_36m=1357.763,
    aging_a_vencer_36m_mais=235.696,
    divida_debentures=156.584,
    divida_cri=1382.702,
    divida_sfh_producao=410.379,
    divida_bruta=2513.799,
    divida_venc_12m=569.199,
    divida_venc_24m_mais=1944.600,
    caixa_aplicacoes=1672.370,
    provisao_garantia_obra=101.337,
    provisao_riscos_civeis=90.929,
    provisao_riscos_trabalhistas=78.987,
    provisao_riscos_total=174.098,
    receitas_financeiras=31.141,
    despesas_financeiras=-27.890,
    resultado_financeiro=3.251,
    receita_fin_aplicacoes=14.463,
    despesa_fin_juros_divida=-23.400,
    encargos_capitalizados=0.939,
)
print(f"Cyrela 1T2020: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("Direcional", "1T2020", "Consolidado",
    recebiveis_circulante=294.958,
    recebiveis_nao_circulante=114.990,
    recebiveis_unidades_concluidas=196.734,
    recebiveis_unidades_construcao=205.573,
    avp_recebiveis=-3.313,
    pdd_provisao=-21.193,
    pecld_adicoes=-4.265,
    aging_a_vencer_12m=265.494,
    aging_vencido_90d=3.961,
    aging_vencido_180d=9.343,
    aging_vencido_180d_mais=16.160,
    aging_a_vencer_24m=53.998,
    aging_a_vencer_36m=44.192,
    aging_a_vencer_36m_mais=16.800,
    divida_cri=776.561,
    divida_sfh_producao=77.488,
    divida_bruta=913.062,
    divida_venc_12m=244.080,
    divida_venc_24m_mais=668.982,
    caixa_aplicacoes=798.028,
    provisao_garantia_obra=36.984,
    provisao_riscos_civeis=17.991,
    provisao_riscos_trabalhistas=11.080,
    provisao_riscos_total=29.177,
    receitas_financeiras=9.304,
    despesas_financeiras=-19.997,
    resultado_financeiro=-10.693,
    receita_fin_aplicacoes=5.472,
    despesa_fin_juros_divida=-15.588,
)
print(f"Direcional 1T2020: {n} row(s)")
count += n

# ============================================================
# TENDA 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("Tenda", "1T2025", "Consolidado",
    recebiveis_circulante=909.376,
    recebiveis_nao_circulante=662.157,
    avp_recebiveis=-150.226,
    pdd_provisao=-508.520,
    aging_vencido_90d=77.379,
    aging_vencido_180d=26.038,
    aging_vencido_180d_mais=149.683,
    aging_a_vencer_12m=959.216,
    aging_a_vencer_24m=619.855,
    aging_a_vencer_36m=205.993,
    aging_a_vencer_36m_mais=230.031,
    pecld_adicoes=-39.821,
    pecld_reversoes=3.994,
    divida_bruta=849.051,
    divida_debentures=562.259,
    divida_venc_12m=246.567,
    divida_venc_24m_mais=602.484,
    caixa_aplicacoes=581.501,
    provisao_riscos_civeis=106.000,
    provisao_riscos_trabalhistas=28.023,
    provisao_riscos_total=134.939,
    cessao_passivo_total=450.151,
    receitas_financeiras=14.096,
    despesas_financeiras=-35.594,
    resultado_financeiro=-21.498,
    encargos_capitalizados=19.835,
)
print(f"Tenda 1T2025: {n} row(s)")
count += n

# ============================================================
# CURY 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("Cury", "1T2025", "Consolidado",
    recebiveis_circulante=602.208,
    recebiveis_nao_circulante=1294.500,
    recebiveis_unidades_concluidas=277.957,
    recebiveis_unidades_construcao=1803.477,
    avp_recebiveis=-44.017,
    pdd_provisao=-154.275,
    aging_a_vencer_12m=682.885,
    aging_a_vencer_24m=345.681,
    aging_a_vencer_36m_mais=948.819,
    aging_vencido_90d=34.185,
    aging_vencido_180d_mais=125.347,
    pecld_adicoes=-23.362,
    pecld_reversoes=1.224,
    divida_bruta=1282.811,
    divida_venc_12m=132.078,
    divida_venc_24m_mais=1150.733,
    caixa_aplicacoes=1543.806,
    provisao_garantia_obra=67.925,
    provisao_riscos_civeis=5.179,
    provisao_riscos_trabalhistas=25.298,
    provisao_riscos_total=30.477,
    receitas_financeiras=36.752,
    despesas_financeiras=-51.332,
    resultado_financeiro=-14.580,
)
print(f"Cury 1T2025: {n} row(s)")
count += n

# ============================================================
# MRV 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("MRV", "1T2025", "Consolidado",
    recebiveis_circulante=3317.032,
    recebiveis_nao_circulante=3226.482,
    avp_recebiveis=-489.322,
    pdd_provisao=-489.322,
    pecld_adicoes=-133.625,
    pecld_reversoes=74.059,
    pecld_baixas=41.807,
    aging_a_vencer_12m=5512.007,
    aging_a_vencer_24m=2184.818,
    aging_a_vencer_36m=1065.213,
    aging_a_vencer_36m_mais=2219.623,
    divida_bruta=9833.032,
    divida_cri=3057.103,
    divida_venc_12m=3932.444,
    divida_venc_24m_mais=5992.608,
    caixa_aplicacoes=3515.056,
    provisao_garantia_obra=311.276,
    provisao_riscos_civeis=60.452,
    provisao_riscos_trabalhistas=51.334,
    provisao_riscos_total=112.799,
    cessao_passivo_total=3945.664,
    receitas_financeiras=86.382,
    despesas_financeiras=-435.404,
    resultado_financeiro=-306.912,
    receita_fin_recebiveis=42.110,
    despesa_fin_juros_divida=-159.125,
    despesa_fin_cessao=-239.274,
    despesa_fin_derivativos=-5.448,
    encargos_capitalizados=141.190,
)
print(f"MRV 1T2025: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("Direcional", "1T2025", "Consolidado",
    recebiveis_circulante=1106.026,
    recebiveis_nao_circulante=987.445,
    recebiveis_unidades_concluidas=533.597,
    recebiveis_unidades_construcao=1677.470,
    avp_recebiveis=-78.508,
    pdd_provisao=-66.877,
    pecld_adicoes=-16.328,
    pecld_reversoes=4.078,
    aging_a_vencer_12m=1077.828,
    aging_vencido_90d=21.380,
    aging_vencido_180d=24.336,
    aging_vencido_180d_mais=53.964,
    aging_a_vencer_24m=475.056,
    aging_a_vencer_36m=207.954,
    aging_a_vencer_36m_mais=378.338,
    divida_cri=1524.388,
    divida_sfh_producao=410.878,
    divida_bruta=1894.094,
    divida_venc_12m=192.587,
    divida_venc_24m_mais=1584.546,
    caixa_aplicacoes=1732.220,
    provisao_garantia_obra=45.161,
    provisao_riscos_civeis=33.985,
    provisao_riscos_trabalhistas=3.265,
    provisao_riscos_total=37.254,
    receitas_financeiras=77.401,
    despesas_financeiras=-61.290,
    resultado_financeiro=16.111,
    receita_fin_aplicacoes=38.055,
    despesa_fin_juros_divida=-29.496,
    despesa_fin_cessao=-3.364,
    despesa_fin_derivativos=-9.407,
)
print(f"Direcional 1T2025: {n} row(s)")
count += n

# ============================================================
# TENDA 2T2025 (balance date 30/06/2025)
# ============================================================
n = update("Tenda", "2T2025", "Consolidado",
    recebiveis_circulante=980.723,
    recebiveis_nao_circulante=709.014,
    avp_recebiveis=-153.152,
    pdd_provisao=-536.621,
    aging_vencido_90d=77.029,
    aging_vencido_180d=42.639,
    aging_vencido_180d_mais=161.348,
    aging_a_vencer_12m=1019.562,
    aging_a_vencer_24m=694.608,
    aging_a_vencer_36m=181.161,
    aging_a_vencer_36m_mais=239.137,
    pecld_adicoes=-92.645,
    pecld_reversoes=28.717,
    divida_bruta=1076.992,
    divida_debentures=712.519,
    divida_venc_12m=141.050,
    divida_venc_24m_mais=935.942,
    caixa_aplicacoes=761.206,
    provisao_riscos_civeis=102.905,
    provisao_riscos_trabalhistas=27.210,
    provisao_riscos_total=131.018,
    cessao_passivo_total=581.736,
    receitas_financeiras=142.928,
    despesas_financeiras=-49.458,
    resultado_financeiro=93.470,
    encargos_capitalizados=-22.419,
)
print(f"Tenda 2T2025: {n} row(s)")
count += n

# ============================================================
# CURY 2T2025 (balance date 30/06/2025)
# ============================================================
n = update("Cury", "2T2025", "Consolidado",
    recebiveis_circulante=552.497,
    recebiveis_nao_circulante=1581.210,
    recebiveis_unidades_concluidas=274.995,
    recebiveis_unidades_construcao=2057.069,
    avp_recebiveis=-49.818,
    pdd_provisao=-163.842,
    aging_a_vencer_12m=651.976,
    aging_a_vencer_24m=510.676,
    aging_a_vencer_36m_mais=1070.534,
    aging_vencido_90d=38.735,
    aging_vencido_180d_mais=122.185,
    pecld_adicoes=-45.592,
    pecld_reversoes=4.686,
    pecld_baixas=9.201,
    divida_bruta=1303.112,
    divida_venc_12m=156.104,
    divida_venc_24m_mais=1147.008,
    caixa_aplicacoes=1530.875,
    provisao_garantia_obra=76.096,
    provisao_riscos_civeis=4.048,
    provisao_riscos_trabalhistas=30.976,
    provisao_riscos_total=35.024,
    receitas_financeiras=35.218,
    despesas_financeiras=-50.846,
    resultado_financeiro=-15.628,
)
print(f"Cury 2T2025: {n} row(s)")
count += n

# ============================================================
# MRV 2T2025 (balance date 30/06/2025)
# ============================================================
n = update("MRV", "2T2025", "Consolidado",
    recebiveis_circulante=3514.437,
    recebiveis_nao_circulante=3298.006,
    avp_recebiveis=-518.792,
    pdd_provisao=-499.558,
    pecld_adicoes=-249.913,
    pecld_reversoes=136.055,
    pecld_baixas=85.863,
    aging_a_vencer_12m=5845.999,
    aging_a_vencer_24m=2152.537,
    aging_a_vencer_36m=1121.763,
    aging_a_vencer_36m_mais=2347.740,
    divida_bruta=9281.355,
    divida_cri=3076.886,
    divida_venc_12m=3786.637,
    divida_venc_24m_mais=5581.636,
    caixa_aplicacoes=3130.823,
    provisao_garantia_obra=323.914,
    provisao_riscos_civeis=63.017,
    provisao_riscos_trabalhistas=51.845,
    provisao_riscos_total=115.899,
    cessao_passivo_total=4044.603,
    receitas_financeiras=148.954,
    despesas_financeiras=-394.340,
    resultado_financeiro=-245.386,
    receita_fin_recebiveis=55.605,
    despesa_fin_juros_divida=-148.625,
    despesa_fin_cessao=-251.888,
    despesa_fin_derivativos=38.063,
    encargos_capitalizados=149.340,
)
print(f"MRV 2T2025: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2025 (balance date 30/06/2025)
# ============================================================
n = update("Direcional", "2T2025", "Consolidado",
    recebiveis_circulante=1280.505,
    recebiveis_nao_circulante=1104.148,
    recebiveis_unidades_concluidas=655.458,
    recebiveis_unidades_construcao=1876.342,
    avp_recebiveis=-89.311,
    pdd_provisao=-80.241,
    pecld_adicoes=-30.519,
    pecld_reversoes=6.253,
    aging_a_vencer_12m=1249.005,
    aging_vencido_90d=26.751,
    aging_vencido_180d=13.893,
    aging_vencido_180d_mais=73.494,
    aging_a_vencer_24m=533.395,
    aging_a_vencer_36m=227.833,
    aging_a_vencer_36m_mais=429.834,
    divida_cri=1523.035,
    divida_sfh_producao=502.752,
    divida_bruta=2026.900,
    divida_venc_12m=252.391,
    divida_venc_24m_mais=1774.509,
    caixa_aplicacoes=2274.403,
    provisao_garantia_obra=46.546,
    provisao_riscos_civeis=34.660,
    provisao_riscos_trabalhistas=3.652,
    provisao_riscos_total=38.316,
    receitas_financeiras=98.161,
    despesas_financeiras=-79.992,
    resultado_financeiro=18.169,
    receita_fin_aplicacoes=58.738,
    despesa_fin_juros_divida=-15.168,
    despesa_fin_cessao=-17.164,
    despesa_fin_derivativos=-4.592,
)
print(f"Direcional 2T2025: {n} row(s)")
count += n

# ============================================================
# CYRELA 2T2025 (balance date 30/06/2025)
# ============================================================
n = update("Cyrela", "2T2025", "Consolidado",
    recebiveis_circulante=3949.072,
    recebiveis_nao_circulante=1340.427,
    recebiveis_unidades_concluidas=1018.027,
    recebiveis_unidades_construcao=4851.201,
    avp_recebiveis=-257.383,
    pdd_provisao=-64.381,
    provisao_distratos=-525.571,
    pecld_adicoes=15.540,
    pecld_reversoes=-10.883,
    pecld_baixas=-0.927,
    aging_a_vencer_12m=5926.046,
    aging_a_vencer_24m=3652.453,
    aging_a_vencer_36m=3182.348,
    aging_a_vencer_36m_mais=2336.351,
    divida_cri=4170.961,
    divida_sfh_producao=2583.421,
    divida_bruta=6889.686,
    divida_venc_12m=1194.126,
    divida_venc_24m_mais=5695.560,
    caixa_aplicacoes=6081.348,
    provisao_garantia_obra=205.732,
    provisao_riscos_civeis=120.900,
    provisao_riscos_trabalhistas=52.309,
    provisao_riscos_total=181.676,
    receitas_financeiras=252.543,
    despesas_financeiras=-186.107,
    resultado_financeiro=66.436,
    receita_fin_aplicacoes=235.506,
    despesa_fin_juros_divida=-205.930,
    despesa_fin_derivativos=-13.649,
    encargos_capitalizados=49.081,
)
print(f"Cyrela 2T2025: {n} row(s)")
count += n

# ============================================================
# CYRELA 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("Cyrela", "1T2025", "Consolidado",
    recebiveis_circulante=3770.590,
    recebiveis_nao_circulante=1121.591,
    recebiveis_unidades_concluidas=978.133,
    recebiveis_unidades_construcao=4552.870,
    avp_recebiveis=-251.416,
    pdd_provisao=-64.282,
    provisao_distratos=-599.516,
    pecld_adicoes=9.709,
    pecld_reversoes=-5.708,
    pecld_baixas=-0.370,
    aging_a_vencer_12m=5322.991,
    aging_a_vencer_24m=3808.769,
    aging_a_vencer_36m=3129.550,
    aging_a_vencer_36m_mais=2002.133,
    divida_debentures=199.589,
    divida_cri=3372.356,
    divida_sfh_producao=2445.513,
    divida_bruta=6017.458,
    divida_venc_12m=1003.400,
    divida_venc_24m_mais=5014.058,
    caixa_aplicacoes=5502.704,
    provisao_garantia_obra=209.311,
    provisao_riscos_civeis=134.030,
    provisao_riscos_trabalhistas=58.638,
    provisao_riscos_total=200.600,
    receitas_financeiras=211.618,
    despesas_financeiras=-152.617,
    resultado_financeiro=59.001,
    receita_fin_aplicacoes=192.619,
    despesa_fin_juros_divida=-181.313,
    despesa_fin_derivativos=-6.099,
    encargos_capitalizados=41.655,
)
print(f"Cyrela 1T2025: {n} row(s)")
count += n

# Cyrela 4T2024 comparative
cur.execute("SELECT recebiveis_circulante FROM dados_trimestrais WHERE empresa='Cyrela' AND periodo='4T2024' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cyrela", "4T2024", "Consolidado",
        recebiveis_circulante=3700.652,
        recebiveis_nao_circulante=1076.047,
        recebiveis_unidades_concluidas=1190.631,
        recebiveis_unidades_construcao=4129.348,
        avp_recebiveis=-217.156,
        pdd_provisao=-60.651,
        provisao_distratos=-486.183,
        aging_a_vencer_12m=5356.965,
        aging_a_vencer_24m=3268.255,
        aging_a_vencer_36m=2827.208,
        aging_a_vencer_36m_mais=2071.428,
        divida_bruta=5974.580,
        divida_venc_12m=1005.064,
        divida_venc_24m_mais=4969.516,
        caixa_aplicacoes=5309.056,
        provisao_garantia_obra=205.347,
        provisao_riscos_civeis=128.518,
        provisao_riscos_trabalhistas=64.613,
        provisao_riscos_total=199.057,
    )
    print(f"Cyrela 4T2024 (comparative): {n} row(s)")
    count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

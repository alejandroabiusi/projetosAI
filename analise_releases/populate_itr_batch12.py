"""
Batch 12 - ITR data from 2T2024 + Cyrela 3T2020 + Cury 1T2020 + MRV 1T2020.
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
# TENDA 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("Tenda", "2T2024", "Consolidado",
    recebiveis_circulante=556.375,
    recebiveis_nao_circulante=763.378,
    avp_recebiveis=-102.315,
    pdd_provisao=-425.410,
    aging_vencido_90d=43.512,
    aging_vencido_180d=16.748,
    aging_vencido_180d_mais=127.498,
    aging_a_vencer_12m=812.145,
    aging_a_vencer_24m=482.558,
    aging_a_vencer_36m=159.210,
    aging_a_vencer_36m_mais=217.999,
    pecld_adicoes=-83.601,
    pecld_reversoes=5.087,
    divida_bruta=1105.690,
    divida_debentures=699.837,
    divida_venc_12m=383.508,
    divida_venc_24m_mais=722.181,
    caixa_aplicacoes=721.903,
    provisao_riscos_civeis=103.150,
    provisao_riscos_trabalhistas=21.312,
    provisao_riscos_total=127.582,
    cessao_passivo_total=352.011,
    receitas_financeiras=13.469,
    despesas_financeiras=-68.009,
    resultado_financeiro=-54.540,
    encargos_capitalizados=-17.133,
)
print(f"Tenda 2T2024: {n} row(s)")
count += n

# ============================================================
# CURY 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("Cury", "2T2024", "Consolidado",
    recebiveis_circulante=494.497,
    recebiveis_nao_circulante=884.755,
    recebiveis_unidades_concluidas=261.875,
    recebiveis_unidades_construcao=1254.089,
    avp_recebiveis=-18.000,
    pdd_provisao=-108.846,
    aging_a_vencer_12m=519.682,
    aging_a_vencer_24m=250.202,
    aging_a_vencer_36m_mais=634.553,
    aging_vencido_90d=28.670,
    aging_vencido_180d_mais=109.871,
    pecld_adicoes=-25.974,
    pecld_reversoes=3.267,
    pecld_baixas=6.360,
    divida_bruta=1080.738,
    divida_venc_12m=120.030,
    divida_venc_24m_mais=960.708,
    caixa_aplicacoes=1558.020,
    provisao_garantia_obra=48.724,
    provisao_riscos_civeis=5.362,
    provisao_riscos_trabalhistas=22.339,
    provisao_riscos_total=27.701,
    receitas_financeiras=25.350,
    despesas_financeiras=-31.099,
    resultado_financeiro=-5.749,
)
print(f"Cury 2T2024: {n} row(s)")
count += n

# ============================================================
# MRV 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("MRV", "2T2024", "Consolidado",
    recebiveis_circulante=2972.985,
    recebiveis_nao_circulante=2829.536,
    avp_recebiveis=-420.781,
    pdd_provisao=-424.503,
    pecld_adicoes=-220.826,
    pecld_reversoes=108.565,
    pecld_baixas=101.162,
    aging_a_vencer_12m=4470.685,
    aging_a_vencer_24m=1972.525,
    aging_a_vencer_36m=986.912,
    aging_a_vencer_36m_mais=1762.943,
    divida_bruta=9147.067,
    divida_cri=2309.986,
    divida_venc_12m=3162.278,
    divida_venc_24m_mais=5984.789,
    caixa_aplicacoes=2915.542,
    provisao_garantia_obra=301.814,
    provisao_riscos_civeis=64.580,
    provisao_riscos_trabalhistas=50.003,
    provisao_riscos_total=115.310,
    cessao_passivo_total=2974.505,
    receitas_financeiras=77.983,
    despesas_financeiras=-302.904,
    resultado_financeiro=-194.042,
    receita_fin_recebiveis=30.879,
    despesa_fin_juros_divida=-79.636,
    despesa_fin_cessao=-113.319,
    despesa_fin_derivativos=-101.025,
    encargos_capitalizados=148.202,
)
print(f"MRV 2T2024: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("Direcional", "2T2024", "Consolidado",
    recebiveis_circulante=789.184,
    recebiveis_nao_circulante=658.010,
    recebiveis_unidades_concluidas=337.597,
    recebiveis_unidades_construcao=1191.511,
    avp_recebiveis=-59.365,
    pdd_provisao=-44.473,
    pecld_adicoes=-12.841,
    pecld_reversoes=4.394,
    aging_a_vencer_12m=718.514,
    aging_vencido_90d=21.064,
    aging_vencido_180d=7.014,
    aging_vencido_180d_mais=42.592,
    aging_a_vencer_24m=294.715,
    aging_a_vencer_36m=127.163,
    aging_a_vencer_36m_mais=236.132,
    divida_cri=1006.026,
    divida_sfh_producao=250.004,
    divida_debentures=102.423,
    divida_bruta=1361.336,
    divida_venc_12m=248.566,
    divida_venc_24m_mais=1088.924,
    caixa_aplicacoes=1476.201,
    provisao_garantia_obra=39.437,
    provisao_riscos_civeis=26.094,
    provisao_riscos_trabalhistas=2.455,
    provisao_riscos_total=28.554,
    receitas_financeiras=69.213,
    despesas_financeiras=-65.707,
    resultado_financeiro=3.506,
    receita_fin_aplicacoes=25.912,
    despesa_fin_juros_divida=-24.711,
    despesa_fin_cessao=-1.260,
    despesa_fin_derivativos=-30.703,
)
print(f"Direcional 2T2024: {n} row(s)")
count += n

# ============================================================
# CYRELA 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("Cyrela", "2T2024", "Consolidado",
    recebiveis_circulante=3150.670,
    recebiveis_nao_circulante=842.387,
    recebiveis_unidades_concluidas=966.961,
    recebiveis_unidades_construcao=3461.095,
    avp_recebiveis=-124.522,
    pdd_provisao=-58.288,
    provisao_distratos=-401.830,
    pecld_adicoes=13.545,
    pecld_reversoes=-9.289,
    pecld_baixas=-1.762,
    aging_a_vencer_12m=4501.100,
    aging_a_vencer_24m=3126.659,
    aging_a_vencer_36m=2595.927,
    aging_a_vencer_36m_mais=870.212,
    divida_debentures=205.327,
    divida_cri=3180.542,
    divida_sfh_producao=1865.494,
    divida_bruta=5251.363,
    divida_venc_12m=894.908,
    divida_venc_24m_mais=4356.455,
    caixa_aplicacoes=4835.215,
    provisao_garantia_obra=167.547,
    provisao_riscos_civeis=136.146,
    provisao_riscos_trabalhistas=88.438,
    provisao_riscos_total=231.303,
    receitas_financeiras=182.301,
    despesas_financeiras=-137.606,
    resultado_financeiro=44.695,
    receita_fin_aplicacoes=167.838,
    despesa_fin_juros_divida=-90.898,
    despesa_fin_derivativos=-19.842,
    encargos_capitalizados=35.072,
)
print(f"Cyrela 2T2024: {n} row(s)")
count += n

# ============================================================
# CYRELA 3T2020 (balance date 30/09/2020)
# ============================================================
n = update("Cyrela", "3T2020", "Consolidado",
    recebiveis_circulante=1339.500,
    recebiveis_nao_circulante=727.337,
    recebiveis_unidades_concluidas=845.884,
    recebiveis_unidades_construcao=1500.047,
    avp_recebiveis=-17.787,
    pdd_provisao=-21.519,
    provisao_distratos=-265.723,
    pecld_adicoes=12.268,
    pecld_reversoes=-4.807,
    pecld_baixas=-6.409,
    aging_a_vencer_12m=1866.178,
    aging_a_vencer_24m=1581.834,
    aging_a_vencer_36m=1230.113,
    aging_a_vencer_36m_mais=380.198,
    divida_debentures=5.712,
    divida_cri=1324.722,
    divida_sfh_producao=108.440,
    divida_bruta=2434.570,
    divida_venc_12m=582.252,
    divida_venc_24m_mais=1852.318,
    caixa_aplicacoes=2396.197,
    provisao_garantia_obra=83.881,
    provisao_riscos_civeis=95.651,
    provisao_riscos_trabalhistas=84.059,
    provisao_riscos_total=187.768,
    receitas_financeiras=55.454,
    despesas_financeiras=-30.983,
    resultado_financeiro=24.471,
    receita_fin_aplicacoes=42.185,
    despesa_fin_juros_divida=-21.212,
    encargos_capitalizados=3.783,
)
print(f"Cyrela 3T2020: {n} row(s)")
count += n

# ============================================================
# CURY 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("Cury", "1T2020", "Consolidado",
    recebiveis_circulante=448.111,
    recebiveis_nao_circulante=157.343,
    recebiveis_unidades_concluidas=184.296,
    recebiveis_unidades_construcao=396.773,
    avp_recebiveis=-0.640,
    pdd_provisao=-53.416,
    aging_a_vencer_12m=306.453,
    aging_a_vencer_24m=45.344,
    aging_a_vencer_36m_mais=112.000,
    aging_vencido_90d=28.914,
    aging_vencido_180d_mais=90.777,
    divida_bruta=295.331,
    divida_venc_12m=60.662,
    divida_venc_24m_mais=234.669,
    caixa_aplicacoes=325.528,
    provisao_garantia_obra=7.309,
    provisao_riscos_civeis=8.763,
    provisao_riscos_trabalhistas=17.258,
    provisao_riscos_total=26.021,
    receitas_financeiras=2.784,
    despesas_financeiras=-4.959,
    resultado_financeiro=-2.175,
)
print(f"Cury 1T2020: {n} row(s)")
count += n

# ============================================================
# MRV 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("MRV", "1T2020", "Consolidado",
    recebiveis_circulante=1738.438,
    recebiveis_nao_circulante=1336.484,
    avp_recebiveis=-63.570,
    pdd_provisao=-235.155,
    pecld_adicoes=-73.733,
    pecld_reversoes=33.337,
    pecld_baixas=28.532,
    aging_a_vencer_12m=2661.105,
    aging_a_vencer_24m=1552.427,
    aging_a_vencer_36m=349.175,
    aging_a_vencer_36m_mais=356.316,
    divida_bruta=4100.979,
    divida_cri=840.807,
    divida_venc_12m=751.365,
    divida_venc_24m_mais=3349.614,
    caixa_aplicacoes=1925.423,
    provisao_garantia_obra=145.139,
    provisao_riscos_civeis=58.609,
    provisao_riscos_trabalhistas=38.573,
    provisao_riscos_total=97.562,
    receitas_financeiras=40.463,
    despesas_financeiras=-16.074,
    resultado_financeiro=24.389,
    receita_fin_recebiveis=26.685,
    despesa_fin_juros_divida=-15.237,
    encargos_capitalizados=38.391,
)
print(f"MRV 1T2020: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

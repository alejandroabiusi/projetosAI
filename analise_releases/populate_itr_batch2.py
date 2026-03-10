"""
Populate dados_trimestrais with ITR data - Batch 2
Covers: 3T2022, 4T2021, 3T2020, 3T2021, 4T2020, 1T2023, 4T2022
All values converted from R$ mil (ITR) to R$ milhoes (database) = divide by 1000
"""
import sqlite3

DB_PATH = "dados_financeiros.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

def update(empresa, periodo, segmento, **fields):
    sets = ", ".join(f"{k} = ?" for k in fields.keys())
    vals = list(fields.values())
    vals.extend([empresa, periodo, segmento])
    cur.execute(f"UPDATE dados_trimestrais SET {sets} WHERE empresa=? AND periodo=? AND segmento=?", vals)
    return cur.rowcount

total = 0

# ============================================================
# TENDA 3T2022 (30/09/2022) - from Tenda_ITR_3T2022.pdf
# ============================================================
total += update("Tenda", "3T2022", "Consolidado",
    recebiveis_circulante=615.279,
    recebiveis_nao_circulante=479.073,
    pdd_provisao=281.064,
    avp_recebiveis=-47.231,
    provisao_distratos=-33.907,
    # Aging
    aging_vencido_90d=22.959,
    aging_vencido_180d=28.433,
    aging_vencido_180d_mais=100.818,
    aging_a_vencer_12m=780.019,
    aging_a_vencer_24m=338.333,
    aging_a_vencer_36m=58.021,
    aging_a_vencer_36m_mais=42.342 + 85.629,  # 4a + 5a+
    # PECLD movement (9M)
    pecld_adicoes=108.782,
    pecld_reversoes=1.067,
    pecld_baixas=25.504,
    # Divida
    divida_debentures=1016.559,
    divida_sfh_producao=232.320,
    divida_venc_12m=512.455,  # circulante
    divida_venc_24m=449.483,  # 2023
    divida_venc_24m_mais=302.972 + 153.772 + 374.412,  # 2024+2025+2026+
    # Provisoes
    provisao_riscos_civeis=66.000,
    provisao_riscos_trabalhistas=13.661,
    provisao_riscos_total=96.730,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=53.867,
    despesa_fin_juros_divida=176.904,  # liq capitalizacao
    encargos_capitalizados=48.255,  # 9M, CPV
    despesa_fin_derivativos=5.689,  # swap hedge
)

# TENDA 4T2021 (31/12/2021 comparative from 3T2022)
total += update("Tenda", "4T2021", "Consolidado",
    recebiveis_circulante=601.886,
    recebiveis_nao_circulante=492.085,
    pdd_provisao=198.854,
    avp_recebiveis=-30.534,
    provisao_distratos=-46.328,
    aging_vencido_90d=19.399,
    aging_vencido_180d=26.589,
    aging_vencido_180d_mais=109.175,
    aging_a_vencer_12m=699.186,
    aging_a_vencer_24m=328.594,
    aging_a_vencer_36m=92.276,
    aging_a_vencer_36m_mais=34.778 + 59.690,
    provisao_riscos_civeis=56.908,
    provisao_riscos_trabalhistas=6.311,
    provisao_riscos_total=76.855,
)

# ============================================================
# TENDA 3T2020 (30/09/2020) - from Tenda_ITR_3T2020.pdf
# ============================================================
total += update("Tenda", "3T2020", "Consolidado",
    recebiveis_circulante=539.874,
    recebiveis_nao_circulante=258.012,
    pdd_provisao=161.382,
    avp_recebiveis=-4.486,
    provisao_distratos=-28.510,
    aging_vencido_90d=16.790,
    aging_vencido_180d=9.883,
    aging_vencido_180d_mais=60.307,
    # PECLD movement (9M)
    pecld_adicoes=85.464,
    pecld_reversoes=21.530,
    pecld_baixas=20.765,
    # Divida
    divida_debentures=818.506,
    divida_sfh_producao=0.025,  # SFH quase zero
    divida_venc_12m=108.415 + 370.114,  # emprestimos + debentures circ
    # Provisoes
    provisao_riscos_civeis=51.975,
    provisao_riscos_trabalhistas=5.919,
    provisao_riscos_total=59.895,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=18.181,
    despesa_fin_juros_divida=32.946,
    encargos_capitalizados=15.935,
)

# ============================================================
# TENDA 1T2023 (31/03/2023) - from Tenda_ITR_1T2023.pdf
# ============================================================
total += update("Tenda", "1T2023", "Consolidado",
    recebiveis_circulante=604.619,
    recebiveis_nao_circulante=503.416,
    pdd_provisao=284.105,
    avp_recebiveis=-68.893,
    provisao_distratos=-6.291,
    aging_vencido_90d=44.001,
    aging_vencido_180d=12.636,
    aging_vencido_180d_mais=107.073,
    aging_a_vencer_12m=744.911,
    aging_a_vencer_24m=309.362,
    aging_a_vencer_36m=95.535,
    aging_a_vencer_36m_mais=47.138 + 106.668,
    # PECLD movement (1T)
    pecld_adicoes=14.749,
    pecld_reversoes=3.009,
    # Divida
    divida_debentures=909.877,  # debentures + outros
    divida_sfh_producao=308.232,
    divida_venc_12m=612.774,  # circulante
    divida_venc_24m=376.196,
    divida_venc_24m_mais=186.725 + 221.108 + 155.330,
    # Provisoes
    provisao_riscos_civeis=94.446,
    provisao_riscos_trabalhistas=15.914,
    provisao_riscos_total=130.694,
    # Resultado financeiro (1T)
    receita_fin_aplicacoes=11.957,
    despesa_fin_juros_divida=58.754,
    encargos_capitalizados=15.538,
)

# TENDA 4T2022 (31/12/2022 comparative from 1T2023)
total += update("Tenda", "4T2022", "Consolidado",
    recebiveis_circulante=549.895,
    recebiveis_nao_circulante=474.817,
    pdd_provisao=272.365,
    avp_recebiveis=-55.659,
    provisao_distratos=-41.260,
    aging_vencido_90d=22.120,
    aging_vencido_180d=8.821,
    aging_vencido_180d_mais=112.221,
    aging_a_vencer_12m=729.391,
    aging_a_vencer_24m=287.388,
    aging_a_vencer_36m=99.049,
    aging_a_vencer_36m_mais=41.811 + 93.195,
    divida_debentures=989.647,
    divida_sfh_producao=339.453,
    divida_venc_12m=589.735,
    provisao_riscos_civeis=86.152,
    provisao_riscos_trabalhistas=16.693,
    provisao_riscos_total=121.864,
)

# ============================================================
# CURY 3T2022 (30/09/2022) - from Cury_ITR_3T2022.pdf
# ============================================================
total += update("Cury", "3T2022", "Consolidado",
    recebiveis_unidades_concluidas=225.861,
    recebiveis_unidades_construcao=1221.786,
    recebiveis_circulante=870.147,
    recebiveis_nao_circulante=529.554,
    pdd_provisao=71.275,
    avp_recebiveis=-5.025,
    provisao_distratos=-38.174,
    pro_soluto_saldo=477.800,  # from release, R$ milhoes
    pro_soluto_pct_carteira=48.8,
    poc_medio=56.93,
    # Aging (different faixas)
    aging_vencido_90d=14.950 + 9.891,  # 1-30d + 31-90d
    aging_vencido_180d_mais=85.347,  # >90d (ITR faixa)
    aging_a_vencer_12m=874.434,  # 1-360d
    aging_a_vencer_24m=222.970,  # 361-720d
    aging_a_vencer_36m_mais=306.583,  # >721d
    # PECLD (9M)
    pecld_adicoes=20.695,
    pecld_reversoes=1.245,
    pecld_baixas=4.078,
    # Divida
    divida_debentures=439.146,  # all 3 emissions
    divida_sfh_producao=19.135,  # CEF
    divida_venc_12m=32.789,
    divida_venc_24m=61.763,
    divida_venc_24m_mais=361.469,
    # Provisoes
    provisao_garantia_obra=18.755,
    provisao_riscos_civeis=5.156,
    provisao_riscos_trabalhistas=12.766,
    provisao_riscos_total=17.922,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=39.296,
    despesa_fin_juros_divida=35.699,
    despesa_fin_derivativos=0.049,
    encargos_capitalizados=2.458,
)

# CURY 4T2021 (31/12/2021 comparative from 3T2022)
total += update("Cury", "4T2021", "Consolidado",
    recebiveis_unidades_concluidas=339.816,
    recebiveis_unidades_construcao=936.038,
    recebiveis_circulante=731.349,
    recebiveis_nao_circulante=546.316,
    pdd_provisao=55.903,
    avp_recebiveis=-3.109,
    provisao_distratos=-29.850,
    aging_vencido_90d=33.249 + 34.947,  # 1-30 + 31-90
    aging_vencido_180d_mais=164.797,  # >90d
    aging_a_vencer_12m=587.218,
    aging_a_vencer_24m=240.934,
    aging_a_vencer_36m_mais=305.382,
    provisao_garantia_obra=13.220,
    provisao_riscos_civeis=6.924,
    provisao_riscos_trabalhistas=14.155,
    provisao_riscos_total=21.079,
)

# ============================================================
# CURY 1T2023 (31/03/2023) - from Cury_ITR_1T2023.pdf
# ============================================================
total += update("Cury", "1T2023", "Consolidado",
    recebiveis_unidades_concluidas=211.855,
    recebiveis_unidades_construcao=917.289,
    recebiveis_circulante=677.724,
    recebiveis_nao_circulante=372.912,
    pdd_provisao=83.072,
    avp_recebiveis=-5.768,
    provisao_distratos=-42.821,
    pro_soluto_saldo=534.900,  # from release
    pro_soluto_pct_carteira=50.3,
    poc_medio=53.17,
    aging_vencido_90d=11.180 + 17.034,
    aging_vencido_180d_mais=85.202,
    aging_a_vencer_12m=695.969,
    aging_a_vencer_24m=147.072,
    aging_a_vencer_36m_mais=225.840,
    pecld_adicoes=8.963,
    pecld_reversoes=0.946,
    # Divida
    divida_debentures=440.514,  # 132.495 + 206.369 + 101.650
    divida_sfh_producao=37.377,
    divida_venc_12m=114.117,
    divida_venc_24m=61.982,
    divida_venc_24m_mais=298.418,
    # Provisoes
    provisao_garantia_obra=23.738,
    provisao_riscos_civeis=4.378,
    provisao_riscos_trabalhistas=15.354,
    provisao_riscos_total=19.732,
    # Resultado financeiro (1T)
    receita_fin_aplicacoes=15.485,
    despesa_fin_juros_divida=15.964,
    despesa_fin_derivativos=0.175,
    encargos_capitalizados=1.299,
)

# CURY 4T2022 (31/12/2022 comparative from 1T2023)
total += update("Cury", "4T2022", "Consolidado",
    recebiveis_unidades_concluidas=235.844,
    recebiveis_unidades_construcao=1010.426,
    recebiveis_circulante=681.536,
    recebiveis_nao_circulante=497.726,
    pdd_provisao=75.055,
    avp_recebiveis=-3.551,
    provisao_distratos=-40.756,
    pro_soluto_saldo=476.100,  # from release
    pro_soluto_pct_carteira=49.1,
    aging_vencido_90d=34.347 + 15.111,
    aging_vencido_180d_mais=83.715,
    aging_a_vencer_12m=667.725,
    aging_a_vencer_24m=204.519,
    aging_a_vencer_36m_mais=293.207,
    provisao_garantia_obra=21.389,
    provisao_riscos_civeis=4.559,
    provisao_riscos_trabalhistas=14.309,
    provisao_riscos_total=18.868,
    divida_debentures=445.147,  # 128.226+214.349+102.572
    divida_sfh_producao=39.534,
    divida_venc_12m=120.906,
    divida_venc_24m=61.663,
    divida_venc_24m_mais=298.419,
)

# ============================================================
# MRV 3T2022 (30/09/2022) - from MRV_ITR_3T2022.pdf
# ============================================================
total += update("MRV", "3T2022", "Consolidado",
    recebiveis_circulante=2528.867,
    recebiveis_nao_circulante=1628.888,
    pdd_provisao=392.730,
    avp_recebiveis=-125.993,
    aging_a_vencer_12m=3385.964,
    aging_a_vencer_24m=1383.427,
    aging_a_vencer_36m=510.786,
    aging_a_vencer_36m_mais=250.740 + 269.568,
    pecld_adicoes=253.998,
    pecld_reversoes=130.410,
    pecld_baixas=78.606,
    cessao_passivo_total=447.828,
    # Divida
    divida_venc_12m=1379.440,
    divida_venc_24m=1216.321,
    divida_venc_24m_mais=2052.901 + 866.728 + 1802.498,
    # Provisoes
    provisao_garantia_obra=226.795,
    provisao_riscos_civeis=43.207,
    provisao_riscos_trabalhistas=40.087,
    provisao_riscos_total=83.676,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=138.329,
    receita_fin_recebiveis=86.249,
    despesa_fin_juros_divida=170.867,
    despesa_fin_cessao=36.583,
    despesa_fin_derivativos=13.980,
    encargos_capitalizados=307.682 + 55.238,  # imoveis + PPI
)

# MRV 4T2021 (31/12/2021 comparative from 3T2022)
total += update("MRV", "4T2021", "Consolidado",
    recebiveis_circulante=2378.157,
    recebiveis_nao_circulante=1737.445,
    pdd_provisao=347.748,
    avp_recebiveis=-75.091,
    aging_a_vencer_12m=3676.194,
    aging_a_vencer_24m=1535.310,
    aging_a_vencer_36m=508.902,
    aging_a_vencer_36m_mais=220.619 + 208.335,
    cessao_passivo_total=122.341,
    divida_venc_12m=868.351,
    divida_venc_24m=1472.352,
    divida_venc_24m_mais=1122.932 + 1000.790 + 796.987,
    provisao_garantia_obra=206.562,
    provisao_riscos_total=94.677,
)

# ============================================================
# MRV 1T2023 (31/03/2023) - from MRV_ITR_1T2023.pdf
# ============================================================
total += update("MRV", "1T2023", "Consolidado",
    recebiveis_circulante=2503.266,
    recebiveis_nao_circulante=1709.682,
    pdd_provisao=389.550,
    avp_recebiveis=-161.967,
    aging_a_vencer_12m=3352.338,
    aging_a_vencer_24m=1347.916,
    aging_a_vencer_36m=522.468,
    aging_a_vencer_36m_mais=294.050 + 378.342,
    pecld_adicoes=137.495,
    pecld_reversoes=47.559,
    pecld_baixas=31.586,
    cessao_passivo_total=705.746,
    # Divida
    divida_venc_12m=2347.339,
    divida_venc_24m=1847.126,
    divida_venc_24m_mais=2384.674 + 261.342 + 1285.951,
    # Provisoes
    provisao_garantia_obra=251.497,
    provisao_riscos_civeis=36.695,
    provisao_riscos_trabalhistas=41.818,
    provisao_riscos_total=78.927,
    # Resultado financeiro (1T)
    receita_fin_aplicacoes=36.263,
    receita_fin_recebiveis=30.353,
    despesa_fin_juros_divida=116.128,
    despesa_fin_cessao=26.816,
    despesa_fin_derivativos=14.999,
    encargos_capitalizados=105.545 + 11.692,
)

# MRV 4T2022 (31/12/2022 comparative from 1T2023)
total += update("MRV", "4T2022", "Consolidado",
    recebiveis_circulante=2487.534,
    recebiveis_nao_circulante=1522.830,
    pdd_provisao=331.200,
    avp_recebiveis=-126.961,
    aging_a_vencer_12m=3296.602,
    aging_a_vencer_24m=1274.509,
    aging_a_vencer_36m=453.183,
    aging_a_vencer_36m_mais=240.750 + 297.370,
    cessao_passivo_total=357.606,
    divida_venc_12m=1159.659,
    divida_venc_24m=1563.877,
    divida_venc_24m_mais=2148.563 + 800.170 + 1807.156,
    provisao_garantia_obra=243.841,
    provisao_riscos_civeis=33.088,
    provisao_riscos_trabalhistas=39.348,
    provisao_riscos_total=72.829,
)

# ============================================================
# PLANOEPLANO 3T2022 (30/09/2022) - from PlanoePlano_ITR_2022_3T.pdf
# ============================================================
total += update("PlanoePlano", "3T2022", "Consolidado",
    recebiveis_unidades_concluidas=37.723,
    recebiveis_unidades_construcao=326.496,
    recebiveis_circulante=196.391,
    recebiveis_nao_circulante=121.870,
    pdd_provisao=14.215,
    avp_recebiveis=-20.956,
    provisao_distratos=-11.134,
    # Aging
    aging_vencido_90d=2.865 + 1.406,  # ate60 + 61-90
    aging_vencido_180d=2.246,  # 91-180
    aging_vencido_180d_mais=3.567,
    aging_a_vencer_12m=186.307,
    aging_a_vencer_36m=145.660,  # ate 3 anos (no 24m separate)
    aging_a_vencer_36m_mais=22.515,
    # PECLD (9M)
    pecld_adicoes=9.350,
    # Divida
    divida_debentures=48.854,
    divida_sfh_producao=416.382,  # CEF financ producao
    divida_venc_12m=105.598,
    # Provisoes
    provisao_garantia_obra=20.367,
    provisao_riscos_civeis=0.091,
    provisao_riscos_trabalhistas=0.644,
    provisao_riscos_total=0.735,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=17.761,
    despesa_fin_juros_divida=14.233,
    encargos_capitalizados=36.291,
)

# PLANOEPLANO 4T2021 (31/12/2021 comparative from 3T2022)
total += update("PlanoePlano", "4T2021", "Consolidado",
    recebiveis_unidades_concluidas=6.257,
    recebiveis_unidades_construcao=231.827,
    recebiveis_circulante=154.009,
    recebiveis_nao_circulante=64.188,
    pdd_provisao=4.865,
    avp_recebiveis=-6.560,
    provisao_distratos=-8.788,
    aging_a_vencer_12m=156.612,
    aging_a_vencer_36m=59.542,
    aging_a_vencer_36m_mais=4.645,
    provisao_garantia_obra=15.825,
    provisao_riscos_total=0.112,
    divida_debentures=60.533,
    divida_sfh_producao=345.437,
)

# ============================================================
# DIRECIONAL 3T2021 (30/09/2021) - from Direcional_ITR_3T2021.pdf
# ============================================================
total += update("Direcional", "3T2021", "Consolidado",
    recebiveis_unidades_concluidas=185.988,
    recebiveis_unidades_construcao=375.515,
    recebiveis_circulante=259.389,
    recebiveis_nao_circulante=277.261,
    pdd_provisao=29.303,
    avp_recebiveis=-11.759,
    taxa_avp_aa=3.69,
    aging_vencido_90d=5.361,
    aging_vencido_180d=6.077,
    aging_vencido_180d_mais=25.919,
    aging_a_vencer_12m=222.032,
    aging_a_vencer_24m=160.977,
    aging_a_vencer_36m=63.581,
    aging_a_vencer_36m_mais=52.703,
    # PECLD (9M)
    pecld_adicoes=19.294,
    pecld_reversoes=7.021,
    # Divida
    divida_cri=795.257,
    divida_debentures=249.381,
    divida_sfh_producao=44.161,
    divida_venc_12m=151.780,
    # Provisoes
    provisao_garantia_obra=40.054,
    provisao_riscos_civeis=17.021,
    provisao_riscos_trabalhistas=7.809,
    provisao_riscos_total=24.942,
    # Resultado financeiro (9M)
    receita_fin_aplicacoes=16.913,
    despesa_fin_juros_divida=74.102,
    despesa_fin_cessao=2.402,
    despesa_fin_derivativos=10.832,
)

# DIRECIONAL 4T2020 (31/12/2020 comparative from 3T2021)
total += update("Direcional", "4T2020", "Consolidado",
    recebiveis_unidades_concluidas=176.015,
    recebiveis_unidades_construcao=192.288,
    recebiveis_circulante=246.717,
    recebiveis_nao_circulante=128.727,
    pdd_provisao=17.030,
    avp_recebiveis=-4.702,
    aging_vencido_90d=18.411,
    aging_vencido_180d=2.425,
    aging_vencido_180d_mais=18.200,
    aging_a_vencer_12m=207.681,
    aging_a_vencer_24m=63.898,
    aging_a_vencer_36m=39.435,
    aging_a_vencer_36m_mais=25.394,
    divida_cri=602.123,
    divida_debentures=251.633,
    divida_sfh_producao=12.607,
    divida_venc_12m=219.061,
    provisao_garantia_obra=38.907,
    provisao_riscos_civeis=17.529,
    provisao_riscos_trabalhistas=11.501,
    provisao_riscos_total=29.142,
)

# ============================================================
# DIRECIONAL 1T2023 (31/03/2023) - from Direcional_ITR_1T2023.pdf
# ============================================================
total += update("Direcional", "1T2023", "Consolidado",
    recebiveis_unidades_concluidas=191.708,
    recebiveis_unidades_construcao=596.120,
    recebiveis_circulante=403.427,
    recebiveis_nao_circulante=334.609,
    pdd_provisao=32.493,
    avp_recebiveis=-36.592,
    taxa_avp_aa=6.98,
    aging_vencido_90d=3.309 + 1.856 + 1.425,
    aging_vencido_180d=2.889 + 2.095,
    aging_vencido_180d_mais=26.220,
    aging_a_vencer_12m=365.633,
    aging_a_vencer_24m=189.647,
    aging_a_vencer_36m=59.736,
    aging_a_vencer_36m_mais=27.080 + 58.146,
    pecld_adicoes=10.750,
    pecld_reversoes=7.822,
    cessao_passivo_total=133.237,
    divida_cri=900.360,
    divida_debentures=358.050,
    divida_sfh_producao=140.432,
    divida_venc_12m=312.729,
    provisao_garantia_obra=42.744,
    provisao_riscos_civeis=21.244,
    provisao_riscos_trabalhistas=6.434,
    provisao_riscos_total=27.814,
    receita_fin_aplicacoes=27.610,
    despesa_fin_juros_divida=39.749,
    despesa_fin_cessao=10.200,
    despesa_fin_derivativos=7.021,
    encargos_capitalizados=4.554,
)

# DIRECIONAL 4T2022 (31/12/2022 comparative from 1T2023)
total += update("Direcional", "4T2022", "Consolidado",
    recebiveis_unidades_concluidas=202.248,
    recebiveis_unidades_construcao=516.271,
    recebiveis_circulante=363.372,
    recebiveis_nao_circulante=314.813,
    pdd_provisao=29.565,
    avp_recebiveis=-32.340,
    aging_vencido_90d=4.986 + 1.973 + 1.327,
    aging_vencido_180d=1.135 + 2.126,
    aging_vencido_180d_mais=24.466,
    aging_a_vencer_12m=327.359,
    aging_a_vencer_24m=180.487,
    aging_a_vencer_36m=56.051,
    aging_a_vencer_36m_mais=27.480 + 50.795,
    cessao_passivo_total=88.320,
    divida_cri=925.631,
    divida_debentures=363.937,
    divida_sfh_producao=115.006,
    divida_venc_12m=309.722,
    provisao_garantia_obra=42.279,
    provisao_riscos_civeis=19.214,
    provisao_riscos_trabalhistas=6.653,
    provisao_riscos_total=26.198,
)

# ============================================================
# CYRELA 1T2023 (31/03/2023) - from Cyrela_ITR_1T2023.pdf
# ============================================================
total += update("Cyrela", "1T2023", "Consolidado",
    recebiveis_unidades_concluidas=811.131,
    recebiveis_unidades_construcao=2319.117,
    recebiveis_circulante=2121.430,
    recebiveis_nao_circulante=544.623,
    pdd_provisao=57.215,
    avp_recebiveis=-69.112,
    provisao_distratos=-359.021,
    aging_a_vencer_12m=3257.669,
    aging_a_vencer_24m=2527.664,
    aging_a_vencer_36m=1754.901,
    aging_a_vencer_36m_mais=595.933 + 52.244,
    pecld_adicoes=10.773,
    pecld_reversoes=3.705,
    pecld_baixas=2.180,
    divida_debentures=1096.036,
    divida_cri=1943.345,
    divida_sfh_producao=1291.655,
    provisao_garantia_obra=119.666,
    provisao_riscos_civeis=144.292,
    provisao_riscos_trabalhistas=81.294,
    provisao_riscos_total=240.585,
    receita_fin_aplicacoes=146.729,
    despesa_fin_juros_divida=120.851 + 33.504,  # emprestimos + SFH
    despesa_fin_derivativos=2.040,
    encargos_capitalizados=26.123,
)

# CYRELA 4T2022 (31/12/2022 comparative from 1T2023)
total += update("Cyrela", "4T2022", "Consolidado",
    recebiveis_unidades_concluidas=831.133,
    recebiveis_unidades_construcao=2348.117,
    recebiveis_circulante=2150.674,
    recebiveis_nao_circulante=558.334,
    pdd_provisao=52.327,
    avp_recebiveis=-80.422,
    provisao_distratos=-343.423,
    aging_a_vencer_12m=3208.574,
    aging_a_vencer_24m=2360.829,
    aging_a_vencer_36m=1743.666,
    aging_a_vencer_36m_mais=545.891 + 51.808,
    divida_debentures=1070.246,
    divida_cri=1949.484,
    divida_sfh_producao=1247.003,
    provisao_garantia_obra=115.904,
    provisao_riscos_civeis=136.508,
    provisao_riscos_trabalhistas=86.581,
    provisao_riscos_total=237.513,
)

conn.commit()
print(f"Total rows updated: {total}")

# Verify coverage
cur.execute("""SELECT empresa, COUNT(*) as n
               FROM dados_trimestrais
               WHERE segmento='Consolidado' AND recebiveis_circulante IS NOT NULL
               GROUP BY empresa ORDER BY empresa""")
print("\nCobertura por empresa:")
for r in cur.fetchall():
    print(f"  {r[0]:15s}: {r[1]} periodos")

cur.execute("""SELECT COUNT(*) FROM dados_trimestrais
               WHERE segmento='Consolidado' AND recebiveis_circulante IS NOT NULL""")
total_filled = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM dados_trimestrais WHERE segmento='Consolidado'")
total_periods = cur.fetchone()[0]
print(f"\nTotal: {total_filled}/{total_periods} periodos com dados de recebiveis")
conn.close()

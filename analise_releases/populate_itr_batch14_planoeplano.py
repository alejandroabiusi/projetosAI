"""
Batch 14 - PlanoePlano ITR/DFP data for ALL 19 periods (1T2020-4T2024, 1T2025-3T2025).
All values in R$ milhoes as float (source R$ mil integers / 1000).
Note: PlanoePlano aging uses different buckets (ate 60d, 61-90d, 91-180d, >180d for vencidos;
      ate 1 ano, ate 3 anos, acima 3 anos for a vencer). Mapped accordingly.
Note: 2T2025 and 3T2025 have image-based notes - partial data only.
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
# PlanoePlano 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("PlanoePlano", "1T2020", "Consolidado",
    recebiveis_circulante=136.832,
    recebiveis_nao_circulante=18.505,
    avp_recebiveis=-4.723,
    pdd_provisao=-4.291,
    provisao_distratos=-4.266,
    aging_vencido_90d=5.345,
    aging_vencido_180d=3.219,
    aging_vencido_180d_mais=2.601,
    aging_a_vencer_12m=138.946,
    aging_a_vencer_24m=17.403,
    aging_a_vencer_36m_mais=1.103,
    pecld_adicoes=-1.205,
    divida_bruta=281.930,
    divida_venc_12m=30.376,
    divida_venc_24m_mais=251.554,
    divida_cri=30.376,
    caixa_aplicacoes=174.183,
    provisao_garantia_obra=12.337,
    provisao_riscos_civeis=0.0,
    provisao_riscos_trabalhistas=1.145,
    provisao_riscos_total=1.145,
    receitas_financeiras=4.887,
    despesas_financeiras=-4.303,
    resultado_financeiro=0.584,
)
print(f"PlanoePlano 1T2020: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("PlanoePlano", "2T2020", "Consolidado",
    recebiveis_circulante=141.244,
    recebiveis_nao_circulante=14.200,
    avp_recebiveis=-3.801,
    pdd_provisao=-4.152,
    provisao_distratos=-8.976,
    aging_vencido_90d=3.169,
    aging_vencido_180d=1.018,
    aging_vencido_180d_mais=2.996,
    aging_a_vencer_12m=150.990,
    aging_a_vencer_36m_mais=14.200,
    pecld_adicoes=-1.066,
    divida_bruta=352.432,
    divida_venc_12m=30.035,
    divida_venc_24m_mais=322.397,
    divida_cri=30.035,
    caixa_aplicacoes=238.204,
    provisao_garantia_obra=12.637,
    provisao_riscos_civeis=0.0,
    provisao_riscos_trabalhistas=1.145,
    provisao_riscos_total=1.145,
    receitas_financeiras=2.663,
    despesas_financeiras=-7.692,
    resultado_financeiro=-5.029,
)
print(f"PlanoePlano 2T2020: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 3T2020 (balance date 30/09/2020)
# ============================================================
n = update("PlanoePlano", "3T2020", "Consolidado",
    recebiveis_circulante=104.232,
    recebiveis_nao_circulante=20.605,
    avp_recebiveis=-2.840,
    pdd_provisao=-4.060,
    provisao_distratos=-6.571,
    aging_vencido_90d=8.019,
    aging_vencido_180d=3.137,
    aging_vencido_180d_mais=2.724,
    aging_a_vencer_12m=103.823,
    aging_a_vencer_36m_mais=20.605,
    pecld_adicoes=-0.974,
    divida_bruta=374.557,
    divida_venc_12m=30.200,
    divida_venc_24m_mais=344.357,
    divida_cri=30.195,
    caixa_aplicacoes=297.988,
    provisao_garantia_obra=12.711,
    provisao_riscos_civeis=0.0,
    provisao_riscos_trabalhistas=1.145,
    provisao_riscos_total=1.145,
    receitas_financeiras=1.041,
    despesas_financeiras=-3.010,
    resultado_financeiro=-1.969,
)
print(f"PlanoePlano 3T2020: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 4T2020 (balance date 31/12/2020)
# ============================================================
n = update("PlanoePlano", "4T2020", "Consolidado",
    recebiveis_circulante=151.693,
    recebiveis_nao_circulante=15.859,
    avp_recebiveis=-2.908,
    pdd_provisao=-3.068,
    provisao_distratos=-10.641,
    aging_vencido_90d=14.721,
    aging_vencido_180d=2.110,
    aging_vencido_180d_mais=2.664,
    aging_a_vencer_12m=148.815,
    aging_a_vencer_24m=15.852,
    aging_a_vencer_36m_mais=0.007,
    pecld_adicoes=0.0,
    pecld_reversoes=0.018,
    divida_bruta=290.689,
    divida_venc_12m=0.119,
    divida_venc_24m_mais=290.570,
    caixa_aplicacoes=204.223,
    provisao_garantia_obra=14.197,
    provisao_riscos_civeis=0.934,
    provisao_riscos_trabalhistas=0.0,
    provisao_riscos_total=0.934,
    receitas_financeiras=9.384,
    despesas_financeiras=-1.292,
    resultado_financeiro=8.092,
)
print(f"PlanoePlano 4T2020: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 1T2021 (balance date 31/03/2021)
# ============================================================
n = update("PlanoePlano", "1T2021", "Consolidado",
    recebiveis_circulante=241.948,
    recebiveis_nao_circulante=16.946,
    avp_recebiveis=-3.089,
    pdd_provisao=-3.674,
    provisao_distratos=-14.520,
    aging_vencido_90d=17.955,
    aging_vencido_180d=3.505,
    aging_vencido_180d_mais=10.991,
    aging_a_vencer_12m=230.780,
    aging_a_vencer_24m=16.946,
    aging_a_vencer_36m_mais=0.0,
    pecld_adicoes=-0.606,
    divida_bruta=372.711,
    divida_venc_12m=0.143,
    divida_venc_24m_mais=372.568,
    caixa_aplicacoes=280.042,
    provisao_garantia_obra=15.983,
    provisao_riscos_civeis=0.0,
    provisao_riscos_trabalhistas=0.085,
    provisao_riscos_total=0.085,
    receitas_financeiras=1.324,
    despesas_financeiras=-0.727,
    resultado_financeiro=0.597,
)
print(f"PlanoePlano 1T2021: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2021 (balance date 30/06/2021)
# ============================================================
n = update("PlanoePlano", "2T2021", "Consolidado",
    recebiveis_circulante=192.930,
    recebiveis_nao_circulante=35.319,
    avp_recebiveis=-3.265,
    pdd_provisao=-3.706,
    provisao_distratos=-11.324,
    aging_vencido_90d=14.162,
    aging_vencido_180d=2.588,
    aging_vencido_180d_mais=3.054,
    aging_a_vencer_12m=191.421,
    aging_a_vencer_24m=34.975,
    aging_a_vencer_36m_mais=0.344,
    pecld_adicoes=-0.638,
    divida_bruta=344.703,
    divida_venc_12m=1.597,
    divida_venc_24m_mais=343.106,
    caixa_aplicacoes=246.178,
    provisao_garantia_obra=17.920,
    provisao_riscos_civeis=0.207,
    provisao_riscos_trabalhistas=0.0,
    provisao_riscos_total=0.207,
    receitas_financeiras=1.128,
    despesas_financeiras=-4.107,
    resultado_financeiro=-2.979,
)
print(f"PlanoePlano 2T2021: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 3T2021 (balance date 30/09/2021)
# ============================================================
n = update("PlanoePlano", "3T2021", "Consolidado",
    recebiveis_circulante=183.360,
    recebiveis_nao_circulante=47.142,
    avp_recebiveis=-6.136,
    pdd_provisao=-4.270,
    provisao_distratos=-10.010,
    aging_vencido_90d=18.076,
    aging_vencido_180d=2.590,
    aging_vencido_180d_mais=3.508,
    aging_a_vencer_12m=179.824,
    aging_a_vencer_24m=45.184,
    aging_a_vencer_36m_mais=1.736,
    pecld_adicoes=-1.202,
    divida_bruta=323.102,
    divida_venc_12m=17.421,
    divida_venc_24m_mais=305.681,
    caixa_aplicacoes=223.953,
    provisao_garantia_obra=15.139,
    provisao_riscos_civeis=0.196,
    provisao_riscos_trabalhistas=0.0,
    provisao_riscos_total=0.196,
    receitas_financeiras=1.559,
    despesas_financeiras=-2.950,
    resultado_financeiro=-1.391,
)
print(f"PlanoePlano 3T2021: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 1T2022 (balance date 31/03/2022)
# ============================================================
n = update("PlanoePlano", "1T2022", "Consolidado",
    recebiveis_circulante=185.085,
    recebiveis_nao_circulante=83.003,
    avp_recebiveis=-10.184,
    pdd_provisao=-8.545,
    provisao_distratos=-11.627,
    aging_vencido_90d=4.689,
    aging_vencido_180d=1.580,
    aging_vencido_180d_mais=4.660,
    aging_a_vencer_12m=204.421,
    aging_a_vencer_24m=72.284,
    aging_a_vencer_36m_mais=10.810,
    pecld_adicoes=-3.680,
    divida_bruta=412.554,
    divida_venc_12m=51.592,
    divida_venc_24m_mais=360.962,
    caixa_aplicacoes=276.868,
    provisao_garantia_obra=16.998,
    provisao_riscos_civeis=0.081,
    provisao_riscos_trabalhistas=0.022,
    provisao_riscos_total=0.103,
    receitas_financeiras=5.494,
    despesas_financeiras=-4.375,
    resultado_financeiro=1.119,
)
print(f"PlanoePlano 1T2022: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2022 (balance date 30/06/2022)
# ============================================================
n = update("PlanoePlano", "2T2022", "Consolidado",
    recebiveis_circulante=175.087,
    recebiveis_nao_circulante=87.422,
    avp_recebiveis=-13.203,
    pdd_provisao=-10.744,
    provisao_distratos=-8.950,
    aging_vencido_90d=5.613,
    aging_vencido_180d=1.678,
    aging_vencido_180d_mais=3.989,
    aging_a_vencer_12m=196.714,
    aging_a_vencer_24m=71.816,
    aging_a_vencer_36m_mais=15.596,
    pecld_adicoes=-5.879,
    divida_bruta=463.706,
    divida_venc_12m=106.702,
    divida_venc_24m_mais=357.004,
    caixa_aplicacoes=293.759,
    provisao_garantia_obra=18.518,
    provisao_riscos_civeis=0.067,
    provisao_riscos_trabalhistas=0.204,
    provisao_riscos_total=0.271,
    receitas_financeiras=7.485,
    despesas_financeiras=-6.040,
    resultado_financeiro=1.445,
)
print(f"PlanoePlano 2T2022: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 4T2022 (balance date 31/12/2022)
# ============================================================
n = update("PlanoePlano", "4T2022", "Consolidado",
    recebiveis_circulante=274.341,
    recebiveis_nao_circulante=114.318,
    avp_recebiveis=-15.442,
    pdd_provisao=-17.641,
    provisao_distratos=-7.735,
    aging_vencido_90d=19.957,
    aging_vencido_180d=3.732,
    aging_vencido_180d_mais=4.466,
    aging_a_vencer_12m=287.004,
    aging_a_vencer_24m=96.291,
    aging_a_vencer_36m_mais=18.027,
    pecld_adicoes=-12.776,
    divida_bruta=532.806,
    divida_venc_12m=118.476,
    divida_venc_24m_mais=414.330,
    caixa_aplicacoes=340.031,
    provisao_garantia_obra=22.260,
    provisao_riscos_civeis=0.093,
    provisao_riscos_trabalhistas=4.856,
    provisao_riscos_total=4.949,
    receitas_financeiras=7.175,
    despesas_financeiras=-6.535,
    resultado_financeiro=0.640,
)
print(f"PlanoePlano 4T2022: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 1T2023 (balance date 31/03/2023)
# ============================================================
n = update("PlanoePlano", "1T2023", "Consolidado",
    recebiveis_circulante=375.625,
    recebiveis_nao_circulante=106.237,
    avp_recebiveis=-10.138,
    pdd_provisao=-30.599,
    provisao_distratos=-22.731,
    aging_vencido_90d=13.338,
    aging_vencido_180d=8.044,
    aging_vencido_180d_mais=6.089,
    aging_a_vencer_12m=348.153,
    aging_a_vencer_24m=142.851,
    aging_a_vencer_36m_mais=26.855,
    pecld_adicoes=-12.958,
    divida_bruta=491.464,
    divida_venc_12m=109.440,
    divida_venc_24m_mais=382.024,
    caixa_aplicacoes=264.327,
    provisao_garantia_obra=24.610,
    provisao_riscos_civeis=0.109,
    provisao_riscos_trabalhistas=4.782,
    provisao_riscos_total=4.891,
    receitas_financeiras=11.630,
    despesas_financeiras=-6.749,
    resultado_financeiro=4.881,
)
print(f"PlanoePlano 1T2023: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2023 (balance date 30/06/2023)
# ============================================================
n = update("PlanoePlano", "2T2023", "Consolidado",
    recebiveis_circulante=422.847,
    recebiveis_nao_circulante=127.064,
    avp_recebiveis=-19.249,
    pdd_provisao=-32.161,
    provisao_distratos=-27.118,
    aging_vencido_90d=64.969,
    aging_vencido_180d=7.758,
    aging_vencido_180d_mais=12.152,
    aging_a_vencer_12m=392.188,
    aging_a_vencer_24m=122.934,
    aging_a_vencer_36m_mais=28.438,
    pecld_adicoes=-14.520,
    divida_bruta=550.470,
    divida_venc_12m=113.149,
    divida_venc_24m_mais=437.321,
    caixa_aplicacoes=325.509,
    provisao_garantia_obra=26.937,
    provisao_riscos_civeis=0.043,
    provisao_riscos_trabalhistas=5.041,
    provisao_riscos_total=5.098,
    receitas_financeiras=6.219,
    despesas_financeiras=-7.530,
    resultado_financeiro=-1.311,
)
print(f"PlanoePlano 2T2023: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 3T2023 (balance date 30/09/2023)
# ============================================================
n = update("PlanoePlano", "3T2023", "Consolidado",
    recebiveis_circulante=407.503,
    recebiveis_nao_circulante=150.657,
    avp_recebiveis=-19.128,
    pdd_provisao=-34.282,
    provisao_distratos=-59.367,
    aging_vencido_90d=19.567,
    aging_vencido_180d=24.361,
    aging_vencido_180d_mais=12.595,
    aging_a_vencer_12m=428.894,
    aging_a_vencer_24m=136.922,
    aging_a_vencer_36m_mais=48.598,
    pecld_adicoes=-16.641,
    divida_bruta=465.040,
    divida_venc_12m=148.865,
    divida_venc_24m_mais=316.175,
    caixa_aplicacoes=346.163,
    provisao_garantia_obra=29.613,
    provisao_riscos_civeis=0.043,
    provisao_riscos_trabalhistas=4.248,
    provisao_riscos_total=4.305,
    receitas_financeiras=10.350,
    despesas_financeiras=-9.626,
    resultado_financeiro=0.724,
)
print(f"PlanoePlano 3T2023: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("PlanoePlano", "1T2024", "Consolidado",
    recebiveis_circulante=519.543,
    recebiveis_nao_circulante=158.567,
    avp_recebiveis=-20.614,
    pdd_provisao=-59.308,
    provisao_distratos=-101.745,
    aging_vencido_90d=17.390,
    aging_vencido_180d=21.531,
    aging_vencido_180d_mais=22.994,
    aging_a_vencer_12m=589.296,
    aging_a_vencer_24m=161.711,
    aging_a_vencer_36m_mais=46.855,
    pecld_adicoes=-17.988,
    divida_bruta=369.845,
    divida_venc_12m=125.444,
    divida_venc_24m_mais=244.401,
    caixa_aplicacoes=283.420,
    provisao_garantia_obra=34.025,
    provisao_riscos_civeis=0.043,
    provisao_riscos_trabalhistas=4.115,
    provisao_riscos_total=4.172,
    receitas_financeiras=10.577,
    despesas_financeiras=-6.754,
    resultado_financeiro=3.823,
)
print(f"PlanoePlano 1T2024: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2024 (balance date 30/06/2024)
# ============================================================
n = update("PlanoePlano", "2T2024", "Consolidado",
    recebiveis_circulante=526.096,
    recebiveis_nao_circulante=218.601,
    avp_recebiveis=-23.805,
    pdd_provisao=-70.525,
    provisao_distratos=-71.589,
    aging_vencido_90d=65.011,
    aging_vencido_180d=10.281,
    aging_vencido_180d_mais=20.052,
    aging_a_vencer_12m=561.053,
    aging_a_vencer_24m=200.715,
    aging_a_vencer_36m_mais=53.504,
    pecld_adicoes=-29.205,
    divida_bruta=377.647,
    divida_venc_12m=103.365,
    divida_venc_24m_mais=274.282,
    caixa_aplicacoes=378.069,
    provisao_garantia_obra=36.155,
    provisao_riscos_civeis=0.046,
    provisao_riscos_trabalhistas=4.197,
    provisao_riscos_total=4.257,
    receitas_financeiras=7.077,
    despesas_financeiras=-7.542,
    resultado_financeiro=-0.465,
)
print(f"PlanoePlano 2T2024: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 4T2024 (balance date 31/12/2024)
# ============================================================
n = update("PlanoePlano", "4T2024", "Consolidado",
    recebiveis_circulante=565.970,
    recebiveis_nao_circulante=261.201,
    avp_recebiveis=-34.358,
    pdd_provisao=-47.194,
    provisao_distratos=-99.157,
    aging_vencido_90d=78.903,
    aging_vencido_180d=19.683,
    aging_vencido_180d_mais=32.933,
    aging_a_vencer_12m=542.771,
    aging_a_vencer_24m=265.633,
    aging_a_vencer_36m_mais=67.957,
    pecld_adicoes=-33.121,
    pecld_reversoes=27.247,
    divida_bruta=588.790,
    divida_venc_12m=30.189,
    divida_venc_24m_mais=558.601,
    divida_cri=359.393,
    caixa_aplicacoes=801.471,
    provisao_garantia_obra=43.623,
    provisao_riscos_civeis=0.067,
    provisao_riscos_trabalhistas=4.079,
    provisao_riscos_total=4.161,
    receitas_financeiras=46.059,
    despesas_financeiras=-44.297,
    resultado_financeiro=1.762,
)
print(f"PlanoePlano 4T2024: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 1T2025 (balance date 31/03/2025)
# ============================================================
n = update("PlanoePlano", "1T2025", "Consolidado",
    recebiveis_circulante=626.344,
    recebiveis_nao_circulante=278.645,
    avp_recebiveis=-32.461,
    pdd_provisao=-53.781,
    provisao_distratos=-115.562,
    aging_vencido_90d=52.384,
    aging_vencido_180d=48.265,
    aging_vencido_180d_mais=41.190,
    aging_a_vencer_12m=674.260,
    aging_a_vencer_24m=258.406,
    aging_a_vencer_36m_mais=32.288,
    pecld_adicoes=-11.483,
    pecld_reversoes=4.896,
    divida_bruta=598.315,
    divida_venc_12m=31.492,
    divida_venc_24m_mais=566.823,
    divida_cri=359.453,
    caixa_aplicacoes=464.794,
    provisao_garantia_obra=44.237,
    provisao_riscos_civeis=0.103,
    provisao_riscos_trabalhistas=4.173,
    provisao_riscos_total=4.291,
    receitas_financeiras=22.403,
    despesas_financeiras=-20.718,
    resultado_financeiro=1.685,
)
print(f"PlanoePlano 1T2025: {n} row(s)")
count += n

# ============================================================
# PlanoePlano 2T2025 (balance date 30/06/2025) - PARTIAL (image-based notes)
# ============================================================
n = update("PlanoePlano", "2T2025", "Consolidado",
    recebiveis_circulante=696.025,
    recebiveis_nao_circulante=375.205,
    divida_bruta=737.803,
    divida_venc_12m=135.037,
    divida_venc_24m_mais=602.766,
    divida_cri=378.636,
    caixa_aplicacoes=553.766,
    provisao_riscos_total=4.493,
    receitas_financeiras=21.632,
    despesas_financeiras=-27.634,
    resultado_financeiro=-6.002,
)
print(f"PlanoePlano 2T2025 (partial): {n} row(s)")
count += n

# ============================================================
# PlanoePlano 3T2025 (balance date 30/09/2025) - PARTIAL (image-based notes)
# ============================================================
n = update("PlanoePlano", "3T2025", "Consolidado",
    recebiveis_circulante=849.344,
    recebiveis_nao_circulante=303.783,
    divida_bruta=1066.600,
    divida_venc_12m=260.711,
    divida_venc_24m_mais=927.263,
    caixa_aplicacoes=975.776,
    provisao_riscos_total=4.462,
    receitas_financeiras=26.395,
    despesas_financeiras=-26.854,
    resultado_financeiro=-0.459,
)
print(f"PlanoePlano 3T2025 (partial): {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

"""
Script para popular dados extraídos dos ITRs no banco de dados.
Valores originais em R$ mil, banco armazena em R$ milhões.
Nota: valores já estão em R$ milhões (formato brasileiro: 1.234.567 mil = 1.234,567 mi)
"""
import sqlite3

conn = sqlite3.connect('dados_financeiros.db')
cur = conn.cursor()

def update(empresa, periodo, segmento, **fields):
    sets = ", ".join(f"{k} = ?" for k in fields.keys())
    vals = list(fields.values())
    vals.extend([empresa, periodo, segmento])
    cur.execute(f"UPDATE dados_trimestrais SET {sets} WHERE empresa=? AND periodo=? AND segmento=?", vals)
    return cur.rowcount

# ===================================================================
# TENDA 3T2024 (30/09/2024)
# ===================================================================
n = update("Tenda", "3T2024", "Consolidado",
    avp_recebiveis=120.972,
    provisao_distratos=20.177,
    recebiveis_circulante=613.355,
    recebiveis_nao_circulante=837.661,
    carteira_recebiveis_total=1451.016,
    pdd_provisao=447.192,
    taxa_avp_aa=6.53,
    aging_vencido_90d=37.911,
    aging_vencido_180d=14.448,
    aging_vencido_180d_mais=131.202,
    aging_a_vencer_12m=903.242,
    aging_a_vencer_24m=505.947,
    aging_a_vencer_36m=201.637,
    aging_a_vencer_36m_mais=244.970,
    pecld_adicoes=112.444,
    pecld_reversoes=12.148,
    divida_bruta=1170.429,
    divida_debentures=567.276,
    divida_sfh_producao=603.153,
    divida_venc_12m=663.396,
    divida_liquida=432.412,
    cessao_passivo_total=331.353,
    cessao_num_operacoes=3,
    provisao_riscos_civeis=102.095,
    provisao_riscos_trabalhistas=24.712,
    provisao_riscos_total=127.596,
    receita_fin_aplicacoes=47.982,
    despesa_fin_juros_divida=171.479,
)
print(f"Tenda 3T2024: {n} rows")

# TENDA 4T2023 (31/12/2023)
n = update("Tenda", "4T2023", "Consolidado",
    avp_recebiveis=83.129,
    provisao_distratos=7.956,
    recebiveis_circulante=544.588,
    recebiveis_nao_circulante=678.686,
    carteira_recebiveis_total=1223.274,
    pdd_provisao=346.896,
    taxa_avp_aa=5.22,
    aging_vencido_90d=66.292,
    aging_vencido_180d=19.259,
    aging_vencido_180d_mais=112.216,
    aging_a_vencer_12m=709.256,
    aging_a_vencer_24m=407.235,
    aging_a_vencer_36m=139.232,
    aging_a_vencer_36m_mais=207.766,
    divida_bruta=1180.095,
    divida_debentures=796.798,
    divida_sfh_producao=383.297,
    divida_liquida=461.279,
    cessao_passivo_total=229.387,
    cessao_num_operacoes=2,
    provisao_riscos_total=150.622,
)
print(f"Tenda 4T2023: {n} rows")

# ===================================================================
# CURY 3T2024 (30/09/2024)
# ===================================================================
n = update("Cury", "3T2024", "Consolidado",
    recebiveis_unidades_concluidas=274.727,
    recebiveis_unidades_construcao=1352.622,
    avp_recebiveis=22.345,
    provisao_distratos=37.040,
    recebiveis_circulante=371.958,
    recebiveis_nao_circulante=1082.272,
    carteira_recebiveis_total=1454.230,
    pdd_provisao=118.358,
    taxa_avp_aa=4.19,
    aging_vencido_90d=26.571,
    aging_vencido_180d_mais=111.981,
    aging_a_vencer_12m=411.149,
    aging_a_vencer_24m=249.274,
    aging_a_vencer_36m_mais=832.998,
    pecld_adicoes=41.120,
    pecld_reversoes=5.889,
    pecld_baixas=9.372,
    divida_bruta=1102.525,
    divida_debentures=1009.729,
    divida_sfh_producao=92.796,
    divida_venc_12m=141.500,
    divida_venc_24m=39.266,
    divida_venc_24m_mais=921.759,
    pro_soluto_saldo=849.2,
    pro_soluto_pct_carteira=43.2,
    poc_medio=53.16,
    provisao_riscos_civeis=4.671,
    provisao_riscos_trabalhistas=22.507,
    provisao_riscos_total=27.178,
    receita_fin_aplicacoes=61.580,
    despesa_fin_juros_divida=67.645,
    despesa_fin_derivativos=-0.404,
)
print(f"Cury 3T2024: {n} rows")

# CURY 4T2023 (31/12/2023)
n = update("Cury", "4T2023", "Consolidado",
    recebiveis_unidades_concluidas=232.729,
    recebiveis_unidades_construcao=997.193,
    avp_recebiveis=8.318,
    provisao_distratos=41.942,
    recebiveis_circulante=481.218,
    recebiveis_nao_circulante=609.583,
    carteira_recebiveis_total=1090.801,
    pdd_provisao=92.499,
    taxa_avp_aa=3.44,
    aging_vencido_90d=54.500,
    aging_vencido_180d_mais=91.761,
    aging_a_vencer_12m=477.716,
    aging_a_vencer_24m=256.018,
    aging_a_vencer_36m_mais=353.565,
    divida_bruta=613.352,
    divida_venc_12m=124.271,
    divida_venc_24m=137.788,
    divida_venc_24m_mais=351.293,
    provisao_riscos_total=23.438,
    pro_soluto_saldo=632.7,
    pro_soluto_pct_carteira=49.5,
)
print(f"Cury 4T2023: {n} rows")

# ===================================================================
# MRV 3T2024 (30/09/2024)
# ===================================================================
n = update("MRV", "3T2024", "Consolidado",
    avp_recebiveis=454.424,
    recebiveis_circulante=3036.416,
    recebiveis_nao_circulante=3112.640,
    carteira_recebiveis_total=6149.056,
    pdd_provisao=444.258,
    aging_a_vencer_12m=4776.479,
    aging_a_vencer_24m=2280.831,
    aging_a_vencer_36m=1089.265,
    aging_a_vencer_36m_mais=1888.782,
    pecld_adicoes=326.689,
    pecld_reversoes=157.800,
    pecld_baixas=138.035,
    divida_bruta=8432.961,
    divida_venc_12m=3409.603,
    divida_venc_24m=2070.897,
    divida_venc_24m_mais=3481.276,
    cessao_passivo_total=3402.256,
    provisao_garantia_obra=317.446,
    provisao_riscos_civeis=61.589,
    provisao_riscos_trabalhistas=52.686,
    provisao_riscos_total=115.114,
    receita_fin_aplicacoes=151.422,
    receita_fin_recebiveis=92.930,
    despesa_fin_juros_divida=369.330,
    despesa_fin_cessao=327.938,
    despesa_fin_derivativos=-162.745,
    poc_medio=76.77,
)
print(f"MRV 3T2024: {n} rows")

# MRV 4T2023 (31/12/2023)
n = update("MRV", "4T2023", "Consolidado",
    avp_recebiveis=339.650,
    recebiveis_circulante=2593.205,
    recebiveis_nao_circulante=2433.792,
    carteira_recebiveis_total=5026.997,
    pdd_provisao=413.404,
    aging_a_vencer_12m=3782.755,
    aging_a_vencer_24m=1698.603,
    aging_a_vencer_36m=802.241,
    aging_a_vencer_36m_mais=1407.992,
    divida_bruta=7847.271,
    cessao_passivo_total=2034.761,
    provisao_garantia_obra=278.504,
    provisao_riscos_total=108.450,
)
print(f"MRV 4T2023: {n} rows")

# ===================================================================
# DIRECIONAL 3T2023 (30/09/2023)
# ===================================================================
n = update("Direcional", "3T2023", "Consolidado",
    recebiveis_unidades_concluidas=213.287,
    recebiveis_unidades_construcao=661.781,
    avp_recebiveis=38.775,
    recebiveis_circulante=407.922,
    recebiveis_nao_circulante=412.751,
    carteira_recebiveis_total=820.673,
    pdd_provisao=35.625,
    taxa_avp_aa=6.98,
    aging_vencido_90d=8.577,
    aging_vencido_180d=4.334,
    aging_vencido_180d_mais=28.952,
    aging_a_vencer_12m=366.059,
    aging_a_vencer_24m=189.732,
    aging_a_vencer_36m=90.239,
    aging_a_vencer_36m_mais=132.780,
    pecld_adicoes=20.215,
    pecld_reversoes=12.955,
    divida_bruta=1291.268,
    divida_cri=879.652,
    divida_debentures=274.144,
    divida_sfh_producao=153.089,
    cessao_passivo_total=112.094,
    divida_liquida=-88.785,
    divida_liquida_pl=-4.24,
    provisao_garantia_obra=41.516,
    provisao_riscos_civeis=24.361,
    provisao_riscos_trabalhistas=3.309,
    provisao_riscos_total=27.705,
    receita_fin_aplicacoes=94.007,
    despesa_fin_juros_divida=112.275,
    despesa_fin_cessao=8.472,
    despesa_fin_derivativos=11.676,
)
print(f"Direcional 3T2023: {n} rows")

# DIRECIONAL 4T2022 (31/12/2022)
n = update("Direcional", "4T2022", "Consolidado",
    recebiveis_unidades_concluidas=202.248,
    recebiveis_unidades_construcao=516.271,
    avp_recebiveis=32.340,
    recebiveis_circulante=363.372,
    recebiveis_nao_circulante=314.813,
    carteira_recebiveis_total=678.185,
    pdd_provisao=29.565,
    aging_vencido_90d=8.286,
    aging_vencido_180d=3.261,
    aging_vencido_180d_mais=24.466,
    aging_a_vencer_12m=327.359,
    aging_a_vencer_24m=180.487,
    aging_a_vencer_36m=56.051,
    aging_a_vencer_36m_mais=78.275,
    divida_bruta=1389.826,
    divida_cri=925.631,
    divida_debentures=363.937,
    divida_sfh_producao=115.006,
    cessao_passivo_total=88.320,
    divida_liquida=193.427,
    divida_liquida_pl=13.34,
    provisao_garantia_obra=42.279,
    provisao_riscos_total=26.198,
)
print(f"Direcional 4T2022: {n} rows")

# ===================================================================
# CYRELA 3T2024 (30/09/2024)
# ===================================================================
n = update("Cyrela", "3T2024", "Consolidado",
    recebiveis_unidades_concluidas=931.511,
    recebiveis_unidades_construcao=3832.758,
    avp_recebiveis=144.467,
    provisao_distratos=418.175,
    recebiveis_circulante=3351.383,
    recebiveis_nao_circulante=963.383,
    carteira_recebiveis_total=4314.766,
    pdd_provisao=54.992,
    taxa_avp_aa=6.66,
    aging_a_vencer_12m=4647.276,
    aging_a_vencer_24m=3446.072,
    aging_a_vencer_36m=2343.618,
    aging_a_vencer_36m_mais=1551.785,
    pecld_adicoes=17.403,
    pecld_reversoes=15.453,
    pecld_baixas=2.752,
    divida_bruta=5467.289,
    divida_debentures=199.365,
    divida_cri=3263.036,
    divida_sfh_producao=2003.924,
    divida_liquida=256.149,
    provisao_garantia_obra=174.736,
    provisao_riscos_civeis=150.809,
    provisao_riscos_trabalhistas=80.968,
    provisao_riscos_total=238.100,
    receita_fin_aplicacoes=469.056,
    despesa_fin_juros_divida=425.523,
    despesa_fin_derivativos=-14.632,
    encargos_capitalizados=82.728,
)
print(f"Cyrela 3T2024: {n} rows")

# CYRELA 4T2023 (31/12/2023)
n = update("Cyrela", "4T2023", "Consolidado",
    recebiveis_unidades_concluidas=1146.874,
    recebiveis_unidades_construcao=2731.815,
    avp_recebiveis=102.291,
    provisao_distratos=373.228,
    recebiveis_circulante=2857.730,
    recebiveis_nao_circulante=596.982,
    carteira_recebiveis_total=3454.712,
    pdd_provisao=55.794,
    taxa_avp_aa=5.97,
    aging_a_vencer_12m=4238.975,
    aging_a_vencer_24m=2411.491,
    aging_a_vencer_36m=2325.379,
    aging_a_vencer_36m_mais=952.912,
    divida_bruta=5157.527,
    divida_debentures=965.831,
    divida_cri=2196.809,
    divida_sfh_producao=1994.898,
    divida_liquida=470.581,
    provisao_garantia_obra=138.629,
    provisao_riscos_total=215.188,
)
print(f"Cyrela 4T2023: {n} rows")

# ===================================================================
# PLANOEPLANO 3T2024 (30/09/2024)
# ===================================================================
n = update("PlanoePlano", "3T2024", "Consolidado",
    recebiveis_unidades_concluidas=314.194,
    recebiveis_unidades_construcao=700.608,
    avp_recebiveis=29.067,
    provisao_distratos=90.754,
    recebiveis_circulante=615.667,
    recebiveis_nao_circulante=235.357,
    carteira_recebiveis_total=851.024,
    pdd_provisao=43.957,
    taxa_avp_aa=7.35,
    aging_vencido_90d=44.351,
    aging_vencido_180d=37.056,
    aging_vencido_180d_mais=21.093,
    aging_a_vencer_12m=635.567,
    aging_a_vencer_36m=220.987,
    aging_a_vencer_36m_mais=55.748,
    pecld_adicoes=29.205,
    pecld_reversoes=26.568,
    divida_bruta=593.947,
    divida_cri=364.818,
    divida_sfh_producao=221.003,
    divida_venc_12m=1.442,
    divida_liquida=-96.929,
    provisao_garantia_obra=39.765,
    provisao_riscos_civeis=43,
    provisao_riscos_trabalhistas=4.109,
    provisao_riscos_total=4.166,
    receita_fin_aplicacoes=22.955,
    despesa_fin_juros_divida=21.941,
    despesa_fin_derivativos=-8.026,
    encargos_capitalizados=20.745,
)
print(f"PlanoePlano 3T2024: {n} rows")

# PLANOEPLANO 4T2023 (31/12/2023)
n = update("PlanoePlano", "4T2023", "Consolidado",
    recebiveis_unidades_concluidas=265.809,
    recebiveis_unidades_construcao=446.380,
    avp_recebiveis=21.016,
    provisao_distratos=79.477,
    recebiveis_circulante=418.485,
    recebiveis_nao_circulante=151.891,
    carteira_recebiveis_total=570.376,
    pdd_provisao=41.320,
    aging_vencido_90d=58.399,
    aging_vencido_180d=24.832,
    aging_vencido_180d_mais=20.923,
    aging_a_vencer_12m=414.627,
    aging_a_vencer_36m=145.109,
    aging_a_vencer_36m_mais=48.299,
    divida_bruta=387.535,
    divida_sfh_producao=387.535,
    divida_liquida=-38.501,
    provisao_garantia_obra=32.310,
    provisao_riscos_total=4.119,
)
print(f"PlanoePlano 4T2023: {n} rows")

conn.commit()

# Summary
cur.execute("""SELECT empresa, periodo,
    CASE WHEN carteira_recebiveis_total IS NOT NULL THEN 1 ELSE 0 END as has_cart,
    CASE WHEN pdd_provisao IS NOT NULL THEN 1 ELSE 0 END as has_pdd,
    CASE WHEN aging_a_vencer_12m IS NOT NULL THEN 1 ELSE 0 END as has_aging,
    CASE WHEN provisao_riscos_total IS NOT NULL THEN 1 ELSE 0 END as has_prov,
    CASE WHEN cessao_passivo_total IS NOT NULL THEN 1 ELSE 0 END as has_cess
    FROM dados_trimestrais WHERE segmento='Consolidado'
    ORDER BY empresa, periodo""")
rows = cur.fetchall()
print("\n=== COBERTURA (Consolidado) ===")
print(f"{'Empresa':15s} {'Periodo':8s} Cart PDD  Age  Prov Cess")
for r in rows:
    marks = ''.join(['X' if v else '.' for v in r[2:]])
    print(f"{r[0]:15s} {r[1]:8s}  {marks}")

# Count filled
cur.execute("""SELECT COUNT(*) FROM dados_trimestrais WHERE segmento='Consolidado'
    AND carteira_recebiveis_total IS NOT NULL""")
filled = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM dados_trimestrais WHERE segmento='Consolidado'")
total = cur.fetchone()[0]
print(f"\nPeriodos com dados de recebiveis: {filled}/{total}")

conn.close()

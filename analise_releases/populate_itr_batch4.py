"""
Batch 4 - ITR data from 1T2022 and 3T2021 ITRs.
All values from ITR are in R$ mil. DB stores R$ milhoes.
E.g., 664.243 R$ mil = 664.243 as float in DB.
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
# TENDA 1T2022 (from ITR 1T2022, balance date 31/03/2022)
# ============================================================
n = update("Tenda", "1T2022", "Consolidado",
    recebiveis_circulante=664.243,
    recebiveis_nao_circulante=475.860,
    avp_recebiveis=-31.598,
    pdd_provisao=-211.773,
    provisao_distratos=-48.280,
    # Aging
    aging_vencido_90d=34.817,
    aging_vencido_180d=9.333,
    aging_vencido_180d_mais=110.532,
    aging_a_vencer_12m=778.151,
    aging_a_vencer_24m=333.850,
    aging_a_vencer_36m=68.009,
    aging_a_vencer_36m_mais=97.062,  # 34536+62526
    # PECLD movement (1T2022)
    pecld_adicoes=-20.378,
    pecld_reversoes=0.117,
    pecld_baixas=7.342,
    # Divida
    divida_debentures=976.182,
    divida_venc_12m=436.633,
    divida_venc_24m_mais=948.626,
    divida_bruta=1385.259,
    # Caixa
    caixa_aplicacoes=803.390,  # 35651+767739
    # Provisoes
    provisao_riscos_civeis=52.204,
    provisao_riscos_trabalhistas=6.888,
    provisao_riscos_total=72.901,
    # Resultado financeiro (1T2022 trimestral)
    receitas_financeiras=17.433,
    despesas_financeiras=-54.949,
    resultado_financeiro=-37.516,
    receita_fin_aplicacoes=17.557,
    encargos_capitalizados=14.522,
)
print(f"Tenda 1T2022: {n} row(s)")
count += n

# Tenda 4T2021 - add missing fields from comparative
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Tenda' AND periodo='4T2021' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Tenda", "4T2021", "Consolidado",
        avp_recebiveis=-30.534,
        provisao_distratos=-46.328,
        aging_vencido_90d=19.399,
        aging_vencido_180d=26.589,
        aging_vencido_180d_mais=109.175,
        aging_a_vencer_12m=699.186,
        aging_a_vencer_24m=328.594,
        aging_a_vencer_36m=92.276,
        aging_a_vencer_36m_mais=94.468,  # 34778+59690
        divida_debentures=974.747,
        divida_venc_12m=325.646,
        divida_venc_24m_mais=1051.903,
        divida_bruta=1377.549,
        caixa_aplicacoes=1064.944,
        provisao_riscos_civeis=56.908,
        provisao_riscos_trabalhistas=6.311,
        provisao_riscos_total=76.855,
    )
    print(f"Tenda 4T2021 (extra): {n} row(s)")
    count += n

# ============================================================
# CURY 1T2022 (from ITR 1T2022, balance date 31/03/2022)
# ============================================================
n = update("Cury", "1T2022", "Consolidado",
    recebiveis_circulante=996.524,
    recebiveis_nao_circulante=634.055,
    recebiveis_unidades_concluidas=326.507,
    recebiveis_unidades_construcao=1320.412,
    avp_recebiveis=-3.975,
    pdd_provisao=-63.835,
    provisao_distratos=-31.422,
    # Aging
    aging_a_vencer_12m=943.274,
    aging_a_vencer_24m=241.949,
    aging_a_vencer_36m_mais=392.106,
    aging_vencido_90d=41.276,   # 9108+32168
    aging_vencido_180d_mais=111.206,
    # PECLD movement (1T2022)
    pecld_adicoes=-9.292,
    pecld_reversoes=1.360,  # "baixas e reversoes" combined
    # Divida
    divida_venc_12m=67.654,
    divida_venc_24m_mais=323.208,
    divida_bruta=390.862,
    # Caixa
    caixa_aplicacoes=602.568,  # 360921+241647
    # Provisoes
    provisao_garantia_obra=15.037,
    provisao_riscos_civeis=6.305,
    provisao_riscos_trabalhistas=13.913,
    provisao_riscos_total=20.218,
    # Resultado financeiro (1T2022 trimestral)
    receitas_financeiras=12.977,
    despesas_financeiras=-17.062,
    resultado_financeiro=-4.085,
    receita_fin_aplicacoes=10.717,
    despesa_fin_juros_divida=-10.319,
    encargos_capitalizados=0,  # not capitalized separately
)
print(f"Cury 1T2022: {n} row(s)")
count += n

# Cury 4T2021 comparative - add missing
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Cury' AND periodo='4T2021' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cury", "4T2021", "Consolidado",
        recebiveis_unidades_concluidas=339.816,
        recebiveis_unidades_construcao=936.038,
        avp_recebiveis=-3.109,
        provisao_distratos=-29.850,
        aging_a_vencer_12m=587.218,
        aging_a_vencer_24m=240.934,
        aging_a_vencer_36m_mais=305.382,
        aging_vencido_90d=68.196,  # 33249+34947
        aging_vencido_180d_mais=164.797,
        divida_venc_12m=68.020,
        divida_venc_24m_mais=323.208,
        divida_bruta=391.228,
        caixa_aplicacoes=594.487,
        provisao_garantia_obra=13.220,
        provisao_riscos_civeis=6.924,
        provisao_riscos_trabalhistas=14.155,
        provisao_riscos_total=21.079,
    )
    print(f"Cury 4T2021 (extra): {n} row(s)")
    count += n

# ============================================================
# CYRELA 1T2022 (from ITR 1T2022, balance date 31/03/2022)
# ============================================================
n = update("Cyrela", "1T2022", "Consolidado",
    recebiveis_circulante=1844.753,
    recebiveis_nao_circulante=557.537,
    recebiveis_unidades_concluidas=899.144,
    recebiveis_unidades_construcao=1908.114,  # net of AVP
    avp_recebiveis=-51.314,
    pdd_provisao=-53.317,
    provisao_distratos=-361.520,
    # PECLD movement (1T2022)
    pecld_adicoes=9.418,
    pecld_reversoes=-14.833,
    pecld_baixas=-0.130,
    # Aging - cronograma
    aging_a_vencer_12m=2916.905,
    aging_a_vencer_24m=1737.250,
    aging_a_vencer_36m=1801.493,
    aging_a_vencer_36m_mais=345.169,  # 284547+60622
    # Divida - from BP
    divida_venc_12m=795.260,
    divida_venc_24m_mais=2875.693,
    divida_bruta=3648.148,  # emprest+financ+debentures+CRI
    divida_debentures=784.472,
    divida_cri=1444.226,
    divida_sfh_producao=822.041,  # financiamentos principal
    # Caixa
    caixa_aplicacoes=3241.434,
    # Provisoes
    provisao_garantia_obra=99.390,
    provisao_riscos_civeis=142.730,
    provisao_riscos_trabalhistas=90.014,
    provisao_riscos_total=246.484,
    # Resultado financeiro (1T2022 trimestral)
    receitas_financeiras=95.875,
    despesas_financeiras=-86.542,
    resultado_financeiro=9.333,
    receita_fin_aplicacoes=107.080,
    despesa_fin_juros_divida=-81.784,  # SFH+emprestimos (19115+62669)
    despesa_fin_derivativos=-5.072,
    encargos_capitalizados=13.289,
)
print(f"Cyrela 1T2022: {n} row(s)")
count += n

# Cyrela 4T2021 comparative
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Cyrela' AND periodo='4T2021' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cyrela", "4T2021", "Consolidado",
        recebiveis_unidades_concluidas=906.394,
        recebiveis_unidades_construcao=1802.251,
        avp_recebiveis=-49.226,
        provisao_distratos=-320.660,
        divida_venc_12m=719.738,
        divida_venc_24m_mais=2921.850,
        divida_bruta=3625.470,
        divida_debentures=762.661,
        divida_cri=1475.475,
        caixa_aplicacoes=3298.793,
        provisao_garantia_obra=93.680,
        provisao_riscos_civeis=120.561,
        provisao_riscos_trabalhistas=90.465,
        provisao_riscos_total=224.364,
    )
    print(f"Cyrela 4T2021 (extra): {n} row(s)")
    count += n

# ============================================================
# DIRECIONAL 1T2022 (from ITR 1T2022, balance date 31/03/2022)
# ============================================================
n = update("Direcional", "1T2022", "Consolidado",
    recebiveis_circulante=307.780,
    recebiveis_nao_circulante=225.808,
    recebiveis_unidades_concluidas=243.259,
    recebiveis_unidades_construcao=312.686,
    avp_recebiveis=-14.360,
    pdd_provisao=-25.942,
    # PECLD movement (1T2022)
    pecld_adicoes=-6.750,
    pecld_reversoes=6.344,
    # Aging circulante
    aging_a_vencer_12m=270.143,
    aging_vencido_90d=6.406,    # 3423+1335+1648
    aging_vencido_180d=4.180,   # 1855+2325
    aging_vencido_180d_mais=27.051,
    # Aging nao circulante
    aging_a_vencer_24m=128.829,
    aging_a_vencer_36m=53.870,
    aging_a_vencer_36m_mais=43.109,  # 14161+28948
    # Divida
    divida_cri=742.816,
    divida_sfh_producao=96.616,
    divida_debentures=354.147,
    divida_bruta=1290.949,
    divida_venc_12m=178.275,
    divida_venc_24m_mais=1112.674,
    # Caixa
    caixa_aplicacoes=753.588,
    # Provisoes
    provisao_garantia_obra=41.233,
    provisao_riscos_civeis=18.866,
    provisao_riscos_trabalhistas=6.736,
    provisao_riscos_total=25.704,
    # Resultado financeiro (1T2022 trimestral)
    receitas_financeiras=36.306,
    despesas_financeiras=-62.140,
    resultado_financeiro=-25.834,
    receita_fin_aplicacoes=18.514,
    despesa_fin_juros_divida=-40.506,
    despesa_fin_cessao=-2.312,
    despesa_fin_derivativos=-11.037,
    encargos_capitalizados=-1.140,
)
print(f"Direcional 1T2022: {n} row(s)")
count += n

# Direcional 4T2021 comparative - update missing fields
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Direcional' AND periodo='4T2021' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Direcional", "4T2021", "Consolidado",
        recebiveis_unidades_concluidas=170.034,
        recebiveis_unidades_construcao=329.174,
        avp_recebiveis=-14.741,
        aging_a_vencer_12m=190.501,
        aging_vencido_90d=11.269,  # 8847+1486+936
        aging_vencido_180d=2.375,  # 1066+1309
        aging_vencido_180d_mais=27.616,
        aging_a_vencer_24m=153.089,
        aging_a_vencer_36m=52.957,
        aging_a_vencer_36m_mais=38.539,  # 12548+25991
        divida_cri=728.884,
        divida_sfh_producao=42.172,
        divida_debentures=357.133,
        divida_bruta=1241.402,
        divida_venc_12m=127.205,
        divida_venc_24m_mais=1114.197,
        caixa_aplicacoes=723.954,
        provisao_garantia_obra=40.194,
        provisao_riscos_civeis=16.398,
        provisao_riscos_trabalhistas=7.508,
        provisao_riscos_total=24.008,
    )
    print(f"Direcional 4T2021 (extra): {n} row(s)")
    count += n

# ============================================================
# MRV 3T2021 (from ITR 3T2021, balance date 30/09/2021)
# ============================================================
n = update("MRV", "3T2021", "Consolidado",
    recebiveis_circulante=2148.665,
    recebiveis_nao_circulante=1570.325,
    avp_recebiveis=-50.419,
    pdd_provisao=-303.657,
    # PECLD movement (9M2021)
    pecld_adicoes=-161.227,
    pecld_baixas=39.581,
    pecld_reversoes=99.256,  # "recebimentos/reversoes"
    # Aging - expectativa recebimento
    aging_a_vencer_12m=3472.712,
    aging_a_vencer_24m=1682.586,
    aging_a_vencer_36m=479.180,
    aging_a_vencer_36m_mais=405.325,  # 239079+166246
    # Divida
    divida_bruta=4952.163,
    divida_venc_12m=964.195,
    divida_venc_24m_mais=3987.968,
    divida_venc_24m=1234.124,  # 13-24m
    # Caixa
    caixa_aplicacoes=2814.305,  # 1207325+1606980
    # Provisoes
    provisao_garantia_obra=189.851,
    provisao_riscos_civeis=52.176,
    provisao_riscos_trabalhistas=40.805,
    provisao_riscos_total=93.216,
    # Cessao - CRI dentro da divida
    divida_cri=1081.304,
    # Resultado financeiro (3T2021 trimestral)
    receitas_financeiras=39.281,
    despesas_financeiras=-115.494,
    resultado_financeiro=-49.994,
    receita_fin_aplicacoes=29.616,
    receita_fin_recebiveis=26.219,
    despesa_fin_juros_divida=-20.411,
    despesa_fin_cessao=-23.325,
    despesa_fin_derivativos=-60.004,
    encargos_capitalizados=58.994,  # 51396+7598 (imoveis+PPI)
)
print(f"MRV 3T2021: {n} row(s)")
count += n

# MRV 4T2020 comparative
n = update("MRV", "4T2020", "Consolidado",
    recebiveis_circulante=1840.376,
    recebiveis_nao_circulante=1641.094,
    avp_recebiveis=-59.532,
    pdd_provisao=-281.267,
    aging_a_vencer_12m=3088.203,
    aging_a_vencer_24m=2023.912,
    aging_a_vencer_36m=456.479,
    aging_a_vencer_36m_mais=424.678,  # 272465+152213
    divida_bruta=4651.531,
    divida_venc_12m=687.520,
    divida_venc_24m=965.330,
    divida_venc_24m_mais=3964.011,
    divida_cri=660.890,
    caixa_aplicacoes=2694.633,
    provisao_garantia_obra=165.899,
    provisao_riscos_civeis=53.979,
    provisao_riscos_trabalhistas=48.053,
    provisao_riscos_total=102.144,
)
print(f"MRV 4T2020: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

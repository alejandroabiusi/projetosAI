"""
Batch 8 - ITR data from 2T2022 ITRs + MRV 1T2021.
All values in R$ mil = R$ milhoes as float (divide agent R$ mil integers by 1000).
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
# MRV 1T2021 (balance date 31/03/2021)
# ============================================================
n = update("MRV", "1T2021", "Consolidado",
    recebiveis_circulante=1957.948,
    recebiveis_nao_circulante=1621.637,
    avp_recebiveis=-57.511,
    pdd_provisao=-308.480,
    # PECLD movement (1T2021)
    pecld_adicoes=-73.460,
    pecld_reversoes=32.137,
    pecld_baixas=14.110,
    # Aging
    aging_a_vencer_12m=3332.476,
    aging_a_vencer_24m=1943.038,
    aging_a_vencer_36m=482.590,
    aging_a_vencer_36m_mais=397.236,
    # Divida
    divida_bruta=5011.272,
    divida_cri=722.742,
    divida_venc_12m=964.422,
    divida_venc_24m_mais=4046.850,
    # Caixa
    caixa_aplicacoes=2489.438,
    # Provisoes
    provisao_garantia_obra=166.849,
    provisao_riscos_civeis=52.546,
    provisao_riscos_trabalhistas=41.115,
    provisao_riscos_total=93.805,
    # Resultado financeiro (1T2021 trimestral)
    receitas_financeiras=27.712,
    despesas_financeiras=-25.421,
    resultado_financeiro=35.418,
    receita_fin_recebiveis=33.127,
    despesa_fin_juros_divida=-16.708,
    encargos_capitalizados=29.423,
)
print(f"MRV 1T2021: {n} row(s)")
count += n

# MRV 4T2020 comparative
cur.execute("SELECT recebiveis_circulante FROM dados_trimestrais WHERE empresa='MRV' AND periodo='4T2020' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("MRV", "4T2020", "Consolidado",
        recebiveis_circulante=1840.376,
        recebiveis_nao_circulante=1641.094,
        avp_recebiveis=-59.532,
        pdd_provisao=-281.267,
        aging_a_vencer_12m=3088.203,
        aging_a_vencer_24m=2023.912,
        aging_a_vencer_36m=456.479,
        aging_a_vencer_36m_mais=424.678,
        divida_bruta=4651.531,
        divida_cri=720.905,
        divida_venc_12m=687.520,
        divida_venc_24m_mais=3964.011,
        caixa_aplicacoes=2694.633,
        provisao_garantia_obra=165.899,
        provisao_riscos_civeis=53.979,
        provisao_riscos_trabalhistas=48.053,
        provisao_riscos_total=102.144,
    )
    print(f"MRV 4T2020 (comparative): {n} row(s)")
    count += n

# ============================================================
# TENDA 2T2022 (balance date 30/06/2022)
# ============================================================
n = update("Tenda", "2T2022", "Consolidado",
    recebiveis_circulante=636.131,
    recebiveis_nao_circulante=500.634,
    avp_recebiveis=-38.603,
    pdd_provisao=-236.947,
    # Aging
    aging_vencido_90d=34.620,
    aging_vencido_180d=23.553,
    aging_vencido_180d_mais=102.450,
    aging_a_vencer_12m=741.468,
    aging_a_vencer_24m=370.281,
    aging_a_vencer_36m=57.954,
    aging_a_vencer_36m_mais=112.892,
    # PECLD movement (1S2022)
    pecld_adicoes=-52.968,
    pecld_reversoes=0.677,
    pecld_baixas=14.198,
    # Divida
    divida_debentures=996.514,
    divida_bruta=1485.834,
    divida_venc_12m=498.324,
    divida_venc_24m_mais=987.510,
    # Caixa
    caixa_aplicacoes=823.949,
    # Provisoes
    provisao_riscos_civeis=56.930,
    provisao_riscos_trabalhistas=8.593,
    provisao_riscos_total=80.986,
    # Resultado financeiro (2T2022 trimestral)
    receitas_financeiras=16.537,
    despesas_financeiras=-48.612,
    resultado_financeiro=-32.075,
    encargos_capitalizados=25.940,
)
print(f"Tenda 2T2022: {n} row(s)")
count += n

# ============================================================
# CURY 2T2022 (balance date 30/06/2022)
# ============================================================
n = update("Cury", "2T2022", "Consolidado",
    recebiveis_circulante=1004.386,
    recebiveis_nao_circulante=552.406,
    recebiveis_unidades_concluidas=269.666,
    recebiveis_unidades_construcao=1318.163,
    avp_recebiveis=-4.464,
    pdd_provisao=-66.637,
    # Aging
    aging_a_vencer_12m=998.582,
    aging_a_vencer_24m=220.549,
    aging_a_vencer_36m_mais=331.856,
    aging_vencido_90d=17.751,
    aging_vencido_180d_mais=94.884,
    # PECLD movement (1S2022)
    pecld_adicoes=-15.344,
    pecld_reversoes=0.532,
    pecld_baixas=4.078,
    # Divida
    divida_bruta=398.972,
    divida_venc_12m=75.764,
    divida_venc_24m_mais=323.208,
    # Caixa
    caixa_aplicacoes=679.977,
    # Provisoes
    provisao_garantia_obra=16.723,
    provisao_riscos_civeis=5.844,
    provisao_riscos_trabalhistas=14.255,
    provisao_riscos_total=20.099,
    # Resultado financeiro (2T2022 trimestral)
    receitas_financeiras=17.605,
    despesas_financeiras=-22.214,
    resultado_financeiro=-4.609,
)
print(f"Cury 2T2022: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2022 (balance date 30/06/2022)
# ============================================================
n = update("Direcional", "2T2022", "Consolidado",
    recebiveis_circulante=351.399,
    recebiveis_nao_circulante=205.210,
    recebiveis_unidades_concluidas=219.559,
    recebiveis_unidades_construcao=358.503,
    avp_recebiveis=-14.427,
    pdd_provisao=-30.077,
    # PECLD movement (1S2022)
    pecld_adicoes=-14.489,
    pecld_reversoes=9.947,
    # Aging circulante
    aging_a_vencer_12m=312.502,
    aging_vencido_90d=5.276,
    aging_vencido_180d=3.247,
    aging_vencido_180d_mais=30.374,
    # Aging nao circulante
    aging_a_vencer_24m=109.356,
    aging_a_vencer_36m=47.791,
    aging_a_vencer_36m_mais=48.063,
    # Divida
    divida_cri=704.108,
    divida_sfh_producao=95.892,
    divida_debentures=361.356,
    divida_bruta=1174.460,
    divida_venc_12m=139.362,
    divida_venc_24m_mais=1035.098,
    # Caixa
    caixa_aplicacoes=959.132,
    # Provisoes
    provisao_garantia_obra=42.539,
    provisao_riscos_civeis=19.607,
    provisao_riscos_trabalhistas=6.112,
    provisao_riscos_total=25.857,
    # Resultado financeiro (2T2022 trimestral)
    receitas_financeiras=34.225,
    despesas_financeiras=-66.752,
    resultado_financeiro=-32.527,
    receita_fin_aplicacoes=20.029,
    despesa_fin_juros_divida=-45.705,
    despesa_fin_cessao=-4.302,
    despesa_fin_derivativos=-13.876,
)
print(f"Direcional 2T2022: {n} row(s)")
count += n

# ============================================================
# MRV 2T2022 (balance date 30/06/2022)
# ============================================================
n = update("MRV", "2T2022", "Consolidado",
    recebiveis_circulante=2493.686,
    recebiveis_nao_circulante=1606.695,
    avp_recebiveis=-91.953,
    pdd_provisao=-380.884,
    # PECLD movement (1S2022)
    pecld_adicoes=-182.511,
    pecld_reversoes=100.622,
    pecld_baixas=48.753,
    # Aging
    aging_a_vencer_12m=3538.179,
    aging_a_vencer_24m=1419.409,
    aging_a_vencer_36m=514.597,
    aging_a_vencer_36m_mais=447.055,
    # Divida
    divida_bruta=6199.152,
    divida_cri=1733.991,
    divida_venc_12m=987.093,
    divida_venc_24m_mais=5502.718,
    # Caixa
    caixa_aplicacoes=3724.901,
    # Provisoes
    provisao_garantia_obra=220.023,
    provisao_riscos_civeis=46.415,
    provisao_riscos_trabalhistas=36.100,
    provisao_riscos_total=82.920,
    # Cessao
    cessao_passivo_total=438.630,
    # Resultado financeiro (2T2022 trimestral)
    receitas_financeiras=65.210,
    despesas_financeiras=-228.992,
    resultado_financeiro=-127.238,
    receita_fin_recebiveis=36.544,
    despesa_fin_juros_divida=-58.304,
    despesa_fin_cessao=-1.870,
    despesa_fin_derivativos=-158.035,
    encargos_capitalizados=100.201,
)
print(f"MRV 2T2022: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

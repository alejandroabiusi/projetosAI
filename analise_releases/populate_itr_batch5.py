"""
Batch 5 - ITR data from 3T2021, 3T2020 ITRs.
All values in R$ mil = R$ milhoes as float.
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
# TENDA 3T2021 (balance date 30/09/2021)
# ============================================================
n = update("Tenda", "3T2021", "Consolidado",
    recebiveis_circulante=737.702,
    recebiveis_nao_circulante=511.651,
    avp_recebiveis=-12.792,
    pdd_provisao=-208.706,
    provisao_distratos=-43.092,
    # Aging
    aging_vencido_90d=24.577,
    aging_vencido_180d=9.031,
    aging_vencido_180d_mais=55.526,
    aging_a_vencer_12m=711.453,  # 2021(134218) + 2022 partial approximation
    aging_a_vencer_24m=577.235,  # 2022
    aging_a_vencer_36m=513.204,  # 2023
    aging_a_vencer_36m_mais=200.152,  # 75736+124416 (2024+2025+)
    # PECLD movement (9M2021)
    pecld_adicoes=-79.637,
    pecld_reversoes=7.957,
    pecld_baixas=37.362,
    # Divida
    divida_debentures=1017.657,
    divida_venc_12m=313.741,
    divida_venc_24m_mais=1026.080,
    divida_bruta=1339.821,
    # Caixa
    caixa_aplicacoes=1041.819,
    # Provisoes
    provisao_riscos_civeis=48.004,
    provisao_riscos_trabalhistas=5.569,
    provisao_riscos_total=65.374,
    # Resultado financeiro (3T2021 trimestral)
    receitas_financeiras=9.879,
    despesas_financeiras=-21.842,
    resultado_financeiro=-11.963,
    receita_fin_aplicacoes=10.219,
    encargos_capitalizados=12.146,
)
print(f"Tenda 3T2021: {n} row(s)")
count += n

# Tenda 4T2020 comparative
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Tenda' AND periodo='4T2020' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Tenda", "4T2020", "Consolidado",
        avp_recebiveis=-5.181,
        provisao_distratos=-29.194,
        divida_debentures=822.576,
        divida_venc_12m=599.666,
        divida_venc_24m_mais=557.451,
        divida_bruta=1157.117,
        caixa_aplicacoes=1305.454,
        provisao_riscos_civeis=57.636,
        provisao_riscos_trabalhistas=5.698,
        provisao_riscos_total=70.087,
    )
    print(f"Tenda 4T2020 (extra): {n} row(s)")
    count += n

# ============================================================
# CURY 3T2021 (balance date 30/09/2021)
# ============================================================
n = update("Cury", "3T2021", "Consolidado",
    recebiveis_circulante=700.282,
    recebiveis_nao_circulante=406.000,
    recebiveis_unidades_concluidas=280.313,
    recebiveis_unidades_construcao=830.057,
    avp_recebiveis=-3.665,
    pdd_provisao=-54.899,
    provisao_distratos=-28.815,
    # Aging
    aging_a_vencer_12m=589.240,
    aging_a_vencer_24m=152.154,
    aging_a_vencer_36m_mais=253.846,
    aging_vencido_90d=56.024,  # 14857+41167
    aging_vencido_180d_mais=142.397,
    # PECLD movement (9M2021)
    pecld_adicoes=-11.769,
    pecld_reversoes=1.259,  # baixas+reversoes combined
    # Divida
    divida_venc_12m=63.863,
    divida_venc_24m_mais=326.361,
    divida_bruta=390.224,
    # Caixa
    caixa_aplicacoes=554.927,
    # Provisoes
    provisao_garantia_obra=11.427,
    provisao_riscos_civeis=7.682,
    provisao_riscos_trabalhistas=14.693,
    provisao_riscos_total=22.375,
    # Resultado financeiro (3T2021 trimestral)
    receitas_financeiras=10.356,
    despesas_financeiras=-11.427,
    resultado_financeiro=-1.071,
    receita_fin_aplicacoes=4.718,
    despesa_fin_juros_divida=-5.267,
)
print(f"Cury 3T2021: {n} row(s)")
count += n

# Cury 4T2020 comparative
cur.execute("SELECT divida_bruta FROM dados_trimestrais WHERE empresa='Cury' AND periodo='4T2020' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cury", "4T2020", "Consolidado",
        recebiveis_unidades_concluidas=270.372,
        recebiveis_unidades_construcao=509.485,
        avp_recebiveis=-1.554,
        provisao_distratos=-16.767,
        aging_a_vencer_12m=516.948,
        aging_a_vencer_24m=55.466,
        aging_a_vencer_36m_mais=116.613,
        aging_vencido_90d=67.523,  # 37375+30148
        aging_vencido_180d_mais=105.482,
        divida_venc_12m=91.704,
        divida_venc_24m_mais=184.999,
        divida_bruta=276.703,
        caixa_aplicacoes=430.898,
        provisao_garantia_obra=9.174,
        provisao_riscos_civeis=9.574,
        provisao_riscos_trabalhistas=17.760,
        provisao_riscos_total=27.334,
    )
    print(f"Cury 4T2020 (extra): {n} row(s)")
    count += n

# ============================================================
# DIRECIONAL 3T2020 (balance date 30/09/2020)
# ============================================================
n = update("Direcional", "3T2020", "Consolidado",
    recebiveis_circulante=253.228,
    recebiveis_nao_circulante=178.104,
    recebiveis_unidades_concluidas=184.665,
    recebiveis_unidades_construcao=247.268,
    avp_recebiveis=-6.854,
    pdd_provisao=-24.788,
    # Aging circulante
    aging_a_vencer_12m=216.051,
    aging_vencido_90d=17.749,  # 16508+749+492
    aging_vencido_180d=1.463,  # 455+1008
    aging_vencido_180d_mais=17.965,
    # Aging nao circulante
    aging_a_vencer_24m=88.672,
    aging_a_vencer_36m=61.511,
    aging_a_vencer_36m_mais=27.921,  # 13142+14779
    # Divida
    divida_cri=686.977,
    divida_sfh_producao=18.174,
    divida_debentures=248.681,
    divida_bruta=1121.350,
    divida_venc_12m=243.292,
    divida_venc_24m_mais=878.058,
    # Caixa
    caixa_aplicacoes=1093.709,  # 886565+207144
    # Provisoes
    provisao_garantia_obra=41.811,
    provisao_riscos_civeis=18.796,
    provisao_riscos_trabalhistas=9.304,
    provisao_riscos_total=28.212,
    # Resultado financeiro (3T2020 trimestral)
    receitas_financeiras=8.381,
    despesas_financeiras=-16.297,
    resultado_financeiro=-7.916,
    receita_fin_aplicacoes=3.105,
    despesa_fin_juros_divida=-10.115,
    despesa_fin_derivativos=-0.107,
    despesa_fin_cessao=0,  # no cessao this period
)
print(f"Direcional 3T2020: {n} row(s)")
count += n

# ============================================================
# CYRELA 3T2021 (balance date 30/09/2021)
# ============================================================
n = update("Cyrela", "3T2021", "Consolidado",
    recebiveis_circulante=1649.122,
    recebiveis_nao_circulante=612.418,
    recebiveis_unidades_concluidas=824.961,
    recebiveis_unidades_construcao=1824.465,  # net of AVP
    avp_recebiveis=-41.607,
    pdd_provisao=-60.186,
    # PECLD movement (9M2021)
    pecld_adicoes=49.617,
    pecld_reversoes=-10.203,
    pecld_baixas=-2.104,
    # Aging cronograma
    aging_a_vencer_12m=2606.324,
    aging_a_vencer_24m=1615.074,
    aging_a_vencer_36m=1390.923,
    aging_a_vencer_36m_mais=445.462,  # 378447+67015
    # Divida
    divida_debentures=770.203,
    divida_cri=1481.230,
    divida_sfh_producao=477.141,  # financiamentos
    divida_bruta=3402.960,
    divida_venc_12m=601.580,
    divida_venc_24m_mais=2801.380,
    # Caixa
    caixa_aplicacoes=2994.496,
    # Provisoes
    provisao_garantia_obra=91.745,
    provisao_riscos_civeis=121.128,
    provisao_riscos_trabalhistas=90.317,
    provisao_riscos_total=222.535,
    # Resultado financeiro (3T2021 trimestral)
    receitas_financeiras=71.474,
    despesas_financeiras=-62.615,
    resultado_financeiro=8.859,
    receita_fin_aplicacoes=61.148,
    despesa_fin_juros_divida=-43.992,  # SFH+emprestimos (7345+36647)
    despesa_fin_derivativos=-16.088,
    encargos_capitalizados=5.477,
)
print(f"Cyrela 3T2021: {n} row(s)")
count += n

# Cyrela 4T2020 comparative
n = update("Cyrela", "4T2020", "Consolidado",
    recebiveis_circulante=1355.208,
    recebiveis_nao_circulante=708.346,
    recebiveis_unidades_concluidas=829.785,
    recebiveis_unidades_construcao=1535.739,
    avp_recebiveis=-26.132,
    pdd_provisao=-22.876,
    aging_a_vencer_12m=2017.830,
    aging_a_vencer_24m=1619.013,
    aging_a_vencer_36m=1407.936,
    aging_a_vencer_36m_mais=340.048,  # 248444+91604
    divida_debentures=5.886,
    divida_cri=1488.497,
    divida_bruta=2703.004,
    divida_venc_12m=563.396,
    divida_venc_24m_mais=2139.609,
    caixa_aplicacoes=2401.337,
    provisao_garantia_obra=80.911,
    provisao_riscos_civeis=94.002,
    provisao_riscos_trabalhistas=87.103,
    provisao_riscos_total=188.725,
)
print(f"Cyrela 4T2020: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

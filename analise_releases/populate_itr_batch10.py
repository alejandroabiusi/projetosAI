"""
Batch 10 - ITR data from 2T2020, 3T2020, 1T2021 ITRs.
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
# CURY 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("Cury", "2T2020", "Consolidado",
    recebiveis_circulante=528.330,
    recebiveis_nao_circulante=157.343,
    recebiveis_unidades_concluidas=161.230,
    recebiveis_unidades_construcao=490.634,
    avp_recebiveis=-0.399,
    pdd_provisao=-43.232,
    # Aging
    aging_a_vencer_12m=389.773,
    aging_a_vencer_24m=45.344,
    aging_a_vencer_36m_mais=111.999,
    aging_vencido_90d=104.937,
    aging_vencido_180d_mais=90.777,
    # PECLD movement (1S2020)
    pecld_adicoes=0.0,
    pecld_reversoes=10.184,
    # Divida
    divida_bruta=335.796,
    divida_venc_12m=102.684,
    divida_venc_24m_mais=233.112,
    # Caixa
    caixa_aplicacoes=425.847,
    # Provisoes
    provisao_garantia_obra=8.360,
    provisao_riscos_civeis=8.763,
    provisao_riscos_trabalhistas=19.876,
    provisao_riscos_total=28.639,
    # Resultado financeiro (2T2020 trimestral)
    receitas_financeiras=2.827,
    despesas_financeiras=-5.719,
    resultado_financeiro=-2.892,
)
print(f"Cury 2T2020: {n} row(s)")
count += n

# ============================================================
# CURY 3T2020 (balance date 30/09/2020)
# ============================================================
n = update("Cury", "3T2020", "Consolidado",
    recebiveis_circulante=647.241,
    recebiveis_nao_circulante=183.813,
    recebiveis_unidades_concluidas=243.152,
    recebiveis_unidades_construcao=554.078,
    avp_recebiveis=-0.399,
    pdd_provisao=-40.385,
    # Aging
    aging_a_vencer_12m=552.942,
    aging_a_vencer_24m=82.710,
    aging_a_vencer_36m_mais=101.103,
    aging_vencido_90d=39.789,
    aging_vencido_180d_mais=106.456,
    # PECLD movement (9M2020)
    pecld_adicoes=0.0,
    pecld_reversoes=13.031,  # combined baixas+reversoes
    # Divida
    divida_bruta=306.192,
    divida_venc_12m=93.603,
    divida_venc_24m_mais=212.589,
    # Caixa
    caixa_aplicacoes=609.849,
    # Provisoes
    provisao_garantia_obra=7.761,
    provisao_riscos_civeis=9.086,
    provisao_riscos_trabalhistas=19.858,
    provisao_riscos_total=28.944,
    # Resultado financeiro (3T2020 trimestral)
    receitas_financeiras=5.428,
    despesas_financeiras=-6.639,
    resultado_financeiro=-1.211,
)
print(f"Cury 3T2020: {n} row(s)")
count += n

# ============================================================
# MRV 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("MRV", "2T2020", "Consolidado",
    recebiveis_circulante=1811.762,
    recebiveis_nao_circulante=1537.831,
    avp_recebiveis=-63.965,
    pdd_provisao=-251.109,
    # PECLD movement (1S2020)
    pecld_adicoes=-131.300,
    pecld_reversoes=50.884,
    pecld_baixas=52.598,
    # Aging
    aging_a_vencer_12m=2790.016,
    aging_a_vencer_24m=1829.804,
    aging_a_vencer_36m=394.261,
    aging_a_vencer_36m_mais=407.348,
    # Divida
    divida_bruta=4840.199,
    divida_cri=778.731,
    divida_venc_12m=914.004,
    divida_venc_24m_mais=3926.195,
    # Caixa
    caixa_aplicacoes=2669.275,
    # Provisoes
    provisao_garantia_obra=148.525,
    provisao_riscos_civeis=59.403,
    provisao_riscos_trabalhistas=46.111,
    provisao_riscos_total=105.872,
    # Resultado financeiro (2T2020 trimestral)
    receitas_financeiras=33.646,
    despesas_financeiras=-23.749,
    resultado_financeiro=9.897,
    receita_fin_recebiveis=15.945,
    despesa_fin_juros_divida=-21.828,
    encargos_capitalizados=32.867,
)
print(f"MRV 2T2020: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("Direcional", "2T2020", "Consolidado",
    recebiveis_circulante=296.633,
    recebiveis_nao_circulante=137.685,
    recebiveis_unidades_concluidas=165.426,
    recebiveis_unidades_construcao=269.350,
    avp_recebiveis=-4.207,
    pdd_provisao=-25.547,
    # Aging circulante
    aging_a_vencer_12m=271.780,
    aging_vencido_90d=3.411,
    aging_vencido_180d=1.780,
    aging_vencido_180d_mais=19.662,
    # Aging nao circulante
    aging_a_vencer_24m=68.903,
    aging_a_vencer_36m=44.392,
    aging_a_vencer_36m_mais=24.390,
    # Divida
    divida_cri=682.867,
    divida_sfh_producao=37.288,
    divida_bruta=880.866,
    divida_venc_12m=235.054,
    divida_venc_24m_mais=645.812,
    # Caixa
    caixa_aplicacoes=835.543,
    # Provisoes
    provisao_garantia_obra=40.436,
    provisao_riscos_civeis=18.253,
    provisao_riscos_trabalhistas=10.200,
    provisao_riscos_total=28.559,
    # Resultado financeiro (2T2020 trimestral)
    receitas_financeiras=15.982,
    despesas_financeiras=-17.375,
    resultado_financeiro=-1.393,
    receita_fin_aplicacoes=4.376,
    despesa_fin_juros_divida=-10.992,
)
print(f"Direcional 2T2020: {n} row(s)")
count += n

# ============================================================
# CYRELA 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("Cyrela", "2T2020", "Consolidado",
    recebiveis_circulante=1447.389,
    recebiveis_nao_circulante=737.081,
    recebiveis_unidades_concluidas=913.139,
    recebiveis_unidades_construcao=1582.825,  # net of AVP
    avp_recebiveis=-33.877,
    pdd_provisao=-26.173,
    provisao_distratos=-292.115,
    # PECLD movement (1S2020)
    pecld_adicoes=9.836,
    pecld_reversoes=-2.839,
    pecld_baixas=-1.291,
    # Aging cronograma
    aging_a_vencer_12m=1889.692,
    aging_a_vencer_24m=1709.853,
    aging_a_vencer_36m=1327.958,
    aging_a_vencer_36m_mais=266.416,
    # Divida
    divida_debentures=157.926,
    divida_cri=1234.578,
    divida_sfh_producao=554.058,
    divida_bruta=2899.261,
    divida_venc_12m=864.238,
    divida_venc_24m_mais=2046.318,
    # Caixa
    caixa_aplicacoes=1993.944,
    # Provisoes
    provisao_garantia_obra=97.225,
    provisao_riscos_civeis=101.295,
    provisao_riscos_trabalhistas=84.340,
    provisao_riscos_total=189.460,
    # Resultado financeiro (2T2020 trimestral)
    receitas_financeiras=42.555,
    despesas_financeiras=-36.054,
    resultado_financeiro=6.501,
    receita_fin_aplicacoes=13.704,
    despesa_fin_juros_divida=-23.689,
    encargos_capitalizados=2.072,
)
print(f"Cyrela 2T2020: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

"""
Batch 9 - ITR data from 2T2021 + 2T2020 ITRs.
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
# TENDA 2T2021 (balance date 30/06/2021)
# ============================================================
n = update("Tenda", "2T2021", "Consolidado",
    recebiveis_circulante=500.000,
    recebiveis_nao_circulante=674.560,
    avp_recebiveis=-6.371,
    pdd_provisao=-210.638,
    # Aging
    aging_vencido_90d=19.059,
    aging_vencido_180d=7.785,
    aging_vencido_180d_mais=53.939,
    aging_a_vencer_12m=203.654,
    aging_a_vencer_24m=728.515,
    aging_a_vencer_36m=273.181,
    aging_a_vencer_36m_mais=146.737,
    # PECLD movement (1S2021)
    pecld_adicoes=-55.970,
    pecld_reversoes=6.952,
    pecld_baixas=12.768,
    # Divida
    divida_debentures=902.007,
    divida_bruta=1130.037,
    divida_venc_12m=250.140,
    divida_venc_24m_mais=879.897,
    # Caixa
    caixa_aplicacoes=945.235,
    # Provisoes
    provisao_riscos_civeis=56.635,
    provisao_riscos_trabalhistas=5.634,
    provisao_riscos_total=72.291,
    # Resultado financeiro (2T2021 trimestral)
    receitas_financeiras=8.450,
    despesas_financeiras=-23.524,
    resultado_financeiro=-15.074,
    encargos_capitalizados=8.032,
)
print(f"Tenda 2T2021: {n} row(s)")
count += n

# ============================================================
# CURY 2T2021 (balance date 30/06/2021)
# ============================================================
n = update("Cury", "2T2021", "Consolidado",
    recebiveis_circulante=835.343,
    recebiveis_nao_circulante=213.781,
    recebiveis_unidades_concluidas=286.879,
    recebiveis_unidades_construcao=758.744,
    avp_recebiveis=-1.649,
    pdd_provisao=-49.250,
    # Aging
    aging_a_vencer_12m=727.644,
    aging_a_vencer_24m=73.144,
    aging_a_vencer_36m_mais=140.637,
    aging_vencido_90d=46.689,
    aging_vencido_180d_mais=134.625,
    # PECLD movement (1S2021) - reversoes+baixas combined as 2.258
    pecld_adicoes=-7.119,
    pecld_reversoes=2.258,  # combined reversoes+baixas
    # Divida
    divida_bruta=270.295,
    divida_venc_12m=116.920,
    divida_venc_24m_mais=153.375,
    # Caixa
    caixa_aplicacoes=491.987,
    # Provisoes
    provisao_garantia_obra=10.377,
    provisao_riscos_civeis=8.549,
    provisao_riscos_trabalhistas=15.766,
    provisao_riscos_total=24.315,
    # Resultado financeiro (2T2021 trimestral)
    receitas_financeiras=3.924,
    despesas_financeiras=-9.300,
    resultado_financeiro=-5.376,
)
print(f"Cury 2T2021: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2021 (balance date 30/06/2021)
# ============================================================
n = update("Direcional", "2T2021", "Consolidado",
    recebiveis_circulante=274.076,
    recebiveis_nao_circulante=209.044,
    recebiveis_unidades_concluidas=171.776,
    recebiveis_unidades_construcao=311.773,
    avp_recebiveis=-9.303,
    pdd_provisao=-22.742,
    # PECLD movement (1S2021)
    pecld_adicoes=-9.960,
    pecld_reversoes=4.248,
    # Aging circulante
    aging_a_vencer_12m=233.310,
    aging_vencido_90d=14.987,
    aging_vencido_180d=2.425,
    aging_vencido_180d_mais=23.354,
    # Aging nao circulante
    aging_a_vencer_24m=125.485,
    aging_a_vencer_36m=50.105,
    aging_a_vencer_36m_mais=33.454,
    # Divida
    divida_cri=787.591,
    divida_sfh_producao=37.430,
    divida_debentures=252.834,
    divida_bruta=1190.639,
    divida_venc_12m=122.243,
    divida_venc_24m_mais=1068.396,
    # Caixa
    caixa_aplicacoes=946.589,
    # Provisoes
    provisao_garantia_obra=40.801,
    provisao_riscos_civeis=16.490,
    provisao_riscos_trabalhistas=9.446,
    provisao_riscos_total=26.048,
    # Resultado financeiro (2T2021 trimestral)
    receitas_financeiras=12.895,
    despesas_financeiras=-27.318,
    resultado_financeiro=-14.423,
    receita_fin_aplicacoes=5.700,
    despesa_fin_juros_divida=-22.398,
    despesa_fin_cessao=-0.467,
    despesa_fin_derivativos=-1.845,
)
print(f"Direcional 2T2021: {n} row(s)")
count += n

# ============================================================
# TENDA 2T2020 (balance date 30/06/2020)
# ============================================================
n = update("Tenda", "2T2020", "Consolidado",
    recebiveis_circulante=578.121,
    recebiveis_nao_circulante=163.037,
    avp_recebiveis=-6.079,
    pdd_provisao=-129.434,
    # Aging
    aging_vencido_90d=16.351,
    aging_vencido_180d=9.623,
    aging_vencido_180d_mais=58.723,
    aging_a_vencer_12m=623.143,
    aging_a_vencer_24m=109.234,
    aging_a_vencer_36m=56.477,
    aging_a_vencer_36m_mais=78.885,
    # PECLD movement (1S2020)
    pecld_adicoes=-29.163,
    pecld_reversoes=8.590,
    pecld_baixas=9.352,
    # Divida
    divida_debentures=817.035,
    divida_bruta=1347.211,
    divida_venc_12m=515.817,
    divida_venc_24m_mais=831.394,
    # Caixa
    caixa_aplicacoes=1534.881,
    # Provisoes
    provisao_riscos_civeis=56.942,
    provisao_riscos_trabalhistas=6.094,
    provisao_riscos_total=63.598,
    # Resultado financeiro (2T2020 trimestral)
    receitas_financeiras=10.262,
    despesas_financeiras=-16.570,
    resultado_financeiro=-6.308,
    encargos_capitalizados=9.444,
)
print(f"Tenda 2T2020: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

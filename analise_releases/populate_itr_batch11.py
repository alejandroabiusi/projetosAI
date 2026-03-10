"""
Batch 11 - ITR data from 1T2024, 1T2020 ITRs.
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
# TENDA 1T2020 (balance date 31/03/2020)
# ============================================================
n = update("Tenda", "1T2020", "Consolidado",
    recebiveis_circulante=549.818,
    recebiveis_nao_circulante=141.472,
    avp_recebiveis=-6.055,
    pdd_provisao=-131.003,
    aging_vencido_90d=18.063,
    aging_vencido_180d=8.301,
    aging_vencido_180d_mais=67.306,
    aging_a_vencer_12m=527.717,
    aging_a_vencer_24m=140.550,
    aging_a_vencer_36m=52.808,
    aging_a_vencer_36m_mais=75.559,
    pecld_adicoes=-14.258,
    pecld_reversoes=-3.964,
    pecld_baixas=5.432,
    divida_debentures=811.969,
    divida_bruta=928.909,
    divida_venc_12m=321.993,
    divida_venc_24m_mais=606.916,
    caixa_aplicacoes=1060.713,
    provisao_riscos_civeis=56.036,
    provisao_riscos_trabalhistas=7.149,
    provisao_riscos_total=63.460,
    receitas_financeiras=9.261,
    despesas_financeiras=-12.212,
    resultado_financeiro=-2.951,
    encargos_capitalizados=4.763,
)
print(f"Tenda 1T2020: {n} row(s)")
count += n

# ============================================================
# TENDA 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("Tenda", "1T2024", "Consolidado",
    recebiveis_circulante=546.642,
    recebiveis_nao_circulante=699.171,
    avp_recebiveis=-87.267,
    pdd_provisao=-394.088,
    aging_vencido_90d=40.366,
    aging_vencido_180d=22.964,
    aging_vencido_180d_mais=127.341,
    aging_a_vencer_12m=763.243,
    aging_a_vencer_24m=435.405,
    aging_a_vencer_36m=139.747,
    aging_a_vencer_36m_mais=206.584,
    pecld_adicoes=-48.831,
    pecld_reversoes=1.639,
    divida_debentures=712.518,
    divida_bruta=1101.160,
    divida_venc_12m=343.259,
    divida_venc_24m_mais=757.901,
    caixa_aplicacoes=747.388,
    provisao_riscos_civeis=102.307,
    provisao_riscos_trabalhistas=20.567,
    provisao_riscos_total=126.098,
    cessao_passivo_total=380.500,
    receitas_financeiras=19.354,
    despesas_financeiras=-72.953,
    resultado_financeiro=-53.599,
    encargos_capitalizados=17.274,
)
print(f"Tenda 1T2024: {n} row(s)")
count += n

# ============================================================
# CURY 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("Cury", "1T2024", "Consolidado",
    recebiveis_circulante=649.909,
    recebiveis_nao_circulante=635.299,
    recebiveis_unidades_concluidas=289.136,
    recebiveis_unidades_construcao=1143.019,
    avp_recebiveis=-10.103,
    pdd_provisao=-100.049,
    aging_a_vencer_12m=645.272,
    aging_a_vencer_24m=232.471,
    aging_a_vencer_36m_mais=402.828,
    aging_vencido_90d=40.360,
    aging_vencido_180d_mais=113.391,
    pecld_adicoes=-11.198,
    pecld_reversoes=0.916,
    pecld_baixas=2.732,
    divida_bruta=601.890,
    divida_venc_12m=121.622,
    divida_venc_24m_mais=480.268,
    caixa_aplicacoes=1011.668,
    provisao_garantia_obra=43.195,
    provisao_riscos_civeis=9.217,
    provisao_riscos_trabalhistas=19.359,
    provisao_riscos_total=28.576,
    receitas_financeiras=19.889,
    despesas_financeiras=-24.806,
    resultado_financeiro=-4.917,
)
print(f"Cury 1T2024: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("Direcional", "1T2024", "Consolidado",
    recebiveis_circulante=634.970,
    recebiveis_nao_circulante=574.104,
    recebiveis_unidades_concluidas=283.425,
    recebiveis_unidades_construcao=995.293,
    avp_recebiveis=-47.638,
    pdd_provisao=-40.985,
    pecld_adicoes=-6.854,
    pecld_reversoes=1.902,
    aging_a_vencer_12m=563.212,
    aging_vencido_90d=19.599,
    aging_vencido_180d=21.125,
    aging_vencido_180d_mais=31.034,
    aging_a_vencer_24m=260.537,
    aging_a_vencer_36m=108.185,
    aging_a_vencer_36m_mais=205.382,
    divida_cri=1024.683,
    divida_sfh_producao=208.017,
    divida_debentures=105.808,
    divida_bruta=1341.760,
    divida_venc_12m=254.827,
    divida_venc_24m_mais=1061.373,
    caixa_aplicacoes=1176.362,
    provisao_garantia_obra=39.780,
    provisao_riscos_civeis=27.237,
    provisao_riscos_trabalhistas=2.440,
    provisao_riscos_total=29.712,
    receitas_financeiras=63.457,
    despesas_financeiras=-40.249,
    resultado_financeiro=23.208,
    receita_fin_aplicacoes=26.592,
    despesa_fin_juros_divida=-22.435,
    despesa_fin_cessao=-1.260,
    despesa_fin_derivativos=-12.017,
)
print(f"Direcional 1T2024: {n} row(s)")
count += n

# ============================================================
# MRV 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("MRV", "1T2024", "Consolidado",
    recebiveis_circulante=2733.816,
    recebiveis_nao_circulante=2631.849,
    avp_recebiveis=-378.207,
    pdd_provisao=-409.481,
    pecld_adicoes=-109.179,
    pecld_reversoes=56.058,
    pecld_baixas=57.044,
    aging_a_vencer_12m=4071.009,
    aging_a_vencer_24m=1777.655,
    aging_a_vencer_36m=862.223,
    aging_a_vencer_36m_mais=1593.093,
    divida_bruta=8075.803,
    divida_cri=2664.747,
    divida_venc_12m=2631.256,
    divida_venc_24m_mais=6237.280,
    caixa_aplicacoes=3347.014,
    provisao_garantia_obra=289.620,
    provisao_riscos_civeis=60.830,
    provisao_riscos_trabalhistas=50.370,
    provisao_riscos_total=111.823,
    cessao_passivo_total=2527.738,
    receitas_financeiras=103.117,
    despesas_financeiras=-341.102,
    resultado_financeiro=-237.985,
    receita_fin_recebiveis=26.723,
    despesa_fin_juros_divida=-93.835,
    despesa_fin_cessao=-83.367,
    despesa_fin_derivativos=-147.247,
    encargos_capitalizados=139.344,
)
print(f"MRV 1T2024: {n} row(s)")
count += n

# ============================================================
# CYRELA 1T2024 (balance date 31/03/2024)
# ============================================================
n = update("Cyrela", "1T2024", "Consolidado",
    recebiveis_circulante=2911.257,
    recebiveis_nao_circulante=723.858,
    recebiveis_unidades_concluidas=941.999,
    recebiveis_unidades_construcao=3147.312,
    avp_recebiveis=-115.061,
    pdd_provisao=-60.703,
    provisao_distratos=-420.684,
    pecld_adicoes=8.841,
    pecld_reversoes=-2.605,
    pecld_baixas=-1.327,
    aging_a_vencer_12m=4437.695,
    aging_a_vencer_24m=2591.814,
    aging_a_vencer_36m=2454.865,
    aging_a_vencer_36m_mais=1068.453,
    divida_debentures=982.624,
    divida_cri=2027.957,
    divida_sfh_producao=1885.222,
    divida_bruta=4895.803,
    divida_venc_12m=941.344,
    divida_venc_24m_mais=3954.459,
    caixa_aplicacoes=4477.983,
    provisao_garantia_obra=154.334,
    provisao_riscos_civeis=133.997,
    provisao_riscos_trabalhistas=81.259,
    provisao_riscos_total=228.130,
    receitas_financeiras=157.466,
    despesas_financeiras=-144.331,
    resultado_financeiro=13.135,
    receita_fin_aplicacoes=144.479,
    despesa_fin_juros_divida=-100.991,
    despesa_fin_derivativos=-8.608,
    encargos_capitalizados=13.914,
)
print(f"Cyrela 1T2024: {n} row(s)")
count += n

# Cyrela 4T2023 comparative (fills Cyrela 4T2023 gap)
cur.execute("SELECT recebiveis_circulante FROM dados_trimestrais WHERE empresa='Cyrela' AND periodo='4T2023' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("Cyrela", "4T2023", "Consolidado",
        recebiveis_circulante=2857.730,
        recebiveis_nao_circulante=596.982,
        recebiveis_unidades_concluidas=1146.874,
        recebiveis_unidades_construcao=2731.815,
        avp_recebiveis=-102.291,
        pdd_provisao=-55.794,
        provisao_distratos=-373.228,
        aging_a_vencer_12m=4238.975,
        aging_a_vencer_24m=2411.491,
        aging_a_vencer_36m=2325.379,
        aging_a_vencer_36m_mais=952.912,
        divida_debentures=965.831,
        divida_cri=2196.809,
        divida_sfh_producao=1994.898,
        divida_bruta=5157.538,
        divida_venc_12m=1405.470,
        divida_venc_24m_mais=3752.068,
        caixa_aplicacoes=4602.606,
        provisao_garantia_obra=138.629,
        provisao_riscos_civeis=121.952,
        provisao_riscos_trabalhistas=80.352,
        provisao_riscos_total=215.188,
    )
    print(f"Cyrela 4T2023 (comparative): {n} row(s)")
    count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

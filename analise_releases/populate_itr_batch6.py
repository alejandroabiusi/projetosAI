"""
Batch 6 - ITR data from 2T2023 ITRs + MRV 1T2022.
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
# TENDA 2T2023 (balance date 30/06/2023)
# ============================================================
n = update("Tenda", "2T2023", "Consolidado",
    recebiveis_circulante=582.839,
    recebiveis_nao_circulante=509.685,
    avp_recebiveis=-84.400,
    pdd_provisao=-296.235,
    # Aging
    aging_vencido_90d=27.717,
    aging_vencido_180d=24.173,
    aging_vencido_180d_mais=95.687,
    aging_a_vencer_12m=753.738,
    aging_a_vencer_24m=294.663,
    aging_a_vencer_36m=99.813,
    aging_a_vencer_36m_mais=184.067,  # 56925+127142
    # PECLD movement (1S2023)
    pecld_adicoes=-36.421,
    pecld_reversoes=12.551,
    pecld_baixas=0,
    # Divida
    divida_debentures=912.698,
    divida_venc_12m=570.595,   # 308624+261971
    divida_venc_24m_mais=788.565,  # 137838+650727
    divida_bruta=1359.160,
    # Caixa
    caixa_aplicacoes=733.512,
    # Provisoes
    provisao_riscos_civeis=98.896,
    provisao_riscos_trabalhistas=16.374,
    provisao_riscos_total=136.985,
    # Cessao
    cessao_passivo_total=274.693,
    # Resultado financeiro (2T2023 trimestral)
    receitas_financeiras=11.990,
    despesas_financeiras=-27.612,
    resultado_financeiro=-15.622,
    encargos_capitalizados=21.077,
)
print(f"Tenda 2T2023: {n} row(s)")
count += n

# ============================================================
# CURY 2T2023 (balance date 30/06/2023)
# ============================================================
n = update("Cury", "2T2023", "Consolidado",
    recebiveis_circulante=678.261,
    recebiveis_nao_circulante=407.428,
    recebiveis_unidades_concluidas=191.517,
    recebiveis_unidades_construcao=978.622,
    avp_recebiveis=-7.761,
    pdd_provisao=-87.753,
    # Aging
    aging_a_vencer_12m=713.814,
    aging_a_vencer_24m=151.465,
    aging_a_vencer_36m_mais=255.964,
    aging_vencido_90d=21.700,   # 10339+11361
    aging_vencido_180d_mais=81.463,
    # PECLD movement (1S2023)
    pecld_adicoes=-19.653,
    pecld_reversoes=0.863,
    pecld_baixas=6.092,
    # Divida
    divida_venc_12m=138.967,
    divida_venc_24m_mais=360.640,
    divida_bruta=499.607,
    # Caixa
    caixa_aplicacoes=835.390,
    # Provisoes
    provisao_garantia_obra=27.158,
    provisao_riscos_civeis=6.648,
    provisao_riscos_trabalhistas=14.353,
    provisao_riscos_total=20.001,  # estimated total
    # Resultado financeiro (2T2023 trimestral)
    receitas_financeiras=20.425,
    despesas_financeiras=-26.026,
    resultado_financeiro=-5.601,
)
print(f"Cury 2T2023: {n} row(s)")
count += n

# ============================================================
# DIRECIONAL 2T2023 (balance date 30/06/2023)
# ============================================================
n = update("Direcional", "2T2023", "Consolidado",
    recebiveis_circulante=399.289,
    recebiveis_nao_circulante=340.820,
    recebiveis_unidades_concluidas=204.798,
    recebiveis_unidades_construcao=581.254,
    avp_recebiveis=-32.577,
    pdd_provisao=-33.540,
    # PECLD movement (1S2023)
    pecld_adicoes=-15.953,
    pecld_reversoes=10.778,
    # Aging circulante
    aging_a_vencer_12m=352.254,
    aging_vencido_90d=14.988,   # 11125+2273+1590
    aging_vencido_180d=4.260,   # 1877+2383
    aging_vencido_180d_mais=27.787,
    # Aging nao circulante
    aging_a_vencer_24m=155.177,
    aging_a_vencer_36m=81.749,
    aging_a_vencer_36m_mais=103.894,  # 36288+67606
    # Divida
    divida_cri=935.371,
    divida_sfh_producao=160.744,
    divida_debentures=363.878,
    divida_bruta=1467.029,
    divida_venc_12m=326.169,
    divida_venc_24m_mais=1118.460,
    # Caixa
    caixa_aplicacoes=1113.203,
    # Provisoes
    provisao_garantia_obra=41.990,
    provisao_riscos_civeis=25.789,
    provisao_riscos_trabalhistas=3.889,
    provisao_riscos_total=29.713,
    # Resultado financeiro (2T2023 trimestral)
    receitas_financeiras=68.147,
    despesas_financeiras=-69.299,
    resultado_financeiro=-1.152,
    receita_fin_aplicacoes=26.559,
    despesa_fin_juros_divida=-35.505,
    despesa_fin_cessao=2.416,
    despesa_fin_derivativos=-7.295,
)
print(f"Direcional 2T2023: {n} row(s)")
count += n

# ============================================================
# MRV 2T2023 (balance date 30/06/2023)
# ============================================================
n = update("MRV", "2T2023", "Consolidado",
    recebiveis_circulante=2469.788,
    recebiveis_nao_circulante=1954.635,
    avp_recebiveis=-232.840,
    pdd_provisao=-415.958,
    # PECLD movement (1S2023)
    pecld_adicoes=-228.800,
    pecld_reversoes=90.963,
    pecld_baixas=53.079,
    # Aging - expectativa recebimento
    aging_a_vencer_12m=3388.568,
    aging_a_vencer_24m=1499.442,
    aging_a_vencer_36m=630.853,
    aging_a_vencer_36m_mais=964.775,  # 373761+591014
    # Divida
    divida_bruta=8128.286,
    divida_venc_12m=1781.876,
    divida_venc_24m_mais=6346.410,
    divida_cri=2331.350,
    # Caixa
    caixa_aplicacoes=2850.493,
    # Provisoes
    provisao_garantia_obra=260.708,
    provisao_riscos_civeis=42.305,
    provisao_riscos_trabalhistas=43.070,
    provisao_riscos_total=85.796,
    # Cessao
    cessao_passivo_total=1072.682,
    # Resultado financeiro (2T2023 trimestral)
    receitas_financeiras=47.362,
    despesas_financeiras=48.673,  # positive due to MTM derivatives gain
    resultado_financeiro=129.480,  # includes receita clientes
    receita_fin_aplicacoes=0,  # not detailed in trimestral
    receita_fin_recebiveis=33.445,
    despesa_fin_juros_divida=-127.287,
    despesa_fin_cessao=-31.602,
    despesa_fin_derivativos=227.403,
    encargos_capitalizados=128.606,
)
print(f"MRV 2T2023: {n} row(s)")
count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

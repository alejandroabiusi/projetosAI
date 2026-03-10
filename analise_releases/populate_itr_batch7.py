"""
Batch 7 - MRV 1T2022 + 4T2021 (from previous session agent data).
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
# MRV 1T2022 (balance date 31/03/2022)
# ============================================================
n = update("MRV", "1T2022", "Consolidado",
    recebiveis_circulante=2439.769,
    recebiveis_nao_circulante=1585.526,
    avp_recebiveis=-76.129,
    pdd_provisao=-349.222,
    # Divida
    divida_bruta=6169.870,
    # Caixa
    caixa_aplicacoes=3023.502,
    # Provisoes
    provisao_garantia_obra=217.568,
    # Cessao
    cessao_passivo_total=80.643,
    # Resultado financeiro (1T2022 trimestral)
    receitas_financeiras=51.962,
    despesas_financeiras=-53.464,
    encargos_capitalizados=105.735,
)
print(f"MRV 1T2022: {n} row(s)")
count += n

# MRV 4T2021 comparative
cur.execute("SELECT recebiveis_circulante FROM dados_trimestrais WHERE empresa='MRV' AND periodo='4T2021' AND segmento='Consolidado'")
r = cur.fetchone()
if r and r[0] is None:
    n = update("MRV", "4T2021", "Consolidado",
        recebiveis_circulante=2378.157,
        recebiveis_nao_circulante=1737.445,
        avp_recebiveis=-75.054,
        pdd_provisao=-347.748,
        divida_bruta=5232.777,
        caixa_aplicacoes=2749.867,
        provisao_garantia_obra=206.562,
        cessao_passivo_total=122.341,
    )
    print(f"MRV 4T2021 (comparative): {n} row(s)")
    count += n

conn.commit()
print(f"\nTotal rows updated: {count}")
conn.close()

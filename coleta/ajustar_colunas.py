"""
Ajusta colunas do banco empreendimentos:
1. Adiciona qty_1dorm, qty_2dorms, qty_3dorms (quantidade de apartamentos por tipo)
2. Adiciona lazer_varanda se nao existir
3. Verifica completude geral
"""
import sys
sys.stdout.reconfigure(errors="replace")

import sqlite3
import os

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "empreendimentos.db")

conn = sqlite3.connect(DB)
cur = conn.cursor()

# 1. Adicionar colunas faltantes
cur.execute("PRAGMA table_info(empreendimentos)")
cols = {r[1] for r in cur.fetchall()}

new_cols = {
    "qty_1dorm": "INTEGER",
    "qty_2dorms": "INTEGER",
    "qty_3dorms": "INTEGER",
    "lazer_varanda": "INTEGER DEFAULT 0",
}
for col, tipo in new_cols.items():
    if col not in cols:
        cur.execute(f"ALTER TABLE empreendimentos ADD COLUMN {col} {tipo}")
        print(f"Coluna adicionada: {col}")
    else:
        print(f"Coluna ja existe: {col}")

conn.commit()

# 2. Completude geral
cur.execute("SELECT COUNT(*) FROM empreendimentos")
total = cur.fetchone()[0]

campos = [
    "total_unidades", "numero_vagas", "endereco", "latitude",
    "dormitorios_descricao", "area_min_m2",
    "lazer_piscina", "lazer_churrasqueira", "apto_terraco", "lazer_varanda",
    "apto_1_dorm", "apto_2_dorms", "apto_3_dorms",
    "qty_1dorm", "qty_2dorms", "qty_3dorms",
]

print(f"\n=== COMPLETUDE GERAL ({total} registros) ===")
for c in campos:
    try:
        cur.execute(f"SELECT COUNT(*) FROM empreendimentos WHERE {c} IS NOT NULL AND {c} != '' AND {c} != 0")
        n = cur.fetchone()[0]
        pct = 100 * n / total
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(f"  {c:25s}: {n:4d}/{total} ({pct:5.1f}%) [{bar}]")
    except Exception as e:
        print(f"  {c:25s}: ERRO - {e}")

# 3. Completude total_unidades por empresa
print(f"\n=== TOTAL_UNIDADES POR EMPRESA ===")
cur.execute("""
    SELECT empresa, COUNT(*) as total,
           SUM(CASE WHEN total_unidades IS NOT NULL AND total_unidades > 0 THEN 1 ELSE 0 END) as preenchido
    FROM empreendimentos
    GROUP BY empresa
    ORDER BY empresa
""")
for r in cur.fetchall():
    pct = 100 * r[2] / r[1] if r[1] > 0 else 0
    print(f"  {r[0]:20s}: {r[2]:3d}/{r[1]:3d} ({pct:5.1f}%)")

# 4. Completude numero_vagas por empresa
print(f"\n=== NUMERO_VAGAS POR EMPRESA ===")
cur.execute("""
    SELECT empresa, COUNT(*) as total,
           SUM(CASE WHEN numero_vagas IS NOT NULL AND numero_vagas != '' THEN 1 ELSE 0 END) as preenchido
    FROM empreendimentos
    GROUP BY empresa
    ORDER BY empresa
""")
for r in cur.fetchall():
    pct = 100 * r[2] / r[1] if r[1] > 0 else 0
    print(f"  {r[0]:20s}: {r[2]:3d}/{r[1]:3d} ({pct:5.1f}%)")

conn.close()

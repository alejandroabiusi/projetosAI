"""
Relatorio de completude do banco de empreendimentos.
Mostra para cada empresa quantos registros tem cada campo preenchido.
"""

import sqlite3

DB_PATH = r'C:\Projetos_AI\coleta\data\empreendimentos.db'

CAMPOS = [
    "cidade", "estado", "bairro", "endereco", "cep",
    "latitude", "longitude", "fase", "preco_a_partir",
    "dormitorios_descricao", "metragens_descricao",
    "area_min_m2", "area_max_m2", "total_unidades",
    "numero_vagas", "area_terreno_m2", "itens_lazer",
    "registro_incorporacao", "imagem_url", "tipo_imovel",
]

conn = sqlite3.connect(DB_PATH)

empresas = conn.execute(
    "SELECT empresa, COUNT(*) as n FROM empreendimentos GROUP BY empresa ORDER BY n DESC"
).fetchall()

colunas_banco = {r[1] for r in conn.execute("PRAGMA table_info(empreendimentos)").fetchall()}
campos_validos = [c for c in CAMPOS if c in colunas_banco]
campos_ausentes = [c for c in CAMPOS if c not in colunas_banco]

if campos_ausentes:
    print(f"Campos nao existentes no banco: {', '.join(campos_ausentes)}\n")

# Cabecalho
print(f"{'Empresa':<22} {'Total':>6}  ", end="")
for campo in campos_validos:
    print(f"{campo[:8]:<10}", end="")
print()
print("-" * (32 + 10 * len(campos_validos)))

for empresa, total in empresas:
    print(f"{empresa:<22} {total:>6}  ", end="")
    for campo in campos_validos:
        n = conn.execute(
            f"SELECT COUNT(*) FROM empreendimentos WHERE empresa = ? AND {campo} IS NOT NULL AND CAST({campo} AS TEXT) != ''",
            (empresa,)
        ).fetchone()[0]
        pct = int(n / total * 100) if total > 0 else 0
        cel = f"{pct}%"
        print(f"{cel:<10}", end="")
    print()

print()
print("Valores: % de registros com o campo preenchido")
conn.close()

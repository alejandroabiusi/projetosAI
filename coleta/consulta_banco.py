import sqlite3

conn = sqlite3.connect("data/empreendimentos.db")

print("=== TOTAIS ===")
print(f"Total: {conn.execute('SELECT COUNT(*) FROM empreendimentos').fetchone()[0]}")
print(f"Com preco: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE preco_a_partir IS NOT NULL').fetchone()[0]}")
print(f"Com unidades: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE total_unidades IS NOT NULL').fetchone()[0]}")
print(f"Com lazer: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE itens_lazer IS NOT NULL').fetchone()[0]}")
print(f"Com endereco: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE endereco IS NOT NULL').fetchone()[0]}")
print(f"Com metragem: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE area_min_m2 IS NOT NULL').fetchone()[0]}")
print(f"Com imagens: {conn.execute('SELECT COUNT(*) FROM empreendimentos WHERE imagens_fachada IS NOT NULL OR imagens_plantas IS NOT NULL OR imagens_areas_comuns IS NOT NULL').fetchone()[0]}")

print()
print("=== POR FASE ===")
for r in conn.execute("SELECT fase, COUNT(*) FROM empreendimentos GROUP BY fase ORDER BY COUNT(*) DESC").fetchall():
    print(f"  {r[0] or '(sem fase)'}: {r[1]}")

print()
print("=== AMOSTRA (Em Construcao com dados) ===")
rows = conn.execute("""
    SELECT nome, fase, preco_a_partir, total_unidades, area_min_m2, itens_lazer
    FROM empreendimentos
    WHERE fase = 'Em Construção' AND preco_a_partir IS NOT NULL
    LIMIT 5
""").fetchall()
for r in rows:
    lazer = (r[5] or "")[:60]
    print(f"  {r[0]} | R${r[2]:,.0f} | {r[3]}un | {r[4]}m2 | {lazer}")

print()
print("=== AMOSTRA (Imovel Pronto com dados) ===")
rows = conn.execute("""
    SELECT nome, fase, total_unidades, area_min_m2, itens_lazer
    FROM empreendimentos
    WHERE fase = 'Imóvel Pronto' AND total_unidades IS NOT NULL
    LIMIT 5
""").fetchall()
for r in rows:
    lazer = (r[4] or "")[:60]
    print(f"  {r[0]} | {r[2]}un | {r[3]}m2 | {lazer}")

conn.close()

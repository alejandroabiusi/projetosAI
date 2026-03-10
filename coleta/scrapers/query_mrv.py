import sqlite3

conn = sqlite3.connect(r'C:\Users\aleja\OneDrive\Projetos_AI\coleta\data\empreendimentos.db')

rows = conn.execute("""
    SELECT DISTINCT cidade, estado, slug
    FROM empreendimentos
    WHERE empresa = 'MRV'
    LIMIT 10
""").fetchall()

for r in rows:
    print(r)

conn.close()

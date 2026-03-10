"""
Recuperacao de banco SQLite corrompido.
Le registro por registro com tolerancia a falhas e salva o que conseguir.
"""

import sqlite3
import os

DB_ORIG = r'C:\Users\aleja\OneDrive\Projetos_AI\coleta\data\empreendimentos.db'
DB_NOVO = r'C:\Users\aleja\OneDrive\Projetos_AI\coleta\data\empreendimentos_recuperado.db'

if os.path.exists(DB_NOVO):
    os.remove(DB_NOVO)

conn_orig = sqlite3.connect(DB_ORIG)
conn_orig.row_factory = sqlite3.Row

conn_novo = sqlite3.connect(DB_NOVO)

try:
    # Copia estrutura da tabela
    schema = conn_orig.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='empreendimentos'"
    ).fetchone()

    if not schema:
        print("Nao foi possivel ler o schema. Banco muito corrompido.")
        exit(1)

    conn_novo.execute(schema[0])
    conn_novo.commit()
    print("Schema criado no banco novo.")

    # Le registros um por um
    recuperados = 0
    erros = 0
    ultimo_id = 0

    while True:
        try:
            rows = conn_orig.execute(
                "SELECT * FROM empreendimentos WHERE id > ? ORDER BY id LIMIT 100",
                (ultimo_id,)
            ).fetchall()
        except Exception as e:
            print(f"Erro ao ler a partir do id {ultimo_id}: {e}")
            ultimo_id += 100
            erros += 1
            if erros > 20:
                print("Muitos erros consecutivos, encerrando.")
                break
            continue

        if not rows:
            break

        for row in rows:
            try:
                cols = row.keys()
                placeholders = ", ".join(["?"] * len(cols))
                cols_sql = ", ".join(cols)
                conn_novo.execute(
                    f"INSERT INTO empreendimentos ({cols_sql}) VALUES ({placeholders})",
                    list(row)
                )
                recuperados += 1
                ultimo_id = row["id"]
            except Exception:
                erros += 1
                continue

        conn_novo.commit()
        print(f"  Processados ate id {ultimo_id} | Recuperados: {recuperados}")

    print(f"\nConcluido: {recuperados} registros recuperados, {erros} erros.")

except Exception as e:
    print(f"Erro fatal: {e}")

finally:
    conn_orig.close()
    conn_novo.close()

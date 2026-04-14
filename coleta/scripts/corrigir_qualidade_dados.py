"""
Correção de qualidade de dados — Itens 1, 2, 3 e 6.
1. Normalizar cidades (acentos, case, duplicatas)
2. Limpar cidades com lixo (cartórios, texto do site)
3. Normalizar fases (5 canônicas)
6. Limpar nomes sujos
"""
import sqlite3
import unicodedata
import re
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

DB_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\data\empreendimentos.db"

CIDADE_CANONICA = {
    "aparecida de goiania": "Aparecida de Goiânia",
    "aracatuba": "Araçatuba",
    "brasilia": "Brasília",
    "campos dos goytacazes": "Campos dos Goytacazes",
    "caxias do sul": "Caxias do Sul",
    "divinopolis": "Divinópolis",
    "duque de caxias": "Duque de Caxias",
    "eusebio": "Eusébio",
    "feira de santana": "Feira de Santana",
    "goiania": "Goiânia",
    "hortolandia": "Hortolândia",
    "ibirite": "Ibirité",
    "jaboatao dos guararapes": "Jaboatão dos Guararapes",
    "jaguariuna": "Jaguariúna",
    "juiz de fora": "Juiz de Fora",
    "lauro de freitas": "Lauro de Freitas",
    "maracanau": "Maracanaú",
    "marilia": "Marília",
    "nova iguacu": "Nova Iguaçu",
    "paulinia": "Paulínia",
    "rio de janeiro": "Rio de Janeiro",
    "sabara": "Sabará",
    "santana de parnaiba": "Santana de Parnaíba",
    "sao goncalo": "São Gonçalo",
    "sao jose de ribamar": "São José de Ribamar",
    "sao jose do rio preto": "São José do Rio Preto",
    "sao jose dos campos": "São José dos Campos",
    "sao luis": "São Luís",
    "uberlandia": "Uberlândia",
    "vitoria da conquista": "Vitória da Conquista",
    "ribeirao das neves": "Ribeirão das Neves",
}

FASE_MAP = {
    "em construcao": "Em Construção",
    "em construção": "Em Construção",
    "em obras": "Em Construção",
    "obra em andamento": "Em Construção",
    "obras em andamento": "Em Construção",
    "lancamento": "Lançamento",
    "lançamento": "Lançamento",
    "breve lancamento": "Breve Lançamento",
    "breve lançamento": "Breve Lançamento",
    "pronto": "Pronto para Morar",
    "pronto para morar": "Pronto para Morar",
    "imóvel pronto": "Pronto para Morar",
    "imovel pronto": "Pronto para Morar",
    "entregue": "Pronto para Morar",
    "100% vendido": "100% Vendido",
    "vendido": "100% Vendido",
    "lotes": "Lotes",
    "aluguel": "Aluguel",
}


def norm_text(t):
    if not t:
        return ""
    n = unicodedata.normalize("NFD", t.strip())
    n = "".join(ch for ch in n if unicodedata.category(ch) != "Mn")
    return n.lower().strip()


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ============================================================
    # ITEM 1: Normalizar cidades
    # ============================================================
    print("=== ITEM 1: NORMALIZAR CIDADES ===")

    cur.execute('SELECT DISTINCT cidade FROM empreendimentos WHERE cidade IS NOT NULL AND cidade != ""')
    cidades = [r[0] for r in cur.fetchall()]

    total_norm = 0
    for cidade in cidades:
        normed = norm_text(cidade)
        if normed in CIDADE_CANONICA:
            canonica = CIDADE_CANONICA[normed]
            if cidade != canonica:
                cur.execute("UPDATE empreendimentos SET cidade = ? WHERE cidade = ?", (canonica, cidade))
                cnt = cur.rowcount
                total_norm += cnt
                print(f'  "{cidade}" -> "{canonica}" ({cnt})')
        else:
            stripped = cidade.strip()
            if stripped != cidade:
                cur.execute("UPDATE empreendimentos SET cidade = ? WHERE cidade = ?", (stripped, cidade))
                total_norm += cur.rowcount

    conn.commit()
    print(f"Total normalizado: {total_norm}")

    # ============================================================
    # ITEM 2: Limpar cidades com lixo
    # ============================================================
    print(f"\n=== ITEM 2: LIMPAR CIDADES COM LIXO ===")

    cur.execute("""SELECT id, empresa, nome, cidade FROM empreendimentos
        WHERE cidade LIKE '%Cartório%' OR cidade LIKE '%Ofício%' OR cidade LIKE '%Registro%'
        OR cidade LIKE '%Comarca%' OR cidade LIKE '%Imóveis%' OR cidade LIKE '%Prefeitura%'
        OR cidade LIKE '%Oficial%' OR cidade LIKE '%registrado%'
        OR LENGTH(cidade) > 60
        OR cidade LIKE '%simulação%' OR cidade LIKE '%facebook%' OR cidade LIKE '%Consultor%'
        OR cidade LIKE '%praticidade%' OR cidade LIKE '%Posicionado%' OR cidade LIKE '%importante%'
        OR cidade LIKE '%desej%' OR cidade LIKE 'Residencial Maria%'
        OR cidade = 'RJ'
    """)
    lixo = cur.fetchall()
    print(f"Encontrados: {len(lixo)} com lixo no campo cidade")

    for emp_id, empresa, nome, cidade in lixo:
        cidade_real = None

        # De cartórios: extrair cidade
        m = re.search(r"(?:Imóveis|Comarca)\s+(?:de|do|da)\s+(.+?)(?:\s*$|\s*\.)", cidade)
        if m:
            cidade_real = m.group(1).strip()

        # Texto com quebra de linha: pegar primeira linha
        if not cidade_real and "\n" in cidade:
            primeira = cidade.split("\n")[0].strip()
            if 3 <= len(primeira) <= 40:
                cidade_real = primeira

        # Lixo total (facebook, simulação) -> extrair da Prefeitura
        if not cidade_real:
            m2 = re.search(r"Prefeitura\s+Municipal\s+de\s+(.+?)$", cidade)
            if m2:
                cidade_real = m2.group(1).strip()

        # RJ sozinho -> NULL
        if cidade == "RJ":
            cidade_real = None

        # Normalizar a cidade encontrada
        if cidade_real:
            normed = norm_text(cidade_real)
            if normed in CIDADE_CANONICA:
                cidade_real = CIDADE_CANONICA[normed]
            cur.execute("UPDATE empreendimentos SET cidade = ? WHERE id = ?", (cidade_real, emp_id))
            print(f'  [{empresa}] "{cidade[:50]}" -> "{cidade_real}"')
        else:
            cur.execute("UPDATE empreendimentos SET cidade = NULL WHERE id = ?", (emp_id,))
            print(f'  [{empresa}] "{cidade[:50]}" -> NULL')

    conn.commit()

    # ============================================================
    # ITEM 3: Normalizar fases
    # ============================================================
    print(f"\n=== ITEM 3: NORMALIZAR FASES ===")

    cur.execute('SELECT DISTINCT fase FROM empreendimentos WHERE fase IS NOT NULL AND fase != ""')
    fases = [r[0] for r in cur.fetchall()]

    total_fase = 0
    for fase in fases:
        normed = fase.strip().lower()
        if normed in FASE_MAP:
            canonica = FASE_MAP[normed]
            if fase != canonica:
                cur.execute("UPDATE empreendimentos SET fase = ? WHERE fase = ?", (canonica, fase))
                cnt = cur.rowcount
                if cnt > 0:
                    total_fase += cnt
                    print(f'  "{fase}" -> "{canonica}" ({cnt})')

    conn.commit()
    print(f"Total normalizado: {total_fase}")

    # ============================================================
    # ITEM 6: Limpar nomes sujos
    # ============================================================
    print(f"\n=== ITEM 6: LIMPAR NOMES SUJOS ===")

    cur.execute("""SELECT id, empresa, nome FROM empreendimentos
        WHERE nome LIKE 'Apartamento%venda%'
        OR nome LIKE 'Lançamento %Construtora%'
        OR nome LIKE 'Lançamento %Canopus%'
        OR nome LIKE '%— BM7 Construtora'
        OR nome LIKE '%BM7 Construtora'
        OR nome = 'Empreendimentos'
        OR nome LIKE 'Bora Incorporadora%'
        OR nome LIKE '%10 Razões%'
        OR nome LIKE '%razões%'
        OR nome LIKE 'Sobrados com%BM7%'
        OR nome LIKE 'Apto.%alugar%'
        OR nome LIKE 'APTO.%VENDA%BM7%'
        OR nome LIKE 'Salão comercial%'
    """)
    nomes_sujos = cur.fetchall()
    print(f"Encontrados: {len(nomes_sujos)} com nome sujo")

    deletar = []
    for emp_id, empresa, nome in nomes_sujos:
        nome_limpo = nome

        # 'Apartamentos à venda no X' ou 'Apartamentos à venda em X - Nome'
        m = re.match(r"Apartamentos?\s+[àa]\s+venda\s+(?:no?|em|na)\s+(.+?)(?:\s*[-–]\s*(.+))?$", nome)
        if m:
            nome_limpo = m.group(2).strip() if m.group(2) else m.group(1).strip()

        # 'Lançamento Today Vila Clementino Construtora Canopus'
        nome_limpo = re.sub(r"^Lançamento\s+", "", nome_limpo)

        # Remover sufixos de construtora
        nome_limpo = re.sub(r"\s*[-–—│]\s*(?:BM7|Canopus|Bora)\s*(?:Construtora|Incorporadora)?$", "", nome_limpo, flags=re.IGNORECASE)
        nome_limpo = re.sub(r"\s+(?:Construtora|Incorporadora)\s+(?:Canopus|BM7|Bora)$", "", nome_limpo, flags=re.IGNORECASE)
        nome_limpo = re.sub(r"\s+Canopus$", "", nome_limpo)
        nome_limpo = re.sub(r"\s+BM7\s+Construtora$", "", nome_limpo)

        # Casos que devem ser deletados (não são empreendimentos)
        is_lixo = False
        if nome_limpo.strip().lower() in ("bora incorporadora", "empreendimentos", ""):
            is_lixo = True
        if "10 razões" in nome.lower() or "razões" in nome.lower():
            is_lixo = True
        if "alugar" in nome.lower() or "salão comercial" in nome.lower():
            is_lixo = True
        if "APTO. MOBILIADO" in nome:
            is_lixo = True

        if is_lixo:
            deletar.append((emp_id, empresa, nome))
            print(f'  [{empresa}] "{nome[:60]}" -> MARCAR P/ REMOÇÃO')
        elif nome_limpo != nome:
            cur.execute("UPDATE empreendimentos SET nome = ? WHERE id = ?", (nome_limpo.strip(), emp_id))
            print(f'  [{empresa}] "{nome[:60]}" -> "{nome_limpo.strip()[:60]}"')

    conn.commit()

    if deletar:
        print(f"\n  {len(deletar)} registros marcados para remoção (não são empreendimentos):")
        for emp_id, empresa, nome in deletar:
            print(f"    [{empresa}] #{emp_id}: {nome[:60]}")
        # Não deleta automaticamente — registra para revisão
        print("  (Não deletados automaticamente — revisar com operador)")

    # ============================================================
    # STATS FINAIS
    # ============================================================
    print(f"\n=== STATS FINAIS ===")
    cur.execute('SELECT COUNT(DISTINCT cidade) FROM empreendimentos WHERE cidade IS NOT NULL AND cidade != ""')
    print(f"Cidades distintas: {cur.fetchone()[0]}")
    cur.execute('SELECT DISTINCT fase FROM empreendimentos WHERE fase IS NOT NULL AND fase != "" ORDER BY fase')
    print(f"Fases: {[r[0] for r in cur.fetchall()]}")
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE cidade IS NULL OR cidade = ''")
    print(f"Sem cidade: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE fase IS NULL OR fase = ''")
    print(f"Sem fase: {cur.fetchone()[0]}")

    conn.close()
    print("\nDONE")


if __name__ == "__main__":
    main()

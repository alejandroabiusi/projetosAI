"""
Modulo de banco de dados SQLite para empreendimentos imobiliarios.
=================================================================
Gerencia a criacao do banco, insercao de registros e extensao
dinamica de colunas (atributos binarios) conforme novos players
sao adicionados.

O banco tem duas categorias de colunas:
  1. Colunas descritivas (texto/numero): nome, endereco, preco, etc.
  2. Colunas de atributos binarios (0/1): tem_piscina, tem_churrasqueira, etc.

Quando um novo player traz atributos que nao existiam, o metodo
garantir_coluna() cria a coluna automaticamente com valor default 0.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATA_DIR
from data.movimentacoes import registrar_movimentacao


DB_PATH = os.path.join(DATA_DIR, "empreendimentos.db")


# Colunas descritivas fixas (nunca sao binarias)
COLUNAS_DESCRITIVAS = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("empresa", "TEXT NOT NULL"),
    ("nome", "TEXT NOT NULL"),
    ("slug", "TEXT"),
    ("url_fonte", "TEXT"),
    ("cidade", "TEXT"),
    ("estado", "TEXT"),
    ("bairro", "TEXT"),
    ("endereco", "TEXT"),
    ("fase", "TEXT"),                     # Lançamento, Em Construção, Imóvel Pronto, Futuro Lançamento, Breve Lançamento
    ("preco_a_partir", "REAL"),           # Valor em reais
    ("renda_minima", "REAL"),             # Renda familiar minima
    ("area_terreno_m2", "REAL"),
    ("numero_torres", "INTEGER"),
    ("total_unidades", "INTEGER"),
    ("unidades_por_andar", "TEXT"),       # Pode variar por torre, guardar como texto
    ("numero_andares", "TEXT"),           # "Terreo + 22" - formato variavel, guardar como texto
    ("numero_vagas", "TEXT"),             # "19 auto + 1 Idoso + 1 PNE" - formato variavel
    ("dormitorios_descricao", "TEXT"),    # "1 e 2 dorms." ou "2 e 3 dorms c/suite"
    ("metragens_descricao", "TEXT"),      # Todas as metragens encontradas, separadas por |
    ("area_min_m2", "REAL"),             # Menor metragem encontrada
    ("area_max_m2", "REAL"),             # Maior metragem encontrada
    ("evolucao_obra_pct", "REAL"),        # Percentual de evolucao da obra
    ("arquitetura", "TEXT"),              # Escritorio de arquitetura
    ("paisagismo", "TEXT"),               # Escritorio de paisagismo
    ("decoracao", "TEXT"),                # Escritorio de decoracao
    ("itens_lazer", "TEXT"),              # Lista completa de itens de lazer, separados por |
    ("registro_incorporacao", "TEXT"),    # Registro de incorporacao (se disponivel)
    ("latitude", "TEXT"),                # Coordenada latitude
    ("longitude", "TEXT"),               # Coordenada longitude
    ("cep", "TEXT"),                     # CEP do empreendimento
    ("cluster_mpr", "TEXT"),             # Cluster MPR (ex: SP1, RMSP3, BA2)
    ("data_coleta", "TEXT NOT NULL"),     # ISO format
    ("data_atualizacao", "TEXT"),         # ISO format
]

# Atributos binarios de LAZER (0/1)
# Mapeamento: nome_coluna -> lista de termos que ativam o atributo
ATRIBUTOS_LAZER = {
    "lazer_piscina": ["piscina"],
    "lazer_churrasqueira": ["churrasqueira", "churrasq"],
    "lazer_fitness": ["fitness", "academia", "sala funcional", "ginástica", "ginastica"],
    "lazer_playground": ["playground"],
    "lazer_brinquedoteca": ["brinquedoteca"],
    "lazer_salao_festas": ["salão de festas", "salao de festas", "salão festas"],
    "lazer_pet_care": ["pet care", "pet place", "pet"],
    "lazer_coworking": ["coworking", "co-working"],
    "lazer_bicicletario": ["bicicletário", "bicicletario"],
    "lazer_quadra": ["quadra", "mini quadra", "beach tennis"],
    "lazer_delivery": ["delivery", "sala delivery"],
    "lazer_horta": ["horta"],
    "lazer_lavanderia": ["lavanderia"],
    "lazer_redario": ["redário", "redario"],
    "lazer_rooftop": ["rooftop", "cobertura"],
    "lazer_sauna": ["sauna"],
    "lazer_spa": ["r:(?<![a-zà-ú])spa(?![a-zà-ú])"],
    "lazer_piquenique": ["piquenique", "picnic"],
    "lazer_sport_bar": ["sport bar", "sportbar"],
    "lazer_cine": ["cinema", "cine open", "cine "],
    "lazer_easy_market": ["easy market", "mini market", "mini mercado"],
    "lazer_espaco_beleza": ["espaço beleza", "espaco beleza", "beleza"],
    "lazer_sala_estudos": ["sala de estudos", "estudos"],
    "lazer_espaco_gourmet": ["gourmet", "espaço gourmet"],
    "lazer_praca": ["praça", "praca", "pracas", "praças"],
    "lazer_solarium": ["solarium", "solário"],
    "lazer_sala_jogos": ["sala de jogos", "jogos", "game"],
}

# Atributos binarios do APARTAMENTO (0/1)
ATRIBUTOS_APARTAMENTO = {
    "apto_studio": ["studio", "studios"],
    "apto_1_dorm": ["1 dorm", "01 dorm", "1 quarto", "01 quarto", "1 e 2 dorm", "1 e 2 quarto"],
    "apto_2_dorms": ["2 dorm", "02 dorm", "2 quarto", "02 quarto", "1 e 2 dorm", "1 e 2 quarto", "2 e 3 dorm", "2 e 3 quarto"],
    "apto_3_dorms": ["3 dorm", "03 dorm", "3 quarto", "03 quarto", "2 e 3 dorm", "2 e 3 quarto"],
    "apto_4_dorms": ["4 dorm", "04 dorm", "4 quarto", "04 quarto"],
    "apto_suite": ["suíte", "suite", "c/suíte", "c/suite"],
    "apto_terraco": ["terraço", "terraco", "varanda"],
    "apto_giardino": ["giardino", "garden"],
    "apto_duplex": ["duplex", "cobertura duplex"],
    "apto_cobertura": ["cobertura"],
    "apto_vaga_garagem": ["vaga", "garagem", "estacionamento"],
}

# Atributos binarios de PROGRAMA HABITACIONAL (0/1)
ATRIBUTOS_PROGRAMA = {
    "prog_mcmv": ["minha casa minha vida", "mcmv", "casa verde e amarela"],
    "prog_his1": ["his 1", "his1", "habitação de interesse social 1"],
    "prog_his2": ["his 2", "his2", "habitação de interesse social 2"],
    "prog_hmp": ["hmp", "habitação de moradia popular"],
    "prog_pode_entrar": ["pode entrar"],
    "prog_casa_paulista": ["casa paulista"],
}

# Todos os atributos binarios combinados
TODOS_ATRIBUTOS_BINARIOS = {}
TODOS_ATRIBUTOS_BINARIOS.update(ATRIBUTOS_LAZER)
TODOS_ATRIBUTOS_BINARIOS.update(ATRIBUTOS_APARTAMENTO)
TODOS_ATRIBUTOS_BINARIOS.update(ATRIBUTOS_PROGRAMA)


def get_connection():
    """Retorna conexao com o banco, criando-o se nao existir."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def criar_banco():
    """Cria a tabela de empreendimentos se nao existir."""
    conn = get_connection()
    cursor = conn.cursor()

    # Monta CREATE TABLE com colunas descritivas
    colunas_sql = ", ".join(f"{nome} {tipo}" for nome, tipo in COLUNAS_DESCRITIVAS)

    cursor.execute(f"CREATE TABLE IF NOT EXISTS empreendimentos ({colunas_sql})")

    # Adiciona colunas binarias (caso a tabela ja exista sem elas)
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}

    for col_name in TODOS_ATRIBUTOS_BINARIOS:
        if col_name not in colunas_existentes:
            cursor.execute(f"ALTER TABLE empreendimentos ADD COLUMN {col_name} INTEGER DEFAULT 0")

    conn.commit()
    conn.close()

    # Cria tabelas de controle (runs + changelog)
    criar_tabelas_controle()


def criar_tabelas_controle():
    """Cria tabelas de controle para rastreamento de mudancas."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            inicio TEXT NOT NULL,
            fim TEXT,
            status TEXT,
            etapas_ok INTEGER DEFAULT 0,
            etapas_erro INTEGER DEFAULT 0,
            novos INTEGER DEFAULT 0,
            mudancas INTEGER DEFAULT 0,
            total_apos INTEGER,
            duracao_segundos INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            empresa TEXT NOT NULL,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,
            campo TEXT,
            valor_anterior TEXT,
            valor_novo TEXT,
            data_evento TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reconciliacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            empresa TEXT NOT NULL,
            tipo TEXT NOT NULL,
            nome_anterior TEXT,
            nome_novo TEXT,
            url_anterior TEXT NOT NULL,
            url_nova TEXT,
            fase_anterior TEXT,
            fase_nova TEXT,
            distancia_metros REAL,
            observacao TEXT
        )
    """)

    conn.commit()
    conn.close()


def garantir_coluna(nome_coluna, tipo="INTEGER DEFAULT 0"):
    """
    Garante que uma coluna existe na tabela. Se nao existir, cria.
    Util para quando um novo player traz atributos ineditos.
    """
    conn = get_connection()
    cursor = conn.cursor()
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}

    if nome_coluna not in colunas_existentes:
        cursor.execute(f"ALTER TABLE empreendimentos ADD COLUMN {nome_coluna} {tipo}")
        conn.commit()

    conn.close()


def detectar_atributos_binarios(texto_completo):
    """
    Recebe um texto (concatenacao de todos os textos da pagina do empreendimento)
    e retorna dict com atributos binarios detectados.
    Termos prefixados com 'r:' sao tratados como regex (para word boundary).
    """
    import re as _re
    texto_lower = texto_completo.lower()
    resultado = {}

    for col_name, termos in TODOS_ATRIBUTOS_BINARIOS.items():
        resultado[col_name] = 0
        for termo in termos:
            if termo.startswith("r:"):
                if _re.search(termo[2:], texto_lower):
                    resultado[col_name] = 1
                    break
            elif termo.lower() in texto_lower:
                resultado[col_name] = 1
                break

    return resultado


def inserir_empreendimento(dados):
    """
    Insere um empreendimento no banco.
    'dados' e um dict com chaves correspondendo aos nomes das colunas.
    Colunas nao presentes no dict serao preenchidas com NULL (descritivas)
    ou 0 (binarias).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Garante que todas as colunas do dict existem na tabela
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}
    for col_name in dados:
        if col_name not in colunas_existentes and col_name != "id":
            garantir_coluna(col_name)

    # Adiciona data_coleta se nao estiver presente
    if "data_coleta" not in dados:
        dados["data_coleta"] = datetime.now().isoformat()

    # Remove 'id' se presente (autoincrement)
    dados.pop("id", None)

    colunas = list(dados.keys())
    valores = list(dados.values())
    placeholders = ", ".join(["?"] * len(colunas))
    colunas_sql = ", ".join(colunas)

    cursor.execute(
        f"INSERT INTO empreendimentos ({colunas_sql}) VALUES ({placeholders})",
        valores,
    )

    conn.commit()
    last_id = cursor.lastrowid
    conn.close()

    # Registrar movimentacao: novo produto
    registrar_movimentacao(
        empresa=dados.get("empresa", ""),
        nome=dados.get("nome", ""),
        tipo="novo",
        url_fonte=dados.get("url_fonte"),
        valor_novo=dados.get("fase"),
        origem="inserir_empreendimento",
    )

    return last_id


def empreendimento_existe(empresa, nome):
    """Verifica se um empreendimento ja foi registrado (por empresa + nome)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM empreendimentos WHERE empresa = ? AND nome = ?",
        (empresa, nome),
    )
    existe = cursor.fetchone()[0] > 0
    conn.close()
    return existe


def atualizar_empreendimento(empresa, nome, dados):
    """Atualiza um empreendimento existente."""
    conn = get_connection()
    cursor = conn.cursor()

    # Garante colunas
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}
    for col_name in dados:
        if col_name not in colunas_existentes and col_name != "id":
            garantir_coluna(col_name)

    dados["data_atualizacao"] = datetime.now().isoformat()
    dados.pop("id", None)
    dados.pop("data_coleta", None)

    set_clause = ", ".join(f"{col} = ?" for col in dados)
    valores = list(dados.values()) + [empresa, nome]

    cursor.execute(
        f"UPDATE empreendimentos SET {set_clause} WHERE empresa = ? AND nome = ?",
        valores,
    )

    conn.commit()
    conn.close()


def empreendimento_existe_por_url(empresa, url_fonte):
    """Verifica se um empreendimento ja foi registrado (por empresa + url_fonte)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM empreendimentos WHERE empresa = ? AND url_fonte = ?",
        (empresa, url_fonte),
    )
    existe = cursor.fetchone()[0] > 0
    conn.close()
    return existe


def atualizar_empreendimento_por_url(empresa, url_fonte, dados):
    """Atualiza um empreendimento existente usando url_fonte como chave."""
    conn = get_connection()
    cursor = conn.cursor()

    # Garante colunas
    colunas_existentes = {row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")}
    for col_name in dados:
        if col_name not in colunas_existentes and col_name != "id":
            garantir_coluna(col_name)

    dados["data_atualizacao"] = datetime.now().isoformat()
    dados.pop("id", None)
    dados.pop("data_coleta", None)

    set_clause = ", ".join(f"{col} = ?" for col in dados)
    valores = list(dados.values()) + [empresa, url_fonte]

    cursor.execute(
        f"UPDATE empreendimentos SET {set_clause} WHERE empresa = ? AND url_fonte = ?",
        valores,
    )

    conn.commit()
    conn.close()


def contar_empreendimentos(empresa=None):
    """Conta empreendimentos no banco, opcionalmente filtrado por empresa."""
    conn = get_connection()
    cursor = conn.cursor()
    if empresa:
        cursor.execute("SELECT COUNT(*) FROM empreendimentos WHERE empresa = ?", (empresa,))
    else:
        cursor.execute("SELECT COUNT(*) FROM empreendimentos")
    total = cursor.fetchone()[0]
    conn.close()
    return total


def listar_colunas():
    """Lista todas as colunas da tabela."""
    conn = get_connection()
    cursor = conn.cursor()
    colunas = [row[1] for row in cursor.execute("PRAGMA table_info(empreendimentos)")]
    conn.close()
    return colunas


# Campos rastreados para deteccao de mudancas
CAMPOS_RASTREADOS = [
    "fase", "preco_a_partir", "total_unidades", "evolucao_obra_pct",
    "dormitorios_descricao", "area_min_m2", "area_max_m2",
]


def snapshot_empreendimentos():
    """
    Retorna snapshot do estado atual de todos os empreendimentos.
    Chave: (empresa, nome) -> dict com campos rastreados.
    """
    conn = get_connection()
    cursor = conn.cursor()
    campos = ", ".join(["empresa", "nome", "cidade"] + CAMPOS_RASTREADOS)
    cursor.execute(f"SELECT {campos} FROM empreendimentos")
    snapshot = {}
    for row in cursor.fetchall():
        chave = (row["empresa"], row["nome"])
        snapshot[chave] = {campo: row[campo] for campo in CAMPOS_RASTREADOS}
        snapshot[chave]["cidade"] = row["cidade"]
    conn.close()
    return snapshot


def registrar_mudanca(run_id, empresa, nome, tipo, campo=None, anterior=None, novo=None):
    """Registra uma mudanca no changelog e no historico de movimentacoes."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO changelog (run_id, empresa, nome, tipo, campo, valor_anterior, valor_novo, data_evento) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, empresa, nome, tipo, campo,
         str(anterior) if anterior is not None else None,
         str(novo) if novo is not None else None,
         datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()

    # Espelhar no banco de movimentacoes
    # Para tipo "novo" vindo de comparar_snapshots, registrar tambem
    # (insercoes diretas ja registram via inserir_empreendimento)
    if tipo != "novo":
        registrar_movimentacao(
            empresa=empresa, nome=nome, tipo=tipo,
            campo=campo, valor_anterior=anterior, valor_novo=novo,
            origem=run_id,
        )


def comparar_snapshots(antes, depois, run_id):
    """
    Compara dois snapshots e registra mudancas no changelog.
    Retorna dict com contadores: {'novos': N, 'mudancas': M}.
    """
    novos = 0
    mudancas = 0

    # Detectar novos empreendimentos
    for chave in depois:
        if chave not in antes:
            empresa, nome = chave
            registrar_mudanca(run_id, empresa, nome, "novo")
            novos += 1

    # Detectar mudancas em empreendimentos existentes
    for chave in antes:
        if chave not in depois:
            continue
        dados_antes = antes[chave]
        dados_depois = depois[chave]
        for campo in CAMPOS_RASTREADOS:
            val_antes = dados_antes.get(campo)
            val_depois = dados_depois.get(campo)
            # Normalizar None vs valores vazios
            if val_antes is None and val_depois is None:
                continue
            if str(val_antes) != str(val_depois):
                empresa, nome = chave
                # Tipo especifico para fase e preco
                if campo == "fase":
                    tipo = "fase_mudou"
                elif campo == "preco_a_partir":
                    tipo = "preco_mudou"
                else:
                    tipo = "campo_mudou"
                registrar_mudanca(run_id, empresa, nome, tipo, campo, val_antes, val_depois)
                mudancas += 1

    return {"novos": novos, "mudancas": mudancas}


def registrar_run(run_id, inicio, fim, status, stats):
    """Registra uma execucao na tabela runs."""
    conn = get_connection()
    cursor = conn.cursor()
    duracao = int((fim - inicio).total_seconds())
    cursor.execute(
        "INSERT OR REPLACE INTO runs (id, inicio, fim, status, etapas_ok, etapas_erro, "
        "novos, mudancas, total_apos, duracao_segundos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, inicio.isoformat(), fim.isoformat(), status,
         stats.get("etapas_ok", 0), stats.get("etapas_erro", 0),
         stats.get("novos", 0), stats.get("mudancas", 0),
         stats.get("total_apos", 0), duracao),
    )
    conn.commit()
    conn.close()


def obter_changelog(run_id):
    """Retorna todas as mudancas de uma execucao."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM changelog WHERE run_id = ? ORDER BY tipo, empresa, nome",
        (run_id,),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def registrar_reconciliacao(empresa, tipo, url_anterior, nome_anterior=None,
                           nome_novo=None, url_nova=None, fase_anterior=None,
                           fase_nova=None, distancia_metros=None, observacao=None):
    """Registra um evento de reconciliacao (renomeado, relancado, cancelado)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO reconciliacao (data, empresa, tipo, nome_anterior, nome_novo, "
        "url_anterior, url_nova, fase_anterior, fase_nova, distancia_metros, observacao) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (datetime.now().isoformat(), empresa, tipo, nome_anterior, nome_novo,
         url_anterior, url_nova, fase_anterior, fase_nova, distancia_metros, observacao),
    )
    conn.commit()
    conn.close()

    # Espelhar no banco de movimentacoes
    obs_parts = []
    if nome_novo and nome_novo != nome_anterior:
        obs_parts.append(f"nome: {nome_anterior} -> {nome_novo}")
    if url_nova:
        obs_parts.append(f"url: {url_nova}")
    if distancia_metros:
        obs_parts.append(f"dist: {distancia_metros:.0f}m")
    if observacao:
        obs_parts.append(observacao)

    registrar_movimentacao(
        empresa=empresa,
        nome=nome_anterior or nome_novo or "",
        tipo=tipo,
        url_fonte=url_anterior,
        campo="url_fonte" if tipo == "renomeado" else None,
        valor_anterior=url_anterior,
        valor_novo=url_nova,
        observacao=" | ".join(obs_parts) if obs_parts else None,
        origem="reconciliacao",
    )


def listar_empresas():
    """Retorna lista de empresas distintas no banco."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT empresa FROM empreendimentos ORDER BY empresa")
    empresas = [row[0] for row in cursor.fetchall()]
    conn.close()
    return empresas


def buscar_empreendimentos_por_empresa(empresa):
    """Retorna todos os empreendimentos de uma empresa com coords e dados basicos."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT nome, url_fonte, fase, latitude, longitude, cidade, bairro "
        "FROM empreendimentos WHERE empresa = ?",
        (empresa,),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def remover_empreendimento_por_url(empresa, url_fonte):
    """Remove um empreendimento pelo url_fonte."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM empreendimentos WHERE empresa = ? AND url_fonte = ?",
        (empresa, url_fonte),
    )
    removidos = cursor.rowcount
    conn.commit()
    conn.close()
    return removidos


def obter_reconciliacao(empresa=None, limit=100):
    """Retorna eventos de reconciliacao, opcionalmente filtrados por empresa."""
    conn = get_connection()
    cursor = conn.cursor()
    if empresa:
        cursor.execute(
            "SELECT * FROM reconciliacao WHERE empresa = ? ORDER BY data DESC LIMIT ?",
            (empresa, limit),
        )
    else:
        cursor.execute(
            "SELECT * FROM reconciliacao ORDER BY data DESC LIMIT ?",
            (limit,),
        )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


# Inicializa o banco ao importar o modulo
criar_banco()

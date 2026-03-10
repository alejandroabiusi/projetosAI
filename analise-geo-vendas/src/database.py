"""
Módulo de banco de dados — conexão MySQL AWS, SQLite local, importação CSV/Excel.
Mapeia colunas SIOPI para schema interno do app.
"""

import os
import re
import sqlite3
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"
SQLITE_PATH = DATA_DIR / "vendas.db"

# Mapeamento: coluna SIOPI → coluna interna do app
MAPA_COLUNAS_SIOPI = {
    "idendificador": "incorporadora",
    "empreendimento": "empreendimento",
    "municipio": "municipio",
    "data_cadastro": "data_venda",
    "avaliacao_do_imovel": "preco",
    "valor_compra_e_venda_ou_orcamento_proposto_pelo_cliente": "valor_compra_venda",
    "valor_financiamento_negociado": "valor_financiado",
    "valor_recursos_proprios": "recursos_proprios",
    "valor_subsidio": "valor_subsidio",
    "fgts_real": "fgts",
    "renda_comprovada": "renda_cliente",
    "renda_total_apurada": "renda_total",
    "quantidade_de_dormitorios": "dormitorios",
    "ocupacao": "profissao_cliente",
    "sexo": "sexo_cliente",
    "estado_civil": "estado_civil",
    "cep_real": "cep_cliente",
    "endereco_da_unidade_habitacional": "endereco_empreendimento",
    "encargo_mensal": "encargo_mensal",
    "taxa_efetiva_anual": "taxa_juros",
    "prazo_de_amortizacao_negociado_meses": "prazo_meses",
    "vagas_de_garagem": "vagas_garagem",
    "fonte_de_recurso": "fonte_recurso",
    "tipo_de_financiamento": "tipo_financiamento",
    "cota_de_financiamento_calculada": "pct_financiamento",
    "ANO": "ano",
}


def get_mysql_engine():
    """Cria engine SQLAlchemy para MySQL AWS."""
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    if not all([host, user, password, database]):
        return None

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def get_sqlite_connection():
    """Retorna conexão SQLite local com timeout para concorrência."""
    return sqlite3.connect(str(SQLITE_PATH), timeout=30)


def _extrair_cidade_estado(municipio: str) -> tuple[str, str]:
    """Extrai cidade e estado de 'TAUBATE/ SP' → ('TAUBATE', 'SP')."""
    if pd.isna(municipio):
        return "", ""
    partes = municipio.split("/")
    cidade = partes[0].strip()
    estado = partes[1].strip() if len(partes) > 1 else ""
    return cidade, estado


def _extrair_metragem(descricao: str) -> float | None:
    """Extrai metragem da descrição da unidade habitacional."""
    if pd.isna(descricao):
        return None
    # Busca 'area privativa de XXX,XXXXm' ou similar
    match = re.search(r"privativa\s+de\s+([\d.,]+)\s*m", str(descricao), re.IGNORECASE)
    if match:
        return float(match.group(1).replace(",", "."))
    return None


def _extrair_cep_empreendimento(endereco: str) -> str | None:
    """Extrai CEP do endereço do empreendimento."""
    if pd.isna(endereco):
        return None
    match = re.search(r"CEP\s*([\d.]+[-\s]?\d+)", str(endereco))
    if match:
        return match.group(1).replace(".", "").replace(" ", "").replace("-", "")
    return None


def _limpar_profissao(profissao: str) -> str:
    """Remove código numérico da profissão: '0000000702 - MECANICO...' → 'MECANICO...'."""
    if pd.isna(profissao):
        return ""
    match = re.match(r"\d+\s*-\s*(.+)", str(profissao))
    return match.group(1).strip() if match else str(profissao).strip()


def _tipologia_dormitorios(n: int) -> str:
    """Converte número de dormitórios em tipologia: 2 → '2D'."""
    if pd.isna(n):
        return "N/D"
    return f"{int(n)}D"


def importar_arquivo(file_path: str, tabela: str = "vendas") -> int:
    """
    Importa CSV ou Excel para SQLite local.
    Detecta se é base SIOPI e mapeia colunas automaticamente.
    Retorna número de linhas importadas.
    """
    path = Path(file_path)

    # Lê arquivo (CSV ou Excel)
    if path.suffix in (".xlsx", ".xls") or ".xlsx" in path.name or ".xls" in path.name:
        df = pd.read_excel(file_path)
    else:
        # Detecta separador (pipe ou vírgula)
        with open(file_path, encoding="utf-8") as f:
            primeira_linha = f.readline()
        sep = "|" if "|" in primeira_linha else ","
        df = pd.read_csv(file_path, sep=sep, encoding="utf-8", low_memory=False,
                         on_bad_lines="skip")

    # Normaliza nomes de colunas
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Detecta se é base SIOPI (tem 'idendificador' ou 'avaliacao_do_imovel')
    colunas_siopi = {"idendificador", "avaliacao_do_imovel", "empreendimento"}
    is_siopi = len(colunas_siopi & set(df.columns)) >= 2

    if is_siopi:
        df = _transformar_siopi(df)
    else:
        df = _transformar_generico(df)

    conn = get_sqlite_connection()
    df.to_sql(tabela, conn, if_exists="replace", index=False)

    # Cria índices para performance
    cursor = conn.cursor()
    for col, idx_name in [
        ("latitude, longitude", "coords"),
        ("data_venda", "data"),
        ("incorporadora", "incorporadora"),
        ("empreendimento", "empreendimento"),
    ]:
        try:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{tabela}_{idx_name}
                ON {tabela} ({col})
            """)
        except sqlite3.OperationalError:
            pass  # Coluna pode não existir

    conn.commit()
    conn.close()

    return len(df)


def _transformar_siopi(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma base SIOPI para schema interno."""
    # Renomeia colunas mapeadas
    rename = {k: v for k, v in MAPA_COLUNAS_SIOPI.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Extrai cidade e estado do município
    if "municipio" in df.columns:
        parsed = df["municipio"].apply(_extrair_cidade_estado)
        df["cidade"] = parsed.apply(lambda x: x[0])
        df["estado"] = parsed.apply(lambda x: x[1])

    # Extrai metragem da descrição
    if "descricao_da_unidade_habitacional" in df.columns:
        df["metragem"] = df["descricao_da_unidade_habitacional"].apply(_extrair_metragem)

    # Extrai CEP do empreendimento
    if "endereco_empreendimento" in df.columns:
        df["cep_empreendimento"] = df["endereco_empreendimento"].apply(
            _extrair_cep_empreendimento
        )

    # Limpa profissão
    if "profissao_cliente" in df.columns:
        df["profissao_cliente"] = df["profissao_cliente"].apply(_limpar_profissao)

    # Tipologia a partir de dormitórios
    if "dormitorios" in df.columns:
        df["tipologia"] = df["dormitorios"].apply(_tipologia_dormitorios)

    # Data da venda
    if "data_venda" in df.columns:
        df["data_venda"] = pd.to_datetime(df["data_venda"], dayfirst=True, errors="coerce")

    # Colunas numéricas — limpa formato BR (240.000,00 → 240000.00)
    for col in ["preco", "valor_compra_venda", "valor_financiado", "recursos_proprios",
                 "renda_cliente", "renda_total", "metragem", "encargo_mensal",
                 "fgts", "valor_subsidio"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(r"[R$\s%]", "", regex=True)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Placeholder para coordenadas (serão preenchidas por geocodificação)
    if "latitude" not in df.columns:
        df["latitude"] = None
    if "longitude" not in df.columns:
        df["longitude"] = None

    return df


def _transformar_generico(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma base genérica (normaliza colunas)."""
    df.columns = df.columns.str.lower()

    # Tenta parsear data_venda
    for col in ["data_venda", "data", "dt_venda", "data_contrato", "data_cadastro"]:
        if col in df.columns:
            df["data_venda"] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            break

    # Converte colunas numéricas
    for col in ["latitude", "longitude", "preco", "metragem", "renda_cliente",
                 "valor_financiado", "recursos_proprios"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "."), errors="coerce"
            )

    return df


# Alias para compatibilidade
importar_csv = importar_arquivo


def carregar_dados_mysql(query: str = "SELECT * FROM vendas") -> pd.DataFrame:
    """Carrega dados do MySQL AWS."""
    engine = get_mysql_engine()
    if engine is None:
        raise ConnectionError("Credenciais MySQL não configuradas em .env")
    return pd.read_sql(query, engine)


def carregar_dados_sqlite(tabela: str = "vendas") -> pd.DataFrame:
    """Carrega todos os dados do SQLite local."""
    conn = get_sqlite_connection()
    df = pd.read_sql(f"SELECT * FROM {tabela}", conn)
    conn.close()

    if "data_venda" in df.columns:
        df["data_venda"] = pd.to_datetime(df["data_venda"], errors="coerce")

    # Garante que lat/lon são numéricos (podem vir como string do SQLite)
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def sqlite_existe() -> bool:
    """Verifica se o banco SQLite local existe e tem dados."""
    if not SQLITE_PATH.exists():
        return False
    conn = get_sqlite_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vendas")
        count = cursor.fetchone()[0]
        return count > 0
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def contar_registros(tabela: str = "vendas") -> int:
    """Retorna total de registros na tabela."""
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def listar_empreendimentos(tabela: str = "vendas") -> pd.DataFrame:
    """Lista empreendimentos únicos com contagem e endereço."""
    conn = get_sqlite_connection()
    df = pd.read_sql(f"""
        SELECT
            empreendimento,
            incorporadora,
            cidade,
            estado,
            COUNT(*) as total_vendas,
            latitude,
            longitude
        FROM {tabela}
        GROUP BY empreendimento
        ORDER BY total_vendas DESC
    """, conn)
    conn.close()
    return df

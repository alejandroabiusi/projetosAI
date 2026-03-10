# -*- coding: utf-8 -*-
"""
Cria o banco SQLite e popula com dados das planilhas XLSX.
Escopo: 1T2020 a 3T2025, empresas: Tenda, MRV, Direcional, Cury, PlanoePlano, Cyrela
"""

import sqlite3
import openpyxl
import re
import os
from pathlib import Path
from datetime import datetime

DB_PATH = "C:/Projetos_AI/analise_releases/dados_financeiros.db"
PLANILHAS_DIR = Path("C:/Projetos_AI/analise_releases/planilhas")

# Períodos de interesse: 1T2020 a 3T2025
PERIODOS_VALIDOS = set()
for ano in range(2020, 2026):
    for tri in range(1, 5):
        p = f"{tri}T{ano}"
        if ano == 2025 and tri > 3:
            continue
        PERIODOS_VALIDOS.add(p)
        # Variações comuns nos headers
        PERIODOS_VALIDOS.add(f"{tri}T{str(ano)[2:]}")  # 1T20


def normalizar_periodo(val):
    """Normaliza header de período para formato xTyyyy."""
    if val is None:
        return None
    s = str(val).strip()

    # Datetime objects (Cury balance sheet)
    if isinstance(val, datetime):
        tri = (val.month - 1) // 3
        if tri == 0:
            tri = 4
            ano = val.year - 1
        else:
            ano = val.year
        # Map: month 3->1T, 6->2T, 9->3T, 12->4T
        month_to_tri = {3: 1, 6: 2, 9: 3, 12: 4, 1: 1, 2: 1, 4: 2, 5: 2, 7: 3, 8: 3, 10: 4, 11: 4}
        tri = month_to_tri.get(val.month, 1)
        return f"{tri}T{val.year}"

    # Pattern: xTyy or xTyyyy
    m = re.match(r'^(\d)[Tt](\d{2,4})$', s)
    if m:
        tri = m.group(1)
        ano = m.group(2)
        if len(ano) == 2:
            ano = f"20{ano}"
        return f"{tri}T{ano}"

    # Pattern: xQyy (English)
    m = re.match(r'^(\d)[Qq](\d{2,4})$', s)
    if m:
        tri = m.group(1)
        ano = m.group(2)
        if len(ano) == 2:
            ano = f"20{ano}"
        return f"{tri}T{ano}"

    # Bilingual: "3T25/3Q25"
    m = re.match(r'^(\d)[Tt](\d{2,4})\s*/\s*\d[Qq]\d{2,4}$', s)
    if m:
        tri = m.group(1)
        ano = m.group(2)
        if len(ano) == 2:
            ano = f"20{ano}"
        return f"{tri}T{ano}"

    # Date format: "2025-09-30"
    m = re.match(r'^(\d{4})-(\d{2})-\d{2}$', s)
    if m:
        ano = int(m.group(1))
        mes = int(m.group(2))
        month_to_tri = {3: 1, 6: 2, 9: 3, 12: 4}
        tri = month_to_tri.get(mes)
        if tri:
            return f"{tri}T{ano}"

    return None


def periodo_valido(p):
    """Verifica se o período está no escopo 1T2020 a 3T2025."""
    if not p:
        return False
    m = re.match(r'^(\d)T(\d{4})$', p)
    if not m:
        return False
    tri = int(m.group(1))
    ano = int(m.group(2))
    if ano < 2020 or ano > 2025:
        return False
    if ano == 2025 and tri > 3:
        return False
    return True


def criar_schema(conn):
    """Cria as tabelas do banco."""
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS dados_trimestrais")
    c.execute("DROP TABLE IF EXISTS empresas")
    c.execute("DROP TABLE IF EXISTS log_ingestao")

    c.execute("""
    CREATE TABLE empresas (
        id INTEGER PRIMARY KEY,
        nome TEXT UNIQUE NOT NULL,
        sigla TEXT,
        segmentos TEXT
    )""")

    c.execute("""
    CREATE TABLE dados_trimestrais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa TEXT NOT NULL,
        segmento TEXT DEFAULT 'Consolidado',
        periodo TEXT NOT NULL,
        ano INTEGER NOT NULL,
        trimestre INTEGER NOT NULL,
        fonte TEXT DEFAULT 'planilha',

        -- Lançamentos
        vgv_lancado REAL,
        empreendimentos_lancados REAL,
        unidades_lancadas REAL,
        preco_medio_lancamento REAL,
        tamanho_medio_empreendimentos REAL,

        -- Vendas
        vendas_brutas_vgv REAL,
        vendas_brutas_unidades REAL,
        distratos_vgv REAL,
        distratos_unidades REAL,
        vendas_liquidas_vgv REAL,
        vendas_liquidas_unidades REAL,
        vso_bruta_trimestral REAL,
        vso_liquida_trimestral REAL,
        vso_bruta_12m REAL,
        vso_liquida_12m REAL,
        preco_medio_vendas REAL,

        -- Repasses e Entregas
        vgv_repassado REAL,
        unidades_repassadas REAL,
        unidades_entregues REAL,
        obras_em_andamento REAL,

        -- Estoque
        estoque_vgv REAL,
        estoque_unidades REAL,
        preco_medio_estoque REAL,
        pct_estoque_pronto REAL,
        giro_estoque_meses REAL,

        -- Landbank
        landbank_vgv REAL,
        landbank_empreendimentos REAL,
        landbank_unidades REAL,
        landbank_preco_medio REAL,
        pct_permuta_total REAL,

        -- Backlog / Resultado a apropriar
        receitas_apropriar REAL,
        custo_apropriar REAL,
        resultado_apropriar REAL,
        margem_apropriar REAL,

        -- DRE
        receita_bruta REAL,
        receita_liquida REAL,
        custo_imoveis_vendidos REAL,
        lucro_bruto REAL,
        margem_bruta REAL,
        lucro_bruto_ajustado REAL,
        margem_bruta_ajustada REAL,
        ebitda REAL,
        ebitda_ajustado REAL,
        margem_ebitda REAL,
        margem_ebitda_ajustada REAL,
        resultado_financeiro REAL,
        receitas_financeiras REAL,
        despesas_financeiras REAL,
        lucro_liquido REAL,
        margem_liquida REAL,
        roe REAL,

        -- SG&A
        despesas_comerciais REAL,
        despesas_ga REAL,
        honorarios_administracao REAL,
        outras_receitas_despesas_op REAL,
        equivalencia_patrimonial REAL,
        total_despesas_operacionais REAL,

        -- SG&A Indicadores (%)
        pct_comerciais_receita_liquida REAL,
        pct_ga_receita_liquida REAL,
        pct_sga_receita_liquida REAL,

        -- Endividamento
        divida_bruta REAL,
        caixa_aplicacoes REAL,
        divida_liquida REAL,
        divida_liquida_pl REAL,
        patrimonio_liquido REAL,

        -- Geração de Caixa
        geracao_caixa REAL,

        UNIQUE(empresa, segmento, periodo)
    )""")

    c.execute("""
    CREATE TABLE log_ingestao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa TEXT,
        planilha TEXT,
        aba TEXT,
        registros_inseridos INTEGER,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Inserir empresas
    empresas = [
        ('Tenda', 'TEND3', 'Tenda Core, Alea, Consolidado'),
        ('MRV', 'MRVE3', 'MRV Incorporação, Resia, Urba, Luggo, Consolidado'),
        ('Direcional', 'DIRR3', 'Direcional, Riva, Consolidado'),
        ('Cury', 'CURY3', 'Consolidado'),
        ('PlanoePlano', 'PLPL3', 'Consolidado'),
        ('Cyrela', 'CYRE3', 'Cyrela, Vivaz, Consolidado'),
    ]
    for nome, sigla, segs in empresas:
        c.execute("INSERT INTO empresas (nome, sigla, segmentos) VALUES (?,?,?)", (nome, sigla, segs))

    conn.commit()
    print("  Schema criado com sucesso")


def ler_planilha_generica(filepath, aba, header_row, label_col, data_start_col, field_map, empresa, segmento='Consolidado', unidade_milhares=False):
    """
    Lê uma aba de planilha e retorna dict {periodo: {campo: valor}}.
    field_map: dict {row_number: campo_db} ou {label_text: campo_db}
    """
    wb = openpyxl.load_workbook(str(filepath), read_only=True, data_only=True)
    ws = wb[aba]

    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows or len(rows) < header_row:
        return {}

    # Build period map: col_index -> periodo normalizado
    header = rows[header_row - 1]  # 0-indexed
    col_periods = {}
    for col_idx, val in enumerate(header):
        p = normalizar_periodo(val)
        if p and periodo_valido(p):
            col_periods[col_idx] = p

    if not col_periods:
        return {}

    # Build label map if field_map uses labels
    label_based = any(isinstance(k, str) for k in field_map.keys())

    # Extract data
    result = {}  # {periodo: {campo: valor}}

    for row_idx, row in enumerate(rows):
        actual_row = row_idx + 1  # 1-indexed

        if label_based:
            label_val = row[label_col - 1] if label_col - 1 < len(row) else None
            if label_val is None:
                continue
            label_str = str(label_val).strip()
            # Find matching field
            campo = None
            for label_pattern, campo_name in field_map.items():
                if isinstance(label_pattern, str):
                    if label_str.lower().startswith(label_pattern.lower()) or label_pattern.lower() in label_str.lower():
                        campo = campo_name
                        break
        else:
            campo = field_map.get(actual_row)

        if not campo:
            continue

        for col_idx, periodo in col_periods.items():
            if col_idx < len(row):
                val = row[col_idx]
                if val is not None:
                    try:
                        num_val = float(val)
                        if unidade_milhares:
                            num_val = num_val / 1000  # converter para milhões
                        if periodo not in result:
                            result[periodo] = {}
                        result[periodo][campo] = num_val
                    except (ValueError, TypeError):
                        pass

    return result


def inserir_dados(conn, empresa, segmento, dados_por_periodo, planilha_nome, aba_nome):
    """Insere dados no banco, fazendo upsert."""
    c = conn.cursor()
    count = 0

    for periodo, campos in dados_por_periodo.items():
        m = re.match(r'(\d)T(\d{4})', periodo)
        if not m:
            continue
        tri = int(m.group(1))
        ano = int(m.group(2))

        # Check if record exists
        c.execute("SELECT id FROM dados_trimestrais WHERE empresa=? AND segmento=? AND periodo=?",
                  (empresa, segmento, periodo))
        existing = c.fetchone()

        if existing:
            # Update only non-null fields
            for campo, valor in campos.items():
                try:
                    c.execute(f"UPDATE dados_trimestrais SET {campo}=? WHERE id=?", (valor, existing[0]))
                except Exception as e:
                    print(f"    ERRO update {campo}: {e}")
        else:
            # Insert new record
            campo_names = ['empresa', 'segmento', 'periodo', 'ano', 'trimestre', 'fonte'] + list(campos.keys())
            campo_vals = [empresa, segmento, periodo, ano, tri, 'planilha'] + list(campos.values())
            placeholders = ','.join(['?'] * len(campo_vals))
            col_str = ','.join(campo_names)
            try:
                c.execute(f"INSERT INTO dados_trimestrais ({col_str}) VALUES ({placeholders})", campo_vals)
                count += 1
            except Exception as e:
                print(f"    ERRO insert {periodo}: {e}")

    conn.commit()

    # Log
    c.execute("INSERT INTO log_ingestao (empresa, planilha, aba, registros_inseridos) VALUES (?,?,?,?)",
              (empresa, planilha_nome, aba_nome, count))
    conn.commit()
    return count


def calcular_indicadores(conn):
    """Calcula indicadores derivados (% receita)."""
    c = conn.cursor()
    c.execute("""
        UPDATE dados_trimestrais SET
            pct_comerciais_receita_liquida = CASE WHEN receita_liquida > 0 THEN despesas_comerciais / receita_liquida END,
            pct_ga_receita_liquida = CASE WHEN receita_liquida > 0 THEN despesas_ga / receita_liquida END,
            pct_sga_receita_liquida = CASE WHEN receita_liquida > 0 THEN total_despesas_operacionais / receita_liquida END
        WHERE receita_liquida IS NOT NULL AND receita_liquida != 0
    """)
    conn.commit()
    updated = c.rowcount
    print(f"  Indicadores calculados para {updated} registros")


# ============================================================
# INGESTÃO POR EMPRESA
# ============================================================

def ingerir_tenda(conn):
    """Ingestão Tenda - 3 segmentos: Tenda, Alea, Consolidado."""
    print("\n  --- TENDA ---")
    fp = PLANILHAS_DIR / "Tenda_Planilha_Fundamentos_2026-03.xlsx"

    segments = {
        'Tenda': ('Tenda - Operacional', 'Tenda - Tenda Financeiro', 'Tenda - DRE'),
        'Alea': ('Alea - Operacional', 'Alea - Financeiro', 'Alea - DRE'),
        'Consolidado': ('Consolidado - Operacional', 'Consolidado - Financeiro', 'Consolidado - DRE'),
    }

    for seg, (aba_op, aba_fin, aba_dre) in segments.items():
        # Operacional - row-based (row numbers from mapping)
        op_map = {
            9: 'vgv_lancado', 18: 'empreendimentos_lancados', 20: 'unidades_lancadas',
            21: 'preco_medio_lancamento', 22: 'tamanho_medio_empreendimentos',
            27: 'vendas_brutas_vgv', 28: 'vso_bruta_trimestral', 29: 'distratos_vgv',
            30: 'vendas_liquidas_vgv', 31: 'vso_liquida_trimestral',
            34: 'vendas_brutas_unidades', 35: 'distratos_unidades', 36: 'vendas_liquidas_unidades',
            38: 'preco_medio_vendas',
            45: 'vgv_repassado', 46: 'unidades_repassadas', 47: 'unidades_entregues', 48: 'obras_em_andamento',
            52: 'estoque_vgv', 53: 'estoque_unidades', 54: 'preco_medio_estoque',
            58: 'landbank_empreendimentos', 59: 'landbank_vgv', 61: 'landbank_unidades',
            62: 'landbank_preco_medio', 63: 'pct_permuta_total',
        }
        dados = ler_planilha_generica(fp, aba_op, 7, 1, 2, op_map, 'Tenda')
        n = inserir_dados(conn, 'Tenda', seg, dados, fp.name, aba_op)
        print(f"    {seg} Operacional: {n} registros")

        # Financeiro
        fin_map = {
            30: 'receita_bruta', 33: 'receita_liquida',
            38: 'lucro_bruto', 39: 'margem_bruta',
            41: 'lucro_bruto_ajustado', 42: 'margem_bruta_ajustada',
            49: 'despesas_comerciais', 50: 'despesas_ga', 51: 'total_despesas_operacionais',
            54: 'pct_comerciais_receita_liquida', 55: 'pct_ga_receita_liquida',
            59: 'outras_receitas_despesas_op', 62: 'equivalencia_patrimonial',
            66: 'receitas_financeiras', 67: 'despesas_financeiras', 68: 'resultado_financeiro',
            74: 'lucro_liquido', 75: 'margem_liquida',
            86: 'ebitda_ajustado', 88: 'margem_ebitda_ajustada',
        }
        # Only for main segments with full financial data
        try:
            dados = ler_planilha_generica(fp, aba_fin, 7, 1, 2, fin_map, 'Tenda')
            n = inserir_dados(conn, 'Tenda', seg, dados, fp.name, aba_fin)
            print(f"    {seg} Financeiro: {n} registros")
        except Exception as e:
            print(f"    {seg} Financeiro: ERRO - {e}")

        # Backlog (only in Consolidado - Financeiro and Tenda - Tenda Financeiro)
        if seg in ('Tenda', 'Consolidado'):
            backlog_map = {93: 'receitas_apropriar', 94: 'custo_apropriar',
                          95: 'resultado_apropriar', 96: 'margem_apropriar'}
            try:
                dados = ler_planilha_generica(fp, aba_fin, 7, 1, 2, backlog_map, 'Tenda')
                inserir_dados(conn, 'Tenda', seg, dados, fp.name, f"{aba_fin}_backlog")
            except:
                pass

        # Endividamento (only Consolidado - Financeiro)
        if seg == 'Consolidado':
            debt_map = {130: 'divida_bruta', 131: 'caixa_aplicacoes', 132: 'divida_liquida',
                        135: 'patrimonio_liquido', 136: 'divida_liquida_pl',
                        29: 'roe'}
            try:
                dados = ler_planilha_generica(fp, aba_fin, 7, 1, 2, debt_map, 'Tenda')
                inserir_dados(conn, 'Tenda', seg, dados, fp.name, f"{aba_fin}_divida")
            except:
                pass

    # Geração de caixa from Consolidado - Financeiro
    # ROE from row 29 of Consolidado - Financeiro
    # Already handled above


def ingerir_cury(conn):
    """Ingestão Cury - Consolidado."""
    print("\n  --- CURY ---")
    fp = PLANILHAS_DIR / "Cury_Planilha_Fundamentos_2026-03.xlsx"

    # Resultados Operacionais (header row 5, labels col B=2, data from col 4)
    op_map = {
        6: 'empreendimentos_lancados', 7: 'vgv_lancado', 12: 'unidades_lancadas',
        13: 'preco_medio_lancamento', 15: 'tamanho_medio_empreendimentos',
        19: 'vendas_brutas_vgv', 20: 'vendas_brutas_unidades', 21: 'preco_medio_vendas',
        22: 'vso_bruta_trimestral', 26: 'vendas_liquidas_vgv', 25: 'distratos_vgv',
        35: 'vso_liquida_trimestral', 36: 'vso_liquida_12m',
        39: 'vendas_brutas_unidades', 40: 'distratos_unidades', 41: 'vendas_liquidas_unidades',
        46: 'vgv_repassado', 47: 'unidades_repassadas',
        51: 'estoque_vgv', 58: 'estoque_unidades', 61: 'preco_medio_estoque', 63: 'giro_estoque_meses',
        67: 'obras_em_andamento', 69: 'unidades_entregues',
        74: 'landbank_vgv', 75: 'landbank_empreendimentos', 76: 'landbank_unidades', 77: 'landbank_preco_medio',
        90: 'geracao_caixa',
    }
    # Cury operational data: VGV is in R$ mil (thousands) -> convert to millions
    dados = ler_planilha_generica(fp, 'Resultados Operacionais', 5, 2, 4, op_map, 'Cury', unidade_milhares=True)
    n = inserir_dados(conn, 'Cury', 'Consolidado', dados, fp.name, 'Resultados Operacionais')
    print(f"    Operacional: {n} registros")

    # DRE (header row 3, labels col B=2, data from col 4)
    dre_map = {
        5: 'receita_liquida', 6: 'custo_imoveis_vendidos', 7: 'lucro_bruto',
        8: 'margem_bruta', 9: 'margem_bruta_ajustada',
        11: 'despesas_comerciais', 12: 'despesas_ga', 13: 'outras_receitas_despesas_op',
        15: 'equivalencia_patrimonial', 16: 'total_despesas_operacionais',
        19: 'despesas_financeiras', 20: 'receitas_financeiras', 21: 'resultado_financeiro',
        27: 'lucro_liquido', 28: 'margem_liquida',
        35: 'ebitda', 36: 'margem_ebitda', 37: 'ebitda_ajustado', 38: 'margem_ebitda_ajustada',
        41: 'receitas_apropriar', 43: 'resultado_apropriar', 44: 'margem_apropriar',
        46: 'roe',
    }
    dados = ler_planilha_generica(fp, 'Demonstrações do Resultado', 3, 2, 4, dre_map, 'Cury')
    n = inserir_dados(conn, 'Cury', 'Consolidado', dados, fp.name, 'Demonstrações do Resultado')
    print(f"    DRE: {n} registros")


def ingerir_direcional(conn):
    """Ingestão Direcional - Consolidado + Riva."""
    print("\n  --- DIRECIONAL ---")
    fp = PLANILHAS_DIR / "Direcional_Planilha_Interativa_2026-03.xlsx"

    # Dados Operacionais (header row 4, labels col B=2, data from col 3)
    op_map = {
        6: 'vgv_lancado', 12: 'unidades_lancadas', 15: 'pct_permuta_total', 16: 'preco_medio_lancamento',
        19: 'vendas_brutas_vgv', 25: 'vendas_brutas_unidades', 28: 'preco_medio_vendas',
        29: 'vso_liquida_trimestral', 30: 'vso_liquida_trimestral',  # VSO MCMV / Consolidada
        33: 'estoque_vgv', 35: 'estoque_unidades',
        40: 'landbank_vgv', 42: 'landbank_unidades',
        45: 'vgv_repassado', 46: 'unidades_repassadas',
    }
    dados = ler_planilha_generica(fp, 'Dados Operacionais', 4, 2, 3, op_map, 'Direcional', unidade_milhares=True)
    n = inserir_dados(conn, 'Direcional', 'Consolidado', dados, fp.name, 'Dados Operacionais')
    print(f"    Operacional: {n} registros")

    # DRE (header row 4, labels col B=2, data from col 3)
    dre_map = {
        5: 'receita_liquida', 6: 'custo_imoveis_vendidos', 7: 'lucro_bruto',
        8: 'despesas_ga', 9: 'despesas_comerciais',
        10: 'equivalencia_patrimonial', 11: 'outras_receitas_despesas_op',
        12: 'despesas_financeiras', 13: 'receitas_financeiras',
        18: 'lucro_liquido',
    }
    dados = ler_planilha_generica(fp, 'Demonstração de Resultados', 4, 2, 3, dre_map, 'Direcional', unidade_milhares=True)
    n = inserir_dados(conn, 'Direcional', 'Consolidado', dados, fp.name, 'Demonstração de Resultados')
    print(f"    DRE: {n} registros")

    # Balanço Patrimonial
    bp_map = {
        6: 'caixa_aplicacoes',  # Caixa e equivalentes
        34: 'divida_bruta',  # Emprestimos CP (will need to add LP)
        62: 'patrimonio_liquido',
    }
    dados = ler_planilha_generica(fp, 'Balanço Patrimonial', 4, 2, 3, bp_map, 'Direcional', unidade_milhares=True)
    n = inserir_dados(conn, 'Direcional', 'Consolidado', dados, fp.name, 'Balanço Patrimonial')
    print(f"    Balanço: {n} registros")

    # RIVA DRE (separate segment)
    riva_dre_map = {
        5: 'receita_liquida', 6: 'custo_imoveis_vendidos', 7: 'lucro_bruto',
        8: 'total_despesas_operacionais', 11: 'resultado_financeiro',
        16: 'lucro_liquido',
    }
    try:
        dados = ler_planilha_generica(fp, 'RIVA - DRE', 4, 2, 3, riva_dre_map, 'Direcional', unidade_milhares=True)
        n = inserir_dados(conn, 'Direcional', 'Riva', dados, fp.name, 'RIVA - DRE')
        print(f"    Riva DRE: {n} registros")
    except Exception as e:
        print(f"    Riva DRE: ERRO - {e}")


def ingerir_planoeplano(conn):
    """Ingestão PlanoePlano - Consolidado."""
    print("\n  --- PLANO&PLANO ---")
    fp = PLANILHAS_DIR / "PlanoePlano_Planilha_Interativa_2026-03.xlsx"

    # Dados Operacionais (header row 7, labels col B=2, data from col 3)
    op_map = {
        8: 'vendas_brutas_vgv', 9: 'vendas_brutas_unidades',
        10: 'distratos_vgv', 11: 'distratos_unidades',
        12: 'vendas_liquidas_vgv', 13: 'vendas_liquidas_unidades',
        15: 'preco_medio_vendas',
        21: 'vgv_lancado', 22: 'unidades_lancadas', 23: 'preco_medio_lancamento',
        24: 'tamanho_medio_empreendimentos',
        30: 'estoque_vgv', 31: 'estoque_unidades',
        37: 'vso_liquida_12m',
    }
    dados = ler_planilha_generica(fp, 'Dados Operacionais', 7, 2, 3, op_map, 'PlanoePlano', unidade_milhares=True)
    n = inserir_dados(conn, 'PlanoePlano', 'Consolidado', dados, fp.name, 'Dados Operacionais')
    print(f"    Operacional: {n} registros")

    # DRE Consolidado (header row 7, labels col B=2, data from col 3)
    dre_map = {
        9: 'receita_liquida', 10: 'custo_imoveis_vendidos', 11: 'lucro_bruto', 12: 'margem_bruta',
        14: 'despesas_comerciais', 15: 'despesas_ga',
        16: 'equivalencia_patrimonial', 17: 'outras_receitas_despesas_op',
        18: 'total_despesas_operacionais',
        22: 'receitas_financeiras', 23: 'despesas_financeiras', 24: 'resultado_financeiro',
        31: 'lucro_liquido', 33: 'margem_liquida',
        40: 'receitas_apropriar', 42: 'resultado_apropriar', 43: 'margem_apropriar',
    }
    dados = ler_planilha_generica(fp, 'DRE Consolidado', 7, 2, 3, dre_map, 'PlanoePlano', unidade_milhares=True)
    n = inserir_dados(conn, 'PlanoePlano', 'Consolidado', dados, fp.name, 'DRE Consolidado')
    print(f"    DRE: {n} registros")

    # Balanço
    bp_map = {
        10: 'caixa_aplicacoes',
        62: 'patrimonio_liquido',
    }
    try:
        dados = ler_planilha_generica(fp, 'Balanço Patrimonial Consolidado', 7, 2, 3, bp_map, 'PlanoePlano', unidade_milhares=True)
        n = inserir_dados(conn, 'PlanoePlano', 'Consolidado', dados, fp.name, 'Balanço Patrimonial')
        print(f"    Balanço: {n} registros")
    except Exception as e:
        print(f"    Balanço: ERRO - {e}")


def ingerir_mrv(conn):
    """Ingestão MRV - Consolidado + segmentos."""
    print("\n  --- MRV ---")
    fp = PLANILHAS_DIR / "MRV_Base_Dados_Operacionais_Financeiros_2026-03.xlsx"

    # DRE Consolidado (header row 2, labels col A=1, data from col 2)
    # Headers are bilingual: "3T25/3Q25"
    dre_map = {
        3: 'receita_liquida', 4: 'custo_imoveis_vendidos', 5: 'lucro_bruto', 6: 'margem_bruta',
        7: 'despesas_comerciais', 8: 'despesas_ga', 9: 'outras_receitas_despesas_op',
        10: 'equivalencia_patrimonial',
        12: 'resultado_financeiro', 13: 'despesas_financeiras', 14: 'receitas_financeiras',
        18: 'lucro_liquido', 21: 'margem_liquida',
        22: 'receitas_apropriar', 24: 'resultado_apropriar', 25: 'margem_apropriar',
    }
    dados = ler_planilha_generica(fp, 'DRE Consolid. | Income Statem.', 2, 1, 2, dre_map, 'MRV')
    n = inserir_dados(conn, 'MRV', 'Consolidado', dados, fp.name, 'DRE Consolid.')
    print(f"    DRE Consolidado: {n} registros")

    # Indicadores Financeiros (header row 2)
    fin_map = {
        3: 'receita_liquida',
        5: 'lucro_bruto', 6: 'margem_bruta', 7: 'margem_bruta_ajustada',
        8: 'despesas_comerciais', 9: 'pct_comerciais_receita_liquida',
        11: 'despesas_ga', 12: 'pct_ga_receita_liquida',
        14: 'ebitda', 15: 'margem_ebitda',
        16: 'lucro_liquido', 17: 'margem_liquida',
        18: 'roe',
        20: 'divida_bruta', 21: 'caixa_aplicacoes',
        23: 'divida_liquida', 24: 'patrimonio_liquido', 25: 'divida_liquida_pl',
        28: 'geracao_caixa',
    }
    dados = ler_planilha_generica(fp, 'Indic.Fin. | Financ.Highlights', 2, 1, 2, fin_map, 'MRV')
    n = inserir_dados(conn, 'MRV', 'Consolidado', dados, fp.name, 'Indic.Fin.')
    print(f"    Indicadores Financeiros: {n} registros")

    # Dados Operacionais MRV&Co (header row 2)
    # This sheet is complex with many sub-sections. We'll target key rows.
    # VGV Lancamentos %MRV rows ~57-78
    op_map = {
        57: 'vgv_lancado',  # VGV Lancamentos %MRV - MRV&Co
        80: 'vendas_brutas_vgv',  # Approximate - may need adjustment
    }
    # This sheet is very complex, better to use label-based extraction
    op_label_map = {
        'VGV Lancamentos (R$ milh': 'vgv_lancado',
    }
    # Will need more detailed row mapping - defer complex operational data for now
    # and get what we can from Indic.Fin.

    # DRE por segmento
    seg_sheets = {
        'MRV Incorporação': 'DRE MRV Inc. | Income Statem.',
        'Resia': 'DRE Resia | Income Statem.',
        'Urba': 'DRE Urba | Income Statem.',
        'Luggo': 'DRE Luggo | Income Statem.',
    }
    for seg_name, aba in seg_sheets.items():
        try:
            dados = ler_planilha_generica(fp, aba, 2, 1, 2, dre_map, 'MRV')
            n = inserir_dados(conn, 'MRV', seg_name, dados, fp.name, aba)
            print(f"    DRE {seg_name}: {n} registros")
        except Exception as e:
            print(f"    DRE {seg_name}: ERRO - {e}")


def ingerir_cyrela(conn):
    """Ingestão Cyrela - Dados operacionais + financeiros."""
    print("\n  --- CYRELA ---")

    # Dados Operacionais (header row 4, data in columns, periods from 2005)
    fp_op = PLANILHAS_DIR / "Cyrela_Dados_Operacionais_2026-03.xlsx"

    # Lançamentos (Ltos sheet)
    ltos_map = {
        5: 'vgv_lancado',  # VGV 100% Total
    }
    try:
        dados = ler_planilha_generica(fp_op, 'Ltos', 4, 1, 2, ltos_map, 'Cyrela')
        # Need to find the "Total" row - it varies. Use label matching instead.
    except:
        pass

    # Use label-based for Cyrela operational sheets
    vendas_label_map = {
        'Total': 'vendas_liquidas_vgv',  # The "Total" row in VGV 100% section
    }

    # Demonstrações Financeiras (single sheet CYRE3)
    fp_df = PLANILHAS_DIR / "Cyrela_Demonstracoes_Financeiras_2026-03.xlsx"

    # This sheet uses date-format headers and has many sections
    # Income statement starts around row 67
    dre_map = {
        68: 'receita_liquida',
        69: 'custo_imoveis_vendidos',
        70: 'lucro_bruto',
        72: 'despesas_comerciais',
        73: 'despesas_ga',
        76: 'equivalencia_patrimonial',
        78: 'ebitda',
        79: 'resultado_financeiro',
        80: 'receitas_financeiras',
        81: 'despesas_financeiras',
        88: 'lucro_liquido',
    }
    try:
        dados = ler_planilha_generica(fp_df, 'CYRE3', 4, 1, 2, dre_map, 'Cyrela')
        n = inserir_dados(conn, 'Cyrela', 'Consolidado', dados, fp_df.name, 'CYRE3_DRE')
        print(f"    DRE: {n} registros")
    except Exception as e:
        print(f"    DRE: ERRO - {e}")

    # Balance sheet (rows 9-65)
    bp_map = {
        10: 'caixa_aplicacoes',  # Caixa e equivalentes
        57: 'patrimonio_liquido',
    }
    try:
        dados = ler_planilha_generica(fp_df, 'CYRE3', 4, 1, 2, bp_map, 'Cyrela')
        n = inserir_dados(conn, 'Cyrela', 'Consolidado', dados, fp_df.name, 'CYRE3_BP')
        print(f"    Balanço: {n} registros")
    except Exception as e:
        print(f"    Balanço: ERRO - {e}")

    # Principais Indicadores (simple summary)
    fp_ind = PLANILHAS_DIR / "Cyrela_Principais_Indicadores_2026-03.xlsx"
    # This is a small summary sheet, not time-series. Skip for now.


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 70)
    print("CRIAÇÃO E POPULAÇÃO DO BANCO DE DADOS SQLite")
    print("=" * 70)

    # Remove existing DB
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("  Banco anterior removido")

    conn = sqlite3.connect(DB_PATH)

    # 1. Criar schema
    print("\n1. Criando schema...")
    criar_schema(conn)

    # 2. Ingerir dados
    print("\n2. Ingerindo dados das planilhas...")
    ingerir_tenda(conn)
    ingerir_cury(conn)
    ingerir_direcional(conn)
    ingerir_planoeplano(conn)
    ingerir_mrv(conn)
    ingerir_cyrela(conn)

    # 3. Calcular indicadores
    print("\n3. Calculando indicadores derivados...")
    calcular_indicadores(conn)

    # 4. Resumo final
    print("\n" + "=" * 70)
    print("RESUMO DA INGESTÃO")
    print("=" * 70)

    c = conn.cursor()
    c.execute("SELECT empresa, segmento, COUNT(*), MIN(periodo), MAX(periodo) FROM dados_trimestrais GROUP BY empresa, segmento ORDER BY empresa, segmento")
    for row in c.fetchall():
        print(f"  {row[0]:15s} | {row[1]:20s} | {row[2]:3d} registros | {row[3]} a {row[4]}")

    c.execute("SELECT COUNT(*) FROM dados_trimestrais")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT empresa) FROM dados_trimestrais")
    n_empresas = c.fetchone()[0]

    # Count non-null fields
    c.execute("SELECT COUNT(*) FROM dados_trimestrais WHERE receita_liquida IS NOT NULL")
    n_receita = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dados_trimestrais WHERE lucro_liquido IS NOT NULL")
    n_lucro = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dados_trimestrais WHERE vgv_lancado IS NOT NULL")
    n_vgv = c.fetchone()[0]

    print(f"\n  Total de registros: {total}")
    print(f"  Empresas: {n_empresas}")
    print(f"  Registros com receita líquida: {n_receita}")
    print(f"  Registros com lucro líquido: {n_lucro}")
    print(f"  Registros com VGV lançado: {n_vgv}")
    print(f"\n  Banco salvo em: {DB_PATH}")

    conn.close()
    print("\nConcluído!")

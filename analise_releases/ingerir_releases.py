# -*- coding: utf-8 -*-
"""
Extrai dados financeiros dos releases em PDF e atualiza o banco SQLite.
Foco: campos que faltam das planilhas (EBITDA, dívida, geração de caixa, ROE, margens, etc.)
"""

import sqlite3
import pdfplumber
import re
import os
import unicodedata
from pathlib import Path

DB_PATH = "C:/Projetos_AI/analise_releases/dados_financeiros.db"
COLETA_DIR = Path("C:/Projetos_AI/coleta/downloads")


def strip_accents(s):
    """Remove accents from string: Dívida → Divida, geração → geracao."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def parse_br_number(s):
    """Converte número formato BR (1.234,5) para float.
    Retorna None se não conseguir parsear."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace(' ', '')
    # Remove parênteses (valores negativos)
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1]
    if s.startswith('-'):
        neg = True
        s = s[1:]
    # Remove % se presente
    s = s.rstrip('%')
    s = s.strip()
    if not s:
        return None
    # BR format: dots for thousands, comma for decimal
    s = s.replace('.', '').replace(',', '.')
    try:
        val = float(s)
        return -val if neg else val
    except ValueError:
        return None


def parse_pct(s):
    """Converte porcentagem BR (26,3%) para float (0.263)."""
    if not s:
        return None
    s = s.strip()
    if '%' not in s and 'p.p' not in s:
        return None
    s = s.replace('%', '').strip()
    val = parse_br_number(s)
    if val is not None:
        return val / 100.0
    return None


def extract_first_number(text, after_label):
    """Extrai o primeiro número BR que aparece após um label na mesma linha."""
    # Find the label in the text (case-insensitive)
    lines = text.split('\n')
    for line in lines:
        if after_label.lower() in line.lower():
            # Get everything after the label
            idx = line.lower().index(after_label.lower())
            rest = line[idx + len(after_label):]
            # Find first number pattern (BR format)
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', rest)
            if m:
                return parse_br_number(m.group())
    return None


def extract_first_pct(text, after_label):
    """Extrai a primeira porcentagem que aparece após um label."""
    lines = text.split('\n')
    for line in lines:
        if after_label.lower() in line.lower():
            idx = line.lower().index(after_label.lower())
            rest = line[idx + len(after_label):]
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', rest)
            if m:
                return parse_pct(m.group())
    return None


def extract_pdf_text(pdf_path):
    """Extrai todo o texto de um PDF. Normaliza acentos para matching consistente."""
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            raw = '\n\n'.join(pages)
            # Normalize accents: Dívida → Divida, geração → geracao, etc.
            return strip_accents(raw)
    except Exception as e:
        print(f"    ERRO lendo {pdf_path}: {e}")
        return ""


def periodo_from_filename(filename):
    """Extrai periodo (xTyyyy) do nome do arquivo. Ex: Tenda_Release_2025_3T.pdf → 3T2025"""
    m = re.search(r'(\d{4})_(\d)T', filename)
    if m:
        return f"{m.group(2)}T{m.group(1)}"
    return None


def atualizar_banco(conn, empresa, segmento, periodo, dados):
    """Atualiza campos no banco. Só atualiza campos que são NULL."""
    if not dados:
        return 0
    c = conn.cursor()
    c.execute("SELECT id FROM dados_trimestrais WHERE empresa=? AND segmento=? AND periodo=?",
              (empresa, segmento, periodo))
    row = c.fetchone()
    if not row:
        return 0

    rec_id = row[0]
    updates = 0
    for campo, valor in dados.items():
        if valor is None:
            continue
        # Só atualiza se campo está NULL
        c.execute(f"SELECT {campo} FROM dados_trimestrais WHERE id=?", (rec_id,))
        current = c.fetchone()[0]
        if current is None:
            try:
                c.execute(f"UPDATE dados_trimestrais SET {campo}=? WHERE id=?", (valor, rec_id))
                updates += 1
            except Exception:
                pass
    conn.commit()
    return updates


# ============================================================
# EXTRACTORS POR EMPRESA
# ============================================================

def extrair_tenda(text, periodo):
    """Extrai dados do release da Tenda. Formato bem estruturado com tabelas texto."""
    dados = {}

    # EBITDA / Margem EBITDA (busca seção Consolidado)
    # Padrão: "EBITDA Ajustado" seguido por número
    val = extract_first_number(text, 'EBITDA Ajustado')
    if val and abs(val) > 1:  # evitar capturar margem
        dados['ebitda_ajustado'] = val

    val = extract_first_number(text, 'Margem EBITDA Ajustada')
    if val is None:
        val = extract_first_pct(text, 'Margem EBITDA Ajustada')
    if val and val < 1:
        dados['margem_ebitda_ajustada'] = val
    elif val and val > 1:
        dados['margem_ebitda_ajustada'] = val / 100.0

    val = extract_first_pct(text, 'Margem EBITDA')
    if val and 'margem_ebitda_ajustada' not in dados:
        dados['margem_ebitda'] = val

    # Dívida
    val = extract_first_number(text, 'Divida Bruta')
    if val and abs(val) > 10:
        dados['divida_bruta'] = val

    val = extract_first_number(text, 'Divida Liquida')
    # Cuidado: "Dívida Líquida / PL" pode capturar antes
    lines = text.split('\n')
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low) and '/pl' not in low and 'patrimonio' not in low and 'ebitda' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[line.lower().index('vida l') + 10:])
            if m:
                v = parse_br_number(m.group())
                if v is not None and abs(v) > 5:
                    dados['divida_liquida'] = v
                    break

    # DL/PL
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low) and ('pl' in low or 'patrimonio' in low):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
            if m:
                v = parse_pct(m.group())
                if v is not None:
                    dados['divida_liquida_pl'] = v
                    break

    # Geração de Caixa
    for line in lines:
        low = line.lower()
        if ('fluxo de caixa operacional consolidado' in low or 'geracao de caixa' in low):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['geracao_caixa'] = v
                    break

    # ROE - Tenda uses "ROE (Ultimos 12 meses)" or "Retorno sobre o Patrimonio Liquido UDM de XX%"
    for line in lines:
        low = line.lower()
        if 'retorno sobre' in low and 'patrimonio' in low and ('udm' in low or '12' in low or 'ultimos' in low):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break
    if 'roe' not in dados:
        for line in lines:
            low = line.lower()
            if 'roe' in low and ('12 meses' in low or 'ltm' in low or 'ultimos' in low or 'udm' in low):
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
                if m:
                    dados['roe'] = parse_pct(m.group())
                    break
    if 'roe' not in dados:
        for line in lines:
            if 'ROE' in line and '%' in line:
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
                if m:
                    dados['roe'] = parse_pct(m.group())
                    break

    # Landbank VGV (Consolidado)
    for line in lines:
        low = line.lower()
        if 'vgv' in low and ('r$ milh' in low or 'milhoes' in low) and ('banco de terreno' in text.lower().split(line.lower())[0][-200:] if line.lower() in text.lower() else False):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 1000:  # landbank should be > R$ 1bi
                    dados['landbank_vgv'] = v
                    break

    # --- VSO ---
    # Tenda reports VSO in narrative text: "VSO Liquida do 3T25 foi de 26,6%"
    # or "'VSO Liquida') de 25,8%" or "VSO Liquida de 35%"
    # CAREFUL: skip "aumento de XX%" patterns (growth rates, not VSO)
    # Look for "Consolidado" section VSO first, then general
    for line in lines:
        low = line.lower()
        if 'vso liquida' in low and 'consolidado' in low:
            # Find % after "VSO Liquida" text
            idx = low.index('vso liquida')
            rest = line[idx:]
            m = re.search(r'\d[\d.]*,?\d*%', rest)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group())
                break
    # Fallback: "'VSO Liquida') de XX%" or "VSO Liquida do xTxx foi de XX%"
    if 'vso_liquida_trimestral' not in dados:
        for line in lines:
            low = line.lower()
            if 'vso liquida' in low and '%' in line and 'aumento' not in low and 'cresc' not in low:
                idx = low.index('vso liquida')
                rest = low[idx:]
                # Pattern: "vso liquida ... de XX%" or "vso liquida ... foi de XX%"
                m = re.search(r'(?:foi de|[)\u201d]\s*de|liquida de)\s+(\d[\d.]*,?\d*%)', rest)
                if m:
                    dados['vso_liquida_trimestral'] = parse_pct(m.group(1))
                    break
    # Last resort: any line with "VSO Liquida" and a % that's not a growth rate
    if 'vso_liquida_trimestral' not in dados:
        for line in lines:
            low = line.lower()
            if 'vso liquida' in low and '%' in line:
                idx = low.index('vso liquida')
                rest = line[idx:]
                m = re.search(r'\d[\d.]*,?\d*%', rest)
                if m:
                    v = parse_pct(m.group())
                    if v and v < 0.8:  # VSO should be < 80%
                        dados['vso_liquida_trimestral'] = v
                        break

    # VSO Bruta - narrative: "VSO Bruta no trimestre foi de 29,8%"
    for line in lines:
        low = line.lower()
        if 'vso bruta' in low and '%' in line:
            idx = low.index('vso bruta')
            rest = line[idx:]
            m = re.search(r'\d[\d.]*,?\d*%', rest)
            if m:
                v = parse_pct(m.group())
                if v and v < 0.8:
                    dados['vso_bruta_trimestral'] = v
                    break

    # --- Vendas Líquidas ---
    # "Vendas liquidas totalizaram R$ 1.232,7 milhoes" or table "Vendas Liquidas (VGV)"
    for line in lines:
        low = line.lower()
        if ('vendas liquidas' in low) and ('r$' in low or 'milh' in low) and ('consolidado' in low or 'totali' in low):
            m = re.search(r'r?\$?\s*(\d[\d.]*,\d+)', low)
            if m:
                v = parse_br_number(m.group(1))
                if v and v > 50:
                    dados['vendas_liquidas_vgv'] = v
                    break
    if 'vendas_liquidas_vgv' not in dados:
        for line in lines:
            low = line.lower()
            if 'vendas liquidas' in low and ('r$' in low or 'milh' in low) and 'aumento' not in low:
                m = re.search(r'r?\$?\s*(\d[\d.]*,\d+)', line)
                if m:
                    v = parse_br_number(m.group(1))
                    if v and v > 100:
                        dados['vendas_liquidas_vgv'] = v
                        break

    # --- Receitas a Apropriar ---
    for line in lines:
        low = line.lower()
        if ('receitas a apropriar' in low or 'receita a apropriar' in low) and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['receitas_apropriar'] = v
                    break

    # --- Resultado a Apropriar ---
    for line in lines:
        low = line.lower()
        if 'resultado a apropriar' in low and 'margem' not in low:
            m = re.search(r'r?\$?\s*(\d[\d.]*,\d+)', line)
            if m:
                v = parse_br_number(m.group(1))
                if v and v > 10:
                    dados['resultado_apropriar'] = v
                    break

    # --- Margem a Apropriar (Margem REF) ---
    for line in lines:
        low = line.lower()
        if ('margem ref' in low or 'margem a apropriar' in low) and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                v = parse_pct(m.group())
                if v and 0.1 < v < 0.7:
                    dados['margem_apropriar'] = v
                    break

    # === RECEBÍVEIS ===
    # Tenda reports: carteira pré/pós-chaves, PDD, aging, cessão, inadimplência, cobertura

    # Carteira Pré-Chaves (R$ milhões)
    for line in lines:
        low = line.lower()
        if 'pre-chaves' in low and ('r$' in low or 'milh' in low or 'carteira' in low):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['carteira_pre_chaves'] = v
                    break
    # Table row: "Pre-Chaves 1.234,5"
    if 'carteira_pre_chaves' not in dados:
        for line in lines:
            low = line.lower()
            if 'pre-chaves' in low and 'pos' not in low and 'total' not in low:
                m = re.search(r'\d[\d.]*,\d+', line)
                if m:
                    v = parse_br_number(m.group())
                    if v and v > 50:
                        dados['carteira_pre_chaves'] = v
                        break

    # Carteira Pós-Chaves (R$ milhões)
    for line in lines:
        low = line.lower()
        if 'pos-chaves' in low and 'pre' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['carteira_pos_chaves'] = v
                    break

    # Carteira Total
    for line in lines:
        low = line.lower()
        if ('carteira total' in low or 'total da carteira' in low) and ('recebi' in low or 'r$' in low or 'milh' in low):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['carteira_recebiveis_total'] = v
                    break
    # Derive from pre + pos if not found directly
    if 'carteira_recebiveis_total' not in dados and 'carteira_pre_chaves' in dados and 'carteira_pos_chaves' in dados:
        dados['carteira_recebiveis_total'] = dados['carteira_pre_chaves'] + dados['carteira_pos_chaves']

    # PDD / Provisão para Devedores Duvidosos
    for line in lines:
        low = line.lower()
        if ('pdd' in low or 'provisao' in low) and ('devedores' in low or 'duvidosos' in low or 'credito' in low):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 1:
                    dados['pdd_provisao'] = abs(v)
                    break

    # Índice de Cobertura PDD (%)
    for line in lines:
        low = line.lower()
        if ('cobertura' in low or 'indice de cobertura' in low) and ('pdd' in low or 'provisao' in low or '%' in line):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['pdd_cobertura_pct'] = parse_pct(m.group())
                break

    # Inadimplência Total (%)
    for line in lines:
        low = line.lower()
        if 'inadimplencia' in low and '%' in line and 'over' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['inadimplencia_total_pct'] = parse_pct(m.group())
                break

    # Aging — Tenda reports: Adimplente, Vencido até 90d, 90-360d, >360d
    for line in lines:
        low = line.lower()
        if 'adimplente' in low and '%' in line and 'inadim' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['aging_adimplente_pct'] = parse_pct(m.group())
                break
    for line in lines:
        low = line.lower()
        if ('ate 90' in low or 'até 90' in low or '< 90' in low or 'vencido 90' in low) and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['aging_vencido_90d_pct'] = parse_pct(m.group())
                break
    for line in lines:
        low = line.lower()
        if ('90' in low and '360' in low) and '%' in line and 'acima' not in low and 'mais' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['aging_vencido_360d_pct'] = parse_pct(m.group())
                break
    for line in lines:
        low = line.lower()
        if ('acima de 360' in low or 'mais de 360' in low or '> 360' in low or 'acima 360' in low) and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['aging_vencido_360d_mais_pct'] = parse_pct(m.group())
                break

    # Cessão de Recebíveis (trimestre)
    for line in lines:
        low = line.lower()
        if ('cessao' in low or 'securitizacao' in low) and ('recebi' in low or 'carteira' in low or 'credito' in low):
            if 'saldo' not in low and 'passivo' not in low and 'estoque' not in low:
                m = re.search(r'\d[\d.]*,\d+', line)
                if m:
                    v = parse_br_number(m.group())
                    if v and v > 10:
                        dados['cessao_recebiveis_trimestre'] = v
                        break

    # Saldo de Cessão
    for line in lines:
        low = line.lower()
        if ('saldo' in low or 'estoque' in low) and ('cessao' in low or 'securitiza' in low):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['saldo_cessao_recebiveis'] = v
                    break

    return dados


def extrair_direcional(text, periodo):
    """Extrai dados do release da Direcional. Tabelas muito limpas."""
    dados = {}
    lines = text.split('\n')

    # EBITDA
    for line in lines:
        low = line.lower()
        if 'ebitda ajustado' in low and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['ebitda_ajustado'] = v
                    break

    # EBITDA (não ajustado)
    for line in lines:
        low = line.lower()
        if low.strip().startswith('ebitda') and 'ajustado' not in low and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['ebitda'] = v
                    break

    # Margem EBITDA
    val = extract_first_pct(text, 'Margem EBITDA Ajustada')
    if val:
        dados['margem_ebitda_ajustada'] = val
    val = extract_first_pct(text, 'Margem EBITDA')
    if val and 'margem_ebitda' not in dados:
        dados['margem_ebitda'] = val

    # Margem Bruta / Ajustada
    val = extract_first_pct(text, 'Margem Bruta Ajustada')
    if val is None:
        val = extract_first_pct(text, 'MB Ajustada')
    if val:
        dados['margem_bruta_ajustada'] = val
    val = extract_first_pct(text, 'Margem Bruta')
    if val and 'margem_bruta_ajustada' not in dados:
        dados['margem_bruta'] = val
    elif val:
        dados['margem_bruta'] = val

    # Margem Líquida
    val = extract_first_pct(text, 'Margem Liquida')
    if val:
        dados['margem_liquida'] = val

    # Dívida
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low) and '/pl' not in low and 'ebitda' not in low and 'patrimônio' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['divida_liquida'] = v
                    break

    for line in lines:
        low = line.lower()
        if ('divida bruta' in low):
            m = re.search(r'\d[\d.]*,\d+', line[12:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['divida_bruta'] = v
                    break

    # DL/PL - Direcional uses "Divida Liquida/PL" or "Divida Liquida Ajustada / PL"
    for line in lines:
        low = line.lower()
        if ('dl/pl' in low or 'divida liquida/pl' in low or 'divida liquida / pl' in low or
            ('divida liquida' in low and 'patrimonio' in low)):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
            if m:
                dados['divida_liquida_pl'] = parse_pct(m.group())
                break

    # Geração de Caixa - table or narrative "Geracao de Caixa de R$ 113 milhoes"
    for line in lines:
        low = line.lower()
        if 'geracao de caixa' in low and ('r$' in low or 'milh' in low):
            m = re.search(r'r?\$?\s*([-−]?\(?\d[\d.]*,?\d+\)?)', line)
            if m:
                v = parse_br_number(m.group(1))
                if v is not None and abs(v) > 1:
                    dados['geracao_caixa'] = v
                    break
    if 'geracao_caixa' not in dados:
        for line in lines:
            low = line.lower()
            if 'geracao de caixa' in low or 'consumo de caixa' in low:
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
                if m:
                    v = parse_br_number(m.group())
                    if v is not None and abs(v) > 1:
                        dados['geracao_caixa'] = v
                        break

    # ROE - "ROE Anualizado" or "ROE Anualizado Ajustado" table rows
    for line in lines:
        low = line.lower()
        if 'roe' in low and ('anualizado' in low or '12' in low or 'ultimos' in low or 'ajustado' in low):
            m = re.search(r'\d[\d.]*,?\d*%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break
    if 'roe' not in dados:
        for line in lines:
            if 'ROE' in line and '%' in line:
                m = re.search(r'\d[\d.]*,?\d*%', line)
                if m:
                    dados['roe'] = parse_pct(m.group())
                    break

    # --- VSO --- Direcional: "VSO Consolidada - (% VGV) 17%" or "VSO de 24% no 3T25"
    for line in lines:
        low = line.lower()
        if ('vso consolidada' in low or 'vso (vendas sobre oferta)' in low) and '100%' in low:
            # Table row with 100% column
            m = re.search(r'100%\s+(\d[\d.]*,?\d*%)', line)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group(1))
                break
    if 'vso_liquida_trimestral' not in dados:
        for line in lines:
            low = line.lower()
            if ('vso consolidada' in low or 'vso (vendas sobre oferta)' in low or 'vso' in low) and '%' in line:
                idx = low.index('vso')
                rest = line[idx:]
                m = re.search(r'\d[\d.]*,?\d*%', rest)
                if m:
                    v = parse_pct(m.group())
                    if v and v < 0.8:
                        dados['vso_liquida_trimestral'] = v
                        break
    # Narrative fallback: "VSO de 24% no 3T25"
    if 'vso_liquida_trimestral' not in dados:
        for line in lines:
            low = line.lower()
            if 'vso' in low and ('de ' in low or 'foi de' in low) and '%' in line and 'aumento' not in low:
                m = re.search(r'(?:vso|oferta)[^%]*?(\d[\d.]*,?\d*%)', low)
                if m:
                    v = parse_pct(m.group(1))
                    if v and v < 0.8:
                        dados['vso_liquida_trimestral'] = v
                        break

    # --- Vendas Líquidas ---
    # Table: "VGV Liquido Contratado (VGV 100%) 1.641,8" or narrative "vendas liquidas de R$ 1,0 bilhao"
    for line in lines:
        low = line.lower()
        if ('vgv liquido contratado' in low or 'vendas liquidas' in low) and ('100%' in low or 'r$' in low or 'mil' in low):
            m = re.search(r'\d[\d.]*,\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['vendas_liquidas_vgv'] = v
                    break
    # Narrative: "Vendas Liquidas atingem o patamar recorde de R$ 1,0 bilhao"
    if 'vendas_liquidas_vgv' not in dados:
        for line in lines:
            low = line.lower()
            if 'vendas liquidas' in low and ('r$' in low or 'bilh' in low or 'milh' in low):
                m = re.search(r'r?\$?\s*(\d[\d.]*,\d+)', line)
                if m:
                    v = parse_br_number(m.group(1))
                    if v and v > 50:
                        # If "bilhao/bilhoes", multiply by 1000
                        if 'bilh' in low:
                            v = v * 1000
                        dados['vendas_liquidas_vgv'] = v
                        break

    # Receitas a Apropriar / Resultado a Apropriar / Margem
    for line in lines:
        low = line.lower()
        if ('receitas a apropriar' in low or 'receita a apropriar' in low) and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['receitas_apropriar'] = v
                    break

    for line in lines:
        low = line.lower()
        if 'resultado a apropriar' in low and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['resultado_apropriar'] = v
                    break

    # Margem a apropriar / Margem REF / Margem do resultado a apropriar
    for line in lines:
        low = line.lower()
        if ('margem ref' in low or 'margem a apropriar' in low or 'margem do resultado a apropriar' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                v = parse_pct(m.group())
                if v and 0.1 < v < 0.7:
                    dados['margem_apropriar'] = v
                    break

    # === RECEBÍVEIS — Direcional ===
    # Direcional reports pré-chaves e pós-chaves in releases
    for line in lines:
        low = line.lower()
        if 'pre-chaves' in low or 'pre chaves' in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['carteira_pre_chaves'] = v
                    break
    for line in lines:
        low = line.lower()
        if ('pos-chaves' in low or 'pos chaves' in low) and 'pre' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['carteira_pos_chaves'] = v
                    break
    if 'carteira_pre_chaves' in dados and 'carteira_pos_chaves' in dados:
        dados['carteira_recebiveis_total'] = dados['carteira_pre_chaves'] + dados['carteira_pos_chaves']

    return dados


def extrair_cury(text, periodo):
    """Extrai dados do release da Cury.
    Cury PDFs use NO accents (divida, geracao, liquida).
    Table format: Label  3TXX  2TXX  %T/T  3TXX-1  %A/A  9MXX  9MXX-1  %A/A
    First number after label = current quarter value.
    """
    dados = {}
    lines = text.split('\n')

    # --- EBITDA ---
    for line in lines:
        low = line.lower()
        if 'ebitda ajustado' in low and 'margem' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['ebitda_ajustado'] = v
                    break

    for line in lines:
        low = line.lower()
        if low.strip().startswith('ebitda') and 'ajustado' not in low and 'margem' not in low and 'r$' not in low.split('ebitda')[0]:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['ebitda'] = v
                    break

    # --- Margem EBITDA ---
    for line in lines:
        low = line.lower()
        if 'margem ebitda ajustada' in low or 'margem ebitda ajust' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_ebitda_ajustada'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'margem ebitda' in low and 'ajust' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_ebitda'] = parse_pct(m.group())
                break

    # --- Margem Bruta ---
    for line in lines:
        low = line.lower()
        if 'margem bruta ajustada' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta_ajustada'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'margem bruta' in low and 'ajust' not in low and 'ref' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta'] = parse_pct(m.group())
                break

    # --- Margem Líquida ---
    for line in lines:
        low = line.lower()
        if 'margem liquida' in low and '100%' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_liquida'] = parse_pct(m.group())
                break
    if 'margem_liquida' not in dados:
        for line in lines:
            low = line.lower()
            if 'margem liquida' in low:
                m = re.search(r'\d[\d.]*,\d*%', line)
                if m:
                    dados['margem_liquida'] = parse_pct(m.group())
                    break

    # --- ROE --- Cury uses "ROE" in recent releases, "ROAE" in early ones (pre-2022)
    for line in lines:
        if ('ROE' in line or 'ROAE' in line) and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break
    # Narrative fallback: "ROE DE 50,8%" or "ROAE de 23%"
    if 'roe' not in dados:
        for line in lines:
            low = line.lower()
            if ('roe' in low or 'roae' in low) and ('de ' in low or 'foi' in low) and '%' in line:
                m = re.search(r'\d[\d.]*,\d*%', line)
                if m:
                    dados['roe'] = parse_pct(m.group())
                    break

    # --- Geração de Caixa ---
    for line in lines:
        low = line.lower()
        if ('geracao de caixa' in low) and ('r$' in low or 'milh' in low):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[line.lower().index('caixa') + 5:])
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['geracao_caixa'] = v
                    break
    # Fallback without R$/milh qualifier
    if 'geracao_caixa' not in dados:
        for line in lines:
            low = line.lower()
            if ('geracao de caixa' in low) and 'ajust' not in low:
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[line.lower().index('caixa') + 5:])
                if m:
                    v = parse_br_number(m.group())
                    if v is not None and abs(v) > 1:
                        dados['geracao_caixa'] = v
                        break

    # --- Dívida Bruta ---
    for line in lines:
        low = line.lower()
        if ('divida bruta' in low) and 'liquida' not in low:
            m = re.search(r'\d[\d.]*,\d+', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['divida_bruta'] = v
                    break

    # --- Dívida Líquida (Cury: "Divida - (Caixa) liquida") ---
    for line in lines:
        low = line.lower()
        if 'divida' in low and 'caixa' in low and 'liquida' in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[20:])
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['divida_liquida'] = v
                    break
    # Fallback: standard "divida liquida" label
    if 'divida_liquida' not in dados:
        for line in lines:
            low = line.lower()
            if ('divida liquida' in low) and '/pl' not in low and 'ebitda' not in low and 'patrimonio' not in low:
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[10:])
                if m:
                    v = parse_br_number(m.group())
                    if v is not None:
                        dados['divida_liquida'] = v
                        break

    # --- VSO ---
    for line in lines:
        low = line.lower()
        if 'vso liquida' in low and ('udm' in low or 'ultimos' in low or '12 meses' in low or 'ultimos' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_liquida_12m'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'vso liquida' in low and 'udm' not in low and 'ultimos' not in low and '12 meses' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'vso bruta' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_bruta_trimestral'] = parse_pct(m.group())
                break

    # --- Receitas / Resultado a Apropriar ---
    for line in lines:
        low = line.lower()
        if 'receitas de vendas a apropriar' in low or 'receita de vendas a apropriar' in low:
            m = re.search(r'\d[\d.]*,\d+', line[20:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['receitas_apropriar'] = v
                    break

    for line in lines:
        low = line.lower()
        if 'resultado de vendas a apropriar' in low or 'resultado a apropriar' in low:
            m = re.search(r'\d[\d.]*,\d+', line[20:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['resultado_apropriar'] = v
                    break

    # Margem Bruta REF (margem a apropriar)
    for line in lines:
        low = line.lower()
        if 'margem bruta ref' in low or ('margem' in low and 'ref' in low and 'a/a' not in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_apropriar'] = parse_pct(m.group())
                break

    # --- Landbank (Banco de Terrenos) ---
    for line in lines:
        low = line.lower()
        if 'banco de terrenos' in low and ('vgv' in low or 'r$' in low):
            m = re.search(r'\d[\d.]*,\d+', line[20:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 500:
                    dados['landbank_vgv'] = v
                    break

    # --- Vendas Líquidas ---
    # Table: "Vendas Liquidas (R$ milhoes) 1.827,0" or narrative "vendas liquidas atingiram R$ 1.552,1 milhoes"
    for line in lines:
        low = line.lower()
        if 'vendas liquidas' in low and ('r$ milh' in low or '(r$' in low):
            m = re.search(r'\d[\d.]*,\d+', line[line.lower().index('vendas liquidas') + 15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['vendas_liquidas_vgv'] = v
                    break
    if 'vendas_liquidas_vgv' not in dados:
        for line in lines:
            low = line.lower()
            if 'vendas liquidas' in low and ('r$' in low or 'milh' in low or 'atingi' in low):
                m = re.search(r'r?\$?\s*(\d[\d.]*,\d+)', line)
                if m:
                    v = parse_br_number(m.group(1))
                    if v and v > 50:
                        dados['vendas_liquidas_vgv'] = v
                        break

    return dados


def extrair_planoeplano(text, periodo):
    """Extrai dados do release da PlanoePlano.
    PlanoePlano uses accented chars. EBITDA and margin may be on same line.
    """
    dados = {}
    lines = text.split('\n')

    # --- EBITDA Ajustado ---
    # PlanoePlano often puts EBITDA + margin on same line: "EBITDA ajustado R$ 129,2 milhoes, margem 15,9%"
    for line in lines:
        low = line.lower()
        if 'ebitda ajustado' in low:
            # Extract first number (absolute value, not percentage)
            idx = low.index('ebitda ajustado') + len('ebitda ajustado')
            rest = line[idx:]
            m = re.search(r'\d[\d.]*,\d+', rest)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['ebitda_ajustado'] = v
                    break

    # EBITDA (non-adjusted)
    if 'ebitda' not in dados:
        for line in lines:
            low = line.lower()
            if low.strip().startswith('ebitda') and 'ajustado' not in low:
                m = re.search(r'\d[\d.]*,\d+', line)
                if m:
                    v = parse_br_number(m.group())
                    if v and v > 10:
                        dados['ebitda'] = v
                        break

    # --- Margem EBITDA Ajustada ---
    for line in lines:
        low = line.lower()
        if ('margem ebitda ajustada' in low or 'margem ebitda ajust' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_ebitda_ajustada'] = parse_pct(m.group())
                break
    # Fallback: extract from inline "margem X%" after EBITDA ajustado
    if 'margem_ebitda_ajustada' not in dados:
        for line in lines:
            low = line.lower()
            if 'ebitda ajustado' in low and 'margem' in low:
                m = re.search(r'margem\s+(\d[\d.]*,\d*%)', low)
                if m:
                    dados['margem_ebitda_ajustada'] = parse_pct(m.group(1))
                    break

    # --- Margem Bruta ---
    for line in lines:
        low = line.lower()
        if 'margem bruta ajustada' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta_ajustada'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'margem bruta' in low and 'ajust' not in low and 'ref' not in low and 'apropriar' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta'] = parse_pct(m.group())
                break

    # --- Margem Líquida ---
    for line in lines:
        low = line.lower()
        if ('margem liquida' in low) and '100%' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_liquida'] = parse_pct(m.group())
                break
    if 'margem_liquida' not in dados:
        for line in lines:
            low = line.lower()
            if 'margem liquida' in low:
                m = re.search(r'\d[\d.]*,\d*%', line)
                if m:
                    dados['margem_liquida'] = parse_pct(m.group())
                    break

    # --- Dívida ---
    for line in lines:
        low = line.lower()
        if ('divida bruta' in low):
            m = re.search(r'\d[\d.]*,\d+', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['divida_bruta'] = v
                    break

    for line in lines:
        low = line.lower()
        if ('divida liquida' in low or 'divida (caixa) liquida' in low) and '/pl' not in low and 'ebitda' not in low and 'patrimonio' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['divida_liquida'] = v
                    break

    # --- DL/PL ---
    # PlanoePlano uses "Divida (Caixa) Liquida / Patrimonio" or "DL / (DL + PL)"
    for line in lines:
        low = line.lower()
        if ('dl/pl' in low or ('divida' in low and 'patrimonio' in low) or
            ('divida' in low and 'patrimonio' in low) or 'dl / (dl + pl)' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['divida_liquida_pl'] = parse_pct(m.group())
                break

    # --- Geração de Caixa ---
    # PlanoePlano uses "(Geração)/Consumo de Caixa Operacional" with INVERTED sign:
    #   positive = consumo (cash burned), negative/parentheses = geração (cash generated)
    # PRIORITY 1: Narrative text (most reliable, avoids multi-column table issues)
    m = re.search(r'(?:apresentou|encerrou)[^.]*?(geracao|consumo)\s+de\s+caixa[^.]*?R?\$?\s*(\d[\d.]*,\d+)\s*milh', text, re.IGNORECASE)
    if m:
        v = parse_br_number(m.group(2))
        if v is not None and abs(v) > 0.1:
            if 'consumo' in m.group(1).lower():
                v = -v  # consumo is negative
            dados['geracao_caixa'] = v
    # PRIORITY 2: Simpler narrative "geração de caixa de R$ XX,X milhões"
    if 'geracao_caixa' not in dados:
        m = re.search(r'(geracao|consumo)\s+de\s+caixa[^.]{0,30}?R?\$?\s*(\d[\d.]*,\d+)\s*milh', text, re.IGNORECASE)
        if m:
            v = parse_br_number(m.group(2))
            if v is not None and abs(v) > 0.1:
                if 'consumo' in m.group(1).lower():
                    v = -v
                dados['geracao_caixa'] = v
    # PRIORITY 3: Simple "geracao de caixa" line with number (old format)
    if 'geracao_caixa' not in dados:
        for line in lines:
            low = line.lower()
            if 'geracao de caixa' in low and 'r$' not in low and 'operacional' not in low:
                m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
                if m:
                    v = parse_br_number(m.group())
                    if v is not None and abs(v) > 1:
                        dados['geracao_caixa'] = v
                        break

    # --- ROE --- (PlanoePlano may not report ROE)
    for line in lines:
        if 'ROE' in line and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break

    # --- Landbank ---
    for line in lines:
        low = line.lower()
        if ('banco de terrenos' in low or 'landbank' in low) and ('vgv' in low or 'r$' in low):
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v:
                    # If in R$ bilhões (small number), convert to R$ milhões
                    if v < 50:
                        dados['landbank_vgv'] = v * 1000
                    else:
                        dados['landbank_vgv'] = v
                    break

    # --- VSO ---
    for line in lines:
        low = line.lower()
        if 'vso' in low and ('12 meses' in low or 'udm' in low or 'ultimos' in low):
            m = re.search(r'\d[\d.]*,?\d*%', line)
            if m:
                dados['vso_liquida_12m'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'vso' in low and ('trimestre' in low or 'trimestral' in low):
            m = re.search(r'\d[\d.]*,?\d*%', line)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group())
                break

    # --- Receita a Apropriar / Margem REF ---
    for line in lines:
        low = line.lower()
        if 'receita a apropriar' in low or 'receitas a apropriar' in low:
            m = re.search(r'\d[\d.]*,?\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['receitas_apropriar'] = v
                    break

    for line in lines:
        low = line.lower()
        if 'resultado a apropriar' in low:
            m = re.search(r'\d[\d.]*,?\d+', line[15:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['resultado_apropriar'] = v
                    break

    for line in lines:
        low = line.lower()
        if 'margem ref' in low or 'margem bruta ref' in low or ('margem' in low and 'ref' in low and 'a/a' not in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_apropriar'] = parse_pct(m.group())
                break

    # --- Vendas Líquidas VGV ---
    for line in lines:
        low = line.lower()
        if ('vendas liquidas' in low or 'venda liquida' in low) and ('r$' in low or 'milh' in low or 'bilh' in low):
            m = re.search(r'R?\$?\s*(\d[\d.]*,\d+)', line, re.IGNORECASE)
            if m:
                v = parse_br_number(m.group(1))
                if v:
                    if 'bilh' in low or (v < 50 and 'milh' not in low):
                        v *= 1000
                    dados['vendas_liquidas_vgv'] = v
                    break
    # Fallback: narrative pattern
    if 'vendas_liquidas_vgv' not in dados:
        m = re.search(r'vendas\s+liquidas.*?(?:de\s+)?R?\$?\s*(\d[\d.]*,\d+)\s*(bilh|milh)?', text, re.IGNORECASE)
        if m:
            v = parse_br_number(m.group(1))
            if v:
                if m.group(2) and 'bilh' in m.group(2).lower():
                    v *= 1000
                dados['vendas_liquidas_vgv'] = v

    # === RECEBÍVEIS — PlanoePlano ===
    # Inadimplência
    for line in lines:
        low = line.lower()
        if 'inadimplencia' in low and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['inadimplencia_total_pct'] = parse_pct(m.group())
                break

    # Cessão / CRI (trimestre)
    for line in lines:
        low = line.lower()
        if ('cessao' in low or 'cri' in low) and ('recebi' in low or 'emiss' in low) and 'saldo' not in low:
            m = re.search(r'\d[\d.]*,\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['cessao_recebiveis_trimestre'] = v
                    break

    return dados


def extrair_cyrela(text, periodo):
    """Extrai dados do release da Cyrela.
    Cyrela does NOT report EBITDA at all.
    Uses accented chars but pdfplumber may garble them.
    VSO reported as "VSO 12 meses", geração de caixa present.
    """
    dados = {}
    lines = text.split('\n')

    # Cyrela does NOT report EBITDA - skip EBITDA extraction entirely

    # --- Margem Bruta / Ajustada ---
    for line in lines:
        low = line.lower()
        if 'margem bruta ajustada' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta_ajustada'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'margem bruta' in low and 'ajust' not in low and 'apropriar' not in low and 'ref' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta'] = parse_pct(m.group())
                break

    # --- Margem Líquida ---
    for line in lines:
        low = line.lower()
        if 'margem l' in low and ('quida' in low or 'liquida' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_liquida'] = parse_pct(m.group())
                break

    # --- ROE ---
    for line in lines:
        low = line.lower()
        if 'roe' in low and ('ajustado' in low or 'ltm' in low or 'udm' in low or '12' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break
    if 'roe' not in dados:
        for line in lines:
            if 'ROE' in line and '%' in line:
                m = re.search(r'\d[\d.]*,\d*%', line)
                if m:
                    dados['roe'] = parse_pct(m.group())
                    break

    # --- Dívida Bruta ---
    for line in lines:
        low = line.lower()
        if ('divida bruta' in low):
            m = re.search(r'\d[\d.]*,?\d+', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['divida_bruta'] = v
                    break

    # --- Dívida Líquida ---
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low) and \
           'ajustada' not in low and '/pl' not in low and 'ebitda' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v is not None:
                    dados['divida_liquida'] = v
                    break

    # --- DL/PL ---
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low) and \
           ('pl' in low or 'patrimonio' in low):
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?%', line)
            if m:
                dados['divida_liquida_pl'] = parse_pct(m.group())
                break

    # --- Geração de Caixa ---
    for line in lines:
        low = line.lower()
        if 'gera' in low and 'caixa' in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
            if m:
                v = parse_br_number(m.group())
                if v is not None and abs(v) > 5:
                    dados['geracao_caixa'] = v
                    break

    # --- VSO ---
    for line in lines:
        low = line.lower()
        if 'vso' in low and ('12 meses' in low or 'udm' in low or 'ltm' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_liquida_12m'] = parse_pct(m.group())
                break

    for line in lines:
        low = line.lower()
        if 'vso' in low and ('trimestre' in low or 'trimestral' in low or '3t' in low or '2t' in low or '1t' in low or '4t' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group())
                break

    # --- Vendas Líquidas ---
    for line in lines:
        low = line.lower()
        if ('vendas contratadas' in low or 'vendas l' in low) and ('r$' in low or 'milh' in low or 'mm' in low):
            m = re.search(r'\d[\d.]*,?\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['vendas_liquidas_vgv'] = v
                    break

    # --- Backlog (Receitas/Resultado a Apropriar) ---
    for line in lines:
        low = line.lower()
        if 'receitas a apropriar' in low or 'receita a apropriar' in low:
            m = re.search(r'\d[\d.]*,?\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['receitas_apropriar'] = v
                    break

    for line in lines:
        low = line.lower()
        if 'resultado a apropriar' in low:
            m = re.search(r'\d[\d.]*,?\d+', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 10:
                    dados['resultado_apropriar'] = v
                    break

    # Margem a apropriar / margem bruta da receita a apropriar
    for line in lines:
        low = line.lower()
        if ('margem a apropriar' in low or 'margem bruta da receita a apropriar' in low or
            ('margem' in low and 'apropriar' in low)):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_apropriar'] = parse_pct(m.group())
                break

    return dados


def extrair_mrv(text, periodo):
    """Extrai dados do release da MRV.
    MRV releases are image-heavy. Text extraction is limited.
    Uses accented chars (geração, dívida, líquida).
    Also try unaccented variants as pdfplumber may strip accents.
    """
    dados = {}
    lines = text.split('\n')
    # Normalize: try both accented and unaccented
    text_lower = text.lower()

    # --- EBITDA ---
    # MRV pre-2024: tabular "EBITDA 386 296 249" or "EBITDA Ajustado 386 296"
    for line in lines:
        low = line.lower()
        if ('ebitda ajustado' in low or 'ebitda aj' in low) and 'margem' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
            if m:
                v = parse_br_number(m.group())
                if v and v > 50:
                    dados['ebitda_ajustado'] = v
                    break

    if 'ebitda_ajustado' not in dados:
        for line in lines:
            low = line.lower().strip()
            if low.startswith('ebitda') and 'margem' not in low and 'ajust' not in low:
                # Tabular: "EBITDA 386 296 249 30,4%"
                # First number after label is current quarter
                rest = line[len('EBITDA'):].strip()
                m = re.search(r'[-−]?\(?\d[\d.]*,?\d+\)?', rest)
                if m:
                    v = parse_br_number(m.group())
                    # Sanity: MRV EBITDA should be > 50 and not a year
                    if v is not None and abs(v) > 50 and abs(v) < 2000:
                        dados['ebitda_ajustado'] = v
                        break
    # Narrative fallback: "Ebitda de R$ 269 milhoes"
    if 'ebitda_ajustado' not in dados:
        m = re.search(r'[Ee][Bb][Ii][Tt][Dd][Aa]\s+(?:de\s+)?R?\$?\s*(\d[\d.]*,?\d+)\s*(milh|bilh)?', text)
        if m:
            v = parse_br_number(m.group(1))
            if v and 50 < v < 2000:
                if m.group(2) and 'bilh' in m.group(2).lower():
                    v *= 1000
                dados['ebitda_ajustado'] = v

    # --- Margem EBITDA ---
    for line in lines:
        low = line.lower()
        if 'margem ebitda' in low or ('margem' in low and 'ebitda' in low):
            m = re.search(r'[-−]?\d[\d.]*,\d*%', line)
            if m:
                dados['margem_ebitda_ajustada'] = parse_pct(m.group())
                break
    # Tabular fallback: line starting with "Margem EBITDA" followed by pct
    if 'margem_ebitda_ajustada' not in dados:
        for line in lines:
            low = line.lower().strip()
            if low.startswith('margem ebitda') or low.startswith('margem do ebitda'):
                m = re.search(r'\d[\d.]*,\d*%', line)
                if m:
                    dados['margem_ebitda_ajustada'] = parse_pct(m.group())
                    break

    # --- Margem Bruta ---
    for line in lines:
        low = line.lower()
        if 'margem bruta' in low and 'ebitda' not in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_bruta'] = parse_pct(m.group())
                break

    # --- Dívida Líquida ---
    for line in lines:
        low = line.lower()
        # MRV uses "A. DIVIDA LIQUIDA" or "DIVIDA LIQUIDA / EBITDA"
        if ('divida liquida' in low) and 'ebitda' not in low and '/pl' not in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v is not None and abs(v) > 10:
                    dados['divida_liquida'] = v
                    break

    # --- DL/PL ---
    for line in lines:
        low = line.lower()
        if ('divida liquida' in low or 'dl' in low) and ('/pl' in low or 'patrimonio' in low) and 'ebitda' not in low:
            m = re.search(r'[-−]?\d[\d.]*,\d*%', line)
            if m:
                dados['divida_liquida_pl'] = parse_pct(m.group())
                break
    # Fallback: look for table row "Dívida Líquida / PL XX,X%"
    if 'divida_liquida_pl' not in dados:
        for line in lines:
            low = line.lower()
            if 'divida liquida / pl' in low or 'dl/pl' in low:
                m = re.search(r'\d[\d.]*,\d+', line)
                if m:
                    v = parse_br_number(m.group())
                    if v is not None and v < 200:  # percentage value
                        dados['divida_liquida_pl'] = v
                        break

    # --- Vendas Líquidas VGV ---
    for line in lines:
        low = line.lower()
        if ('vendas liquidas' in low or 'venda liquida' in low) and ('r$' in low or 'bilh' in low or 'milh' in low):
            m = re.search(r'R?\$?\s*(\d[\d.]*,\d+)', line, re.IGNORECASE)
            if m:
                v = parse_br_number(m.group(1))
                if v:
                    # Check if in bilhões
                    if 'bilh' in low or (v < 50 and 'milh' not in low):
                        v *= 1000
                    dados['vendas_liquidas_vgv'] = v
                    break
    # Fallback: narrative "vendas liquidas de R$ X bilhoes"
    if 'vendas_liquidas_vgv' not in dados:
        m = re.search(r'vendas\s+l[ií]quidas.*?(?:de\s+)?R?\$?\s*(\d[\d.]*,\d+)\s*(bilh|milh)?', text, re.IGNORECASE)
        if m:
            v = parse_br_number(m.group(1))
            if v:
                if m.group(2) and 'bilh' in m.group(2).lower():
                    v *= 1000
                dados['vendas_liquidas_vgv'] = v

    # --- Receitas a Apropriar (Backlog) ---
    for line in lines:
        low = line.lower()
        if ('receita' in low and 'apropriar' in low) and 'margem' not in low and 'resultado' not in low:
            m = re.search(r'\d[\d.]*,?\d+', line[10:])
            if m:
                v = parse_br_number(m.group())
                if v and v > 100:
                    dados['receitas_apropriar'] = v
                    break

    # --- Resultado a Apropriar ---
    for line in lines:
        low = line.lower()
        if 'resultado a apropriar' in low or 'resultado bruto a apropriar' in low:
            if 'margem' not in low:
                m = re.search(r'\d[\d.]*,?\d+', line[10:])
                if m:
                    v = parse_br_number(m.group())
                    if v and v > 10:
                        dados['resultado_apropriar'] = v
                        break

    # --- Margem do Resultado a Apropriar (REF) ---
    for line in lines:
        low = line.lower()
        if ('margem' in low and 'apropriar' in low) or 'margem ref' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_apropriar'] = parse_pct(m.group())
                break

    # --- Geração de Caixa ---
    for line in lines:
        low = line.lower()
        if 'geracao de caixa' in low:
            m = re.search(r'[-−]?\(?\d[\d.]*,\d+\)?', line)
            if m:
                v = parse_br_number(m.group())
                if v is not None and abs(v) > 5:
                    dados['geracao_caixa'] = v
                    break

    # --- ROE ---
    for line in lines:
        low = line.lower()
        if 'roe' in low and '%' in line:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['roe'] = parse_pct(m.group())
                break
    # Fallback: narrative "ROE (LTM)* de 13,3%"
    if 'roe' not in dados:
        m = re.search(r'ROE.*?(?:de\s+)?(\d[\d.]*,\d*%)', text)
        if m:
            dados['roe'] = parse_pct(m.group(1))

    # --- VSO ---
    # MRV pre-2024 tabular: "VSO - vendas liquidas 20% 19% 23%" (first pct is current quarter)
    for line in lines:
        low = line.lower()
        if 'vso' in low and ('vendas liquidas' in low or ('liquida' in low and 'bruta' not in low)) and ('12' not in low and 'udm' not in low and 'ltm' not in low):
            m = re.search(r'(\d[\d.]*,?\d*)\s*%', line)
            if m:
                dados['vso_liquida_trimestral'] = parse_pct(m.group())
                break
    # VSO bruta from table
    for line in lines:
        low = line.lower()
        if 'vso' in low and ('vendas brutas' in low or ('bruta' in low and 'liquida' not in low)) and ('12' not in low):
            m = re.search(r'(\d[\d.]*,?\d*)\s*%', line)
            if m:
                dados['vso_bruta_trimestral'] = parse_pct(m.group())
                break
    # Narrative: "VSO de 33,8% no 2T24" — only if explicitly trimestral or no qualifier
    if 'vso_liquida_trimestral' not in dados:
        m = re.search(r'VSO\s+(?:trimestral\s+)?(?:de\s+|foi\s+de\s+)(\d[\d.]*,?\d*)\s*%', text)
        if m:
            v = parse_pct(m.group(1) + '%')
            if v and v < 0.6:  # VSO trimestral should be < 60%
                dados['vso_liquida_trimestral'] = v
    # VSO 12 meses / UDM
    for line in lines:
        low = line.lower()
        if 'vso' in low and ('12 meses' in low or 'udm' in low or 'ltm' in low):
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['vso_liquida_12m'] = parse_pct(m.group())
                break

    # --- Margem Líquida ---
    for line in lines:
        low = line.lower()
        if 'margem liquida' in low:
            m = re.search(r'\d[\d.]*,\d*%', line)
            if m:
                dados['margem_liquida'] = parse_pct(m.group())
                break

    # === RECEBÍVEIS — MRV ===
    # MRV reports cessão de recebíveis no release
    for line in lines:
        low = line.lower()
        if ('cessao' in low or 'securitizacao' in low) and ('recebi' in low or 'credito' in low or 'carteira' in low):
            if 'saldo' not in low and 'passivo' not in low:
                m = re.search(r'\d[\d.]*,\d+', line)
                if m:
                    v = parse_br_number(m.group())
                    if v and v > 50:
                        dados['cessao_recebiveis_trimestre'] = v
                        break

    return dados


# ============================================================
# MAIN
# ============================================================
def processar_empresa(conn, empresa, subdir, extractor):
    """Processa todos os releases de uma empresa."""
    release_dir = COLETA_DIR / subdir / "releases"
    if not release_dir.exists():
        print(f"  Diretório não encontrado: {release_dir}")
        return

    pdfs = sorted(release_dir.glob(f"{empresa}_Release_*.pdf"))
    total_updates = 0
    total_files = 0

    for pdf_path in pdfs:
        periodo = periodo_from_filename(pdf_path.name)
        if not periodo:
            continue

        # Filtrar escopo: 1T2020 a 3T2025
        m = re.match(r'(\d)T(\d{4})', periodo)
        if not m:
            continue
        tri, ano = int(m.group(1)), int(m.group(2))
        if ano < 2020 or ano > 2025:
            continue
        if ano == 2025 and tri > 3:
            continue

        text = extract_pdf_text(pdf_path)
        if not text:
            continue

        dados = extractor(text, periodo)
        if dados:
            updates = atualizar_banco(conn, empresa, 'Consolidado', periodo, dados)
            total_updates += updates
            total_files += 1
            if updates > 0:
                print(f"    {periodo}: {updates} campos atualizados {list(dados.keys())}")

    print(f"  Total: {total_files} releases processados, {total_updates} campos atualizados")


if __name__ == "__main__":
    print("=" * 70)
    print("INGESTÃO DE DADOS DOS RELEASES (PDF)")
    print("=" * 70)

    conn = sqlite3.connect(DB_PATH)

    empresas = [
        ('Tenda', 'tenda', extrair_tenda),
        ('Cury', 'cury', extrair_cury),
        ('Direcional', 'direcional', extrair_direcional),
        ('PlanoePlano', 'planoeplano', extrair_planoeplano),
        ('MRV', 'mrv', extrair_mrv),
        ('Cyrela', 'cyrela', extrair_cyrela),
    ]

    for empresa, subdir, extractor in empresas:
        print(f"\n--- {empresa} ---")
        processar_empresa(conn, empresa, subdir, extractor)

    # ============================================================
    # CAMPOS CALCULADOS
    # ============================================================
    print("\n" + "=" * 70)
    print("CAMPOS CALCULADOS")
    print("=" * 70)

    c = conn.cursor()

    # 1. pct_sga_receita_liquida = (despesas_comerciais + despesas_ga) / receita_liquida
    #    Note: despesas_comerciais may be negative (expense), receita_liquida positive
    c.execute("""
        UPDATE dados_trimestrais
        SET pct_sga_receita_liquida = ABS(despesas_comerciais + despesas_ga) / receita_liquida
        WHERE despesas_comerciais IS NOT NULL
          AND despesas_ga IS NOT NULL
          AND receita_liquida IS NOT NULL
          AND receita_liquida != 0
          AND pct_sga_receita_liquida IS NULL
    """)
    sga_updates = c.rowcount
    print(f"  pct_sga_receita_liquida calculado: {sga_updates} registros")

    # 2. PlanoePlano VSO trimestral = vendas_brutas_vgv / (estoque_inicio + vgv_lancado)
    #    estoque_inicio = estoque_vgv do período anterior
    c.execute("""
        SELECT id, periodo, vendas_brutas_vgv, estoque_vgv, vgv_lancado
        FROM dados_trimestrais
        WHERE empresa = 'PlanoePlano' AND segmento = 'Consolidado'
        ORDER BY ano, trimestre
    """)
    pep_rows = c.fetchall()
    vso_updates = 0
    for i, (rec_id, periodo, vendas, estoque, lancamentos) in enumerate(pep_rows):
        if i == 0:
            continue  # No previous period for first record
        prev_estoque = pep_rows[i-1][3]  # estoque_vgv of previous period
        if vendas and prev_estoque and lancamentos and (prev_estoque + lancamentos) > 0:
            vso_tri = vendas / (prev_estoque + lancamentos)
            if 0 < vso_tri < 1:  # sanity check
                c.execute("""
                    UPDATE dados_trimestrais SET vso_liquida_trimestral = ?
                    WHERE id = ? AND vso_liquida_trimestral IS NULL
                """, (vso_tri, rec_id))
                if c.rowcount > 0:
                    vso_updates += 1
    print(f"  PlanoePlano vso_liquida_trimestral calculado: {vso_updates} registros")

    # 3. carteira_pos_chaves_pct = carteira_pos_chaves / carteira_recebiveis_total
    c.execute("""
        UPDATE dados_trimestrais
        SET carteira_pos_chaves_pct = carteira_pos_chaves / carteira_recebiveis_total
        WHERE carteira_pos_chaves IS NOT NULL
          AND carteira_recebiveis_total IS NOT NULL
          AND carteira_recebiveis_total != 0
          AND carteira_pos_chaves_pct IS NULL
    """)
    print(f"  carteira_pos_chaves_pct calculado: {c.rowcount} registros")

    # 4. carteira_pct_pl = carteira_recebiveis_total / patrimonio_liquido
    c.execute("""
        UPDATE dados_trimestrais
        SET carteira_pct_pl = carteira_recebiveis_total / patrimonio_liquido
        WHERE carteira_recebiveis_total IS NOT NULL
          AND patrimonio_liquido IS NOT NULL
          AND patrimonio_liquido != 0
          AND carteira_pct_pl IS NULL
    """)
    print(f"  carteira_pct_pl calculado: {c.rowcount} registros")

    # 5. pdd_cobertura_pct = pdd_provisao / carteira_pos_chaves (ou carteira_recebiveis_total)
    c.execute("""
        UPDATE dados_trimestrais
        SET pdd_cobertura_pct = pdd_provisao / carteira_pos_chaves
        WHERE pdd_provisao IS NOT NULL
          AND carteira_pos_chaves IS NOT NULL
          AND carteira_pos_chaves != 0
          AND pdd_cobertura_pct IS NULL
    """)
    print(f"  pdd_cobertura_pct calculado: {c.rowcount} registros")

    conn.commit()

    # Summary
    print("\n" + "=" * 70)
    print("FILL RATES PÓS-RELEASES")
    print("=" * 70)

    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM dados_trimestrais")
    total = c.fetchone()[0]

    campos = ['ebitda', 'ebitda_ajustado', 'margem_ebitda', 'margem_ebitda_ajustada',
              'margem_bruta', 'margem_liquida', 'roe',
              'divida_bruta', 'divida_liquida', 'divida_liquida_pl',
              'geracao_caixa', 'landbank_vgv', 'vso_liquida_trimestral', 'vso_liquida_12m',
              'vendas_liquidas_vgv', 'receitas_apropriar', 'resultado_apropriar', 'margem_apropriar',
              'pct_sga_receita_liquida',
              'carteira_recebiveis_total', 'carteira_pre_chaves', 'carteira_pos_chaves',
              'pdd_provisao', 'pdd_cobertura_pct', 'inadimplencia_total_pct',
              'aging_adimplente_pct', 'aging_vencido_90d_pct',
              'cessao_recebiveis_trimestre', 'saldo_cessao_recebiveis',
              'carteira_pct_pl', 'carteira_pos_chaves_pct']

    print(f"\n  {'Campo':35s} {'Preenchidos':>12s} {'%':>6s}")
    print(f"  {'-'*35} {'-'*12} {'-'*6}")
    for campo in campos:
        c.execute(f"SELECT COUNT(*) FROM dados_trimestrais WHERE {campo} IS NOT NULL")
        cnt = c.fetchone()[0]
        print(f"  {campo:35s} {cnt:12d} {cnt/total*100:5.0f}%")

    conn.close()
    print(f"\nBanco atualizado: {DB_PATH}")

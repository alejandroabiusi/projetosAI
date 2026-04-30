#!/usr/bin/env python3
"""
Extrai dados financeiros de ITRs/DFPs em formato CVM (PDFs padronizados)
e popula o banco dados_financeiros.db.

Formato CVM:
- Balanco Patrimonial: 2 colunas de valor (ITR) ou 3 (DFP)
- DRE: 4 colunas de valor (ITR) ou 3 (DFP)
- Sempre usar primeira coluna de valor (Trimestre Atual / Ultimo Exercicio)
- DFs Consolidadas apenas (ignorar Individuais)
- Valores em R$ mil -> converter para R$ milhoes (/1000)

Para DFP (4T): a DRE contem valores ANUAIS (nao trimestrais).
O script armazena o valor anual; para isolar Q4 e preciso subtrair Q3 acumulado.
"""

import os
import re
import sys
import glob
import sqlite3
import logging
import traceback
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("ERRO: pdfplumber nao instalado. Execute: pip install pdfplumber")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
DB_PATH = BASE_DIR / "dados_financeiros.db"

EMPRESAS = ["cury", "cyrela", "direcional", "mouradubeux", "mrv", "planoeplano", "tenda"]

# Mapa: empresa (nome da pasta) -> nome no banco
EMPRESA_NOME_DB = {
    "cury": "Cury",
    "cyrela": "Cyrela",
    "direcional": "Direcional",
    "mouradubeux": "MouraDubeux",
    "mrv": "MRV",
    "planoeplano": "PlanoePlano",
    "tenda": "Tenda",
}

# Codigos CVM a extrair do Balanco Patrimonial
BP_CODES = {
    "1":       "ativo_total",
    "1.01":    "ativo_circulante",
    "1.01.01": "_caixa_equiv",          # auxiliar
    "1.01.02": "_aplicacoes_fin",       # auxiliar
    "2":       "passivo_total",
    "2.01":    "passivo_circulante",
    "2.01.04": "divida_venc_12m",
    "2.02.01": "divida_venc_24m_mais",
    "2.03":    "patrimonio_liquido",
}

# Codigos CVM a extrair da DRE (usar primeira coluna = trimestre atual)
DRE_CODES = {
    "3.01":    "receita_bruta",
    "3.03":    "lucro_bruto",
    "3.04.01": "despesas_comerciais",
    "3.04.02": "despesas_ga",
    "3.05":    "ebit",
    "3.06":    "resultado_financeiro",
    "3.06.01": "receitas_financeiras",
    "3.06.02": "despesas_financeiras",
    "3.07":    "_resultado_antes_tributos",  # auxiliar
    "3.08":    "ir_csll",
    "3.09":    "lucro_liquido",
    "3.11":    "_lucro_liquido_alt",         # fallback para lucro_liquido
}

# Campos que SEMPRE sobrescrevem (mesmo se ja tem valor no banco)
ALWAYS_OVERWRITE = {
    "ativo_total", "passivo_circulante", "passivo_total", "ebit", "ir_csll",
    "ativo_circulante",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parsing de valores
# ---------------------------------------------------------------------------
def parse_value(text: str) -> float | None:
    """Converte texto numerico CVM para float.
    '1.234.567' -> 1234567.0
    '-1.234' -> -1234.0
    Retorna None se nao conseguir parsear.
    """
    if not text or text.strip() == "":
        return None
    s = text.strip()
    # Remover espacos internos
    s = s.replace(" ", "")
    # Detectar negativo
    neg = False
    if s.startswith("-"):
        neg = True
        s = s[1:]
    # Remover pontos (separador de milhar brasileiro)
    s = s.replace(".", "")
    # Substituir virgula por ponto (para decimais, se houver)
    s = s.replace(",", ".")
    try:
        val = float(s)
        return -val if neg else val
    except ValueError:
        return None


def value_to_millions(val: float | None) -> float | None:
    """Converte de R$ mil para R$ milhoes."""
    if val is None:
        return None
    return round(val / 1000.0, 3)


# ---------------------------------------------------------------------------
# Deteccao do periodo a partir do nome do arquivo
# ---------------------------------------------------------------------------
def detect_periodo_from_filename(filename: str) -> tuple[str, int, int] | None:
    """Extrai periodo do nome do arquivo.
    Formatos:
      Empresa_ITR_YYYY_QT.pdf -> QT+YYYY
      Empresa_ITR_QTyyyy.pdf -> QT+yyyy
    Retorna (periodo, ano, trimestre) ou None.
    """
    name = Path(filename).stem
    # Padrao 1: Empresa_ITR_2024_3T
    m = re.search(r'_ITR_(\d{4})_(\d)T', name, re.IGNORECASE)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        return f"{tri}T{ano}", ano, tri
    # Padrao 2: Empresa_ITR_3T2025
    m = re.search(r'_ITR_(\d)T(\d{4})', name, re.IGNORECASE)
    if m:
        tri = int(m.group(1))
        ano = int(m.group(2))
        return f"{tri}T{ano}", ano, tri
    return None


def detect_periodo_from_pdf_header(full_text: str) -> tuple[str, int, int] | None:
    """Extrai periodo do texto do PDF (primeiras paginas).
    Procura padroes como '30/09/2024', '31/12/2024', etc.
    """
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', full_text)
    if not m:
        return None
    dia, mes, ano = int(m.group(1)), int(m.group(2)), int(m.group(3))
    mes_to_tri = {3: 1, 6: 2, 9: 3, 12: 4}
    tri = mes_to_tri.get(mes)
    if tri is None:
        if mes <= 3:
            tri = 1
        elif mes <= 6:
            tri = 2
        elif mes <= 9:
            tri = 3
        else:
            tri = 4
    return f"{tri}T{ano}", ano, tri


def is_cvm_format(pdf_path: str) -> bool:
    """Verifica se o PDF esta no formato padronizado CVM.
    PDFs CVM comecam com 'ITR - Informacoes Trimestrais' ou
    'DFP - Demonstracoes Financeiras Padronizadas' e tem
    secoes 'DFs Individuais' / 'DFs Consolidadas'.
    """
    try:
        pdf = pdfplumber.open(pdf_path)
        # Checar primeira pagina
        text = pdf.pages[0].extract_text() or ""
        pdf.close()
        # Formato CVM tem header padronizado
        if re.search(r'ITR\s*-\s*Informa', text, re.IGNORECASE):
            return True
        if re.search(r'DFP\s*-\s*Demonstra', text, re.IGNORECASE):
            return True
        # Alternativa: checar se tem "DFs Individuais" ou "DFs Consolidadas"
        if 'DFs Individuais' in text or 'DFs Consolidadas' in text:
            return True
        return False
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Extracao de dados das paginas do PDF
# ---------------------------------------------------------------------------
def is_consolidated_section(page_text: str) -> bool:
    """Verifica se a pagina pertence a DFs Consolidadas."""
    return bool(re.search(r'DFs\s+Consolidadas', page_text, re.IGNORECASE))


def is_individual_section(page_text: str) -> bool:
    """Verifica se a pagina pertence a DFs Individuais."""
    return bool(re.search(r'DFs\s+Individuais', page_text, re.IGNORECASE))


def detect_section_type(page_text: str) -> str | None:
    """Detecta tipo de secao: 'bp_ativo', 'bp_passivo', 'dre' ou None."""
    # Usar apenas as primeiras linhas (header da secao) para deteccao
    header = '\n'.join(page_text.split('\n')[:6]).lower()
    if 'balan' in header and 'ativo' in header and 'passivo' not in header:
        return 'bp_ativo'
    if 'balan' in header and 'passivo' in header:
        return 'bp_passivo'
    if 'demonstra' in header and 'resultado' in header:
        if 'abrangente' not in header and 'muta' not in header:
            return 'dre'
    return None


def extract_account_values(page_text: str) -> dict[str, float]:
    """Extrai pares (codigo_conta, valor_primeira_coluna) de uma pagina de texto.

    Sempre usa a primeira coluna de valor (Trimestre Atual / Ultimo Exercicio).

    Retorna dict {codigo_conta: valor_em_reais_mil}
    """
    results = {}
    lines = page_text.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Linhas de conta comecam com digito seguido de ponto ou espaco
        # Ex: "1 Ativo Total 3.957.292 3.092.023"
        # Ex: "1.01 Ativo Circulante 2.628.427 2.212.808"
        # Ex: "3.01 Receita de Venda... 1.055.872 2.891.339 751.874 2.074.576"
        m = re.match(r'^(\d+(?:\.\d+)*)\s+', line)
        if not m:
            continue

        codigo = m.group(1)

        # Ignorar "Lucro por Acao" (3.99)
        if codigo.startswith('3.99'):
            continue

        # Pegar o texto apos o codigo
        rest = line[m.end():]

        # Coletar valores numericos do final da linha
        tokens = rest.split()
        values = []
        for token in reversed(tokens):
            val = parse_value(token)
            if val is not None:
                values.insert(0, val)
            else:
                break  # parou de encontrar numeros

        if not values:
            continue

        # Primeira coluna de valor = values[0]
        results[codigo] = values[0]

    return results


def extract_data_from_pdf(pdf_path: str) -> dict[str, float]:
    """Extrai todos os dados financeiros consolidados de um PDF CVM.

    Retorna dict com campos do banco e valores em R$ milhoes.
    Retorna dict vazio se nao for formato CVM ou nao encontrar dados.
    """
    raw_data = {}  # codigo_conta -> valor (R$ mil)

    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        log.error(f"  Erro ao abrir PDF {pdf_path}: {e}")
        return {}

    # Estado: estamos na secao consolidada?
    in_consolidated = False
    last_section = None  # para continuacao de paginas

    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue

        # Detectar se estamos em secao consolidada ou individual
        if is_consolidated_section(text):
            in_consolidated = True
        elif is_individual_section(text):
            in_consolidated = False

        if not in_consolidated:
            last_section = None
            continue

        section = detect_section_type(text)
        if section is not None:
            last_section = section
        else:
            # Pode ser continuacao de secao anterior (ex: passivo que continua
            # com PL na pagina seguinte)
            # Verificar se ha linhas com codigos de conta que correspondem ao tipo anterior
            lines = text.split('\n')
            has_bp_passivo = any(re.match(r'^\s*2\.', line) for line in lines)
            has_bp_ativo = any(re.match(r'^\s*1\.', line) for line in lines)
            has_dre = any(re.match(r'^\s*3\.', line) for line in lines)

            if has_bp_passivo:
                section = 'bp_passivo'
            elif has_bp_ativo:
                section = 'bp_ativo'
            elif has_dre:
                section = 'dre'
            elif last_section:
                # Usar a secao anterior como fallback se houver contas genericas
                has_accounts = bool(re.search(r'^\d+(?:\.\d+)*\s+', text, re.MULTILINE))
                if has_accounts:
                    section = last_section

        if section is None:
            continue

        values = extract_account_values(text)
        # Merge: manter o primeiro valor encontrado para cada codigo
        # (evita sobrescrever com dados de paginas duplicadas)
        for code, val in values.items():
            if code not in raw_data:
                raw_data[code] = val

    pdf.close()

    if not raw_data:
        return {}

    # Converter raw_data (codigos CVM) para campos do banco
    result = {}

    # Balanco Patrimonial
    for code, field in BP_CODES.items():
        if code in raw_data:
            result[field] = raw_data[code]

    # Calcular caixa_aplicacoes = caixa + aplicacoes
    caixa = result.pop("_caixa_equiv", None)
    aplic = result.pop("_aplicacoes_fin", None)
    if caixa is not None or aplic is not None:
        result["caixa_aplicacoes"] = (caixa or 0) + (aplic or 0)

    # DRE
    for code, field in DRE_CODES.items():
        if code in raw_data:
            result[field] = raw_data[code]

    # Fallback: lucro_liquido pode estar em 3.09 ou 3.11
    if "lucro_liquido" not in result and "_lucro_liquido_alt" in result:
        result["lucro_liquido"] = result["_lucro_liquido_alt"]

    # Limpar campos auxiliares
    for key in list(result.keys()):
        if key.startswith("_"):
            del result[key]

    # Converter de R$ mil para R$ milhoes
    for key in list(result.keys()):
        result[key] = value_to_millions(result[key])

    return result


# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------
def get_existing_record(conn: sqlite3.Connection, empresa: str, periodo: str) -> dict | None:
    """Busca registro existente no banco."""
    cursor = conn.execute(
        "SELECT * FROM dados_trimestrais WHERE empresa = ? AND segmento = 'Consolidado' AND periodo = ?",
        (empresa, periodo)
    )
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    if row:
        return dict(zip(columns, row))
    return None


def upsert_record(conn: sqlite3.Connection, empresa: str, periodo: str,
                  ano: int, trimestre: int, data: dict, is_dfp: bool):
    """Insere ou atualiza registro no banco.

    - Campos em ALWAYS_OVERWRITE: sempre sobrescrever
    - Demais campos: so preencher se NULL no banco
    """
    if not data:
        return

    existing = get_existing_record(conn, empresa, periodo)
    fonte = "dfp_cvm" if is_dfp else "itr_cvm"

    if existing is None:
        # INSERT novo registro
        fields = ["empresa", "segmento", "periodo", "ano", "trimestre", "fonte"]
        values = [empresa, "Consolidado", periodo, ano, trimestre, fonte]

        for field, val in data.items():
            if val is not None:
                fields.append(field)
                values.append(val)

        placeholders = ", ".join(["?"] * len(values))
        field_names = ", ".join(fields)
        conn.execute(
            f"INSERT INTO dados_trimestrais ({field_names}) VALUES ({placeholders})",
            values
        )
        log.info(f"    INSERT {empresa} {periodo}: {len(data)} campos")
    else:
        # UPDATE: aplicar regras de sobrescrita
        updates = {}
        for field, val in data.items():
            if val is None:
                continue
            if field in ALWAYS_OVERWRITE:
                updates[field] = val
            elif existing.get(field) is None:
                updates[field] = val

        if updates:
            set_clause = ", ".join([f"{f} = ?" for f in updates.keys()])
            values = list(updates.values()) + [empresa, periodo]
            conn.execute(
                f"UPDATE dados_trimestrais SET {set_clause} "
                "WHERE empresa = ? AND segmento = 'Consolidado' AND periodo = ?",
                values
            )
            log.info(f"    UPDATE {empresa} {periodo}: {len(updates)} campos "
                      f"({', '.join(updates.keys())})")
        else:
            log.info(f"    SKIP {empresa} {periodo}: nenhum campo a atualizar")


# ---------------------------------------------------------------------------
# Processamento principal
# ---------------------------------------------------------------------------
def find_itr_files(empresa: str) -> list[str]:
    """Encontra todos os PDFs de ITR/DFP para uma empresa.
    Filtra por 2020-2025 e remove duplicatas (mesmo periodo, nomes diferentes).
    Retorna lista de paths unicos por periodo.
    """
    itr_dir = DOWNLOADS_DIR / empresa / "itr_dfp"
    if not itr_dir.exists():
        log.warning(f"  Diretorio nao encontrado: {itr_dir}")
        return []

    pdf_files = sorted(glob.glob(str(itr_dir / "*.pdf")))

    # Agrupar por periodo para deduplicar
    by_periodo: dict[str, list[str]] = {}
    no_periodo: list[str] = []

    for f in pdf_files:
        info = detect_periodo_from_filename(f)
        if info:
            periodo, ano, _ = info
            if 2020 <= ano <= 2025:
                by_periodo.setdefault(periodo, []).append(f)
        else:
            no_periodo.append(f)

    # Para cada periodo, escolher um arquivo (preferir formato _YYYY_QT)
    result = []
    for periodo, files in sorted(by_periodo.items()):
        if len(files) == 1:
            result.append(files[0])
        else:
            # Preferir formato Empresa_ITR_YYYY_QT.pdf (mais antigo/padrao)
            chosen = None
            for f in files:
                if re.search(r'_ITR_\d{4}_\d+T', os.path.basename(f)):
                    chosen = f
                    break
            result.append(chosen or files[0])

    # Adicionar arquivos sem periodo detectado (serao analisados pelo header)
    result.extend(no_periodo)

    return result


def process_empresa(conn: sqlite3.Connection, empresa: str) -> tuple[int, int, int]:
    """Processa todos os ITRs de uma empresa.
    Retorna (total_arquivos, sucesso, erro).
    """
    nome_db = EMPRESA_NOME_DB.get(empresa, empresa)
    log.info(f"\n{'='*60}")
    log.info(f"Empresa: {empresa} (banco: {nome_db})")
    log.info(f"{'='*60}")

    files = find_itr_files(empresa)
    if not files:
        log.warning(f"  Nenhum arquivo encontrado para {empresa}")
        return 0, 0, 0

    log.info(f"  {len(files)} arquivos a processar (apos dedup)")

    total = len(files)
    sucesso = 0
    erro = 0

    for pdf_path in files:
        filename = os.path.basename(pdf_path)
        log.info(f"\n  Processando: {filename}")

        # Verificar se e formato CVM
        if not is_cvm_format(pdf_path):
            log.warning(f"    PDF nao esta em formato CVM padronizado, pulando: {filename}")
            erro += 1
            continue

        # Detectar periodo pelo nome do arquivo
        info = detect_periodo_from_filename(pdf_path)

        if info:
            periodo, ano, tri = info
            if ano < 2020 or ano > 2025:
                log.info(f"    Fora do escopo (ano={ano}), pulando")
                continue
        else:
            # Tentar detectar pelo conteudo do PDF
            try:
                pdf = pdfplumber.open(pdf_path)
                header = ""
                for p in pdf.pages[:3]:
                    t = p.extract_text() or ""
                    header += t + "\n"
                pdf.close()
                info = detect_periodo_from_pdf_header(header)
                if info:
                    periodo, ano, tri = info
                    if ano < 2020 or ano > 2025:
                        log.info(f"    Fora do escopo (ano={ano}), pulando")
                        continue
                else:
                    log.warning(f"    Nao foi possivel detectar periodo de {filename}")
                    erro += 1
                    continue
            except Exception as e:
                log.error(f"    Erro ao ler header de {filename}: {e}")
                erro += 1
                continue

        is_dfp = (tri == 4)
        log.info(f"    Periodo: {periodo} ({'DFP' if is_dfp else 'ITR'})")

        try:
            data = extract_data_from_pdf(pdf_path)
            if not data:
                log.warning(f"    Nenhum dado extraido de {filename}")
                erro += 1
                continue

            log.info(f"    Extraidos {len(data)} campos: {sorted(data.keys())}")

            # Upsert no banco
            upsert_record(conn, nome_db, periodo, ano, tri, data, is_dfp)
            sucesso += 1

        except Exception as e:
            log.error(f"    ERRO ao processar {filename}: {e}")
            traceback.print_exc()
            erro += 1

    return total, sucesso, erro


def main():
    log.info(f"Base dir: {BASE_DIR}")
    log.info(f"DB path: {DB_PATH}")
    log.info(f"Downloads dir: {DOWNLOADS_DIR}")

    if not DB_PATH.exists():
        log.error(f"Banco de dados nao encontrado: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)

    total_geral = 0
    sucesso_geral = 0
    erro_geral = 0

    for empresa in EMPRESAS:
        try:
            t, s, e = process_empresa(conn, empresa)
            total_geral += t
            sucesso_geral += s
            erro_geral += e
            conn.commit()
        except Exception as ex:
            log.error(f"ERRO GERAL em {empresa}: {ex}")
            traceback.print_exc()
            conn.rollback()

    conn.close()

    log.info(f"\n{'='*60}")
    log.info(f"RESUMO FINAL")
    log.info(f"{'='*60}")
    log.info(f"Total de arquivos processados: {total_geral}")
    log.info(f"Sucesso: {sucesso_geral}")
    log.info(f"Erros/pulados: {erro_geral}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()

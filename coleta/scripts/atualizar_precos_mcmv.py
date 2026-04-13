"""
Cruzar empreendimentos do SQLite com Excel de preços MCMV.
Atualiza campos qty_uh_mcmv e preco_medio_lancamento onde encontrar match confiável.
Usa a MESMA lógica de matching do atualizar_datas_lancamento.py.
"""

import sqlite3
import re
import unicodedata
import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
from pathlib import Path
from datetime import datetime

DB_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\data\empreendimentos.db"
EXCEL_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\precos_mcmv_lcto.xlsx"
REPORT_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\build_logs\relatorio_precos_mcmv.md"

SIMILARITY_THRESHOLD = 85

# Sufixos removíveis para normalização de incorporadoras
REMOVE_SUFFIXES = [
    "engenharia", "construtora", "incorporadora", "construcoes",
    "empreendimentos", "incorporacoes", "construções", "incorporações",
    "desenvolvimento", "imobiliario", "imobiliária", "imobiliaria",
    "participacoes", "participações", "residencial",
]

# Padrões de fase no final do nome
PHASE_PATTERN = re.compile(
    r'\s*[-–—]?\s*'
    r'(?:'
    r'(?:fase|etapa)\s*(\d+|[IVX]+)'
    r'|(\b[IVX]{1,4})$'
    r')',
    re.IGNORECASE
)

ROMAN_MAP = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}


def normalize_text(text):
    """Remove acentos, lowercase, strip."""
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.strip().lower()


def normalize_empresa(name):
    """Normaliza nome de incorporadora para matching."""
    n = normalize_text(name)
    n = re.sub(r'[^\w\s&]', ' ', n)
    for suf in REMOVE_SUFFIXES:
        n = re.sub(r'\b' + suf + r'\b', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    n = n.replace("&", "e")
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def normalize_cidade(city):
    """Normaliza cidade."""
    return normalize_text(city).strip()


def extract_phase(name):
    """
    Extrai sufixo de fase do nome do empreendimento.
    Retorna (nome_base, fase_numero_ou_None).
    """
    name_stripped = name.strip()
    m = PHASE_PATTERN.search(name_stripped)
    if m:
        raw = m.group(1) or m.group(2)
        if raw:
            raw = raw.strip().upper()
            if raw in ROMAN_MAP:
                phase = ROMAN_MAP[raw]
            elif raw.isdigit():
                phase = int(raw)
            else:
                phase = ROMAN_MAP.get(raw, None)
            base = name_stripped[:m.start()].strip()
            if len(base) >= 3:
                return base, phase
    return name_stripped, None


def normalize_nome(name):
    """Normaliza nome de empreendimento (sem extrair fase)."""
    n = normalize_text(name)
    n = re.sub(r'[^\w\s]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def build_empresa_mapping(db_empresas, excel_empresas):
    """
    Cria mapeamento: empresa_normalizada_db -> [empresas_normalizadas_excel].
    """
    db_norms = {normalize_empresa(e): e for e in db_empresas}
    excel_norms = {normalize_empresa(e): e for e in excel_empresas}

    manual_overrides = {
        "canopus construcoes": {"canopus"},
        "torreao villarim": {"torreao villarim"},
        "torre villarim": {"torreao villarim"},
        "viva benx": {"benx", "viva benx"},
        "econ": {"econ"},
        "hm": {"hm"},
        "sol": {"sol"},
        "vic": {"vic"},
        "sr": {"sr"},
        "smart": {"smart"},
        "pacaembu": {"pacaembu"},
        "vitta": {"vitta"},
        "vibra": {"vibra"},
        "plano e plano": {"plano e plano"},
        "metrocasa": {"metrocasa"},
    }

    mapping = {}

    for db_norm, db_orig in db_norms.items():
        if db_norm in manual_overrides:
            override_norms = manual_overrides[db_norm]
            found = set()
            for ex_norm in excel_norms:
                for ov in override_norms:
                    if ex_norm == ov:
                        found.add(ex_norm)
            if found:
                mapping[db_norm] = found
                continue

        matches = set()
        for ex_norm, ex_orig in excel_norms.items():
            if not db_norm or not ex_norm:
                continue
            if db_norm == ex_norm:
                matches.add(ex_norm)
                continue
            len_max = max(len(db_norm), len(ex_norm))
            len_min = min(len(db_norm), len(ex_norm))
            if len_max == 0:
                continue
            len_ratio = len_min / len_max
            if len_ratio >= 0.5:
                ratio = fuzz.ratio(db_norm, ex_norm)
                threshold = 95 if len_min <= 5 else 85
                if ratio >= threshold:
                    matches.add(ex_norm)
        if matches:
            mapping[db_norm] = matches

    return mapping, db_norms, excel_norms


def ensure_columns(conn):
    """Garante que as colunas qty_uh_mcmv e preco_medio_lancamento existem."""
    cursor = conn.cursor()
    # Verificar colunas existentes
    cursor.execute("PRAGMA table_info(empreendimentos)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    added = []
    if "qty_uh_mcmv" not in existing_cols:
        cursor.execute("ALTER TABLE empreendimentos ADD COLUMN qty_uh_mcmv INTEGER")
        added.append("qty_uh_mcmv")
    if "preco_medio_lancamento" not in existing_cols:
        cursor.execute("ALTER TABLE empreendimentos ADD COLUMN preco_medio_lancamento REAL")
        added.append("preco_medio_lancamento")

    if added:
        conn.commit()
        print(f"Colunas adicionadas: {', '.join(added)}")
    else:
        print("Colunas qty_uh_mcmv e preco_medio_lancamento já existem.")


def main():
    print("=" * 60)
    print("Atualização de Preços MCMV e QTD Unidades")
    print("=" * 60)
    print()

    print("Carregando dados...")

    # Load Excel
    df_excel = pd.read_excel(EXCEL_PATH)
    # Normalizar nomes das colunas (encoding issues)
    df_excel.columns = [
        "empreendimento", "cidade", "estado", "incorporadora",
        "data_lancamento", "ano", "qty_uh_mcmv", "preco_medio_lancamento"
    ]
    df_excel["incorporadora"] = df_excel["incorporadora"].astype(str)
    df_excel["empreendimento"] = df_excel["empreendimento"].astype(str)
    df_excel["cidade"] = df_excel["cidade"].astype(str).fillna("")
    df_excel["estado"] = df_excel["estado"].astype(str).fillna("")

    # Pre-normalize Excel
    df_excel["_norm_empresa"] = df_excel["incorporadora"].apply(normalize_empresa)
    df_excel["_norm_cidade"] = df_excel["cidade"].apply(normalize_cidade)
    df_excel["_norm_estado"] = df_excel["estado"].apply(normalize_text)
    df_excel["_norm_nome"] = df_excel["empreendimento"].apply(normalize_nome)
    phase_results = df_excel["empreendimento"].apply(lambda x: extract_phase(normalize_nome(x)))
    df_excel["_nome_base"] = phase_results.apply(lambda x: x[0])
    df_excel["_fase"] = phase_results.apply(lambda x: x[1]).astype(object)
    df_excel.loc[df_excel["_fase"].isna(), "_fase"] = None

    # Index Excel by (norm_empresa, norm_estado, norm_cidade)
    excel_index = defaultdict(list)
    for idx, row in df_excel.iterrows():
        key = (row["_norm_empresa"], row["_norm_estado"], row["_norm_cidade"])
        excel_index[key].append(row)

    # Load SQLite
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)

    df_db = pd.read_sql_query(
        "SELECT id, empresa, nome, cidade, estado, qty_uh_mcmv, preco_medio_lancamento FROM empreendimentos",
        conn
    )
    total_db = len(df_db)
    print(f"Total empreendimentos na base: {total_db}")
    print(f"Total linhas no Excel: {len(df_excel)}")

    # Apenas processar os que não têm NENHUM dos dois campos preenchidos
    df_to_match = df_db[
        (df_db["qty_uh_mcmv"].isna() | (df_db["qty_uh_mcmv"] == 0)) &
        (df_db["preco_medio_lancamento"].isna() | (df_db["preco_medio_lancamento"] == 0))
    ].copy()

    # Também processar quem tem um mas não outro
    df_partial = df_db[
        ((df_db["qty_uh_mcmv"].isna() | (df_db["qty_uh_mcmv"] == 0)) &
         df_db["preco_medio_lancamento"].notna() & (df_db["preco_medio_lancamento"] != 0)) |
        (df_db["qty_uh_mcmv"].notna() & (df_db["qty_uh_mcmv"] != 0) &
         (df_db["preco_medio_lancamento"].isna() | (df_db["preco_medio_lancamento"] == 0)))
    ].copy()

    df_to_process = pd.concat([df_to_match, df_partial]).drop_duplicates(subset=["id"])
    already_complete = total_db - len(df_to_process)

    print(f"Empreendimentos a processar (sem preço e/ou qty): {len(df_to_process)}")
    print(f"Empreendimentos já completos: {already_complete}")

    # Build empresa mapping
    db_empresas = df_db["empresa"].unique().tolist()
    excel_empresas = df_excel["incorporadora"].unique().tolist()
    empresa_map, db_norms, excel_norms = build_empresa_mapping(db_empresas, excel_empresas)

    print(f"\nEmpresas na base com match no Excel: {len(empresa_map)}/{len(db_norms)}")
    for db_n, ex_matches in sorted(empresa_map.items()):
        orig_db = db_norms[db_n]
        orig_excels = [excel_norms[m] for m in ex_matches]
        print(f"  {orig_db} -> {orig_excels}")

    # Match empreendimentos
    matches_confirmed = []
    matches_ambiguous = []
    no_match = []

    for _, row in df_to_process.iterrows():
        emp_id = row["id"]
        empresa = row["empresa"]
        nome = row["nome"]
        cidade = row["cidade"] or ""
        estado = row["estado"] or ""
        existing_qty = row["qty_uh_mcmv"]
        existing_preco = row["preco_medio_lancamento"]

        norm_emp = normalize_empresa(empresa)
        norm_estado = normalize_text(estado)
        norm_cidade = normalize_cidade(cidade)
        norm_nome = normalize_nome(nome)
        nome_base, fase_db = extract_phase(norm_nome)

        matched_excel_empresas = empresa_map.get(norm_emp, set())
        if not matched_excel_empresas:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": "empresa_not_found"
            })
            continue

        candidates = []
        for ex_emp_norm in matched_excel_empresas:
            key = (ex_emp_norm, norm_estado, norm_cidade)
            candidates.extend(excel_index.get(key, []))

        if not candidates:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": "cidade_estado_not_found"
            })
            continue

        scored = []
        for cand in candidates:
            cand_nome = cand["_norm_nome"]
            cand_base = cand["_nome_base"]
            cand_fase = cand["_fase"]

            score_full = fuzz.ratio(norm_nome, cand_nome)
            score_token = fuzz.token_sort_ratio(norm_nome, cand_nome)
            score_base = fuzz.ratio(nome_base, cand_base)
            score = max(score_full, score_token, score_base)

            scored.append({
                "score": score,
                "cand_nome_orig": cand["empreendimento"],
                "cand_base": cand_base,
                "cand_fase": cand_fase,
                "qty_uh_mcmv": cand["qty_uh_mcmv"],
                "preco_medio_lancamento": cand["preco_medio_lancamento"],
                "cand_incorporadora": cand["incorporadora"],
            })

        scored.sort(key=lambda x: x["score"], reverse=True)

        if not scored or scored[0]["score"] < SIMILARITY_THRESHOLD:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": f"low_similarity (best={scored[0]['score'] if scored else 0})"
            })
            continue

        best = scored[0]

        # Phase check
        has_phase_db = fase_db is not None
        has_phase_excel = best["cand_fase"] is not None

        if has_phase_db and has_phase_excel:
            if fase_db != best["cand_fase"]:
                no_match.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "reason": f"phase_mismatch (db={fase_db}, excel={best['cand_fase']})"
                })
                continue
        elif not has_phase_db and has_phase_excel:
            same_base_candidates = [s for s in scored if s["score"] >= SIMILARITY_THRESHOLD and s["cand_fase"] is not None]
            distinct_phases = set(s["cand_fase"] for s in same_base_candidates)
            if len(distinct_phases) > 1:
                matches_ambiguous.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "candidates": [(s["cand_nome_orig"], s["cand_fase"], s["qty_uh_mcmv"], s["preco_medio_lancamento"], s["score"]) for s in same_base_candidates[:5]]
                })
                continue

        # Check for ties with different values
        top_score = best["score"]
        ties = [s for s in scored if s["score"] == top_score]
        if len(ties) > 1:
            unique_precos = set(round(s["preco_medio_lancamento"], 0) for s in ties if pd.notna(s["preco_medio_lancamento"]))
            if len(unique_precos) > 1:
                matches_ambiguous.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "candidates": [(s["cand_nome_orig"], s["cand_fase"], s["qty_uh_mcmv"], s["preco_medio_lancamento"], s["score"]) for s in ties[:5]]
                })
                continue

        # Confirmed match - preparar valores
        new_qty = None
        new_preco = None

        if pd.notna(best["qty_uh_mcmv"]) and best["qty_uh_mcmv"] > 0:
            if pd.isna(existing_qty) or existing_qty == 0:
                new_qty = int(best["qty_uh_mcmv"])

        if pd.notna(best["preco_medio_lancamento"]) and best["preco_medio_lancamento"] > 0:
            if pd.isna(existing_preco) or existing_preco == 0:
                new_preco = round(float(best["preco_medio_lancamento"]), 2)

        if new_qty is not None or new_preco is not None:
            matches_confirmed.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "match_nome": best["cand_nome_orig"],
                "match_incorporadora": best["cand_incorporadora"],
                "score": best["score"],
                "new_qty": new_qty,
                "new_preco": new_preco,
                "fase_db": fase_db,
                "fase_excel": best["cand_fase"],
            })
        else:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": "match_found_but_no_new_data"
            })

    # Update SQLite
    print(f"\nResultados:")
    print(f"  Matches confirmados: {len(matches_confirmed)}")
    print(f"  Matches ambíguos: {len(matches_ambiguous)}")
    print(f"  Sem match: {len(no_match)}")

    cursor = conn.cursor()
    updated_qty = 0
    updated_preco = 0
    for m in matches_confirmed:
        if m["new_qty"] is not None:
            cursor.execute(
                "UPDATE empreendimentos SET qty_uh_mcmv = ? WHERE id = ?",
                (m["new_qty"], m["id"])
            )
            updated_qty += cursor.rowcount
        if m["new_preco"] is not None:
            cursor.execute(
                "UPDATE empreendimentos SET preco_medio_lancamento = ? WHERE id = ?",
                (m["new_preco"], m["id"])
            )
            updated_preco += cursor.rowcount

    conn.commit()
    print(f"  Registros atualizados qty_uh_mcmv: {updated_qty}")
    print(f"  Registros atualizados preco_medio_lancamento: {updated_preco}")

    # Generate report
    report = []
    report.append("# Relatório — Atualização de Preços MCMV e QTD Unidades\n")
    report.append(f"**Data de execução:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    report.append("## Resumo\n")
    report.append("| Métrica | Valor |")
    report.append("|---|---|")
    report.append(f"| Total de empreendimentos na base | {total_db} |")
    report.append(f"| Já completos (preservados) | {already_complete} |")
    report.append(f"| Processados (sem preço e/ou qty) | {len(df_to_process)} |")
    report.append(f"| Matches confirmados | {len(matches_confirmed)} |")
    report.append(f"| Matches ambíguos (revisão manual) | {len(matches_ambiguous)} |")
    report.append(f"| Sem match | {len(no_match)} |")
    report.append(f"| **Registros atualizados — qty_uh_mcmv** | **{updated_qty}** |")
    report.append(f"| **Registros atualizados — preco_medio_lancamento** | **{updated_preco}** |")
    report.append("")

    report.append("## Parâmetros utilizados\n")
    report.append(f"- Threshold de similaridade: {SIMILARITY_THRESHOLD}%")
    report.append(f"- Fonte Excel: {len(df_excel)} linhas, {df_excel['incorporadora'].nunique()} incorporadoras")
    report.append(f"- Match por: incorporadora (normalizada) + estado + cidade + nome fuzzy")
    report.append(f"- Campos atualizados: qty_uh_mcmv (int), preco_medio_lancamento (float, 2 decimais)")
    report.append(f"- Regra: NÃO sobrescrever valores existentes (só preencher NULL/0)")
    report.append("")

    # Mapeamento de empresas
    report.append("## Mapeamento de Incorporadoras\n")
    report.append("| Base | Excel |")
    report.append("|---|---|")
    for db_n, ex_matches in sorted(empresa_map.items()):
        orig_db = db_norms[db_n]
        orig_excels = [excel_norms[m] for m in ex_matches]
        report.append(f"| {orig_db} | {', '.join(orig_excels)} |")
    report.append("")

    # Sample confirmed matches
    report.append("## Amostra de Matches Confirmados (primeiros 30)\n")
    report.append("| # | Empresa | Nome (Base) | Nome (Excel) | Score | QTD UH | Preço Médio |")
    report.append("|---|---|---|---|---|---|---|")
    for i, m in enumerate(matches_confirmed[:30], 1):
        qty_str = str(m["new_qty"]) if m["new_qty"] is not None else "-"
        preco_str = f"R$ {m['new_preco']:,.2f}" if m["new_preco"] is not None else "-"
        report.append(f"| {i} | {m['empresa']} | {m['nome']} | {m['match_nome']} | {m['score']} | {qty_str} | {preco_str} |")
    report.append("")

    # Ambiguous matches
    if matches_ambiguous:
        report.append("## Matches Ambíguos (para revisão manual)\n")
        report.append(f"Total: {len(matches_ambiguous)}\n")
        for i, a in enumerate(matches_ambiguous[:20], 1):
            report.append(f"### {i}. {a['empresa']} — {a['nome']} ({a['cidade']}/{a['estado']})")
            report.append("Candidatos no Excel:")
            for cand_nome, cand_fase, cand_qty, cand_preco, cand_score in a["candidates"]:
                preco_str = f"R$ {cand_preco:,.2f}" if pd.notna(cand_preco) else "N/A"
                report.append(f"  - `{cand_nome}` (fase={cand_fase}, qty={cand_qty}, preco={preco_str}, score={cand_score})")
            report.append("")

    # No match breakdown
    report.append("## Sem Match — Breakdown por Motivo\n")
    reason_counts = defaultdict(int)
    for nm in no_match:
        reason = nm["reason"].split(" ")[0]
        reason_counts[reason] += 1
    report.append("| Motivo | Quantidade |")
    report.append("|---|---|")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        report.append(f"| {reason} | {count} |")
    report.append("")

    # Write report
    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    print(f"\nRelatório salvo em: {REPORT_PATH}")
    conn.close()


if __name__ == "__main__":
    main()

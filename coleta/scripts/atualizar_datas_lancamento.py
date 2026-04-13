"""
Missão 1: Cruzar empreendimentos do SQLite com Excel de datas de lançamento.
Atualiza campo data_lancamento onde encontrar match confiável.
"""

import sqlite3
import re
import unicodedata
import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
from pathlib import Path

DB_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\data\empreendimentos.db"
EXCEL_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\datas_lancamento.xlsx"
REPORT_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\build_logs\relatorio_missao1_datas_lancamento.md"

SIMILARITY_THRESHOLD = 85

# Sufixos removíveis para normalização de incorporadoras
REMOVE_SUFFIXES = [
    "engenharia", "construtora", "incorporadora", "construcoes",
    "empreendimentos", "incorporacoes", "construções", "incorporações",
    "desenvolvimento", "imobiliario", "imobiliária", "imobiliaria",
    "participacoes", "participações", "residencial",
]

# Padrões de fase no final do nome
# Só captura sufixos que claramente indicam fases:
# - "Fase X", "Etapa X", "Bloco X" explícitos
# - Numerais romanos I-X soltos no final (mas NÃO números arábicos soltos,
#   pois "Mirante 7" ou "Portugal 2207" não são fases)
PHASE_PATTERN = re.compile(
    r'\s*[-–—]?\s*'
    r'(?:'
    r'(?:fase|etapa)\s*(\d+|[IVX]+)'
    r'|(\b[IVX]{1,4})$'           # numerais romanos soltos no final
    r')',
    re.IGNORECASE
)

# Mapa de romanos
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
    # Remove pontuação exceto &
    n = re.sub(r'[^\w\s&]', ' ', n)
    # Remove sufixos comuns
    for suf in REMOVE_SUFFIXES:
        n = re.sub(r'\b' + suf + r'\b', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    # Normalizar & -> e
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
    fase_numero é int se detectado, None se não há fase.
    """
    name_stripped = name.strip()
    m = PHASE_PATTERN.search(name_stripped)
    if m:
        # Determinar qual grupo bateu
        raw = m.group(1) or m.group(2)
        if raw:
            raw = raw.strip().upper()
            if raw in ROMAN_MAP:
                phase = ROMAN_MAP[raw]
            elif raw.isdigit():
                phase = int(raw)
            else:
                # Tentar interpretar como romano
                phase = ROMAN_MAP.get(raw, None)
            base = name_stripped[:m.start()].strip()
            # Evitar falsos positivos: base precisa ter conteúdo
            if len(base) >= 3:
                return base, phase
    return name_stripped, None


def normalize_nome(name):
    """Normaliza nome de empreendimento (sem extrair fase)."""
    n = normalize_text(name)
    # Remove pontuação
    n = re.sub(r'[^\w\s]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def convert_date(date_val):
    """Converte MM/YYYY (string) ou datetime para YYYY-MM."""
    import datetime
    if isinstance(date_val, datetime.datetime):
        return f"{date_val.year}-{date_val.month:02d}"
    if isinstance(date_val, str):
        m = re.match(r'^(\d{2})/(\d{4})$', date_val.strip())
        if m:
            return f"{m.group(2)}-{m.group(1)}"
    return None


def build_empresa_mapping(db_empresas, excel_empresas):
    """
    Cria mapeamento: empresa_normalizada_db -> [empresas_normalizadas_excel].
    Retorna dict: norm_db_empresa -> set of norm_excel_empresa.
    """
    db_norms = {normalize_empresa(e): e for e in db_empresas}
    excel_norms = {normalize_empresa(e): e for e in excel_empresas}

    # Manual overrides for known variations
    manual_overrides = {
        "canopus construcoes": {"canopus"},  # Canopus Construcoes = CANOPUS
        "torreao villarim": {"torreao villarim"},
        "torre villarim": {"torreao villarim"},
        "viva benx": {"benx", "viva benx"},
        "econ": {"econ"},  # Avoid matching Precon, Elcon etc
        "hm": {"hm"},
        "sol": {"sol"},
        "vic": {"vic"},
        "sr": {"sr"},
        "smart": {"smart"},
        "pacaembu": {"pacaembu"},
    }

    mapping = {}  # norm_db -> set of norm_excel

    for db_norm, db_orig in db_norms.items():
        # Check manual overrides first
        if db_norm in manual_overrides:
            override_norms = manual_overrides[db_norm]
            # Find matching excel norms
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
            # Exact match after normalization
            if db_norm == ex_norm:
                matches.add(ex_norm)
                continue
            # Fuzzy similarity - require similar lengths
            len_max = max(len(db_norm), len(ex_norm))
            len_min = min(len(db_norm), len(ex_norm))
            if len_max == 0:
                continue
            len_ratio = len_min / len_max
            if len_ratio >= 0.5:
                ratio = fuzz.ratio(db_norm, ex_norm)
                # For short names (<= 5 chars), require exact or very high match
                threshold = 95 if len_min <= 5 else 85
                if ratio >= threshold:
                    matches.add(ex_norm)
        if matches:
            mapping[db_norm] = matches

    return mapping, db_norms, excel_norms


def main():
    print("Carregando dados...")

    # Load Excel
    df_excel = pd.read_excel(EXCEL_PATH)
    date_col = df_excel.columns[4]  # 'Data de Lançamento' (encoding issues)
    df_excel.columns = ["incorporadora", "empreendimento", "cidade", "estado", "data_lancamento"]
    df_excel["incorporadora"] = df_excel["incorporadora"].astype(str)
    df_excel["empreendimento"] = df_excel["empreendimento"].astype(str)
    df_excel["cidade"] = df_excel["cidade"].astype(str).fillna("")
    df_excel["estado"] = df_excel["estado"].astype(str).fillna("")
    # Keep raw date for conversion (may be string "MM/YYYY" or datetime)
    df_excel["_data_iso"] = df_excel["data_lancamento"].apply(convert_date)

    # Pre-normalize Excel
    df_excel["_norm_empresa"] = df_excel["incorporadora"].apply(normalize_empresa)
    df_excel["_norm_cidade"] = df_excel["cidade"].apply(normalize_cidade)
    df_excel["_norm_estado"] = df_excel["estado"].apply(normalize_text)
    df_excel["_norm_nome"] = df_excel["empreendimento"].apply(normalize_nome)
    phase_results = df_excel["empreendimento"].apply(lambda x: extract_phase(normalize_nome(x)))
    df_excel["_nome_base"] = phase_results.apply(lambda x: x[0])
    # Use object dtype to keep None as None (not NaN)
    df_excel["_fase"] = phase_results.apply(lambda x: x[1]).astype(object)
    df_excel.loc[df_excel["_fase"].isna(), "_fase"] = None

    # Index Excel by (norm_empresa, norm_estado, norm_cidade)
    excel_index = defaultdict(list)
    for idx, row in df_excel.iterrows():
        key = (row["_norm_empresa"], row["_norm_estado"], row["_norm_cidade"])
        excel_index[key].append(row)

    # Load SQLite
    conn = sqlite3.connect(DB_PATH)
    df_db = pd.read_sql_query(
        "SELECT id, empresa, nome, cidade, estado, data_lancamento FROM empreendimentos",
        conn
    )
    total_db = len(df_db)
    print(f"Total empreendimentos na base: {total_db}")
    print(f"Total linhas no Excel: {len(df_excel)}")

    # Apenas processar os que não têm data
    df_to_match = df_db[
        (df_db["data_lancamento"].isna()) | (df_db["data_lancamento"] == "")
    ].copy()
    print(f"Empreendimentos sem data (a processar): {len(df_to_match)}")
    already_have = total_db - len(df_to_match)
    print(f"Empreendimentos já com data: {already_have}")

    # Build empresa mapping
    db_empresas = df_db["empresa"].unique().tolist()
    excel_empresas = df_excel["incorporadora"].unique().tolist()
    empresa_map, db_norms, excel_norms = build_empresa_mapping(db_empresas, excel_empresas)

    print(f"\nEmpresas na base com match no Excel: {len(empresa_map)}/{len(db_norms)}")
    for db_n, ex_matches in sorted(empresa_map.items()):
        orig_db = db_norms[db_n]
        orig_excels = [excel_norms[m] for m in ex_matches]
        print(f"  {orig_db} -> {orig_excels}")

    # Now match empreendimentos
    matches_confirmed = []
    matches_ambiguous = []
    no_match = []

    for _, row in df_to_match.iterrows():
        emp_id = row["id"]
        empresa = row["empresa"]
        nome = row["nome"]
        cidade = row["cidade"] or ""
        estado = row["estado"] or ""

        norm_emp = normalize_empresa(empresa)
        norm_estado = normalize_text(estado)
        norm_cidade = normalize_cidade(cidade)
        norm_nome = normalize_nome(nome)
        nome_base, fase_db = extract_phase(norm_nome)

        # Find matching Excel empresas
        matched_excel_empresas = empresa_map.get(norm_emp, set())
        if not matched_excel_empresas:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": "empresa_not_found"
            })
            continue

        # Gather candidates from Excel: same empresa + estado + cidade
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

        # Fuzzy match on name
        scored = []
        for cand in candidates:
            cand_nome = cand["_norm_nome"]
            cand_base = cand["_nome_base"]
            cand_fase = cand["_fase"]

            # Compare full names using multiple methods
            score_full = fuzz.ratio(norm_nome, cand_nome)
            score_token = fuzz.token_sort_ratio(norm_nome, cand_nome)
            # Compare bases
            score_base = fuzz.ratio(nome_base, cand_base)
            # Use the better score
            score = max(score_full, score_token, score_base)

            scored.append({
                "score": score,
                "score_full": score_full,
                "score_token": score_token,
                "score_base": score_base,
                "cand_nome_orig": cand["empreendimento"],
                "cand_base": cand_base,
                "cand_fase": cand_fase,
                "data_iso": cand["_data_iso"],
                "cand_incorporadora": cand["incorporadora"],
            })

        # Sort by score descending
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
                # Fases diferentes - sem match
                no_match.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "reason": f"phase_mismatch (db={fase_db}, excel={best['cand_fase']})"
                })
                continue
        elif not has_phase_db and has_phase_excel:
            # Excel tem fase, DB não.
            # Só é ambíguo se existem MÚLTIPLAS fases diferentes no Excel
            # para o mesmo nome base com score alto.
            same_base_candidates = [s for s in scored if s["score_base"] >= SIMILARITY_THRESHOLD and s["cand_fase"] is not None]
            distinct_phases = set(s["cand_fase"] for s in same_base_candidates)
            if len(distinct_phases) > 1:
                # Múltiplas fases (ex: "Produto I" e "Produto II") -> ambíguo
                matches_ambiguous.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "candidates": [(s["cand_nome_orig"], s["cand_fase"], s["data_iso"], s["score"]) for s in same_base_candidates[:5]]
                })
                continue
            # Só uma fase no Excel -> aceitar como match (provavelmente mesmo produto)
        elif has_phase_db and not has_phase_excel:
            # DB tem fase, Excel não - pode ser match se score alto
            pass  # continua normalmente

        # Check for ties with different dates (ambiguous)
        top_score = best["score"]
        ties = [s for s in scored if s["score"] == top_score]
        if len(ties) > 1:
            unique_dates = set(s["data_iso"] for s in ties if s["data_iso"])
            if len(unique_dates) > 1:
                matches_ambiguous.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "cidade": cidade, "estado": estado,
                    "candidates": [(s["cand_nome_orig"], s["cand_fase"], s["data_iso"], s["score"]) for s in ties[:5]]
                })
                continue

        # Confirmed match
        if best["data_iso"]:
            matches_confirmed.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "match_nome": best["cand_nome_orig"],
                "match_incorporadora": best["cand_incorporadora"],
                "score": best["score"],
                "data_iso": best["data_iso"],
                "fase_db": fase_db,
                "fase_excel": best["cand_fase"],
            })
        else:
            no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado,
                "reason": "match_found_but_no_valid_date"
            })

    # Update SQLite
    print(f"\nResultados:")
    print(f"  Matches confirmados: {len(matches_confirmed)}")
    print(f"  Matches ambíguos: {len(matches_ambiguous)}")
    print(f"  Sem match: {len(no_match)}")

    cursor = conn.cursor()
    updated = 0
    for m in matches_confirmed:
        cursor.execute(
            "UPDATE empreendimentos SET data_lancamento = ? WHERE id = ?",
            (m["data_iso"], m["id"])
        )
        updated += cursor.rowcount

    conn.commit()
    print(f"  Registros atualizados no SQLite: {updated}")

    # Generate report
    report = []
    report.append("# Relatório Missão 1 — Atualização de Datas de Lançamento\n")
    report.append(f"**Data de execução:** 2026-04-08\n")
    report.append("## Resumo\n")
    report.append(f"| Métrica | Valor |")
    report.append(f"|---|---|")
    report.append(f"| Total de empreendimentos na base | {total_db} |")
    report.append(f"| Já tinham data_lancamento (preservados) | {already_have} |")
    report.append(f"| Processados (sem data) | {len(df_to_match)} |")
    report.append(f"| Matches confirmados (data atualizada) | {len(matches_confirmed)} |")
    report.append(f"| Matches ambíguos (revisão manual) | {len(matches_ambiguous)} |")
    report.append(f"| Sem match | {len(no_match)} |")
    report.append(f"| **Total com data após atualização** | **{already_have + len(matches_confirmed)}** |")
    report.append("")

    report.append("## Parâmetros utilizados\n")
    report.append(f"- Threshold de similaridade: {SIMILARITY_THRESHOLD}%")
    report.append(f"- Fonte Excel: {len(df_excel)} linhas, {df_excel['incorporadora'].nunique()} incorporadoras")
    report.append(f"- Match por: incorporadora (normalizada) + estado + cidade + nome fuzzy")
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
    report.append("| # | Empresa | Nome (Base) | Nome (Excel) | Score | Data |")
    report.append("|---|---|---|---|---|---|")
    for i, m in enumerate(matches_confirmed[:30], 1):
        report.append(f"| {i} | {m['empresa']} | {m['nome']} | {m['match_nome']} | {m['score']} | {m['data_iso']} |")
    report.append("")

    # Ambiguous matches
    report.append("## Matches Ambíguos (para revisão manual)\n")
    report.append(f"Total: {len(matches_ambiguous)}\n")
    for i, a in enumerate(matches_ambiguous, 1):
        report.append(f"### {i}. {a['empresa']} — {a['nome']} ({a['cidade']}/{a['estado']})")
        report.append("Candidatos no Excel:")
        for cand_nome, cand_fase, cand_data, cand_score in a["candidates"]:
            report.append(f"  - `{cand_nome}` (fase={cand_fase}, data={cand_data}, score={cand_score})")
        report.append("")

    # No match breakdown by reason
    report.append("## Sem Match — Breakdown por Motivo\n")
    reason_counts = defaultdict(int)
    for nm in no_match:
        reason = nm["reason"].split(" ")[0]  # simplify
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

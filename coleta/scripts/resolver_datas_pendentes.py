"""
Missão 1 continuação — Resolver datas de lançamento pendentes.

Ponto 1: Resolver 73 ambíguos (fase no nome → match direto, senão → data mais antiga)
Ponto 3: Adicionar mapeamentos faltantes (Metrocasa, VIC/Vic, etc)
Ponto 4: Analisar match por cidade+uf+nome ignorando incorporadora

Roda APÓS o script atualizar_datas_lancamento.py original.
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
REPORT_PATH = r"C:\Users\aabiusi\ProjetosAI\coleta\build_logs\relatorio_missao1_continuacao.md"

SIMILARITY_THRESHOLD = 85

REMOVE_SUFFIXES = [
    "engenharia", "construtora", "incorporadora", "construcoes",
    "empreendimentos", "incorporacoes", "construcoes", "incorporacoes",
    "desenvolvimento", "imobiliario", "imobiliaria",
    "participacoes", "participacoes", "residencial",
]

ROMAN_MAP = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
             "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}

# Padrões de fase no nome (ampliado para pegar F1, F2, 1ª Fase, etc.)
PHASE_PATTERNS = [
    re.compile(r'\s*[-–—]?\s*(?:fase|etapa)\s*(\d+)', re.IGNORECASE),
    re.compile(r'\s*[-–—]?\s*F(\d+)\b', re.IGNORECASE),
    re.compile(r'\s*[-–—]?\s*(\d+)[ªº]?\s*fase', re.IGNORECASE),
    re.compile(r'\s+([IVX]{1,4})$'),  # romanos no final
    re.compile(r'\s+(\d+)$'),  # número solto no final (cuidado: contexto)
]


def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.strip().lower()


def normalize_empresa(name):
    n = normalize_text(name)
    n = re.sub(r'[^\w\s&]', ' ', n)
    for suf in REMOVE_SUFFIXES:
        n = re.sub(r'\b' + suf + r'\b', '', n)
    n = n.replace("&", "e")
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def normalize_nome(name):
    n = normalize_text(name)
    n = re.sub(r'[^\w\s]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def extract_phase_number(name):
    """Tenta extrair um número de fase do nome. Retorna (nome_base, fase_int_ou_None)."""
    norm = name.strip()
    for pat in PHASE_PATTERNS[:-1]:  # Todas menos número solto no final
        m = pat.search(norm)
        if m:
            raw = m.group(1).strip().upper()
            if raw in ROMAN_MAP:
                return norm[:m.start()].strip(), ROMAN_MAP[raw]
            elif raw.isdigit():
                return norm[:m.start()].strip(), int(raw)
    # Romanos no final
    m = PHASE_PATTERNS[-2].search(norm)  # [IVX]
    if m:
        raw = m.group(1).upper()
        if raw in ROMAN_MAP:
            base = norm[:m.start()].strip()
            if len(base) >= 3:
                return base, ROMAN_MAP[raw]
    return norm, None


def convert_date(date_val):
    import datetime
    if isinstance(date_val, datetime.datetime):
        return f"{date_val.year}-{date_val.month:02d}"
    if isinstance(date_val, str):
        m = re.match(r'^(\d{2})/(\d{4})$', date_val.strip())
        if m:
            return f"{m.group(2)}-{m.group(1)}"
    return None


# ===== Mapeamento de empresas AMPLIADO =====
EMPRESA_MAP_MANUAL = {
    # Adicionais que faltaram no script original
    "metrocasa": ["metrocasa"],
    "vic": ["vic"],
    "viva benx": ["benx", "viva benx"],
    "canopus construcoes": ["canopus"],
    "hm": ["hm"],
    "pacaembu": ["pacaembu"],
    "econ": ["econ"],
    "sol": ["sol"],
    "sr": ["sr"],
    "smart": ["smart"],
    "grafico": ["grafico"],
    "trisul": ["trisul"],
    "cac": ["cac"],
    "tenda": ["tenda"],
    "mrv": ["mrv"],
    "cury": ["cury"],
    "plano e plano": ["plano e plano", "plano plano"],
    "vivaz": ["vivaz"],
    "direcional": ["direcional"],
    "magik jc": ["magik jc"],
    "vitta": ["vitta"],
    "ebm": ["ebm"],
    "arbore": ["arbore"],
    "sugoi": ["sugoi"],
    "vinx": ["vinx"],
    "eph": ["eph"],
    "exata": ["exata"],
    "belmais": ["bel mais", "belmais"],
    "cavazani": ["cavazani"],
    "novolar": ["novolar"],
    "kazzas": ["kazzas"],
    "conx": ["conx"],
    "vibra": ["vibra"],
    "sousa araujo": ["souza araujo", "sousa araujo"],
    "mundo apto": ["mundo apto"],
    "vega": ["vega"],
    "mgf": ["mgf"],
    "graal": ["graal"],
    "ap ponto": ["ap ponto"],
    "aclf": ["aclf"],
    "acl": ["acl"],
    "bm7": ["bm7"],
    "bp8": ["bp8"],
    "fyp": ["fyp"],
    "emccamp": ["emccamp"],
    "stanza": ["stanza"],
    "vila brasil": ["vila brasil", "villa brasil"],
    "rev3": ["rev3"],
    "construtora open": ["open"],
    "house inc": ["house inc"],
    "lotus": ["lotus"],
    "morana": ["morana"],
    "vasco": ["vasco"],
    "carrilho": ["carrilho"],
    "jotanunes": ["jotanunes"],
    "domma": ["domma"],
    "dimensional": ["dimensional"],
    "mirantes": ["mirantes", "mirante"],
    "bora": ["bora"],
    "novvo": ["novvo"],
    "voce": ["voce"],
    "m lar": ["m lar", "m. lar"],
    "maccris": ["maccris"],
    "art": ["art"],
    "somos": ["somos"],
    "cosbat": ["cosbat"],
    "riformato": ["riformato"],
    "quartzo": ["quartzo"],
    "ampla": ["ampla"],
    "tenorio simoes": ["tenorio simoes"],
    "torreao villarim": ["torreao villarim"],
    "victa": ["victa"],
    "grupo delta": ["grupo delta"],
    "sertenge": ["sertenge"],
    "versati": ["versati"],
    "un1ca": ["un1ca", "unica"],
}


def main():
    print("=" * 60)
    print("MISSÃO 1 CONTINUAÇÃO — Resolver datas pendentes")
    print("=" * 60)

    # Carregar Excel
    df_excel = pd.read_excel(EXCEL_PATH)
    df_excel.columns = ["incorporadora", "empreendimento", "cidade", "estado", "data_lancamento"]
    df_excel["incorporadora"] = df_excel["incorporadora"].astype(str).str.strip()
    df_excel["empreendimento"] = df_excel["empreendimento"].astype(str).str.strip()
    df_excel["cidade"] = df_excel["cidade"].astype(str).fillna("").str.strip()
    df_excel["estado"] = df_excel["estado"].astype(str).fillna("").str.strip()
    df_excel["_data_iso"] = df_excel["data_lancamento"].apply(convert_date)
    df_excel["_norm_empresa"] = df_excel["incorporadora"].apply(normalize_empresa)
    df_excel["_norm_cidade"] = df_excel["cidade"].apply(normalize_text)
    df_excel["_norm_estado"] = df_excel["estado"].apply(normalize_text)
    df_excel["_norm_nome"] = df_excel["empreendimento"].apply(normalize_nome)

    # Extrair fase do Excel
    phase_data = df_excel["empreendimento"].apply(lambda x: extract_phase_number(normalize_nome(x)))
    df_excel["_nome_base"] = phase_data.apply(lambda x: x[0])
    df_excel["_fase"] = phase_data.apply(lambda x: x[1])

    # Indexar por (empresa, estado, cidade)
    excel_idx = defaultdict(list)
    for _, row in df_excel.iterrows():
        for emp_norm in [row["_norm_empresa"]]:
            key = (emp_norm, row["_norm_estado"], row["_norm_cidade"])
            excel_idx[key].append(row)

    # Carregar DB
    conn = sqlite3.connect(DB_PATH)
    df_db = pd.read_sql_query(
        "SELECT id, empresa, nome, cidade, estado, data_lancamento FROM empreendimentos",
        conn
    )

    sem_data = df_db[(df_db["data_lancamento"].isna()) | (df_db["data_lancamento"] == "")].copy()
    print(f"Total na base: {len(df_db)}")
    print(f"Sem data_lancamento: {len(sem_data)}")

    # ===== PONTO 1 + PONTO 3: Resolver ambíguos + adicionar empresas =====
    print("\n--- PONTO 1+3: Matching ampliado ---")

    resolved = []
    ambiguous_resolved = []
    still_no_match = []
    updates = []

    for _, row in sem_data.iterrows():
        emp_id = row["id"]
        empresa = row["empresa"]
        nome = row["nome"] or ""
        cidade = row["cidade"] or ""
        estado = row["estado"] or ""

        norm_emp = normalize_empresa(empresa)
        norm_estado = normalize_text(estado)
        norm_cidade = normalize_text(cidade)
        norm_nome = normalize_nome(nome)
        db_base, db_fase = extract_phase_number(norm_nome)

        # Encontrar empresas no Excel via mapa manual
        excel_emp_norms = set()
        if norm_emp in EMPRESA_MAP_MANUAL:
            excel_emp_norms = set(EMPRESA_MAP_MANUAL[norm_emp])
        else:
            # Fuzzy fallback
            for em_norm in df_excel["_norm_empresa"].unique():
                if not em_norm or not norm_emp:
                    continue
                if norm_emp == em_norm:
                    excel_emp_norms.add(em_norm)
                elif len(norm_emp) > 5 and fuzz.ratio(norm_emp, em_norm) >= 85:
                    excel_emp_norms.add(em_norm)

        if not excel_emp_norms:
            still_no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado, "reason": "empresa_not_found"
            })
            continue

        # Candidatos por empresa+estado+cidade
        candidates = []
        for en in excel_emp_norms:
            key = (en, norm_estado, norm_cidade)
            candidates.extend(excel_idx.get(key, []))

        if not candidates:
            still_no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado, "reason": "cidade_not_found"
            })
            continue

        # Fuzzy match por nome
        scored = []
        for cand in candidates:
            cand_nome = cand["_norm_nome"]
            cand_base = cand["_nome_base"]
            cand_fase = cand["_fase"]
            cand_data = cand["_data_iso"]

            if not cand_data:
                continue

            score = max(fuzz.ratio(norm_nome, cand_nome),
                        fuzz.token_sort_ratio(norm_nome, cand_nome))

            # Também comparar nome base (sem fase)
            score_base = max(fuzz.ratio(db_base, cand_base),
                             fuzz.token_sort_ratio(db_base, cand_base))
            score = max(score, score_base)

            if score >= SIMILARITY_THRESHOLD:
                scored.append({
                    "nome_excel": cand["empreendimento"],
                    "nome_norm": cand_nome,
                    "base": cand_base,
                    "fase": cand_fase,
                    "data": cand_data,
                    "score": score,
                })

        if not scored:
            still_no_match.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "cidade": cidade, "estado": estado, "reason": "nome_no_match"
            })
            continue

        # Agrupar por fases
        fases_no_excel = set(s["fase"] for s in scored if s["fase"] is not None)

        if len(scored) == 1:
            # Match único → usar direto
            best = scored[0]
            updates.append((emp_id, best["data"]))
            resolved.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "matched": best["nome_excel"], "score": best["score"],
                "data": best["data"], "method": "unique_match"
            })
        elif len(fases_no_excel) <= 1 and len(scored) <= 2:
            # Todos da mesma fase ou sem fase → usar data mais antiga
            oldest = min(scored, key=lambda x: x["data"])
            updates.append((emp_id, oldest["data"]))
            resolved.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "matched": oldest["nome_excel"], "score": oldest["score"],
                "data": oldest["data"], "method": "oldest_same_phase"
            })
        elif db_fase is not None and db_fase in fases_no_excel:
            # Nossa base tem número de fase → match direto com a fase certa
            fase_matches = [s for s in scored if s["fase"] == db_fase]
            if fase_matches:
                best = max(fase_matches, key=lambda x: x["score"])
                updates.append((emp_id, best["data"]))
                ambiguous_resolved.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "matched": best["nome_excel"], "score": best["score"],
                    "data": best["data"], "db_fase": db_fase,
                    "method": "phase_match_from_name"
                })
            else:
                # Não encontrou a fase exata → usar mais antiga
                oldest = min(scored, key=lambda x: x["data"])
                updates.append((emp_id, oldest["data"]))
                ambiguous_resolved.append({
                    "id": emp_id, "empresa": empresa, "nome": nome,
                    "matched": oldest["nome_excel"], "score": oldest["score"],
                    "data": oldest["data"], "db_fase": db_fase,
                    "method": "oldest_fallback"
                })
        else:
            # Múltiplas fases no Excel, nossa base não indica fase → usar data mais antiga (Fase 1)
            oldest = min(scored, key=lambda x: x["data"])
            updates.append((emp_id, oldest["data"]))
            ambiguous_resolved.append({
                "id": emp_id, "empresa": empresa, "nome": nome,
                "matched": oldest["nome_excel"], "score": oldest["score"],
                "data": oldest["data"], "db_fase": db_fase,
                "fases_excel": sorted(fases_no_excel),
                "method": "oldest_multi_phase"
            })

    # Aplicar updates
    print(f"\nAplicando {len(updates)} atualizações...")
    cur = conn.cursor()
    for emp_id, data in updates:
        cur.execute("UPDATE empreendimentos SET data_lancamento = ? WHERE id = ?", (data, emp_id))
    conn.commit()

    # Stats finais
    cur.execute("SELECT COUNT(*) FROM empreendimentos WHERE data_lancamento IS NOT NULL AND data_lancamento != ''")
    total_com_data = cur.fetchone()[0]
    total = len(df_db)

    print(f"\n{'=' * 60}")
    print(f"RESULTADOS PONTO 1+3")
    print(f"{'=' * 60}")
    print(f"Matches novos (ponto 1+3): {len(updates)}")
    print(f"  - Resolved (match direto/unico): {len(resolved)}")
    print(f"  - Ambíguos resolvidos (fase ou oldest): {len(ambiguous_resolved)}")
    print(f"Ainda sem match: {len(still_no_match)}")
    print(f"Total com data agora: {total_com_data}/{total} ({100*total_com_data/total:.1f}%)")

    # ===== PONTO 4: Análise de match por cidade+uf+nome (SEM incorporadora) =====
    print(f"\n{'=' * 60}")
    print("PONTO 4: Análise de match por cidade+UF+nome (sem incorporadora)")
    print(f"{'=' * 60}")

    # Recarregar sem_data atualizado
    df_db2 = pd.read_sql_query(
        "SELECT id, empresa, nome, cidade, estado, data_lancamento FROM empreendimentos",
        conn
    )
    sem_data2 = df_db2[(df_db2["data_lancamento"].isna()) | (df_db2["data_lancamento"] == "")].copy()
    print(f"Empreendimentos ainda sem data: {len(sem_data2)}")

    # Indexar Excel por (estado, cidade)
    excel_by_geo = defaultdict(list)
    for _, row in df_excel.iterrows():
        key = (row["_norm_estado"], row["_norm_cidade"])
        excel_by_geo[key].append(row)

    potential_p4 = []
    for _, row in sem_data2.iterrows():
        norm_estado = normalize_text(row["estado"] or "")
        norm_cidade = normalize_text(row["cidade"] or "")
        norm_nome = normalize_nome(row["nome"] or "")

        candidates = excel_by_geo.get((norm_estado, norm_cidade), [])
        if not candidates:
            continue

        for cand in candidates:
            score = max(fuzz.ratio(norm_nome, cand["_norm_nome"]),
                        fuzz.token_sort_ratio(norm_nome, cand["_norm_nome"]))
            if score >= 90:  # Threshold alto sem incorporadora
                potential_p4.append({
                    "id": row["id"],
                    "empresa_db": row["empresa"],
                    "nome_db": row["nome"],
                    "cidade": row["cidade"],
                    "estado": row["estado"],
                    "empresa_excel": cand["incorporadora"],
                    "nome_excel": cand["empreendimento"],
                    "data": cand["_data_iso"],
                    "score": score,
                })
                break  # Pegar só o melhor match

    print(f"Potenciais matches por cidade+UF+nome (score >= 90): {len(potential_p4)}")

    # Agrupar por razão da divergência
    empresa_mismatch = [p for p in potential_p4 if normalize_empresa(p["empresa_db"]) != normalize_empresa(p["empresa_excel"])]
    print(f"  - Empresa diferente: {len(empresa_mismatch)}")

    conn.close()

    # ===== GERAR RELATÓRIO =====
    print(f"\nGerando relatório em {REPORT_PATH}...")

    lines = [
        "# Relatório Missão 1 Continuação — Datas de Lançamento",
        "",
        f"**Data de execução:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "## Resumo",
        "",
        "| Métrica | Valor |",
        "|---|---|",
        f"| Total na base | {total} |",
        f"| Total com data (ANTES desta rodada) | {total_com_data - len(updates)} |",
        f"| Matches novos nesta rodada | {len(updates)} |",
        f"|   - Match direto/único | {len(resolved)} |",
        f"|   - Ambíguos resolvidos (fase ou oldest) | {len(ambiguous_resolved)} |",
        f"| Ainda sem data | {len(sem_data2)} |",
        f"| **Total com data AGORA** | **{total_com_data} ({100*total_com_data/total:.1f}%)** |",
        "",
        "## Ponto 1+3: Matches novos",
        "",
        "### Matches diretos/únicos (amostra, primeiros 30)",
        "",
        "| Empresa | Nome (Base) | Nome (Excel) | Score | Data | Método |",
        "|---|---|---|---|---|---|",
    ]
    for r in resolved[:30]:
        lines.append(f"| {r['empresa']} | {r['nome']} | {r['matched']} | {r['score']} | {r['data']} | {r['method']} |")

    lines += [
        "",
        "### Ambíguos resolvidos (todos)",
        "",
        "| Empresa | Nome (Base) | Nome (Excel) | Fase DB | Data | Método |",
        "|---|---|---|---|---|---|",
    ]
    for r in ambiguous_resolved:
        lines.append(f"| {r['empresa']} | {r['nome']} | {r['matched']} | {r.get('db_fase', '-')} | {r['data']} | {r['method']} |")

    lines += [
        "",
        "### Sem match — Breakdown por razão",
        "",
    ]
    reasons = defaultdict(int)
    for nm in still_no_match:
        reasons[nm["reason"]] += 1
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        lines.append(f"- **{reason}**: {count}")

    lines += [
        "",
        "### Sem match — Top empresas",
        "",
    ]
    emp_no_match = defaultdict(int)
    for nm in still_no_match:
        emp_no_match[nm["empresa"]] += 1
    for emp, count in sorted(emp_no_match.items(), key=lambda x: -x[1])[:20]:
        lines.append(f"- {emp}: {count}")

    lines += [
        "",
        "## Ponto 4: Match por cidade+UF+nome (análise sem aplicar)",
        "",
        f"**Potenciais matches encontrados:** {len(potential_p4)} (score >= 90, empresa ignorada)",
        f"**Empresa diferente:** {len(empresa_mismatch)}",
        "",
    ]
    if potential_p4:
        lines += [
            "### Amostra (primeiros 30)",
            "",
            "| Empresa (DB) | Nome (DB) | Empresa (Excel) | Nome (Excel) | Cidade/UF | Score | Data |",
            "|---|---|---|---|---|---|---|",
        ]
        for p in potential_p4[:30]:
            lines.append(f"| {p['empresa_db']} | {p['nome_db']} | {p['empresa_excel']} | {p['nome_excel']} | {p['cidade']}/{p['estado']} | {p['score']} | {p['data']} |")

    lines += [
        "",
        "---",
        "",
        f"*Relatório gerado automaticamente em {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}*",
    ]

    Path(REPORT_PATH).write_text("\n".join(lines), encoding="utf-8")
    print(f"Relatório salvo: {REPORT_PATH}")
    print("DONE")


if __name__ == "__main__":
    main()

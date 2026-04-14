"""
Auditoria de qualidade da base de empreendimentos.
Detecta anomalias em cidades, fases, tipologias, nomes, enderecos, metragens e precos.
Classifica cada anomalia como empresa ORIGINAL (77 validadas) ou NOVA (expansao).
Output: build_logs/auditoria_qualidade.md

Uso:
    python scripts/validar_qualidade.py
    python scripts/validar_qualidade.py --saida build_logs/auditoria_qualidade_antes.md
"""
import sqlite3
import sys
import io
import os
import argparse
import unicodedata
from collections import defaultdict
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")
BUILD_LOGS = os.path.join(PROJECT_ROOT, "build_logs")

# ============================================================
# 77 EMPRESAS ORIGINAIS — dados ja validados
# ============================================================
EMPRESAS_ORIGINAIS = {
    "Tenda", "MRV", "Cury", "VIC Engenharia", "Plano&Plano",
    "Vitta Residencial", "Magik JC", "Direcional", "Metrocasa", "Vivaz",
    "EBM", "Trisul", "HM Engenharia", "Grafico", "Pacaembu",
    "Econ Construtora", "Vibra Residencial", "CAC", "Kazzas", "Conx",
    "MGF", "Vega", "Canopus Construcoes", "Novolar", "Graal Engenharia",
    "Árbore", "Viva Benx", "Quartzo", "Vasco Construtora", "Mundo Apto",
    "Vinx", "SR Engenharia", "Canopus", "Riformato", "AP Ponto",
    "SUGOI", "ACLF", "Exata", "Somos", "Emccamp",
    "BM7", "FYP Engenharia", "Stanza", "Sousa Araujo", "Vila Brasil",
    "Smart Construtora", "EPH", "Cosbat", "Belmais", "Jotanunes",
    "Cavazani", "Victa Engenharia", "SOL Construtora", "Morana", "Lotus",
    "House Inc", "ART Construtora", "Rev3", "Carrilho", "BP8",
    "ACL Incorporadora", "Maccris", "Ampla", "Tenório Simões",
    "Grupo Delta", "Domma", "Dimensional", "Mirantes", "Torreão Villarim",
    "Bora", "Novvo", "Você", "M.Lar", "Construtora Open",
    "Ún1ca", "Versati", "Sertenge",
}

# ============================================================
# LISTAS DE REFERENCIA
# ============================================================

PALAVRAS_JURIDICAS = [
    "cartório", "cartorio", "ofício", "oficio", "registro",
    "comarca", "imóveis", "imoveis", "tabelião", "tabeliao",
    "serviço", "servico", "notarial",
]

PALAVRAS_SITE = [
    "simulação", "simulacao", "facebook", "instagram", "consultor",
    "http", "www.", ".com", "whatsapp", "atendimento",
    "cookie", "privacidade", "lgpd", "newsletter",
]

BAIRROS_SP = [
    "Butantã", "Mooca", "Tatuapé", "Vila Prudente", "Penha",
    "Pinheiros", "Itaim Bibi", "Moema", "Lapa", "Perdizes",
    "Santana", "Tucuruvi", "Mandaqui", "Jaçanã",
    "Casa Verde", "Limão", "Vila Maria", "Vila Guilherme",
    "Belém", "Brás", "Cambuci", "Liberdade", "Ipiranga",
    "Sacomã", "Jabaquara", "Campo Belo", "Santo Amaro",
    "Campo Limpo", "Capão Redondo", "Jardim São Luís",
    "Vila Andrade", "Morumbi", "Brooklin", "Vila Olímpia",
    "Consolação", "Bela Vista", "Higienópolis", "República",
    "Santa Cecília", "Bom Retiro", "Vila Mariana",
    "Saúde", "Cursino", "Vila Matilde", "Aricanduva",
    "São Mateus", "Ermelino Matarazzo", "São Miguel Paulista",
    "Itaquera", "Guaianases", "Cidade Tiradentes",
    "Pirituba", "Freguesia do Ó", "Brasilândia",
    "Perus", "Jaraguá", "Vila Sônia", "Raposo Tavares",
]

BAIRROS_RJ = [
    "Copacabana", "Ipanema", "Leblon", "Botafogo", "Flamengo",
    "Laranjeiras", "Catete", "Glória", "Leme", "Urca",
    "Tijuca", "Grajaú", "Vila Isabel", "Maracanã",
    "São Cristóvão", "Caju", "Centro", "Lapa",
    "Barra da Tijuca", "Recreio", "Jacarepaguá",
    "Taquara", "Pechincha", "Tanque", "Freguesia",
    "Méier", "Engenho Novo", "Cachambi", "Todos os Santos",
    "Del Castilho", "Benfica", "Bonsucesso",
    "Olaria", "Ramos", "Penha", "Ilha do Governador",
    "Campo Grande", "Santa Cruz", "Bangu", "Realengo",
]

FASES_CANONICAS = [
    "Breve Lançamento", "Lançamento", "Em Construção",
    "Pronto para Morar", "100% Vendido",
]

FASES_ACEITAVEIS = FASES_CANONICAS + ["Aluguel", "Lotes"]


def normalizar(texto):
    """Remove acentos e converte para minusculas."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def grupo_empresa(empresa):
    """Retorna 'ORIGINAL' ou 'NOVA'."""
    return "ORIGINAL" if empresa in EMPRESAS_ORIGINAIS else "NOVA"


def carregar_dados():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM empreendimentos")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _anomalia(tipo, empresa, nome, id_, detalhe):
    return {
        "tipo": tipo,
        "empresa": empresa,
        "nome": nome or "",
        "id": id_,
        "detalhe": detalhe,
        "grupo": grupo_empresa(empresa),
    }


def auditar_cidades(rows):
    anomalias = []
    bairros_norm = set()
    for b in BAIRROS_SP + BAIRROS_RJ:
        bairros_norm.add(normalizar(b))

    for r in rows:
        cidade = r["cidade"]
        emp = r["empresa"]
        nome = r["nome"] or ""
        rid = r["id"]

        if not cidade or not cidade.strip():
            anomalias.append(_anomalia("cidade_null", emp, nome, rid, "Cidade NULL ou vazia"))
            continue

        cidade_strip = cidade.strip()

        if len(cidade_strip) < 3:
            anomalias.append(_anomalia("cidade_curta", emp, nome, rid,
                                       f"Cidade muito curta: '{cidade_strip}' ({len(cidade_strip)} chars)"))

        if len(cidade_strip) > 40:
            anomalias.append(_anomalia("cidade_longa", emp, nome, rid,
                                       f"Cidade muito longa: '{cidade_strip[:60]}...' ({len(cidade_strip)} chars)"))

        cidade_lower = cidade_strip.lower()
        for pj in PALAVRAS_JURIDICAS:
            if pj in cidade_lower:
                anomalias.append(_anomalia("cidade_juridica", emp, nome, rid,
                                           f"Cidade contém termo jurídico: '{cidade_strip}' ('{pj}')"))
                break

        for ps in PALAVRAS_SITE:
            if ps in cidade_lower:
                anomalias.append(_anomalia("cidade_lixo_site", emp, nome, rid,
                                           f"Cidade contém texto de site: '{cidade_strip}' ('{ps}')"))
                break

        cn = normalizar(cidade_strip)
        if cn in bairros_norm:
            anomalias.append(_anomalia("cidade_e_bairro", emp, nome, rid,
                                       f"Cidade é bairro conhecido: '{cidade_strip}'"))

        if "\n" in cidade_strip or "\r" in cidade_strip:
            anomalias.append(_anomalia("cidade_multilinhas", emp, nome, rid,
                                       f"Cidade contém quebra de linha: '{cidade_strip[:60]}'"))

    # Duplicatas por acentuacao
    cidades_norm = defaultdict(set)
    for r in rows:
        if r["cidade"] and r["cidade"].strip():
            cn = normalizar(r["cidade"].strip())
            cidades_norm[cn].add(r["cidade"].strip())

    for cn, variantes in cidades_norm.items():
        if len(variantes) > 1:
            anomalias.append(_anomalia("cidade_duplicata_acento", "(global)", "", 0,
                                       f"Variantes: {', '.join(sorted(variantes))}"))

    return anomalias


def auditar_fases(rows):
    anomalias = []
    por_empresa = defaultdict(list)
    for r in rows:
        por_empresa[r["empresa"]].append(r)

    for emp, emps in por_empresa.items():
        total = len(emps)
        if total < 5:
            continue

        fases = defaultdict(int)
        for r in emps:
            f = r["fase"] or "NULL"
            fases[f] += 1

        for fase, cnt in fases.items():
            if fase != "NULL" and cnt / total > 0.9:
                anomalias.append(_anomalia("fase_contaminada", emp, "", 0,
                                           f"{cnt}/{total} ({100*cnt/total:.0f}%) com fase '{fase}'"))

    for r in rows:
        fase = r["fase"]
        if fase and fase not in FASES_ACEITAVEIS:
            anomalias.append(_anomalia("fase_nao_canonica", r["empresa"], r["nome"] or "", r["id"],
                                       f"Fase não canônica: '{fase}'"))

    return anomalias


def auditar_tipologias(rows):
    anomalias = []
    por_empresa = defaultdict(list)
    for r in rows:
        por_empresa[r["empresa"]].append(r)

    for emp, emps in por_empresa.items():
        total = len(emps)
        if total < 5:
            continue

        studios = sum(1 for r in emps if r.get("apto_studio"))
        if studios / total > 0.9:
            anomalias.append(_anomalia("tipologia_studio_contaminado", emp, "", 0,
                                       f"{studios}/{total} ({100*studios/total:.0f}%) com apto_studio=1"))

        flags_cols = [
            "apto_1_dorm", "apto_2_dorms", "apto_3_dorms", "apto_4_dorms",
            "apto_suite", "apto_terraco", "apto_giardino", "apto_duplex",
            "apto_cobertura",
        ]
        patterns = set()
        for r in emps:
            p = tuple(r.get(c, 0) for c in flags_cols)
            patterns.add(p)

        if len(patterns) == 1:
            pat = list(patterns)[0]
            pat_str = "".join(str(v or 0) for v in pat)
            anomalias.append(_anomalia("tipologia_flags_identicos", emp, "", 0,
                                       f"Todos os {total} produtos com flags idênticos: {pat_str}"))

    return anomalias


def auditar_nomes(rows):
    anomalias = []
    termos_sujos = [
        "apartamentos à venda", "apartamentos a venda",
        "construtora", "incorporadora", "empreendimentos",
        "imobiliária", "imobiliaria",
    ]

    for r in rows:
        nome = r["nome"]
        emp = r["empresa"]
        rid = r["id"]

        if not nome or not nome.strip():
            anomalias.append(_anomalia("nome_null", emp, "(null)", rid, "Nome NULL ou vazio"))
            continue

        nome_strip = nome.strip()

        if len(nome_strip) < 3:
            anomalias.append(_anomalia("nome_curto", emp, nome_strip, rid,
                                       f"Nome muito curto: '{nome_strip}' ({len(nome_strip)} chars)"))

        if len(nome_strip) > 80:
            anomalias.append(_anomalia("nome_longo", emp, nome_strip[:40] + "...", rid,
                                       f"Nome muito longo: '{nome_strip[:80]}...' ({len(nome_strip)} chars)"))

        nome_lower = nome_strip.lower()
        for ts in termos_sujos:
            if ts in nome_lower:
                anomalias.append(_anomalia("nome_sujo", emp, nome_strip[:60], rid,
                                           f"Nome contém '{ts}': '{nome_strip[:80]}'"))
                break

        if normalizar(nome_strip) == normalizar(emp):
            anomalias.append(_anomalia("nome_e_empresa", emp, nome_strip, rid,
                                       "Nome igual ao nome da empresa"))

    return anomalias


def auditar_enderecos(rows):
    anomalias = []

    for r in rows:
        end = r["endereco"]
        if not end:
            continue

        end_strip = end.strip()
        end_lower = end_strip.lower()
        emp = r["empresa"]
        nome = r["nome"] or ""
        rid = r["id"]

        for pj in PALAVRAS_JURIDICAS:
            if pj in end_lower:
                anomalias.append(_anomalia("endereco_juridico", emp, nome, rid,
                                           f"Endereço contém '{pj}': '{end_strip[:80]}'"))
                break

        if len(end_strip) > 200:
            anomalias.append(_anomalia("endereco_longo", emp, nome, rid,
                                       f"Endereço com {len(end_strip)} chars: '{end_strip[:100]}...'"))

        for ps in PALAVRAS_SITE:
            if ps in end_lower:
                anomalias.append(_anomalia("endereco_lixo_site", emp, nome, rid,
                                           f"Endereço contém '{ps}': '{end_strip[:80]}'"))
                break

    return anomalias


def auditar_metragens(rows):
    """Detecta metragens improvaveis: <10m2 ou >500m2."""
    anomalias = []

    for r in rows:
        emp = r["empresa"]
        nome = r["nome"] or ""
        rid = r["id"]

        for campo, label in [("area_min_m2", "area_min"), ("area_max_m2", "area_max")]:
            val = r.get(campo)
            if val is None:
                continue
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue

            if val < 10:
                anomalias.append(_anomalia("metragem_baixa", emp, nome, rid,
                                           f"{label}={val}m2 (< 10m2)"))
            elif val > 500:
                anomalias.append(_anomalia("metragem_alta", emp, nome, rid,
                                           f"{label}={val}m2 (> 500m2)"))

    return anomalias


def auditar_precos(rows):
    """Detecta precos improvaveis: <50k ou >20M."""
    anomalias = []

    for r in rows:
        preco = r.get("preco_a_partir")
        if preco is None:
            continue
        try:
            preco = float(preco)
        except (ValueError, TypeError):
            continue

        emp = r["empresa"]
        nome = r["nome"] or ""
        rid = r["id"]

        if preco < 50_000:
            anomalias.append(_anomalia("preco_baixo", emp, nome, rid,
                                       f"preco_a_partir=R${preco:,.0f} (< R$50k)"))
        elif preco > 20_000_000:
            anomalias.append(_anomalia("preco_alto", emp, nome, rid,
                                       f"preco_a_partir=R${preco:,.0f} (> R$20M)"))

    return anomalias


def gerar_relatorio(todas_anomalias, saida, total_registros, rows):
    os.makedirs(os.path.dirname(saida), exist_ok=True)

    # Contar originais vs novas
    total_originais = sum(1 for r in rows if r["empresa"] in EMPRESAS_ORIGINAIS)
    total_novas = total_registros - total_originais
    empresas_originais_set = set(r["empresa"] for r in rows if r["empresa"] in EMPRESAS_ORIGINAIS)
    empresas_novas_set = set(r["empresa"] for r in rows if r["empresa"] not in EMPRESAS_ORIGINAIS)

    por_tipo = defaultdict(list)
    for a in todas_anomalias:
        por_tipo[a["tipo"]].append(a)

    categorias = {
        "CIDADES": [
            "cidade_null", "cidade_curta", "cidade_longa",
            "cidade_juridica", "cidade_lixo_site", "cidade_e_bairro",
            "cidade_multilinhas", "cidade_duplicata_acento",
        ],
        "FASES": ["fase_contaminada", "fase_nao_canonica"],
        "TIPOLOGIAS": ["tipologia_studio_contaminado", "tipologia_flags_identicos"],
        "NOMES": ["nome_null", "nome_curto", "nome_longo", "nome_sujo", "nome_e_empresa"],
        "ENDERECOS": ["endereco_juridico", "endereco_longo", "endereco_lixo_site"],
        "METRAGENS": ["metragem_baixa", "metragem_alta"],
        "PRECOS": ["preco_baixo", "preco_alto"],
    }

    # Contagem por grupo
    anomalias_orig = [a for a in todas_anomalias if a["grupo"] == "ORIGINAL"]
    anomalias_novas = [a for a in todas_anomalias if a["grupo"] == "NOVA"]

    with open(saida, "w", encoding="utf-8") as f:
        f.write(f"# Auditoria de Qualidade\n\n")
        f.write(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"Total de registros: {total_registros}\n")
        f.write(f"- ORIGINAIS (77 empresas): {total_originais} registros\n")
        f.write(f"- NOVAS ({len(empresas_novas_set)} empresas): {total_novas} registros\n\n")
        f.write(f"Total de anomalias: {len(todas_anomalias)}\n")
        f.write(f"- Anomalias ORIGINAIS: {len(anomalias_orig)} (somente reportar, NAO alterar)\n")
        f.write(f"- Anomalias NOVAS: {len(anomalias_novas)} (candidatas a correcao)\n\n")

        # Resumo por categoria e grupo
        f.write("## Resumo\n\n")
        f.write("| Categoria | Tipo | ORIGINAL | NOVA | Total |\n")
        f.write("|-----------|------|----------|------|-------|\n")
        for cat, tipos in categorias.items():
            for t in tipos:
                if t in por_tipo:
                    lista = por_tipo[t]
                    cnt_o = sum(1 for a in lista if a["grupo"] == "ORIGINAL")
                    cnt_n = sum(1 for a in lista if a["grupo"] == "NOVA")
                    f.write(f"| {cat} | {t} | {cnt_o} | {cnt_n} | {len(lista)} |\n")
        f.write("\n")

        # Detalhes
        for cat, tipos in categorias.items():
            tem_anomalia = any(t in por_tipo for t in tipos)
            if not tem_anomalia:
                continue

            f.write(f"## {cat}\n\n")
            for t in tipos:
                if t not in por_tipo:
                    continue
                lista = por_tipo[t]
                f.write(f"### {t} ({len(lista)})\n\n")

                if lista[0].get("id") == 0:
                    for a in lista:
                        f.write(f"- [{a['grupo']}] **{a['empresa']}**: {a['detalhe']}\n")
                else:
                    for a in lista[:50]:
                        f.write(f"- [{a['grupo']}] [id={a['id']}] **{a['empresa']}** — {a['nome']}: {a['detalhe']}\n")
                    if len(lista) > 50:
                        f.write(f"\n... e mais {len(lista) - 50} registros\n")

                f.write("\n")

        # Secao separada: anomalias de empresas originais (para revisao manual)
        f.write("## EMPRESAS ORIGINAIS — Anomalias para revisao manual\n\n")
        f.write("**ATENCAO: Nao alterar estes dados. Reportar ao operador.**\n\n")
        orig_por_empresa = defaultdict(list)
        for a in anomalias_orig:
            orig_por_empresa[a["empresa"]].append(a)

        if not orig_por_empresa:
            f.write("Nenhuma anomalia em empresas originais.\n\n")
        else:
            for emp in sorted(orig_por_empresa.keys()):
                lista = orig_por_empresa[emp]
                f.write(f"### {emp} ({len(lista)} anomalias)\n\n")
                for a in lista[:20]:
                    if a["id"]:
                        f.write(f"- [{a['tipo']}] id={a['id']} {a['nome']}: {a['detalhe']}\n")
                    else:
                        f.write(f"- [{a['tipo']}] {a['detalhe']}\n")
                if len(lista) > 20:
                    f.write(f"\n... e mais {len(lista) - 20}\n")
                f.write("\n")

    print(f"Relatorio salvo em: {saida}")
    print(f"Total anomalias: {len(todas_anomalias)} (ORIGINAL: {len(anomalias_orig)}, NOVA: {len(anomalias_novas)})")
    for cat, tipos in categorias.items():
        total_cat = sum(len(por_tipo.get(t, [])) for t in tipos)
        if total_cat:
            print(f"  {cat}: {total_cat}")


def main():
    parser = argparse.ArgumentParser(description="Auditoria de qualidade da base")
    parser.add_argument("--saida", default=os.path.join(BUILD_LOGS, "auditoria_qualidade.md"),
                        help="Caminho do relatório de saída")
    args = parser.parse_args()

    print(f"Carregando dados de {DB_PATH}...")
    rows = carregar_dados()
    print(f"Total: {len(rows)} registros")

    originais = sum(1 for r in rows if r["empresa"] in EMPRESAS_ORIGINAIS)
    novas = len(rows) - originais
    print(f"  ORIGINAIS: {originais}, NOVAS: {novas}")

    print("Auditando cidades...")
    a_cidades = auditar_cidades(rows)

    print("Auditando fases...")
    a_fases = auditar_fases(rows)

    print("Auditando tipologias...")
    a_tipologias = auditar_tipologias(rows)

    print("Auditando nomes...")
    a_nomes = auditar_nomes(rows)

    print("Auditando endereços...")
    a_enderecos = auditar_enderecos(rows)

    print("Auditando metragens...")
    a_metragens = auditar_metragens(rows)

    print("Auditando preços...")
    a_precos = auditar_precos(rows)

    todas = a_cidades + a_fases + a_tipologias + a_nomes + a_enderecos + a_metragens + a_precos
    gerar_relatorio(todas, args.saida, len(rows), rows)


if __name__ == "__main__":
    main()

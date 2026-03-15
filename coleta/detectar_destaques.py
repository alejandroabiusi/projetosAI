"""
Deteccao de destaques e padroes atipicos.
==========================================
Analisa empreendimentos por regional e identifica:
  - Precos outliers (fora de 1.5x IQR)
  - Metragens atipicas
  - Amenidades raras (< 5% na regional)
  - Torres atipicas
  - Novos breve lancamentos

Uso:
    python detectar_destaques.py
    python detectar_destaques.py --regional SP_SPRM
"""

import os
import sys
import json
import argparse
import sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.settings import DATA_DIR
from config.regionais import classificar_regional, nome_regional, REGIONAIS
from data.database import get_connection, ATRIBUTOS_LAZER


def carregar_empreendimentos():
    """Carrega todos os empreendimentos do banco."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM empreendimentos")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def agrupar_por_regional(empreendimentos):
    """Agrupa empreendimentos por regional."""
    grupos = defaultdict(list)
    for emp in empreendimentos:
        regional = classificar_regional(emp.get("cidade"), emp.get("estado"))
        grupos[regional].append(emp)
    return dict(grupos)


def calcular_iqr(valores):
    """Calcula quartis e IQR de uma lista de valores."""
    if len(valores) < 4:
        return None
    valores = sorted(valores)
    n = len(valores)
    q1 = valores[n // 4]
    q3 = valores[3 * n // 4]
    iqr = q3 - q1
    return {"q1": q1, "q3": q3, "iqr": iqr, "lower": q1 - 1.5 * iqr, "upper": q3 + 1.5 * iqr}


def detectar_preco_outlier(empreendimentos):
    """Detecta empreendimentos com preco fora do IQR."""
    precos = [(e, e["preco_a_partir"]) for e in empreendimentos
              if e.get("preco_a_partir") and e["preco_a_partir"] > 0]
    if len(precos) < 4:
        return []

    valores = [p[1] for p in precos]
    stats = calcular_iqr(valores)
    if not stats:
        return []

    destaques = []
    for emp, preco in precos:
        if preco < stats["lower"] or preco > stats["upper"]:
            direcao = "acima" if preco > stats["upper"] else "abaixo"
            destaques.append({
                "empresa": emp["empresa"],
                "nome": emp["nome"],
                "cidade": emp.get("cidade", ""),
                "tipo": "preco_outlier",
                "detalhe": f"R$ {preco:,.0f} ({direcao} do esperado, IQR: R$ {stats['q1']:,.0f} - R$ {stats['q3']:,.0f})",
            })
    return destaques


def detectar_metragem_atipica(empreendimentos):
    """Detecta empreendimentos com metragem fora do padrao."""
    areas = [(e, e["area_min_m2"]) for e in empreendimentos
             if e.get("area_min_m2") and e["area_min_m2"] > 0]
    if len(areas) < 4:
        return []

    valores = [a[1] for a in areas]
    stats = calcular_iqr(valores)
    if not stats:
        return []

    destaques = []
    for emp, area in areas:
        if area < stats["lower"] or area > stats["upper"]:
            direcao = "acima" if area > stats["upper"] else "abaixo"
            destaques.append({
                "empresa": emp["empresa"],
                "nome": emp["nome"],
                "cidade": emp.get("cidade", ""),
                "tipo": "metragem_atipica",
                "detalhe": f"{area:.0f}m² ({direcao} da mediana, IQR: {stats['q1']:.0f} - {stats['q3']:.0f}m²)",
            })
    return destaques


def detectar_amenidade_rara(empreendimentos):
    """Detecta empreendimentos com amenidades raras (< 5% na regional)."""
    if len(empreendimentos) < 20:
        return []

    # Contar frequencia de cada amenidade
    freq = defaultdict(int)
    total = len(empreendimentos)
    for emp in empreendimentos:
        for col in ATRIBUTOS_LAZER:
            if emp.get(col) == 1:
                freq[col] += 1

    # Amenidades raras (< 5%)
    limiar = total * 0.05
    raras = {col for col, count in freq.items() if 0 < count < limiar}

    destaques = []
    for emp in empreendimentos:
        amenidades_raras = [col.replace("lazer_", "").replace("_", " ").title()
                           for col in raras if emp.get(col) == 1]
        if amenidades_raras:
            destaques.append({
                "empresa": emp["empresa"],
                "nome": emp["nome"],
                "cidade": emp.get("cidade", ""),
                "tipo": "amenidade_rara",
                "detalhe": f"Amenidades incomuns: {', '.join(amenidades_raras)}",
            })
    return destaques


def detectar_breve_lancamento(empreendimentos):
    """Detecta empreendimentos em fase de breve/futuro lancamento."""
    fases_breve = {"Breve Lançamento", "Futuro Lançamento", "Breve lançamento", "Futuro lançamento"}
    destaques = []
    for emp in empreendimentos:
        fase = emp.get("fase", "")
        if fase in fases_breve:
            detalhe = f"Fase: {fase}"
            if emp.get("dormitorios_descricao"):
                detalhe += f" | {emp['dormitorios_descricao']}"
            if emp.get("preco_a_partir") and emp["preco_a_partir"] > 0:
                detalhe += f" | A partir de R$ {emp['preco_a_partir']:,.0f}"
            destaques.append({
                "empresa": emp["empresa"],
                "nome": emp["nome"],
                "cidade": emp.get("cidade", ""),
                "tipo": "breve_lancamento",
                "detalhe": detalhe,
            })
    return destaques


def detectar_destaques(regional_filtro=None):
    """
    Detecta todos os destaques, agrupados por regional.
    Retorna: {regional: {"nome": str, "destaques": [...]}}
    """
    empreendimentos = carregar_empreendimentos()
    grupos = agrupar_por_regional(empreendimentos)

    resultado = {}
    for regional, emps in grupos.items():
        if regional_filtro and regional != regional_filtro:
            continue

        destaques = []
        destaques.extend(detectar_preco_outlier(emps))
        destaques.extend(detectar_metragem_atipica(emps))
        destaques.extend(detectar_amenidade_rara(emps))
        destaques.extend(detectar_breve_lancamento(emps))

        resultado[regional] = {
            "nome": nome_regional(regional),
            "total": len(emps),
            "destaques": destaques,
        }

    return resultado


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--regional", help="Filtrar por regional (ex: SP_SPRM)")
    args = parser.parse_args()

    resultado = detectar_destaques(args.regional)

    for regional, dados in sorted(resultado.items()):
        print(f"\n{'=' * 60}")
        print(f"{dados['nome']} ({dados['total']} empreendimentos)")
        print(f"{'=' * 60}")

        if not dados["destaques"]:
            print("  Nenhum destaque detectado.")
            continue

        # Agrupar por tipo
        por_tipo = defaultdict(list)
        for d in dados["destaques"]:
            por_tipo[d["tipo"]].append(d)

        for tipo, items in por_tipo.items():
            print(f"\n  --- {tipo.replace('_', ' ').title()} ({len(items)}) ---")
            for item in items[:10]:  # Limitar output
                print(f"  [{item['empresa']}] {item['nome']} ({item['cidade']})")
                print(f"    {item['detalhe']}")

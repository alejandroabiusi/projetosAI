#!/usr/bin/env python3
"""
Previsao de perguntas de analistas em earnings calls.

Dado um release de resultados, preve quais perguntas cada analista fara
no proximo call, baseado em:
1. Historico individual do analista na empresa
2. Historico do analista em calls de outras construtoras
3. Destaques/surpresas do release
4. Temas quentes do setor

Uso:
    python prever_perguntas.py tenda downloads/tenda/releases/Tenda_Release_4T2025.pdf
    python prever_perguntas.py tenda downloads/tenda/releases/Tenda_Release_4T2025.pdf --output previsoes_tenda_4t25.md
    python prever_perguntas.py tenda downloads/tenda/releases/Tenda_Release_4T2025.pdf --top 8
"""

import sqlite3
import json
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime

import pypdf
import anthropic

DB_PATH = Path(__file__).parent / "data" / "transcricoes.db"
SPEAKER_PATTERN = re.compile(r'\*\*\[([^\]]+)\]:\*\*')
ANALYST_PATTERN = re.compile(r'(.+?)\s*-\s*Analista(?:\s*\(([^)]+)\))?')

# Empresas do setor para busca cruzada
EMPRESAS_SETOR = ["tenda", "cury", "mrv", "direcional", "planoeplano",
                  "mouradubeux", "cyrela"]


def extrair_texto_pdf(pdf_path: str) -> str:
    """Extrai texto de todas as paginas de um PDF."""
    reader = pypdf.PdfReader(pdf_path)
    text_parts = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            text_parts.append(text)
    return "\n".join(text_parts)


def extrair_perguntas_analistas(texto_processado: str) -> dict[str, str]:
    """Extrai blocos de fala de analistas de um texto processado.

    Retorna: {speaker_tag: texto}
    """
    blocks = SPEAKER_PATTERN.split(texto_processado)
    result = {}
    for i in range(1, len(blocks) - 1, 2):
        speaker_tag = blocks[i].strip()
        content = blocks[i + 1].strip()
        if "Analista" in speaker_tag:
            match = ANALYST_PATTERN.match(speaker_tag)
            if match:
                name = match.group(1).strip()
                banco = (match.group(2) or "?").strip()
                key = f"{name}|{banco}"
                if key in result:
                    result[key] += "\n\n" + content
                else:
                    result[key] = content
    return result


def buscar_historico_empresa(conn: sqlite3.Connection, empresa: str) -> dict:
    """Busca historico de perguntas de analistas em calls da empresa.

    Retorna: {nome_analista: {banco, interventions: [{periodo, texto}]}}
    """
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, periodo, texto_processado
           FROM transcricoes WHERE empresa = ?
           AND texto_processado IS NOT NULL
           ORDER BY ano, trimestre""",
        (empresa,)
    )

    analyst_history = {}
    for tid, periodo, texto in cursor.fetchall():
        if not texto:
            continue
        perguntas = extrair_perguntas_analistas(texto)
        for key, content in perguntas.items():
            name, banco = key.split("|", 1)
            if name not in analyst_history:
                analyst_history[name] = {"banco": banco, "interventions": []}
            analyst_history[name]["interventions"].append({
                "periodo": periodo,
                "texto": content
            })

    return analyst_history


def buscar_historico_setor(conn: sqlite3.Connection, empresa: str,
                           analistas_alvo: list[str],
                           periodos_recentes: int = 3) -> dict:
    """Busca perguntas dos analistas-alvo em calls de OUTRAS empresas do setor.

    Retorna: {nome_analista: [{empresa, periodo, texto}]}
    """
    cursor = conn.cursor()
    # Buscar calls recentes de outras empresas
    cursor.execute(
        """SELECT empresa, periodo, texto_processado
           FROM transcricoes WHERE empresa != ?
           AND texto_processado IS NOT NULL
           ORDER BY ano DESC, trimestre DESC""",
        (empresa,)
    )

    cross_data = {}
    rows = cursor.fetchall()

    # Agrupar por empresa e pegar os N mais recentes
    empresa_calls = {}
    for emp, periodo, texto in rows:
        if emp not in empresa_calls:
            empresa_calls[emp] = []
        empresa_calls[emp].append((periodo, texto))

    for emp, calls in empresa_calls.items():
        for periodo, texto in calls[:periodos_recentes]:
            if not texto:
                continue
            perguntas = extrair_perguntas_analistas(texto)
            for key, content in perguntas.items():
                name, banco = key.split("|", 1)
                if name in analistas_alvo:
                    if name not in cross_data:
                        cross_data[name] = []
                    cross_data[name].append({
                        "empresa": emp,
                        "periodo": periodo,
                        "texto": content[:1500]  # limitar para caber no contexto
                    })

    return cross_data


def identificar_analistas_provaveis(historico: dict, top_n: int = 10) -> list[str]:
    """Identifica os N analistas mais provaveis de participar do proximo call.

    Criterio: frequencia de participacao, com peso maior para calls recentes.
    """
    scores = {}
    for name, data in historico.items():
        interventions = data["interventions"]
        # Peso por recencia: ultimo call = 4pts, penultimo = 2pts, ante = 1pt
        score = 0
        periodos = [i["periodo"] for i in interventions]
        n = len(periodos)
        for idx, _ in enumerate(periodos):
            if idx >= n - 1:
                score += 4
            elif idx >= n - 2:
                score += 2
            else:
                score += 1
        scores[name] = score

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [name for name, _ in ranked[:top_n]]


def montar_prompt_previsao(empresa: str, release_text: str,
                           analista: str, banco: str,
                           historico_empresa: list[dict],
                           historico_setor: list[dict]) -> str:
    """Monta o prompt para Claude gerar previsoes de perguntas."""

    # Limitar release a ~8000 chars para caber no contexto
    release_resumido = release_text[:12000]

    # Historico na empresa: ultimos 4 calls
    hist_empresa_text = ""
    for h in historico_empresa[-4:]:
        hist_empresa_text += f"\n--- {h['periodo']} ---\n{h['texto'][:1200]}\n"

    # Historico em outras empresas
    hist_setor_text = ""
    for h in historico_setor[:6]:
        hist_setor_text += f"\n--- {h['empresa'].upper()} {h['periodo']} ---\n{h['texto'][:800]}\n"

    prompt = f"""Voce e um especialista em earnings calls de construtoras brasileiras listadas na B3.

Sua tarefa: prever as 2-3 perguntas que o analista **{analista}** ({banco}) fara no proximo earnings call da **{empresa.upper()}**, referente ao **4T2025**.

## Release de Resultados 4T2025 - {empresa.upper()}
{release_resumido}

## Historico de perguntas de {analista} nos calls da {empresa.upper()}
{hist_empresa_text if hist_empresa_text else "(Nenhum historico disponivel nesta empresa)"}

## Perguntas recentes de {analista} em calls de OUTRAS construtoras
{hist_setor_text if hist_setor_text else "(Nenhum historico disponivel em outras empresas)"}

## Instrucoes
Com base em:
1. Os destaques e possiveis surpresas do release 4T2025
2. O padrao historico de perguntas deste analista (temas recorrentes, estilo, vies)
3. Os temas que este analista tem explorado recentemente em outras empresas do setor

Gere 2-3 perguntas que {analista} provavelmente fara neste call.

Para cada pergunta:
- Escreva a pergunta como o analista a formularia (em portugues, tom profissional)
- Inclua uma justificativa breve (1-2 frases) explicando por que esta pergunta e provavel
- Indique o nivel de confianca (Alta/Media/Baixa)

Formato:
### Pergunta 1
**Pergunta:** [texto da pergunta]
**Justificativa:** [por que e provavel]
**Confianca:** [Alta/Media/Baixa]
"""
    return prompt


def gerar_previsoes(empresa: str, pdf_path: str, top_n: int = 10,
                    output_path: str | None = None):
    """Pipeline principal: gera previsoes de perguntas para o proximo call."""

    print(f"[1/5] Extraindo texto do release: {pdf_path}")
    release_text = extrair_texto_pdf(pdf_path)
    print(f"  -> {len(release_text)} caracteres extraidos")

    print(f"[2/5] Buscando historico de analistas na {empresa.upper()}")
    conn = sqlite3.connect(DB_PATH)
    historico = buscar_historico_empresa(conn, empresa)
    print(f"  -> {len(historico)} analistas encontrados")

    print(f"[3/5] Identificando {top_n} analistas mais provaveis")
    analistas_provaveis = identificar_analistas_provaveis(historico, top_n)
    print(f"  -> Analistas: {', '.join(analistas_provaveis)}")

    print(f"[4/5] Buscando historico cruzado no setor")
    historico_setor = buscar_historico_setor(conn, empresa, analistas_provaveis)
    for name in analistas_provaveis:
        n = len(historico_setor.get(name, []))
        if n > 0:
            print(f"  -> {name}: {n} intervencoes em outras empresas")

    print(f"[5/5] Gerando previsoes com Claude API")
    client = anthropic.Anthropic()

    resultados = {}
    for analista in analistas_provaveis:
        info = historico.get(analista, {"banco": "?", "interventions": []})
        banco = info["banco"]
        hist_emp = info["interventions"]
        hist_set = historico_setor.get(analista, [])

        prompt = montar_prompt_previsao(
            empresa, release_text, analista, banco, hist_emp, hist_set
        )

        print(f"  -> Gerando previsoes para {analista} ({banco})...")

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )

        resposta = message.content[0].text
        resultados[analista] = {
            "banco": banco,
            "num_calls_historico": len(hist_emp),
            "periodos": [h["periodo"] for h in hist_emp],
            "previsao": resposta
        }

    conn.close()

    # Montar output
    output = gerar_output_markdown(empresa, resultados)

    if output_path:
        out_file = Path(output_path)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        out_file = Path(f"previsoes_{empresa}_4t25_{timestamp}.md")

    out_file.write_text(output, encoding="utf-8")
    print(f"\nPrevisoes salvas em: {out_file}")

    # Tambem imprimir no console
    print("\n" + "=" * 80)
    print(output)

    return resultados


def gerar_output_markdown(empresa: str, resultados: dict) -> str:
    """Gera output formatado em markdown."""
    lines = [
        f"# Previsao de Perguntas - {empresa.upper()} 4T2025",
        f"_Gerado em {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n",
        "---\n",
    ]

    for analista, data in resultados.items():
        banco = data["banco"]
        n_calls = data["num_calls_historico"]
        periodos = ", ".join(data["periodos"][-4:])

        lines.append(f"## {analista} ({banco})")
        lines.append(f"_Historico: {n_calls} calls ({periodos})_\n")
        lines.append(data["previsao"])
        lines.append("\n---\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Preve perguntas de analistas em earnings calls"
    )
    parser.add_argument("empresa", help="Nome da empresa (ex: tenda, cury, mrv)")
    parser.add_argument("pdf", help="Caminho para o release PDF")
    parser.add_argument("--output", "-o", help="Arquivo de saida (default: auto)")
    parser.add_argument("--top", "-t", type=int, default=10,
                        help="Numero de analistas a prever (default: 10)")

    args = parser.parse_args()

    if not Path(args.pdf).exists():
        print(f"Erro: arquivo {args.pdf} nao encontrado")
        sys.exit(1)

    gerar_previsoes(args.empresa, args.pdf, args.top, args.output)


if __name__ == "__main__":
    main()

"""Pipeline de previsao de perguntas — entrypoint."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from . import client as cli_client
from . import history as hist_mod
from . import pool as pool_mod
from . import prompt as prompt_mod
from . import themes as themes_mod
from .output import gerar_output_markdown
from .pdf import extrair_texto_pdf

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "transcricoes.db"
CROSS_COMPANY_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "cross_company_analysts.json"
)


def _carregar_cross_company(path: Path = CROSS_COMPANY_PATH) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def gerar_previsoes(
    empresa: str,
    pdf_path: str,
    *,
    periodo: str = "4T2025",
    top_n: int = 10,
    output_path: str | None = None,
    db_path: Path = DB_PATH,
    guidance_referencia: dict[str, tuple[float, float]] | None = None,
) -> dict:
    """Pipeline principal. Retorna o dict de resultados."""
    print(f"[1/6] Extraindo release: {pdf_path}")
    release_text = extrair_texto_pdf(pdf_path)
    print(f"  -> {len(release_text)} caracteres")

    print(f"[2/6] Analisando temas e surpresas no release")
    analise = themes_mod.analisar(release_text, guidance_referencia)
    print(
        f"  -> surpresa_forte={analise.surpresa_negativa_forte} "
        f"tema={analise.tema_dominante} flags={len(analise.flags)}"
    )

    print(f"[3/6] Buscando historico de analistas em {empresa.upper()}")
    conn = sqlite3.connect(db_path)
    try:
        historico = hist_mod.buscar_historico_empresa(conn, empresa)
        print(f"  -> {len(historico)} analistas")

        cross_data = _carregar_cross_company()
        pool = pool_mod.consolidar_pool(historico, cross_data)
        print(f"  -> pool de {len(pool)} candidatos (incl. cross-setor)")

        analistas_provaveis = [c.primario for c in pool[:top_n]]
        print(f"[4/6] Buscando historico cruzado no setor")
        historico_setor = hist_mod.buscar_historico_setor(
            conn, empresa, analistas_provaveis
        )
    finally:
        conn.close()

    print(f"[5/6] Gerando previsoes (Claude Opus 4.7)")
    bloco_compartilhado = prompt_mod.montar_bloco_compartilhado(
        empresa, periodo, release_text, analise, pool
    )

    resultados: dict = {}
    total_cache_read = 0
    total_cache_write = 0
    for cand in pool[:top_n]:
        analista = cand.primario
        info = historico.get(analista, {"banco": cand.banco, "interventions": []})
        banco = cand.banco if cand.banco != "?" else info.get("banco", "?")
        hist_emp = info.get("interventions", [])
        hist_set = historico_setor.get(analista, [])

        bloco_analista = prompt_mod.montar_bloco_analista(
            empresa, periodo, analista, banco, hist_emp, hist_set,
            alternativos=cand.alternativos,
            sem_historico_empresa=not cand.tem_historico_empresa,
        )

        print(f"  -> {analista} ({banco})")
        texto, usage = cli_client.gerar_previsao(bloco_compartilhado, bloco_analista)
        total_cache_read += usage.cache_read_input_tokens
        total_cache_write += usage.cache_creation_input_tokens

        resultados[analista] = {
            "banco": banco,
            "num_calls_historico": len(hist_emp),
            "periodos": [h["periodo"] for h in hist_emp],
            "previsao": texto,
        }

    print(
        f"[6/6] Caching: cache_read={total_cache_read} tokens, "
        f"cache_write={total_cache_write} tokens"
    )

    md = gerar_output_markdown(empresa, periodo, resultados, analise=analise, pool=pool)
    if output_path:
        out = Path(output_path)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        out = Path(f"previsoes_{empresa}_{periodo.lower()}_{ts}.md")
    out.write_text(md, encoding="utf-8")
    print(f"\nPrevisoes salvas em: {out}")
    return resultados


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preve perguntas de analistas em earnings calls (Opus 4.7).",
    )
    parser.add_argument("empresa", help="Nome da empresa (ex: tenda, cury, mrv)")
    parser.add_argument("pdf", help="Caminho para o release PDF")
    parser.add_argument("--periodo", default="4T2025", help="Periodo do call (default: 4T2025)")
    parser.add_argument("--output", "-o", help="Arquivo de saida (default: auto)")
    parser.add_argument("--top", "-t", type=int, default=10, help="Numero de analistas (default: 10)")
    args = parser.parse_args()

    if not Path(args.pdf).exists():
        print(f"Erro: {args.pdf} nao encontrado", file=sys.stderr)
        sys.exit(1)
    gerar_previsoes(
        args.empresa, args.pdf,
        periodo=args.periodo, top_n=args.top, output_path=args.output,
    )


if __name__ == "__main__":
    main()

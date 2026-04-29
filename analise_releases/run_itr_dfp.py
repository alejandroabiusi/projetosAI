"""
Runner para executar scrapers de ITR/DFP e Demonstracoes Financeiras.

Uso:
    python run_itr_dfp.py                        # ITR/DFP de todas as empresas
    python run_itr_dfp.py tenda cury              # Empresas especificas
    python run_itr_dfp.py --tipo demonstracoes    # Demonstracoes Financeiras
    python run_itr_dfp.py --tipo ambos            # ITR/DFP + Demonstracoes
    python run_itr_dfp.py --tipo ambos tenda mrv  # Ambos, empresas especificas
"""

import sys
from scrapers.mzgroup_ri import MZGroupRI, EMPRESAS as EMPRESAS_MZ
from scrapers.planoeplano_ri import PlanoEPlanoRI
from scrapers.tenda_ri import TendaRI


# Empresas disponiveis (sem aliases duplicados)
EMPRESAS_UNICAS_MZ = {
    "cury": "CURY3",
    "mrv": "MRVE3",
    "direcional": "DIRR3",
    "cyrela": "CYRE3",
    "mouradubeux": "MDNE3",
}

TODAS_CHAVES = list(EMPRESAS_UNICAS_MZ.keys()) + ["planoeplano", "tenda"]


def rodar_empresa(chave, tipo_documento):
    """Instancia o scraper correto e executa. Retorna (nome, resultado)."""
    if chave == "tenda":
        scraper = TendaRI(tipo_documento=tipo_documento)
        resultado = scraper.executar()
        return ("Tenda", resultado)
    elif chave == "planoeplano":
        scraper = PlanoEPlanoRI(tipo_documento=tipo_documento)
        resultado = scraper.executar()
        return ("Plano&Plano", resultado)
    else:
        config = EMPRESAS_MZ[chave]
        scraper = MZGroupRI(config, tipo_documento=tipo_documento)
        resultado = scraper.executar()
        return (config["nome"], resultado)


def executar_tipo(empresas_rodar, tipo_documento):
    """Executa um tipo de documento para todas as empresas selecionadas."""
    tipo_label = tipo_documento.upper().replace("_", "/")
    print(f"\n{'=' * 60}")
    print(f"EXECUCAO: {tipo_label}")
    print(f"{'=' * 60}")

    resultados = {}
    for chave in empresas_rodar:
        nome, resultado = rodar_empresa(chave, tipo_documento)
        resultados[nome] = resultado

    return resultados


def main():
    # Parsear argumentos
    args = sys.argv[1:]
    tipo = "itr_dfp"  # Default: ITR/DFP

    if "--tipo" in args:
        idx = args.index("--tipo")
        if idx + 1 < len(args):
            tipo = args[idx + 1]
            args = args[:idx] + args[idx + 2:]
        else:
            print("Erro: --tipo requer um valor (itr_dfp, demonstracoes, ambos)")
            sys.exit(1)

    if tipo not in ("itr_dfp", "demonstracoes", "ambos"):
        print(f"Tipo '{tipo}' invalido. Opcoes: itr_dfp, demonstracoes, ambos")
        sys.exit(1)

    # Determinar empresas
    if args:
        empresas_rodar = []
        for nome in args:
            nome_lower = nome.lower()
            # Aceitar aliases do MZ Group
            if nome_lower in ("plano",):
                nome_lower = "planoeplano"
            if nome_lower in ("moura",):
                nome_lower = "mouradubeux"
            if nome_lower in TODAS_CHAVES:
                empresas_rodar.append(nome_lower)
            else:
                print(f"Empresa '{nome}' nao encontrada. Opcoes: {', '.join(sorted(TODAS_CHAVES))}")
                sys.exit(1)
    else:
        empresas_rodar = list(TODAS_CHAVES)

    # Montar nomes para exibicao
    nomes_display = []
    for chave in empresas_rodar:
        if chave == "tenda":
            nomes_display.append("Tenda (TEND3)")
        elif chave == "planoeplano":
            nomes_display.append("Plano&Plano (PLPL3)")
        else:
            cfg = EMPRESAS_MZ[chave]
            nomes_display.append(f"{cfg['nome']} ({cfg['ticker']})")

    tipos_rodar = ["itr_dfp", "demonstracoes"] if tipo == "ambos" else [tipo]

    print("=" * 60)
    print("EXECUCAO DE SCRAPERS DE ITR/DFP E DEMONSTRACOES")
    print(f"Empresas: {', '.join(nomes_display)}")
    print(f"Tipos: {', '.join(t.upper().replace('_', '/') for t in tipos_rodar)}")
    print("=" * 60)

    todos_resultados = {}
    for tipo_doc in tipos_rodar:
        resultados = executar_tipo(empresas_rodar, tipo_doc)
        for nome, r in resultados.items():
            chave = f"{nome} ({tipo_doc})"
            todos_resultados[chave] = r

    # Resumo geral
    print(f"\n{'=' * 60}")
    print("RESUMO GERAL")
    print("=" * 60)
    total_novos = 0
    total_erros = 0
    for nome, r in todos_resultados.items():
        print(f"  {nome:40s}: {r['novos']:3d} novos | {r['existentes']:3d} existentes | {r['erros']:3d} erros")
        total_novos += r["novos"]
        total_erros += r["erros"]

    print(f"\n  TOTAL: {total_novos} novos downloads, {total_erros} erros")


if __name__ == "__main__":
    main()

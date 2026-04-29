"""
Runner para baixar apresentacoes de resultados de todas as empresas.

Uso:
    python run_apresentacoes.py              # Todas as empresas
    python run_apresentacoes.py cury mrv     # Empresas especificas
"""

import sys
from scrapers.mzgroup_ri import MZGroupRI, EMPRESAS as EMPRESAS_MZ
from scrapers.tenda_ri import TendaRI

TODAS = dict(EMPRESAS_MZ)
TODAS["tenda"] = "TENDA"


def rodar_empresa(chave):
    if chave == "tenda":
        scraper = TendaRI(tipo_documento="apresentacao")
        resultado = scraper.executar()
        return ("Tenda", resultado)
    else:
        config = EMPRESAS_MZ[chave]
        scraper = MZGroupRI(config, tipo_documento="apresentacao")
        resultado = scraper.executar()
        return (config["nome"], resultado)


def main():
    if len(sys.argv) <= 1:
        nomes_unicos = {}
        for chave, config in EMPRESAS_MZ.items():
            ticker = config["ticker"]
            if ticker not in nomes_unicos:
                nomes_unicos[ticker] = chave
        empresas_rodar = list(nomes_unicos.values()) + ["tenda"]
    else:
        empresas_rodar = []
        for nome in sys.argv[1:]:
            nome_lower = nome.lower()
            if nome_lower in TODAS:
                empresas_rodar.append(nome_lower)
            else:
                print(f"Empresa '{nome}' nao encontrada. Opcoes: {', '.join(sorted(TODAS.keys()))}")
                sys.exit(1)

    print("=" * 60)
    print("DOWNLOAD DE APRESENTACOES DE RESULTADOS")
    print(f"Empresas: {len(empresas_rodar)}")
    print("=" * 60)

    resultados = {}
    for chave in empresas_rodar:
        nome, resultado = rodar_empresa(chave)
        resultados[nome] = resultado

    print("\n" + "=" * 60)
    print("RESUMO GERAL")
    print("=" * 60)
    total_novos = 0
    total_erros = 0
    for nome, r in resultados.items():
        print(f"  {nome:15s}: {r['novos']:3d} novos | {r['existentes']:3d} existentes | {r['erros']:3d} erros")
        total_novos += r["novos"]
        total_erros += r["erros"]
    print(f"\n  TOTAL: {total_novos} novos downloads, {total_erros} erros")


if __name__ == "__main__":
    main()

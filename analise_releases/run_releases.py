"""
Runner para executar scrapers de releases de RI de todas as empresas.

Uso:
    python run_releases.py              # Todas as empresas (MZ Group + Tenda)
    python run_releases.py cury mrv     # Apenas Cury e MRV
    python run_releases.py tenda        # Apenas Tenda
    python run_releases.py tenda cury   # Tenda + Cury
"""

import sys
from scrapers.mzgroup_ri import MZGroupRI, EMPRESAS as EMPRESAS_MZ
from scrapers.tenda_ri import TendaRI


# Unifica o mapeamento: empresas MZ + Tenda
TODAS = dict(EMPRESAS_MZ)
TODAS["tenda"] = "TENDA"  # Marcador especial (scraper proprio)


def rodar_empresa(chave):
    """Instancia o scraper correto e executa. Retorna (nome, resultado)."""
    if chave == "tenda":
        scraper = TendaRI()
        resultado = scraper.executar()
        return ("Tenda", resultado)
    else:
        config = EMPRESAS_MZ[chave]
        scraper = MZGroupRI(config)
        resultado = scraper.executar()
        return (config["nome"], resultado)


def main():
    # Se nenhum argumento, roda todas
    if len(sys.argv) <= 1:
        # Todas as MZ (sem duplicatas de alias)
        nomes_unicos = {}
        for chave, config in EMPRESAS_MZ.items():
            ticker = config["ticker"]
            if ticker not in nomes_unicos:
                nomes_unicos[ticker] = chave
        empresas_rodar = list(nomes_unicos.values())
        # Adiciona Tenda
        empresas_rodar.append("tenda")
    else:
        empresas_rodar = []
        for nome in sys.argv[1:]:
            nome_lower = nome.lower()
            if nome_lower in TODAS:
                empresas_rodar.append(nome_lower)
            else:
                print(f"Empresa '{nome}' nao encontrada. Opcoes: {', '.join(sorted(TODAS.keys()))}")
                sys.exit(1)

    # Monta nomes para exibicao
    nomes_display = []
    for chave in empresas_rodar:
        if chave == "tenda":
            nomes_display.append("Tenda (TEND3)")
        else:
            cfg = EMPRESAS_MZ[chave]
            nomes_display.append(f"{cfg['nome']} ({cfg['ticker']})")

    print("=" * 60)
    print("EXECUCAO DE SCRAPERS DE RELEASES")
    print(f"Empresas: {', '.join(nomes_display)}")
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

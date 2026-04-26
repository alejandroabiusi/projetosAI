"""
Lê a planilha mestre do projeto NCA e renderiza carrosséis aprovados.

Fonte de dados: Google Sheets pública, exportada via endpoint CSV sem auth.
Filtra linhas com a coluna de aprovação (índice 3, header literal 'TRUE')
marcada como TRUE. Para cada linha aprovada, gera um carrossel de três cards:
Card 1 = Tirada feminina, Card 2 = Tirada masculina, Card 3 = CTA.

Saída: out/AAAA_MM_DD/card{1,2,3}.png
       AAAA_MM_DD vem de 'Data agendada' da planilha. Quando essa coluna
       está vazia (estado atual de toda a planilha), aplicamos um fallback
       local que calcula a data a partir do número sequencial do ID: NCA0001
       = DATA_INICIAL_FALLBACK, NCA0002 = um dia depois, e assim por diante.
       Esse fallback some assim que a coluna for preenchida no Sheets.

Uso:
    python gerar_de_planilha.py            # gera todos os aprovados
    python gerar_de_planilha.py NCA0001    # gera só os IDs passados
"""

import csv
import re
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from compor_carrossel import compor_card, renderizar_base

PLANILHA_ID = '1yquavD8hGqtKc9sE0MKR6lFq57plV-Tj-XE_GZAic6I'
URL_CSV = (
    f'https://docs.google.com/spreadsheets/d/{PLANILHA_ID}/export?format=csv'
)

# A coluna de aprovação não tem nome de cabeçalho; o CSV exporta o valor
# atual da primeira linha de dados como header literal. Usamos posição.
IDX_APROVACAO = 3

# Fallback temporário enquanto a coluna 'Data agendada' não está preenchida
# no Sheets. NCA0001 = 27/04/2026 (primeira postagem), NCA0002 = 28/04, etc.
DATA_INICIAL_FALLBACK = date(2026, 4, 27)
PADRAO_ID = re.compile(r'^NCA(\d+)$')

# Mapa do nome do signo na planilha para a chave usada em TEMPLATES
# (chaves minúsculas sem acento, como definido em compor_carrossel.py).
SIGNO_PARA_TEMPLATE = {
    'Áries': 'aries',
    'Touro': 'touro',
    'Gêmeos': 'gemeos',
    'Câncer': 'cancer',
    'Leão': 'leao',
    'Virgem': 'virgem',
    'Libra': 'libra',
    'Escorpião': 'escorpiao',
    'Sagitário': 'sagitario',
    'Capricórnio': 'capricornio',
    'Aquário': 'aquario',
    'Peixes': 'peixes',
    'institucional': 'geral',
}


def baixar_planilha() -> list[dict]:
    """Baixa a planilha como CSV e retorna lista de dicionários por linha."""
    with urllib.request.urlopen(URL_CSV) as resp:
        texto = resp.read().decode('utf-8')

    reader = csv.reader(texto.splitlines())
    next(reader)  # primeira linha é cabeçalho de seções, descarta
    headers = next(reader)

    registros = []
    for linha in reader:
        if not linha or not linha[0].strip():
            continue
        registro = dict(zip(headers, linha))
        # Preserva o valor da coluna de aprovação por posição, já que o
        # cabeçalho dela é o valor literal 'TRUE' e pode colidir.
        registro['_aprovado'] = linha[IDX_APROVACAO].strip().upper() == 'TRUE'
        registros.append(registro)
    return registros


def data_da_pasta(registro: dict) -> str:
    """Retorna AAAA_MM_DD a partir de 'Data agendada' do Sheets.

    Quando a coluna está vazia (estado atual da planilha), calcula a data
    sequencialmente do número do ID a partir de DATA_INICIAL_FALLBACK.
    """
    data_agendada = registro.get('Data agendada', '').strip()
    if data_agendada:
        return data_agendada.replace('-', '_')

    match = PADRAO_ID.match(registro['ID'])
    if not match:
        raise ValueError(
            f'Data agendada vazia e ID {registro["ID"]!r} não bate o padrão '
            f'NCA####, não consigo aplicar fallback de data.'
        )
    offset = int(match.group(1)) - 1
    data = DATA_INICIAL_FALLBACK + timedelta(days=offset)
    return data.strftime('%Y_%m_%d')


def gerar_carrossel_de_registro(registro: dict, out_root: str = 'out') -> list[str]:
    """Renderiza os 3 cards de um registro e salva em out/AAAA_MM_DD/{ID}/."""
    nome_signo = registro['Signo principal'].strip()
    if nome_signo not in SIGNO_PARA_TEMPLATE:
        raise ValueError(f'Signo desconhecido: {nome_signo!r} (registro {registro["ID"]})')

    template_key = SIGNO_PARA_TEMPLATE[nome_signo]
    base = renderizar_base(template_key)

    cards_texto = [
        registro['Tirada feminina'].strip(),
        registro['Tirada masculina'].strip(),
        registro['CTA'].strip(),
    ]

    pasta = Path(out_root) / data_da_pasta(registro)
    pasta.mkdir(parents=True, exist_ok=True)

    paths = []
    for i, texto in enumerate(cards_texto, start=1):
        img = compor_card(base, texto)
        out_path = pasta / f'card{i}.png'
        img.save(out_path)
        paths.append(str(out_path))
    return paths


def main(filtro_ids: list[str] | None = None):
    print(f'Baixando planilha {PLANILHA_ID}...')
    registros = baixar_planilha()
    print(f'  {len(registros)} linhas no total')

    aprovados = [r for r in registros if r['_aprovado']]
    print(f'  {len(aprovados)} aprovados (coluna [{IDX_APROVACAO}] == TRUE)')

    if filtro_ids:
        aprovados = [r for r in aprovados if r['ID'] in filtro_ids]
        print(f'  {len(aprovados)} dentro do filtro de IDs {filtro_ids}')

    print()
    for r in aprovados:
        try:
            paths = gerar_carrossel_de_registro(r)
            pasta = Path(paths[0]).parent
            print(f'{r["ID"]} ({r["Signo principal"]}): {len(paths)} cards em {pasta}')
        except Exception as e:
            print(f'{r["ID"]} ERRO: {e}')


if __name__ == '__main__':
    filtro = sys.argv[1:] if len(sys.argv) > 1 else None
    main(filtro)

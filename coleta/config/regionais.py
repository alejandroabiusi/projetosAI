"""
Classificacao de cidades em regionais.
========================================
Cada empreendimento e classificado em uma regional para
agrupamento nos relatorios executivos.
"""

REGIONAIS = {
    "SP_SPRM": {
        "nome": "SP + Região Metropolitana",
        "cidades": [
            "São Paulo", "Guarulhos", "Osasco", "Diadema", "Santo André",
            "São Bernardo do Campo", "Suzano", "Mauá", "Carapicuíba",
            "Barueri", "Cotia", "Taboão da Serra", "Itaquaquecetuba",
            "Ferraz de Vasconcelos", "Francisco Morato", "Franco da Rocha",
            "Itapecerica da Serra", "Itapevi", "Jandira", "Mairiporã",
            "Poá", "Ribeirão Pires", "Rio Grande da Serra", "Santana de Parnaíba",
            "Arujá", "Caieiras", "Embu das Artes", "Embu-Guaçu",
        ],
        "estados_fallback": ["SP"],
    },
    "RJ_CO": {
        "nome": "RJ + Centro-Oeste",
        "cidades": [
            "Rio de Janeiro", "Niterói", "São Gonçalo", "Duque de Caxias",
            "Nova Iguaçu", "Belford Roxo", "São João de Meriti", "Mesquita",
            "Nilópolis", "Queimados", "Itaboraí", "Magé",
            "Goiânia", "Aparecida de Goiânia", "Cuiabá", "Várzea Grande",
            "Campo Grande", "Brasília",
        ],
        "estados_fallback": ["RJ", "GO", "MT", "MS", "DF"],
    },
    "NE": {
        "nome": "Nordeste",
        "cidades": [
            "Salvador", "Lauro de Freitas", "Camaçari",
            "Recife", "Jaboatão dos Guararapes", "Olinda", "Paulista",
            "Fortaleza", "Caucaia", "Maracanaú", "Eusébio",
            "João Pessoa", "Campina Grande",
            "Natal", "Parnamirim",
            "São Luís", "Maceió", "Aracaju", "Teresina",
        ],
        "estados_fallback": ["BA", "PE", "CE", "PB", "RN", "MA", "AL", "SE", "PI"],
    },
    "SUL": {
        "nome": "Sul",
        "cidades": [
            "Porto Alegre", "Canoas", "Gravataí", "Cachoeirinha", "Alvorada",
            "Viamão", "Novo Hamburgo", "São Leopoldo",
            "Curitiba", "São José dos Pinhais", "Colombo", "Araucária",
            "Pinhais", "Campo Largo",
            "Florianópolis", "São José", "Palhoça", "Biguaçu",
        ],
        "estados_fallback": ["RS", "PR", "SC"],
    },
    "SP_INTERIOR": {
        "nome": "SP Interior + Litoral",
        "cidades": [
            "Campinas", "Sorocaba", "Ribeirão Preto", "São José dos Campos",
            "Santos", "São José do Rio Preto", "Piracicaba", "Jundiaí",
            "Bauru", "Taubaté", "Limeira", "Franca", "Presidente Prudente",
            "Marília", "Araraquara", "São Carlos", "Indaiatuba", "Hortolândia",
            "Americana", "Sumaré", "Praia Grande", "São Vicente", "Guarujá",
        ],
        "estados_fallback": [],  # Sem fallback - SP ja cai em SP_SPRM
    },
    "MG_N": {
        "nome": "MG + Norte",
        "cidades": [
            "Belo Horizonte", "Contagem", "Betim", "Ribeirão das Neves",
            "Santa Luzia", "Ibirité", "Sabará",
            "Uberlândia", "Juiz de Fora", "Montes Claros",
            "Belém", "Ananindeua", "Manaus",
        ],
        "estados_fallback": ["MG", "PA", "AM", "TO", "AP", "RR", "AC", "RO"],
    },
}

# Indice reverso: cidade -> regional (para busca rapida)
_CIDADE_REGIONAL = {}
for chave, dados in REGIONAIS.items():
    for cidade in dados["cidades"]:
        _CIDADE_REGIONAL[cidade.lower()] = chave

# Indice reverso: estado -> regional (fallback)
_ESTADO_REGIONAL = {}
for chave, dados in REGIONAIS.items():
    for estado in dados.get("estados_fallback", []):
        _ESTADO_REGIONAL[estado.upper()] = chave


def classificar_regional(cidade, estado):
    """
    Classifica um empreendimento em uma regional.
    Tenta primeiro por cidade exata, depois por estado (fallback).
    Retorna chave da regional ou 'OUTROS'.
    """
    if cidade:
        regional = _CIDADE_REGIONAL.get(cidade.strip().lower())
        if regional:
            return regional

    if estado:
        regional = _ESTADO_REGIONAL.get(estado.strip().upper())
        if regional:
            return regional

    return "OUTROS"


def nome_regional(chave):
    """Retorna o nome legivel da regional."""
    if chave in REGIONAIS:
        return REGIONAIS[chave]["nome"]
    return "Outros"

"""
Classificacao de cidades em regionais.
========================================
Segue a lógica de regionais da Tenda:
  1) SP+SPRM — São Paulo + Região Metropolitana
  2) Nordeste — BA, CE, PE, PB
  3) Sul+MG — RS, PR, MG
  4) RJ+CO+CPS — RJ, MT, MS, GO, Campinas (Campinas+Hortolândia+Valinhos+Vinhedo)
  5) SP Interior — Restante de SP
  6) Outros — DF e demais

Ordem de exibição: SP+SPRM, Nordeste, Sul+MG, RJ+CO+CPS, SP Interior, Outros
"""

REGIONAIS = {
    "SP_SPRM": {
        "nome": "SP + SPRM",
        "ordem": 1,
        "cidades": [
            "São Paulo", "Guarulhos", "Osasco", "Diadema", "Santo André",
            "São Bernardo do Campo", "São Caetano do Sul", "Suzano", "Mauá",
            "Carapicuíba", "Barueri", "Cotia", "Taboão da Serra",
            "Itaquaquecetuba", "Ferraz de Vasconcelos", "Francisco Morato",
            "Franco da Rocha", "Itapecerica da Serra", "Itapevi", "Jandira",
            "Mairiporã", "Poá", "Ribeirão Pires", "Rio Grande da Serra",
            "Santana de Parnaíba", "Arujá", "Caieiras", "Embu das Artes",
            "Embu-Guaçu", "Mogi das Cruzes", "Cajamar",
        ],
        "estados_fallback": [],
    },
    "NE": {
        "nome": "Nordeste",
        "ordem": 2,
        "cidades": [
            "Salvador", "Lauro de Freitas", "Camaçari",
            "Recife", "Jaboatão dos Guararapes", "Olinda", "Paulista",
            "Fortaleza", "Caucaia", "Maracanaú", "Eusébio",
            "João Pessoa", "Campina Grande", "Cabedelo",
            "Natal", "Parnamirim",
            "São Luís", "Maceió", "Aracaju", "Teresina",
            "Imperatriz", "Timon",
        ],
        "estados_fallback": ["BA", "PE", "CE", "PB", "RN", "MA", "AL", "SE", "PI"],
    },
    "SUL_MG": {
        "nome": "Sul + MG",
        "ordem": 3,
        "cidades": [
            "Porto Alegre", "Canoas", "Gravataí", "Cachoeirinha", "Alvorada",
            "Viamão", "Novo Hamburgo", "São Leopoldo", "Guaíba", "Esteio",
            "Caxias do Sul", "Bento Gonçalves", "Passo Fundo", "Farroupilha",
            "Marau",
            "Curitiba", "São José dos Pinhais", "Colombo", "Araucária",
            "Pinhais", "Campo Largo",
            "Belo Horizonte", "Contagem", "Betim", "Ribeirão das Neves",
            "Santa Luzia", "Ibirité", "Sabará", "Vespasiano",
            "Uberlândia", "Juiz de Fora", "Montes Claros", "Uberaba",
            "Nova Lima", "Itaúna",
        ],
        "estados_fallback": ["RS", "PR", "SC", "MG"],
    },
    "RJ_CO_CPS": {
        "nome": "RJ + CO + CPS",
        "ordem": 4,
        "cidades": [
            "Rio de Janeiro", "Niterói", "São Gonçalo", "Duque de Caxias",
            "Nova Iguaçu", "Belford Roxo", "São João de Meriti", "Mesquita",
            "Nilópolis", "Queimados", "Itaboraí", "Magé",
            "Goiânia", "Aparecida de Goiânia", "Anápolis", "Jataí",
            "Cuiabá", "Várzea Grande",
            "Campo Grande",
            "Campinas", "Hortolândia", "Valinhos", "Vinhedo",
        ],
        "estados_fallback": ["RJ", "GO", "MT", "MS"],
    },
    "SP_INTERIOR": {
        "nome": "SP Interior",
        "ordem": 5,
        "cidades": [
            "Sorocaba", "Ribeirão Preto", "São José dos Campos",
            "Santos", "São José do Rio Preto", "Piracicaba", "Jundiaí",
            "Bauru", "Taubaté", "Limeira", "Franca", "Presidente Prudente",
            "Marília", "Araraquara", "São Carlos", "Indaiatuba",
            "Americana", "Sumaré", "Praia Grande", "São Vicente", "Guarujá",
            "Botucatu", "Jaboticabal", "Sertãozinho", "Matão", "Jaú",
            "Votorantim", "Itu", "Salto", "Itatiba", "Bragança Paulista",
            "Jacareí", "Peruíbe", "Paulínia", "Rio Claro", "Catanduva",
            "Leme", "Cravinhos", "Serrana", "Mirassol", "Taquaritinga",
            "Birigui", "Araçatuba", "Assis", "Londrina", "Itupeva",
            "Lins", "Monte Mor", "Santa Bárbara d'Oeste", "Nova Odessa",
            "Jaguariúna", "Atibaia", "Caçapava", "Passos",
        ],
        "estados_fallback": [],
    },
}

# Indice reverso: cidade -> regional
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
    if cidade:
        regional = _CIDADE_REGIONAL.get(cidade.strip().lower())
        if regional:
            return regional
    if estado:
        uf = estado.strip().upper()
        if uf == "SP":
            return "SP_INTERIOR"
        regional = _ESTADO_REGIONAL.get(uf)
        if regional:
            return regional
    return "OUTROS"


def nome_regional(chave):
    if chave in REGIONAIS:
        return REGIONAIS[chave]["nome"]
    return "Outros"


# Ordem de exibição
ORDEM_REGIONAIS = ["SP_SPRM", "NE", "SUL_MG", "RJ_CO_CPS", "SP_INTERIOR", "OUTROS"]

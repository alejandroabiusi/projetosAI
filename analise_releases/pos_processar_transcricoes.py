#!/usr/bin/env python3
"""
Pos-processamento de transcricoes de calls de resultados.

Pipeline:
1. Correcao de nomes proprios (empresas, executivos, analistas, bancos)
2. Deteccao de falantes baseada em texto (speaker attribution)
3. Normalizacao de periodos ("primeiro trimestre de 2024" -> "1T2024")
4. Gravacao em coluna texto_processado + speakers_detectados (JSON)

Uso:
    python pos_processar_transcricoes.py                  # Processa todas
    python pos_processar_transcricoes.py mrv              # Apenas MRV
    python pos_processar_transcricoes.py --reprocessar    # Reprocessa mesmo ja feitas
    python pos_processar_transcricoes.py mrv --reprocessar
"""

import sqlite3
import re
import json
import sys
import unicodedata
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "data" / "transcricoes.db"


def _ws(text: str) -> str:
    """Normaliza whitespace: colapsa newlines/tabs/spaces multiplos em espaco unico."""
    return re.sub(r'\s+', ' ', text).strip()


# ---------------------------------------------------------------------------
# 1. Dicionario de correcao de nomes por empresa
# ---------------------------------------------------------------------------

# Correcoes globais (aplicam-se a todas as empresas)
CORRECOES_GLOBAIS = {
    # === Bancos / Corretoras ===
    "Jeep Morgan": "JP Morgan",
    "Deep Morgan": "JP Morgan",
    "Bradesco BVI": "Bradesco BBI",
    "Bradesco BPI": "Bradesco BBI",
    "Bradesco VBI": "Bradesco BBI",
    " IUBS": " UBS",
    " IBS": " UBS",
    "(IUBS)": "(UBS)",
    "(IBS)": "(UBS)",
    "Itau BVA": "Itau BBA",
    "Itau BDA": "Itau BBA",
    "Italo BBA": "Itau BBA",
}

# Correcoes por empresa
CORRECOES_POR_EMPRESA = {
    "cyrela": {
        "Cirela": "Cyrela",
        "Cerela": "Cyrela",
        "cirela": "Cyrela",
        "cerela": "Cyrela",
        "ri.cirela": "ri.cyrela",
        "Miguel Mikkelberg": "Miguel Mickelberg",
        "Mikkelberg": "Mickelberg",
        "Rafael Horn": "Raphael Horn",
    },
    "mrv": {
        "Eduardo Fisher": "Eduardo Fischer",
    },
    "tenda": {
        "Construtora Tena": "Construtora Tenda",
        " Tena ": " Tenda ",
        " Tena.": " Tenda.",
        "Mazine": "Mazini",
    },
    "mouradubeux": {
        "Moura do Beio": "Moura Dubeux",
        "Moura Dubeuxi": "Moura Dubeux",
        "Moura do Bejo": "Moura Dubeux",
        "Moura de Beaux": "Moura Dubeux",
        "Alain Aquino": "Alan Aquino",
        "Diego Vilar,": "Diego Villar,",
        "Diego Vilar ": "Diego Villar ",
    },
    "planoeplano": {
        "Plano e Plano": "Plano&Plano",
    },
}

# Executivos conhecidos por empresa (para speaker attribution)
# Keys: canonical full name -> role
EXECUTIVOS_POR_EMPRESA = {
    "cyrela": {
        "Raphael Horn": "CEO",
        "Miguel Mickelberg": "CFO e DRI",
        "Iuri Campos": "Gerente Senior de RI",
    },
    "mrv": {
        "Rafael Menin": "Co-CEO",
        "Eduardo Fischer": "Co-CEO",
        "Ricardo Paixao": "CFO e DRI",
        "Leonardo Correa": "VP Financeiro",
        "Rafael Pinho": "VP Comercial",
        "Nicole Hirakawa": "DRI",
        "Eduardo Silveira": "Diretor",
        "Antonio Castrucci": "CFO Resia",
    },
    "cury": {
        "Fabio Cury": "CEO",
        "Ronaldo Cury": "VP",
        "Leonardo Mesquita": "CFO",
    },
    "direcional": {
        "Ricardo Ribeiro": "CEO",
        "Paulo Sousa": "CFO e DRI",
        "Andre Damiao": "RI",
    },
    "tenda": {
        "Rodrigo Osmo": "CEO",
        "Luiz Mauricio": "CFO e DRI",
        "Leonardo Dias": "Coordenador de RI",
    },
    "mouradubeux": {
        "Diego Villar": "CEO",
        "Diego Vanderlei": "CFO",
        "Diogo Barral": "DRI",
        "Alan Aquino": "Moderador",
    },
    "planoeplano": {
        "Rodrigo Luna": "Vice-Presidente",
        "Joao Hoppe": "CFO e DRI",
        "Anselmo Rodrigues": "CEO",
    },
}

# Short-name aliases to canonical exec name (per company)
EXEC_ALIASES = {
    "cyrela": {"Raphael": "Raphael Horn", "Rafa": "Raphael Horn",
               "Miguel": "Miguel Mickelberg", "Iuri": "Iuri Campos"},
    "mrv": {"Rafael": "Rafael Menin", "Menin": "Rafael Menin",
            "Ricardo": "Ricardo Paixao", "Paixao": "Ricardo Paixao",
            "Kaka": "Leonardo Correa", "Caca": "Leonardo Correa", "Leonardo": "Leonardo Correa",
            "Eduardo": "Eduardo Fischer", "Fischer": "Eduardo Fischer",
            "Nicole": "Nicole Hirakawa"},
    "cury": {"Fabio": "Fabio Cury", "Ronaldo": "Ronaldo Cury",
             "Leonardo": "Leonardo Mesquita", "Leo": "Leonardo Mesquita"},
    "direcional": {"Ricardo": "Ricardo Ribeiro", "Paulo": "Paulo Sousa",
                   "Andre": "Andre Damiao"},
    "tenda": {"Osmo": "Rodrigo Osmo", "Rodrigo": "Rodrigo Osmo",
              "Luiz": "Luiz Mauricio", "Leonardo": "Leonardo Dias"},
    "mouradubeux": {"Alan": "Alan Aquino", "Vanderlei": "Diego Vanderlei",
                    "Villar": "Diego Villar", "Vilar": "Diego Villar",
                    "Diogo": "Diogo Barral", "Barral": "Diogo Barral"},
    "planoeplano": {"Rodrigo": "Rodrigo Luna", "Luna": "Rodrigo Luna",
                    "Hoppe": "Joao Hoppe", "Anselmo": "Anselmo Rodrigues"},
}

# Dicionario canonico de analistas sell-side (confirmados nos PDFs oficiais + pesquisa)
# Mapeia nome canonico -> (banco, variacoes whisper)
ANALISTAS_CANONICOS = {
    "Fanny Oreng": ("Santander", ["Fanny Oring", "Fanny Oren", "Fanny Orang", "Fanny"]),
    "Rafael Rehder": ("Safra", ["Rafael Hede", "Rafael Heder", "Rafael Reder", "Rafael Eder"]),
    "Bruno Mendonca": ("Bradesco BBI", ["Bruno Mendonça", "Bruno Medonça", "Bruno Bendonça"]),
    "Pedro Lobato": ("Bradesco BBI", []),
    "Ygor Altero": ("XP", ["Igor Altero", "Igor Alteiro", "Igor Alter", "Igor Altera"]),
    "Tainan Costa": ("UBS", ["Taina Costa"]),
    "Elvis Credendio": ("Itau BBA", ["Elvis Credende"]),  # era BTG Pactual ate 2024
    "Gustavo Cambauva": ("BTG Pactual", ["Gustavo Cambaúva", "Cambauva", "Cambaúva"]),
    "Marcelo Motta": ("JP Morgan", ["Marcelo Mota", "Marcelo Mouto", "Mota"]),
    "Aline Caldeira": ("Kinea Investimentos", []),  # ex-Bank of America
    "Andre Mazini": ("Citi", ["Andre Mazzini", "Andre Mazine", "Andrea Mazini", "André Mazini", "André Mancini", "André Manzini"]),
    "Jorel Guilloty": ("Goldman Sachs", ["Jorel Guilhoti", "Jorel Guilhote", "Jorel Guilotti", "Jorel Gilot"]),
    "Victor Tapia": ("UBS", ["Vitor Tapia"]),
    "Daniel Gasparete": ("Itau BBA", ["Daniel Gasparetti", "Daniel Gasparete"]),
    "Antonio Castrucci": ("Santander", ["Antonio Castruti", "Antônio Castrucci", "Antônio Castruti", "Antônio Castrute"]),
    "Hugo Grassi": ("Citi", ["Hugo Grasso"]),
    "Matheus Meloni": ("Santander", ["Mateus Meloni"]),
    "Ruan Argenton": ("XP", ["Juan Argenton", "Juan Argento", "Juan Janiton"]),
    "Pedro Hajnal": ("Vila Rica Capital", ["Pedro Raginal", "Pedro Rajno"]),  # ex-Credit Suisse
    "Enrico Trotta": ("Tivio Capital", ["Piero Trota"]),  # ex-Itau BBA
    "Mariangela de Castro": ("Itau BBA", ["Mariangela Castro"]),
    "Marcello Milman": ("Bank of America", []),
    "Fred Mendes": ("Bank of America", []),
    "Herman Lee": ("Bradesco BBI", ["Herman Li"]),
    "Carla Graca": ("Bank of America", ["Carla Garcia", "Carla Garca", "Carla Garça"]),
    "Paola Mello": ("BTG Pactual", []),
    "Sami Karlik": ("Barclays", []),
    "Alejandra Obregon": ("Morgan Stanley", []),
    "Andre Dibe": ("Itau BBA", ["Andre Dib", "André Dib", "André Dibi", "André Dibbe", "André Dipe"]),  # saindo pro Schonfeld jun/2026
    "Bruno Montanari": ("Morgan Stanley", []),
    "Luiz Capistrano": ("Itau BBA", []),
    "Jonathan Coutras": ("JP Morgan", []),
    "Igor Machado": ("Goldman Sachs", []),
    "Juliana Veiga": ("Itau BBA", []),
    "Guilherme Vilazante": ("Goldman Sachs", []),
    "David Lawant": ("Morgan Stanley", []),
    "Luis Stacchini": ("Goldman Sachs", []),
    "Kiefer Kennedy": ("Citi", ["Kieper Kennedy", "Kiper"]),  # nao verificado online
    "Alain Nicolau": ("HSBC", []),
    "Ariel Amar": ("UBS", []),
    "Lucas Dias": ("XP", []),
    "Guilherme Capparelli": ("Goldman Sachs", []),
    "Andre Canuto Baia": ("Estrela da Manha", ["Andre Canuto", "Andre Bahia", "André Bahia", "André Baia"]),  # CEO, nao analista
    "Mario Simplicio": ("Morgan Stanley", ["Mário Simplício"]),
    "Gustavo Fabris": ("BTG Pactual", ["Gustavo Fabrício", "Gustavo Fabricio"]),
    "Ana Julia Zerkowski": ("UBS", ["Ana Júlia Zerkowski"]),
    "Marcelo Audi": ("Cardinal Partners", ["Marcelo Aldi"]),  # buy-side, socio
    "Felipe Lenza": ("Citi", []),
    "Luma Paias": ("UBS", ["Luma Payas"]),
    "Olavo Fleming": ("Safra", []),
    # Kiefer Kennedy duplicata removida - ja definido acima
    "Joao Silva": ("XP", []),
    "Gabriel Moreira": ("XP", []),
}


# ---------------------------------------------------------------------------
# 2. Normalizacao de periodos
# ---------------------------------------------------------------------------

ORDINAL_MAP = {
    "primeiro": "1", "primeira": "1",
    "segundo": "2", "segunda": "2",
    "terceiro": "3", "terceira": "3",
    "quarto": "4", "quarta": "4",
}


def normalizar_periodos(texto: str) -> str:
    """Converte 'primeiro trimestre de 2024' -> '1T2024', etc."""
    def _replace_trimestre(m):
        ordinal = m.group(1).lower()
        ano = m.group(2)
        num = ORDINAL_MAP.get(ordinal, ordinal)
        return f"{num}T{ano}"

    texto = re.sub(
        r'\b(primeiro|segundo|terceiro|quarto|primeira|segunda|terceira|quarta)\s+trimestre\s+de\s+(\d{4})\b',
        _replace_trimestre,
        texto,
        flags=re.IGNORECASE,
    )

    texto = re.sub(
        r'\b(primeiro|segundo|terceiro|quarto)\s+tri\s+de\s+(\d{4})\b',
        _replace_trimestre,
        texto,
        flags=re.IGNORECASE,
    )

    def _replace_semestre(m):
        ordinal = m.group(1).lower()
        ano = m.group(2)
        num = ORDINAL_MAP.get(ordinal, ordinal)
        return f"{num}S{ano}"

    texto = re.sub(
        r'\b(primeiro|segundo)\s+semestre\s+de\s+(\d{4})\b',
        _replace_semestre,
        texto,
        flags=re.IGNORECASE,
    )

    return texto


# ---------------------------------------------------------------------------
# 3. Correcao de nomes
# ---------------------------------------------------------------------------

# Regex-based corrections (word boundary sensitive)
# Canonical names confirmed from official PDF transcriptions (MRV + Cyrela)
CORRECOES_REGEX = [
    # --- Analistas (cross-company, confirmados nos PDFs oficiais) ---
    # Fanny Oreng (Santander)
    (r'\bFanny Orang\b', 'Fanny Oreng'),
    (r'\bFanny Orring\b', 'Fanny Oreng'),
    (r'\bFanny Orange\b', 'Fanny Oreng'),
    (r'\bFanny Oring\b', 'Fanny Oreng'),
    (r'\bFanny Oren\b', 'Fanny Oreng'),
    (r'\bFania\b(?=.*?Santander)', 'Fanny Oreng'),
    # Rafael Rehder (Safra)
    (r'\bRafael Hede\b', 'Rafael Rehder'),
    (r'\bRafael Heder\b', 'Rafael Rehder'),
    (r'\bRafael Reder\b', 'Rafael Rehder'),
    (r'\bRafael Eder\b', 'Rafael Rehder'),
    (r'\bRafael Rehderr\b', 'Rafael Rehder'),
    # Jorel Guilloty (Goldman Sachs)
    (r'\bJorel Guilhoti\b', 'Jorel Guilloty'),
    (r'\bJorel Guilhote\b', 'Jorel Guilloty'),
    (r'\bJorel Guilotti\b', 'Jorel Guilloty'),
    (r'\bJorel Gilot\b', 'Jorel Guilloty'),
    (r'\bJorel Ghiotti\b', 'Jorel Guilloty'),
    (r'\bJurel Guilhoti\b', 'Jorel Guilloty'),
    # Ygor Altero (XP)
    (r'\bIgor Altero\b', 'Ygor Altero'),
    (r'\bIgor Alteiro\b', 'Ygor Altero'),
    # Elvis Credendio (BTG Pactual)
    (r'\bElvis Crendendio\b', 'Elvis Credendio'),
    (r'\bElvis Credende\b', 'Elvis Credendio'),
    # Andre Mazini (Citi)
    (r'\bAndr[eé] Mazzini\b', 'Andre Mazini'),
    (r'\bAndr[eé] Mazine\b', 'Andre Mazini'),
    (r'\bAndrea Mazini\b', 'Andre Mazini'),
    # Antonio Castrucci (Santander)
    (r'\bAntonio Castruti\b', 'Antonio Castrucci'),
    (r'\bAnt[oô]nio Castruti\b', 'Antonio Castrucci'),
    # Marcelo Motta (JP Morgan)
    (r'\bMarcelo Mota\b', 'Marcelo Motta'),
    (r'\bMarcelo Mouto\b', 'Marcelo Motta'),
    # Victor Tapia (UBS -> Bank of America)
    (r'\bVitor Tapia\b', 'Victor Tapia'),
    # Gustavo Cambauva (BTG Pactual) - normalizar acentuacao
    (r'\bGustavo Camba[uú]va\b', 'Gustavo Cambauva'),
    # Daniel Gasparete (Itau BBA) - nome correto e Gasparete
    (r'\bDaniel Gasparetti\b', 'Daniel Gasparete'),
    # Matheus Meloni (Santander) - normalizar
    (r'\bMateus Meloni\b', 'Matheus Meloni'),
    # Ruan Argenton (XP) - whisper ouve "Juan"
    (r'\bJuan Argenton\b', 'Ruan Argenton'),
    (r'\bJuan Argento\b', 'Ruan Argenton'),
    # Tainan Costa (UBS) - variacao
    (r'\bTaina Costa\b', 'Tainan Costa'),
    # Pedro Hajnal (Credit Suisse)
    (r'\bPedro Raginal\b', 'Pedro Hajnal'),
    (r'\bPedro Rajno\b', 'Pedro Hajnal'),
    # Enrico Trotta
    (r'\bPiero Trota\b', 'Enrico Trotta'),
    # Mariangela de Castro (Itau BBA)
    (r'\bMari[aâ]ngela Castro\b', 'Mariangela de Castro'),
    (r'\bMari[aâ]ngela de Castro\b', 'Mariangela de Castro'),
    # Herman Lee (Bradesco BBI)
    (r'\bHerman Li\b', 'Herman Lee'),
    # Carla Graca (Bank of America) - nome correto e Graca, nao Garcia
    (r'\bCarla Garcia\b', 'Carla Graca'),
    (r'\bCarla Gar[cç]a\b', 'Carla Graca'),
    (r'\bCarla Gra[cç]a\b', 'Carla Graca'),

    # More Fanny Oreng variations
    (r'\bFanny Orenge\b', 'Fanny Oreng'),
    (r'\bFanny Orengre\b', 'Fanny Oreng'),
    (r'\bFanny Orengues\b', 'Fanny Oreng'),
    (r'\bFanny Oreiro\b', 'Fanny Oreng'),
    (r'\bFanny Avino\b', 'Fanny Oreng'),
    (r'\bFani Oreng\b', 'Fanny Oreng'),
    (r'\bFani Orengue\b', 'Fanny Oreng'),
    (r'\bFenioreng\b', 'Fanny Oreng'),
    # More Rafael Rehder variations
    (r'\bRafael Herder\b', 'Rafael Rehder'),
    (r'\bRafael Heller\b', 'Rafael Rehder'),
    (r'\bRafael Reeder\b', 'Rafael Rehder'),
    # More Andre Mazini variations
    (r'\bAndr[eé] Mancini\b', 'Andre Mazini'),
    (r'\bAndr[eé] Manzini\b', 'Andre Mazini'),
    # More Jorel Guilloty variations
    (r'\bJorel Guilote\b', 'Jorel Guilloty'),
    (r'\bJorel Gilotti\b', 'Jorel Guilloty'),
    (r'\bJurel Guilhoti\b', 'Jorel Guilloty'),
    (r'\bJoram Gillette\b', 'Jorel Guilloty'),
    (r'\bJurel Gillett\b', 'Jorel Guilloty'),
    # More Tainan Costa variations
    (r'\bThayman Costa\b', 'Tainan Costa'),
    (r'\bTain[aá] Costa\b', 'Tainan Costa'),
    # More Elvis Credendio variations
    (r'\bElvis Credente\b', 'Elvis Credendio'),
    # More Bruno Mendonca variations
    (r'\bBruno Bendon[cç]a\b', 'Bruno Mendonca'),
    (r'\bBruno Medon[cç]a\b', 'Bruno Mendonca'),
    # Andre Dibe variations (nome correto e Dibe)
    (r'\bAndr[eé] Dibb[ei]?\b', 'Andre Dibe'),
    (r'\bAndr[eé] Dib\b', 'Andre Dibe'),
    (r'\bAndr[eé] Dipe\b', 'Andre Dibe'),
    # Gustavo Cambauva variations
    (r'\bGustavo Cambu[aá]va\b', 'Gustavo Cambauva'),
    (r'\bGustavo Cambaurra\b', 'Gustavo Cambauva'),
    (r'\bGustavo Cambu[aá]\b', 'Gustavo Cambauva'),
    (r'\bGustavo Camba[uú]ba\b', 'Gustavo Cambauva'),
    (r'\bGustavo Cambuva\b', 'Gustavo Cambauva'),
    # Igor Alter/Altera -> Ygor Altero
    (r'\bIgor Alter\b', 'Ygor Altero'),
    (r'\bIgor Altera\b', 'Ygor Altero'),
    # Marcelo Garaldi Mota -> Marcelo Motta
    (r'\bMarcelo Garaldi\b', 'Marcelo Motta'),
    # Bruno Mendonça accent normalization
    (r'\bBruno Mendon[cç]a\b', 'Bruno Mendonca'),
    # Luiz Wadis/Valde -> probably Luiz Wadis (Santander analyst, not in canonical yet)
    # Ana Julia trailing "do"
    (r'\bAna J[uú]lia Zerkowski do\b', 'Ana Julia Zerkowski'),
    (r'\bAna J[uú]lia Zerkowski\b', 'Ana Julia Zerkowski'),
    # Jonathan Coutras variations
    (r'\bJonathan Coltras\b', 'Jonathan Coutras'),
    # Hugo Grassi variations
    (r'\bHugo Grasso\b', 'Hugo Grassi'),
    # Marcelo Motta - strip trailing prepositions captured in name
    (r'\bMarcelo Garaldi Mota\b', 'Marcelo Motta'),
    (r'\bMarcelo Morgan\b', 'Marcelo Motta'),
    # Mario Simplicio (Morgan Stanley)
    (r'\bM[aá]rio Simpl[ií]cio\b', 'Mario Simplicio'),
    # Gustavo Fabris/Fabricio (BTG Pactual) - normalize
    (r'\bGustavo Fabr[ií][cz]i?o?\b', 'Gustavo Fabris'),
    # Antonio Castrucci - accent normalization
    (r'\bAnt[oô]nio Castrucci\b', 'Antonio Castrucci'),
    (r'\bAnt[oô]nio Pascalli\b', 'Antonio Castrucci'),
    # Pedro Hajnal variations
    (r'\bPedro Rain[aá]\b', 'Pedro Hajnal'),
    (r'\bPedro Rajnal\b', 'Pedro Hajnal'),
    # Juan Janiton -> Ruan Argenton
    (r'\bJuan Janiton\b', 'Ruan Argenton'),
    # Luiz Capistrano accent
    (r'\bLu[ií]s Capistrano\b', 'Luiz Capistrano'),
    # Kiefer Kennedy variations
    (r'\bKi[eé]per Kennedy\b', 'Kiefer Kennedy'),
    (r'\bKiper\b(?=.*?Citi)', 'Kiefer Kennedy'),

    # Marcelo Audi (Cardinal Partners) - nome correto e Audi
    (r'\bMarcelo Aldi\b', 'Marcelo Audi'),
    # Luma Paias (UBS) - nome correto e Paias
    (r'\bLuma Payas\b', 'Luma Paias'),
    # Andre Canuto Baia (CEO Estrela da Manha) - unificar variantes
    (r'\bAndr[eé] Bahia\b', 'Andre Canuto Baia'),
    (r'\bAndr[eé] Baia\b', 'Andre Canuto Baia'),

    # --- Bancos (regex para pegar variantes com contexto) ---
    (r'\bdo CIT\b', 'do Citi'),
    (r'\bdo Ciri\b', 'do Citi'),
    (r'\bdo City\b', 'do Citi'),
    (r'\bJeep Morgan\b', 'JP Morgan'),
    (r'\bDeep Morgan\b', 'JP Morgan'),
]


def corrigir_nomes(texto: str, empresa: str) -> str:
    """Aplica correcoes de nomes globais e por empresa."""
    correcoes_emp = CORRECOES_POR_EMPRESA.get(empresa, {})
    for errado, correto in correcoes_emp.items():
        if errado != correto:
            texto = texto.replace(errado, correto)

    for errado, correto in CORRECOES_GLOBAIS.items():
        if errado != correto:
            texto = texto.replace(errado, correto)

    # Regex-based corrections (handles word boundaries properly)
    for pattern, replacement in CORRECOES_REGEX:
        texto = re.sub(pattern, replacement, texto)

    return texto


# ---------------------------------------------------------------------------
# 4. Deteccao de falantes (speaker attribution)
# ---------------------------------------------------------------------------

# The key insight: whisper transcriptions have newlines mid-sentence because
# whisper outputs subtitle-style segments. We need regexes that span newlines.

# Name pattern: 1-4 capitalized words. Excludes common non-name words.
_STOP_WORDS = {'Para', 'Por', 'Que', 'Com', 'Sem', 'Seu', 'Sua', 'Uma', 'Passo', 'Nossa',
               'Agora', 'Antes', 'Ainda', 'Assim', 'Bom', 'Boa', 'Esta', 'Este', 'Aqui',
               'Presidente', 'Diretor', 'Operador', 'Operadora',
               'Ele', 'Ela', 'Muito', 'Caso', 'Novo', 'Nova'}
_NAME_PAT = r'([A-Z\u00C0-\u00FF][a-z\u00E0-\u00FF]+(?:\s+[A-Z\u00C0-\u00FF][a-z\u00E0-\u00FF]+){0,3})'

# "passo/passar a palavra para [Name]"
PATTERN_PASSO_PALAVRA = re.compile(
    r'(?:gostaria\s+(?:agora\s+)?de\s+)?'
    r'(?:passo|passar)\s+a\s+palavra\s+'
    r'(?:aqui\s+)?'
    r'(?:para\s+o\s+|ao\s+|para\s+a?\s*)?'
    r'(?:(?:Sr\.|Sr|senhor|senhora|doutor|doutora)\s+)?'
    + _NAME_PAT,
    re.IGNORECASE | re.UNICODE,
)

PATTERN_PODE_PROSSEGUIR = re.compile(
    r'(?:pode\s+prosseguir|pode\s+seguir|pode\s+come[c\u00e7]ar)',
    re.IGNORECASE,
)

# "Nossa proxima pergunta vem de [Name], do/da [Bank]"
PATTERN_PROXIMA_PERGUNTA = re.compile(
    r'(?:Nossa\s+)?'
    r'(?:pr[o\u00f3]xima|primeira)\s+'
    r'pergunta\s+'
    r'(?:vem\s+(?:de|do|da)\s*|[e\u00e9]\s+(?:de|do|da)\s*|vai\s+para\s+(?:o\s+|a\s+)?)'
    r'(?:(?:Sr\.|Sr|senhor|senhora|Sra\.?)\s+)?'
    + _NAME_PAT,
    re.IGNORECASE | re.UNICODE,
)

# Direcional-style: "A proxima pergunta e do [Name], do [Bank]"
PATTERN_PERGUNTA_ALTERNATIVA = re.compile(
    r'(?:A\s+)?pr[o\u00f3]xima\s+pergunta\s+'
    r'(?:[e\u00e9]\s+)?'
    r'(?:d[oae]\s+)'
    r'(?:(?:Sr\.|Sr|senhor|senhora)\s+)?'
    + _NAME_PAT,
    re.IGNORECASE | re.UNICODE,
)

# Mouradubeux text-based Q&A: "A primeira pergunta aqui vem de [Name], agente da [Bank]"
PATTERN_PERGUNTA_TEXTO = re.compile(
    r'(?:pergunta\s+(?:aqui\s+)?(?:vem\s+de|[e\u00e9]\s+de)\s+)'
    + _NAME_PAT,
    re.IGNORECASE | re.UNICODE,
)

PATTERN_MICROFONE = re.compile(
    r'(?:seu\s+)?microfone\s+(?:est[a\u00e1]\s+)?(?:liberado|aberto)',
    re.IGNORECASE,
)

PATTERN_ENCERRA_QA = re.compile(
    r'(?:encerr[ae]mos|encerramos|est[a\u00e1]\s+encerrada)\s+'
    r'(?:neste\s+momento\s+)?'
    r'(?:a\s+)?sess[a\u00e3]o\s+de\s+perguntas',
    re.IGNORECASE,
)

PATTERN_INICIO_QA = re.compile(
    r'(?:iniciar(?:emos)?|daremos\s+in[i\u00ed]cio\s+[a\u00e0])\s+'
    r'(?:agora\s+)?'
    r'(?:a\s+)?(?:nossa\s+)?sess[a\u00e3]o\s+de\s+perguntas',
    re.IGNORECASE,
)

# Tenda/Direcional style: "[Name] da fila e o [analyst] do [bank]"
# or moderator says "primeiro da fila e o Mazini do Citi"
PATTERN_FILA = re.compile(
    r'(?:da\s+fila\s+[e\u00e9]\s+(?:o\s+|a\s+)?)'
    + _NAME_PAT,
    re.IGNORECASE | re.UNICODE,
)


def _is_valid_name(name: str) -> bool:
    """Check if extracted text looks like a real person name."""
    name = _ws(name)
    if not name or len(name) < 3:
        return False
    # Reject if it's a stop word or role title
    first_word = name.split()[0]
    if first_word in _STOP_WORDS:
        return False
    # Reject if contains obvious non-name phrases
    lower = name.lower()
    bad_phrases = ['considera', 'finais', 'encerr', 'pergunta', 'sessao', 'sessão',
                   'microfone', 'apresenta', 'telefone', 'companhia', 'trimestre',
                   'para as', 'para o', 'para a', 'que fa']
    if any(bp in lower for bp in bad_phrases):
        return False
    # Must have at least one word starting with uppercase letter
    if not re.search(r'[A-Z\u00C0-\u00FF]', name):
        return False
    return True


_KNOWN_BANKS_LOWER = ['itau', 'itaú', 'bradesco', 'safra', 'santander', 'btg',
                      'xp', 'jp morgan', 'goldman', 'ubs', 'citi', 'bank of',
                      'morgan', 'cardinal', 'estrela', 'genial', 'inter']


def _split_name_bank(nome: str) -> tuple:
    """Split 'Andre Dib do Itau BBA' into ('Andre Dib', 'Itau BBA').
    Only splits if the trailing part looks like a known bank."""
    m = re.match(r'^(.+?)\s+d[oae]\s+(.+)$', nome)
    if m:
        potential_bank = m.group(2)
        if any(kb in potential_bank.lower() for kb in _KNOWN_BANKS_LOWER):
            return m.group(1).strip(), potential_bank.strip()
    return nome, ""


def _extrair_nome_banco(texto: str, pos_nome_fim: int) -> str:
    """Tenta extrair banco/instituicao apos nome do analista."""
    trecho = _ws(texto[pos_nome_fim:pos_nome_fim + 120])
    m = re.search(
        r',?\s+d[oae]\s+([A-Z\u00C0-\u00FF][A-Za-z\u00C0-\u00FF\s&]+?)(?:\.|,|\s+(?:senhor|senhora|Sr|Sra|seu|sua|por|ele|ela|$))',
        trecho,
        re.UNICODE,
    )
    if m:
        banco = m.group(1).strip()
        banco = re.sub(r'\s+(senhor|senhora|Sr|Sra|seu|sua|por).*$', '', banco).strip()
        if 2 < len(banco) < 40:
            return banco
    return ""


def _encontrar_posicao_fala(texto: str, pos_transicao: int) -> int:
    """Encontra onde o falante comeca a falar apos a transicao.
    Busca 'pode prosseguir', 'microfone liberado', ou proxima sentenca."""
    trecho = texto[pos_transicao:pos_transicao + 400]

    m_prosseguir = PATTERN_PODE_PROSSEGUIR.search(trecho)
    m_mic = PATTERN_MICROFONE.search(trecho)

    candidates = []
    for m_marker in [m_prosseguir, m_mic]:
        if m_marker:
            end_pos = m_marker.end()
            rest = trecho[end_pos:]
            m_next = re.search(r'[\.\n]\s*([A-Z\u00C0-\u00FF])', rest)
            if m_next:
                candidates.append(end_pos + m_next.start() + 1)
            else:
                candidates.append(end_pos)

    if candidates:
        return pos_transicao + min(candidates)

    # Fallback: next sentence after a short skip
    m_next = re.search(r'[\.\n]\s*([A-Z\u00C0-\u00FF])', trecho[20:])
    if m_next:
        return pos_transicao + 20 + m_next.start() + 1
    return pos_transicao + min(len(trecho), 100)


class SpeakerTransition:
    """Represents a detected speaker transition."""
    def __init__(self, pos_insert: int, speaker_name: str, role: str = "",
                 bank: str = "", transition_type: str = ""):
        self.pos_insert = pos_insert
        self.speaker_name = _ws(speaker_name)  # normalize whitespace in name
        self.role = role
        self.bank = _normalize_bank(_ws(bank)) if bank else ""
        self.transition_type = transition_type

    def label(self) -> str:
        parts = [self.speaker_name]
        if self.role:
            parts.append(f" - {self.role}")
        if self.bank:
            parts.append(f" ({self.bank})")
        return f"\n\n**[{''.join(parts)}]:**\n"

    def __repr__(self):
        return f"Speaker({self.speaker_name}, role={self.role}, bank={self.bank}, type={self.transition_type}, pos={self.pos_insert})"


def _strip_accents(s: str) -> str:
    """Remove accents from string for fuzzy matching."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def _resolve_analyst_name(name_raw: str) -> tuple:
    """Resolve analyst name against ANALISTAS_CANONICOS.
    Returns (canonical_name, bank) or (original_name, '') if not found."""
    name = _ws(name_raw)

    # Strip trailing artifacts like "do Bank", "com Itaú", "que", "Bradesco" from captured name
    name = re.sub(r'\s+(?:do|da|de|com|que)\s+.*$', '', name).strip()
    # Strip trailing bank names that got captured
    name = re.sub(r'\s+(?:Bradesco|Santander|XP|BTG|Itaú?|JP|Goldman|UBS|Citi|Morgan|Safra|Bank)\b.*$', '', name).strip()

    name_lower = _strip_accents(name).lower()

    # Direct match on canonical name (accent-insensitive)
    for canonical, (banco, variacoes) in ANALISTAS_CANONICOS.items():
        if _strip_accents(canonical).lower() == name_lower:
            return canonical, banco
        # Match on variations
        for var in variacoes:
            if _strip_accents(var).lower() == name_lower:
                return canonical, banco

    # Partial match: check if extracted name is a substring of canonical or vice-versa
    for canonical, (banco, variacoes) in ANALISTAS_CANONICOS.items():
        canonical_norm = _strip_accents(canonical).lower()
        canonical_parts = canonical_norm.split()
        # Last name match (e.g. "Oreng" -> "Fanny Oreng")
        if len(canonical_parts) >= 2 and name_lower == canonical_parts[-1]:
            return canonical, banco
        # First name only match (only if unique enough, >= 5 chars)
        if len(canonical_parts) >= 2 and name_lower == canonical_parts[0] and len(name_lower) >= 5:
            return canonical, banco
        # First + partial last
        if name_lower in canonical_norm and len(name_lower) >= 6:
            return canonical, banco

    return name, ""


_BANK_NORMALIZATION = {
    "bradesco": "Bradesco BBI", "bradesco bbi": "Bradesco BBI", "bradesco bba": "Bradesco BBI",
    "bradesco pbi": "Bradesco BBI",
    "itau bba": "Itau BBA", "itaú bba": "Itau BBA", "itaú": "Itau BBA",
    "itau": "Itau BBA", "ibba": "Itau BBA", "itaú bda": "Itau BBA",
    "itaú beira": "Itau BBA",
    "btg": "BTG Pactual", "btg pactual": "BTG Pactual", "banco btg": "BTG Pactual",
    "banco btg pactual": "BTG Pactual",
    "santander": "Santander", "banco santander": "Santander",
    "safra": "Safra", "banco safra": "Safra", "safra rafael": "Safra",
    "jp morgan": "JP Morgan", "jpmorgan": "JP Morgan", "banco jp morgan": "JP Morgan",
    "goldman": "Goldman Sachs", "goldman sachs": "Goldman Sachs",
    "citi": "Citi", "citibank": "Citi", "speed": "Citi", "citi ": "Citi",
    "bank of america": "Bank of America", "bofa": "Bank of America",
    "ubs": "UBS",
    "xp": "XP", "xp investimentos": "XP",
    "morgan stanley": "Morgan Stanley", "morgan": "Morgan Stanley",
    "morgenstern": "Morgan Stanley",
    "credit suisse": "Credit Suisse", "credi suíce": "Credit Suisse",
    "cardinal": "Cardinal Partners", "cardinal partners": "Cardinal Partners",
    "estrela da manha": "Estrela da Manha",
    "kinea": "Kinea Investimentos", "kinea investimentos": "Kinea Investimentos",
    "vila rica": "Vila Rica Capital", "vila rica capital": "Vila Rica Capital",
    "tivio": "Tivio Capital", "tivio capital": "Tivio Capital",
    "warren": "Warren Investimentos", "warren investimentos": "Warren Investimentos",
    "barclays": "Barclays",
    "hsbc": "HSBC",
    "castro": "Itau BBA",  # Mariangela de Castro gets "Castro" extracted as bank
    "spf": "JP Morgan",
    "axp": "XP",
}


def _normalize_bank(banco: str) -> str:
    """Normalize bank name to canonical form."""
    if not banco:
        return banco
    lower = banco.strip().lower()
    return _BANK_NORMALIZATION.get(lower, banco)


def _resolve_exec_name(name_raw: str, empresa: str) -> tuple:
    """Resolve a name to canonical exec name and role.
    Returns (canonical_name, role) or (original_name, '') if not found."""
    execs = EXECUTIVOS_POR_EMPRESA.get(empresa, {})
    aliases = EXEC_ALIASES.get(empresa, {})
    name = _ws(name_raw)

    # Direct match on full name
    for exec_name, role in execs.items():
        if exec_name.lower() == name.lower() or name.lower() in exec_name.lower() or exec_name.lower() in name.lower():
            return exec_name, role

    # Try alias
    for alias, canonical in aliases.items():
        if alias.lower() == name.lower():
            role = execs.get(canonical, "")
            return canonical, role

    return name, ""


def detectar_falantes(texto: str, empresa: str) -> tuple:
    """
    Detecta transicoes de falantes no texto da transcricao.

    Returns:
        (texto_com_labels, speakers_json_string)
    """
    execs = EXECUTIVOS_POR_EMPRESA.get(empresa, {})
    aliases = EXEC_ALIASES.get(empresa, {})
    transitions = []

    # --- Opening: operator ---
    transitions.append(SpeakerTransition(
        pos_insert=0,
        speaker_name="Operador(a)",
        transition_type="abertura",
    ))

    # --- "passo a palavra" transitions ---
    for m in PATTERN_PASSO_PALAVRA.finditer(texto):
        nome_raw = _ws(m.group(1))

        # If we captured a title like "Presidente", look further ahead for actual name
        if not _is_valid_name(nome_raw):
            # Try to find a real name after this match
            after = texto[m.end():m.end() + 150]
            m_name = re.search(
                r'(?:Sr\.|Sr|senhor|senhora)\s+([A-Z\u00C0-\u00FF][a-z\u00E0-\u00FF]+(?:\s+[A-Z\u00C0-\u00FF][a-z\u00E0-\u00FF]+){0,3})',
                after,
                re.IGNORECASE | re.UNICODE,
            )
            if m_name:
                nome_raw = _ws(m_name.group(1))
            else:
                continue  # skip this transition if no valid name found

        if not _is_valid_name(nome_raw):
            continue

        nome, role = _resolve_exec_name(nome_raw, empresa)
        pos_fala = _encontrar_posicao_fala(texto, m.start())

        # Determine transition type
        ttype = "apresentacao"
        nearby = texto[max(0, m.start() - 120):m.start() + 50].lower()
        if ("considera" in nearby and "finais" in nearby) or "encerr" in nearby:
            ttype = "encerramento"

        transitions.append(SpeakerTransition(
            pos_insert=pos_fala,
            speaker_name=nome,
            role=role,
            transition_type=ttype,
        ))

    # --- "proxima pergunta vem de" (Q&A) ---
    for m in PATTERN_PROXIMA_PERGUNTA.finditer(texto):
        nome_analista = _ws(m.group(1))
        if not _is_valid_name(nome_analista):
            continue
        nome_analista, banco = _split_name_bank(nome_analista)
        if not banco:
            banco = _extrair_nome_banco(texto, m.end())
        # Resolve against canonical analyst dictionary
        nome_canonico, banco_canonico = _resolve_analyst_name(nome_analista)
        nome_analista = nome_canonico
        if banco_canonico:
            banco = banco_canonico
        pos_fala = _encontrar_posicao_fala(texto, m.start())

        transitions.append(SpeakerTransition(
            pos_insert=pos_fala,
            speaker_name=nome_analista,
            role="Analista",
            bank=banco,
            transition_type="qa_pergunta",
        ))

    # --- Alternative pattern (direcional-style) ---
    for m in PATTERN_PERGUNTA_ALTERNATIVA.finditer(texto):
        nome_analista = _ws(m.group(1))
        if not _is_valid_name(nome_analista):
            continue
        already_found = any(
            abs(t.pos_insert - m.start()) < 150 and t.transition_type == "qa_pergunta"
            for t in transitions
        )
        if already_found:
            continue
        nome_analista, banco = _split_name_bank(nome_analista)
        if not banco:
            banco = _extrair_nome_banco(texto, m.end())
        nome_canonico, banco_canonico = _resolve_analyst_name(nome_analista)
        nome_analista = nome_canonico
        if banco_canonico:
            banco = banco_canonico
        pos_fala = _encontrar_posicao_fala(texto, m.start())
        transitions.append(SpeakerTransition(
            pos_insert=pos_fala,
            speaker_name=nome_analista,
            role="Analista",
            bank=banco,
            transition_type="qa_pergunta",
        ))

    # --- Mouradubeux text Q&A pattern ---
    for m in PATTERN_PERGUNTA_TEXTO.finditer(texto):
        nome_analista = _ws(m.group(1))
        if not _is_valid_name(nome_analista):
            continue
        already_found = any(
            abs(t.pos_insert - m.start()) < 150 and t.transition_type == "qa_pergunta"
            for t in transitions
        )
        if already_found:
            continue
        m_bank_in_name = re.match(r'^(.+?)\s+d[oae]\s+(.+)$', nome_analista)
        banco = ""
        if m_bank_in_name:
            nome_analista = m_bank_in_name.group(1)
            banco = m_bank_in_name.group(2)
        if not banco:
            banco = _extrair_nome_banco(texto, m.end())
        nome_canonico, banco_canonico = _resolve_analyst_name(nome_analista)
        nome_analista = nome_canonico
        if banco_canonico:
            banco = banco_canonico
        transitions.append(SpeakerTransition(
            pos_insert=m.start(),
            speaker_name=nome_analista,
            role="Analista",
            bank=banco,
            transition_type="qa_pergunta",
        ))

    # --- Tenda "da fila" pattern ---
    for m in PATTERN_FILA.finditer(texto):
        nome_analista = _ws(m.group(1))
        if not _is_valid_name(nome_analista):
            continue
        already_found = any(
            abs(t.pos_insert - m.start()) < 150 and t.transition_type == "qa_pergunta"
            for t in transitions
        )
        if already_found:
            continue
        m_bank_in_name = re.match(r'^(.+?)\s+d[oae]\s+(.+)$', nome_analista)
        banco = ""
        if m_bank_in_name:
            nome_analista = m_bank_in_name.group(1)
            banco = m_bank_in_name.group(2)
        if not banco:
            banco = _extrair_nome_banco(texto, m.end())
        nome_canonico, banco_canonico = _resolve_analyst_name(nome_analista)
        nome_analista = nome_canonico
        if banco_canonico:
            banco = banco_canonico
        pos_fala = _encontrar_posicao_fala(texto, m.start())
        transitions.append(SpeakerTransition(
            pos_insert=pos_fala,
            speaker_name=nome_analista,
            role="Analista",
            bank=banco,
            transition_type="qa_pergunta",
        ))

    # --- "Encerramos a sessao" ---
    for m in PATTERN_ENCERRA_QA.finditer(texto):
        already_near = any(abs(t.pos_insert - m.start()) < 80 for t in transitions)
        if not already_near:
            transitions.append(SpeakerTransition(
                pos_insert=m.start(),
                speaker_name="Operador(a)",
                transition_type="encerramento_qa",
            ))

    # Sort all transitions by position
    transitions.sort(key=lambda t: t.pos_insert)

    # --- Infer executive responses after analyst questions ---
    qa_transitions = [t for t in transitions if t.transition_type == "qa_pergunta"]
    new_response_transitions = []

    for qa_t in qa_transitions:
        # Region between this Q and the next transition
        next_after = [t for t in transitions if t.pos_insert > qa_t.pos_insert + 30]
        end_region = next_after[0].pos_insert if next_after else len(texto)
        region = texto[qa_t.pos_insert:end_region]

        if len(region) < 100:
            continue

        best_pos = None
        best_name = None
        best_role = None

        # Strategy: look for exec full names or aliases being mentioned/addressed
        all_names = {}
        for exec_name, role in execs.items():
            all_names[exec_name] = (exec_name, role)
        for alias, canonical in aliases.items():
            if len(alias) >= 4:
                all_names[alias] = (canonical, execs.get(canonical, ""))

        for search_name, (canonical, role) in all_names.items():
            # Look for the pattern where analyst finishes and exec starts
            # Common: "Obrigado, [name]" or "[name], obrigado" marks analyst done
            # Or just find exec name after a reasonable offset (after the question)
            mentions = list(re.finditer(re.escape(search_name), region, re.IGNORECASE))
            for em in mentions:
                # Skip mentions very close to start (analyst greeting the exec)
                if em.start() < 80:
                    continue
                # Look for a sentence boundary before this mention
                before = region[max(0, em.start() - 150):em.start()]
                # Find the last sentence end in `before`
                sent_ends = list(re.finditer(r'[.\n]', before))
                if sent_ends:
                    last_end = sent_ends[-1].end()
                    response_pos = qa_t.pos_insert + max(0, em.start() - 150) + last_end
                    if best_pos is None or response_pos < best_pos:
                        best_pos = response_pos
                        best_name = canonical
                        best_role = role
                break  # Only check first relevant exec

        if best_pos and best_name:
            overlap = any(abs(t.pos_insert - best_pos) < 40 for t in transitions + new_response_transitions)
            if not overlap:
                new_response_transitions.append(SpeakerTransition(
                    pos_insert=best_pos,
                    speaker_name=best_name,
                    role=best_role,
                    transition_type="qa_resposta",
                ))

    transitions.extend(new_response_transitions)
    transitions.sort(key=lambda t: t.pos_insert)

    # --- Remove duplicate/overlapping transitions ---
    filtered = []
    for t in transitions:
        if not filtered or abs(t.pos_insert - filtered[-1].pos_insert) > 15:
            filtered.append(t)
        else:
            # Keep the more informative one
            if t.role and not filtered[-1].role:
                filtered[-1] = t
    transitions = filtered

    # --- Insert labels into text (work backwards) ---
    texto_labeled = texto
    for t in reversed(transitions):
        pos = t.pos_insert
        label = t.label()
        texto_labeled = texto_labeled[:pos] + label + texto_labeled[pos:]

    # --- Build speakers list ---
    speakers = []
    seen = set()
    for t in transitions:
        key = t.speaker_name
        if key not in seen:
            seen.add(key)
            info = {"nome": t.speaker_name, "role": t.role}
            if t.bank:
                info["banco"] = t.bank
            speakers.append(info)

    return texto_labeled, json.dumps(speakers, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 5. Pipeline principal
# ---------------------------------------------------------------------------

def processar_transcricao(texto: str, empresa: str) -> tuple:
    """Pipeline completo de pos-processamento."""
    texto = corrigir_nomes(texto, empresa)
    texto = normalizar_periodos(texto)
    texto_com_speakers, speakers_json = detectar_falantes(texto, empresa)
    return texto_com_speakers, speakers_json


def garantir_colunas(conn: sqlite3.Connection):
    """Cria colunas texto_processado e speakers_detectados se nao existirem."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(transcricoes)")
    colunas = {row[1] for row in cur.fetchall()}

    if "texto_processado" not in colunas:
        cur.execute("ALTER TABLE transcricoes ADD COLUMN texto_processado TEXT")
        print("Coluna 'texto_processado' criada.")
    if "speakers_detectados" not in colunas:
        cur.execute("ALTER TABLE transcricoes ADD COLUMN speakers_detectados TEXT")
        print("Coluna 'speakers_detectados' criada.")
    conn.commit()


def main():
    import sys as _sys
    # Force UTF-8 output on Windows
    if hasattr(_sys.stdout, 'reconfigure'):
        _sys.stdout.reconfigure(encoding='utf-8')

    filtro_empresa = None
    reprocessar = False

    args = sys.argv[1:]
    for arg in args:
        if arg == "--reprocessar":
            reprocessar = True
        elif not arg.startswith("-"):
            filtro_empresa = arg.lower()

    if not DB_PATH.exists():
        print(f"ERRO: Banco de dados nao encontrado em {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    garantir_colunas(conn)
    cur = conn.cursor()

    query = "SELECT id, empresa, periodo, texto_completo, texto_processado FROM transcricoes WHERE modelo_whisper = 'large-v3'"
    params = []
    if filtro_empresa:
        query += " AND empresa = ?"
        params.append(filtro_empresa)
    if not reprocessar:
        query += " AND (texto_processado IS NULL OR texto_processado = '')"

    cur.execute(query, params)
    registros = cur.fetchall()

    if not registros:
        print("Nenhum registro para processar.")
        if not reprocessar:
            print("Use --reprocessar para reprocessar registros ja feitos.")
        return

    print(f"Processando {len(registros)} transcricoes...")
    print("=" * 70)

    resultados = []
    for reg in registros:
        rid = reg["id"]
        empresa = reg["empresa"]
        periodo = reg["periodo"]
        texto = reg["texto_completo"]

        if not texto:
            print(f"  [{rid}] {empresa} {periodo} - SEM TEXTO, pulando.")
            continue

        texto_processado, speakers_json = processar_transcricao(texto, empresa)

        conn.execute(
            "UPDATE transcricoes SET texto_processado = ?, speakers_detectados = ? WHERE id = ?",
            (texto_processado, speakers_json, rid),
        )

        speakers = json.loads(speakers_json)
        n_speakers = len(speakers)
        n_analistas = sum(1 for s in speakers if s.get("role") == "Analista")

        resultados.append({
            "id": rid,
            "empresa": empresa,
            "periodo": periodo,
            "n_speakers": n_speakers,
            "n_analistas": n_analistas,
            "speakers": speakers,
            "texto_preview": texto_processado,
        })

        print(f"  [{rid}] {empresa} {periodo} -> {n_speakers} speakers ({n_analistas} analistas)")

    conn.commit()
    print("=" * 70)
    print(f"Processados {len(resultados)} registros. Dados salvos em texto_processado e speakers_detectados.")

    # --- Exibir amostra detalhada para 2+ empresas ---
    # Pick MRV and Cyrela if available, else first 2 distinct
    priority = ["mrv", "cyrela", "tenda", "cury", "direcional", "planoeplano", "mouradubeux"]
    empresas_mostradas = set()
    amostras = []
    for prio in priority:
        if len(amostras) >= 3:
            break
        for res in resultados:
            if res["empresa"] == prio and prio not in empresas_mostradas:
                amostras.append(res)
                empresas_mostradas.add(prio)
                break

    for res in amostras:
        print()
        print("=" * 70)
        print(f"AMOSTRA: {res['empresa'].upper()} - {res['periodo']} (ID={res['id']})")
        print("=" * 70)

        print(f"\nSpeakers detectados ({res['n_speakers']}):")
        for s in res["speakers"]:
            banco_str = f" ({s['banco']})" if s.get("banco") else ""
            role_str = f" - {s['role']}" if s.get("role") else ""
            print(f"  - {s['nome']}{role_str}{banco_str}")

        texto_p = res["texto_preview"]

        # Show opening (first speaker labels)
        print(f"\nPreview da abertura:")
        print("-" * 50)
        # Show first 600 chars
        print(texto_p[:600])
        print("-" * 50)

        # Show Q&A section preview
        m_qa = re.search(r'\*\*\[.+?Analista.+?\]:\*\*', texto_p)
        if m_qa:
            start = max(0, m_qa.start() - 30)
            second_analyst = re.search(r'\*\*\[.+?Analista.+?\]:\*\*', texto_p[m_qa.end():])
            if second_analyst:
                end = m_qa.end() + second_analyst.end() + 500
            else:
                end = m_qa.end() + 800
            end = min(end, len(texto_p))
            print(f"\nPreview do Q&A:")
            print("-" * 50)
            print(texto_p[start:end])
            print("-" * 50)

    conn.close()
    print("\nConcluido!")


if __name__ == "__main__":
    main()

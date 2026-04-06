"""
Configurações do Dashboard de Inteligência Competitiva.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "empreendimentos.db")

# Cores por empresa (do gerar_mapa.py)
CORES = {
    "MRV": "#e74c3c", "Cury": "#3498db", "Plano&Plano": "#2ecc71",
    "Magik JC": "#9b59b6", "Vivaz": "#f39c12", "VIC Engenharia": "#1abc9c",
    "Direcional": "#e67e22", "Metrocasa": "#34495e", "Conx": "#c0392b",
    "Pacaembu": "#27ae60", "HM Engenharia": "#2980b9", "Grafico": "#8e44ad",
    "Kazzas": "#d35400", "Viva Benx": "#16a085", "Novolar": "#7f8c8d",
    "Vibra Residencial": "#f1c40f", "Mundo Apto": "#6c3483", "Riformato": "#2c3e50",
    "Vinx": "#a93226", "Emccamp": "#148f77", "SUGOI": "#b7950b",
    "Árbore": "#633974", "Econ Construtora": "#1a5276", "ACL Incorporadora": "#cb4335",
    "Vasco Construtora": "#117a65", "ACLF": "#d4ac0d", "Novvo": "#839192",
    "Stanza": "#a04000", "Sousa Araujo": "#1b4f72", "Smart Construtora": "#7d3c98",
    "Rev3": "#196f3d", "Carrilho": "#b03a2e", "BM7": "#1f618d",
    "EPH": "#b9770e", "FYP Engenharia": "#17202a", "Jotanunes": "#78281f",
    "M.Lar": "#4a235a", "Construtora Open": "#0e6655", "Graal Engenharia": "#7e5109",
    "Cavazani": "#154360", "BP8": "#641e16", "Ún1ca": "#0b5345",
    "Ampla": "#784212",
    # Novas empresas (2026-04)
    "Tenda": "#e63946", "Vitta Residencial": "#457b9d", "EBM": "#2a9d8f",
    "Trisul": "#264653", "CAC": "#e9c46a", "MGF": "#f4a261",
    "Vega": "#606c38", "Canopus": "#283618", "Quartzo": "#dda15e",
    "SR Engenharia": "#bc6c25", "Canopus Construcoes": "#540b0e",
    "AP Ponto": "#9b2226", "Exata": "#005f73", "Somos": "#0a9396",
    "Vila Brasil": "#94d2bd", "Cosbat": "#ee9b00", "Belmais": "#ca6702",
    "Victa Engenharia": "#bb3e03", "SOL Construtora": "#ae2012",
    "Morana": "#3d405b", "Lotus": "#81b29a", "House Inc": "#f2cc8f",
    "ART Construtora": "#e07a5f", "Maccris": "#3a0ca3", "Bora": "#7209b7",
    "Domma": "#560bad", "Grupo Delta": "#480ca8", "Dimensional": "#b5179e",
    "Mirantes": "#f72585", "Torreao Villarim": "#4361ee",
    "Versati": "#4cc9f0", "Sertenge": "#4895ef", "Tenório Simões": "#3f37c9",
}

FASES_ORDEM = [
    "Breve Lançamento", "Lançamento", "Em Construção",
    "Pronto", "100% Vendido",
]

FASE_CORES = {
    "Breve Lançamento": "#95a5a6",
    "Lançamento": "#3498db",
    "Em Construção": "#f39c12",
    "Pronto": "#2ecc71",
    "100% Vendido": "#e74c3c",
}

LAZER_COLUNAS = [
    "lazer_piscina", "lazer_churrasqueira", "lazer_fitness", "lazer_playground",
    "lazer_brinquedoteca", "lazer_salao_festas", "lazer_pet_care", "lazer_coworking",
    "lazer_bicicletario", "lazer_quadra", "lazer_delivery", "lazer_horta",
    "lazer_lavanderia", "lazer_redario", "lazer_rooftop", "lazer_sauna",
    "lazer_spa", "lazer_piquenique", "lazer_sport_bar", "lazer_cine",
    "lazer_easy_market", "lazer_espaco_beleza", "lazer_sala_estudos",
    "lazer_espaco_gourmet", "lazer_praca", "lazer_solarium", "lazer_sala_jogos",
    "lazer_varanda",
]

APTO_COLUNAS = [
    "apto_studio", "apto_1_dorm", "apto_2_dorms", "apto_3_dorms",
    "apto_suite", "apto_giardino", "apto_duplex",
    "apto_cobertura", "apto_vaga_garagem",
]

LAZER_NOMES = {c: c.replace("lazer_", "").replace("_", " ").title() for c in LAZER_COLUNAS}
APTO_NOMES = {
    "apto_studio": "ST", "apto_1_dorm": "1D", "apto_2_dorms": "2D", "apto_3_dorms": "3D",
    "apto_suite": "Suíte", "apto_giardino": "Garden", "apto_duplex": "Duplex",
    "apto_cobertura": "Cobertura", "apto_vaga_garagem": "Vaga",
}

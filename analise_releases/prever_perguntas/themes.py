"""Detector de temas dominantes, surpresas e divergencias vs guidance.

Implementa as licoes 1, 3 e 5 do backtest Tenda 4T25:
1. Tema dominante: quando ha surpresa negativa forte, concentrar 60%+ em um unico tema.
3. Curto prazo > estrutural: temas de surpresa imediata pesam mais que estruturais.
5. Flag automatico de numeros divergentes do guidance vira "tema obrigatorio".

Heuristica e proposital. O LLM faz o juizo final no prompt; aqui so produzimos
sinais textuais que orientam a analise.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# --- Sinais de surpresa negativa forte (curto prazo) ---------------------------
PADROES_DESVIO_CUSTO = re.compile(
    r"(desvio\s+de\s+custo|estouro\s+de\s+custo|custo\s+(adicional|nao\s+previsto)|"
    r"(adicional|extra)\s+de\s+R\$\s*[\d\.,]+\s*(MM|milh[oõ]es))",
    re.IGNORECASE,
)
PADROES_PROVISAO = re.compile(
    r"(provis[aã]o.{0,40}(aumentou|elevou|de\s+\d+[\.,]?\d*%)|"
    r"refor[cç]o\s+de\s+provis[aã]o)",
    re.IGNORECASE,
)
PADROES_REVISAO_NEG = re.compile(
    r"(revis[aã]o.{0,30}(para\s+baixo|negativ)|guidance.{0,30}(revisado|nao\s+atingido)|"
    r"abaixo\s+do\s+guidance)",
    re.IGNORECASE,
)
PADROES_QUEDA = re.compile(
    r"(queda\s+de\s+\d+[\.,]?\d*%|caiu\s+\d+[\.,]?\d*%|"
    r"redu[cç][aã]o\s+de\s+R\$\s*[\d\.,]+)",
    re.IGNORECASE,
)

# --- Temas estruturais (peso menor em call com surpresa forte, licao 3) -------
TEMAS_ESTRUTURAIS = (
    "reforma tributaria", "iva", "cbs 2027", "longo prazo",
    "estrutural", "tese de", "endgame",
)

# --- Numeros chave a serem capturados ------------------------------------------
PADRAO_VALOR = re.compile(
    r"R\$\s*([\d]+(?:[\.,]\d+)?)\s*(MM|mi|milh[oõ]es|bi|bilh[oõ]es)",
    re.IGNORECASE,
)
PADRAO_PCT = re.compile(r"(\d+[\.,]?\d*)\s*%")


@dataclass
class FlagTema:
    """Um sinal extraido do release sobre um possivel tema dominante."""

    rotulo: str
    trecho: str
    severidade: str  # "alta" | "media" | "baixa"
    tipo: str  # "desvio_custo" | "provisao" | "revisao_negativa" | "queda" | "divergencia_guidance"


@dataclass
class AnaliseTemas:
    """Resultado da analise de temas do release."""

    flags: list[FlagTema] = field(default_factory=list)
    tema_dominante: str | None = None
    surpresa_negativa_forte: bool = False
    temas_obrigatorios: list[str] = field(default_factory=list)


def _trecho_em_volta(texto: str, m: re.Match, janela: int = 200) -> str:
    inicio = max(0, m.start() - janela)
    fim = min(len(texto), m.end() + janela)
    return texto[inicio:fim].strip().replace("\n", " ")


def detectar_flags(texto_release: str) -> list[FlagTema]:
    """Procura padroes de surpresa negativa no release. Retorna lista de flags."""
    flags: list[FlagTema] = []
    for m in PADROES_DESVIO_CUSTO.finditer(texto_release):
        flags.append(FlagTema(
            rotulo="Desvio de custo",
            trecho=_trecho_em_volta(texto_release, m),
            severidade="alta",
            tipo="desvio_custo",
        ))
    for m in PADROES_PROVISAO.finditer(texto_release):
        flags.append(FlagTema(
            rotulo="Provisao elevada",
            trecho=_trecho_em_volta(texto_release, m),
            severidade="alta",
            tipo="provisao",
        ))
    for m in PADROES_REVISAO_NEG.finditer(texto_release):
        flags.append(FlagTema(
            rotulo="Revisao negativa de guidance",
            trecho=_trecho_em_volta(texto_release, m),
            severidade="alta",
            tipo="revisao_negativa",
        ))
    for m in PADROES_QUEDA.finditer(texto_release):
        flags.append(FlagTema(
            rotulo="Queda relevante",
            trecho=_trecho_em_volta(texto_release, m),
            severidade="media",
            tipo="queda",
        ))
    return flags


def detectar_divergencias_guidance(
    texto_release: str,
    guidance_referencia: dict[str, tuple[float, float]] | None = None,
    tolerancia_pct: float = 5.0,
) -> list[FlagTema]:
    """Compara numeros do release com guidance prévio.

    `guidance_referencia` e um dict {metrica: (min, max)} (em pontos percentuais
    para margens, em milhoes para valores absolutos). Se nao informado, retorna
    [] — usuario passa quando tem o guidance estruturado.
    """
    if not guidance_referencia:
        return []
    flags: list[FlagTema] = []
    for metrica, (low, high) in guidance_referencia.items():
        # busca o numero proximo da palavra-chave da metrica no release
        idx = texto_release.lower().find(metrica.lower())
        if idx == -1:
            continue
        janela = texto_release[idx: idx + 400]
        for m_pct in PADRAO_PCT.finditer(janela):
            val = float(m_pct.group(1).replace(",", "."))
            if val < low - tolerancia_pct or val > high + tolerancia_pct:
                flags.append(FlagTema(
                    rotulo=f"{metrica} fora do guidance ({low}-{high}%)",
                    trecho=janela.strip().replace("\n", " "),
                    severidade="alta",
                    tipo="divergencia_guidance",
                ))
                break
    return flags


def analisar(
    texto_release: str,
    guidance_referencia: dict[str, tuple[float, float]] | None = None,
) -> AnaliseTemas:
    """Pipeline completo: detecta flags, decide tema dominante e surpresa forte."""
    flags = detectar_flags(texto_release) + detectar_divergencias_guidance(
        texto_release, guidance_referencia
    )

    # Surpresa negativa forte: 2+ flags de severidade alta OU 1 desvio de custo
    altas = [f for f in flags if f.severidade == "alta"]
    surpresa_forte = len(altas) >= 2 or any(
        f.tipo == "desvio_custo" for f in flags
    )

    # Tema dominante: o rotulo que mais aparece entre flags altas
    tema_dominante: str | None = None
    if altas:
        contagem: dict[str, int] = {}
        for f in altas:
            contagem[f.rotulo] = contagem.get(f.rotulo, 0) + 1
        tema_dominante = max(contagem.items(), key=lambda x: x[1])[0]

    # Temas obrigatorios: todos os rotulos de divergencia + os 2 com mais ocorrencias
    obrigatorios: list[str] = []
    for f in flags:
        if f.tipo == "divergencia_guidance" and f.rotulo not in obrigatorios:
            obrigatorios.append(f.rotulo)
    if tema_dominante and tema_dominante not in obrigatorios:
        obrigatorios.insert(0, tema_dominante)

    return AnaliseTemas(
        flags=flags,
        tema_dominante=tema_dominante,
        surpresa_negativa_forte=surpresa_forte,
        temas_obrigatorios=obrigatorios,
    )

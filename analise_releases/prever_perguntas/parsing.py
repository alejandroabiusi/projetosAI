"""Parsing de transcricoes processadas e tags de speaker."""
from __future__ import annotations

import re

SPEAKER_PATTERN = re.compile(r'\*\*\[([^\]]+)\]:\*\*')
ANALYST_PATTERN = re.compile(r'(.+?)\s*-\s*Analista(?:\s*\(([^)]+)\))?')


def parse_speaker_tag(tag: str) -> tuple[str, str] | None:
    """Recebe 'Nome - Analista (Banco)' e devolve (nome, banco)."""
    m = ANALYST_PATTERN.match(tag)
    if not m:
        return None
    nome = m.group(1).strip()
    banco = (m.group(2) or "?").strip()
    return nome, banco


def extrair_perguntas_analistas(texto_processado: str) -> dict[str, str]:
    """Extrai blocos de fala de analistas de um texto processado.

    Retorna {f"{nome}|{banco}": texto_concatenado}.
    """
    blocks = SPEAKER_PATTERN.split(texto_processado)
    result: dict[str, str] = {}
    for i in range(1, len(blocks) - 1, 2):
        speaker_tag = blocks[i].strip()
        content = blocks[i + 1].strip()
        if "Analista" not in speaker_tag:
            continue
        parsed = parse_speaker_tag(speaker_tag)
        if parsed is None:
            continue
        nome, banco = parsed
        key = f"{nome}|{banco}"
        if key in result:
            result[key] += "\n\n" + content
        else:
            result[key] = content
    return result

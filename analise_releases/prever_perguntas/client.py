"""Cliente Anthropic para gerar previsoes via Claude Opus 4.7.

Usa adaptive thinking + effort high + prompt caching no bloco compartilhado
(release + analise de temas + pool), que e identico em todas as chamadas do
trimestre. Espera-se ~90% de economia em cache_read apos a primeira chamada.

Migrado de claude-sonnet-4-20250514 para claude-opus-4-7 em 2026-04-29.
"""
from __future__ import annotations

from dataclasses import dataclass

import anthropic

MODEL = "claude-opus-4-7"
MAX_TOKENS = 16000


@dataclass
class PrevisaoUsage:
    """Tokens consumidos por chamada (util pra auditar caching)."""

    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


def gerar_previsao(
    bloco_compartilhado: str,
    bloco_analista: str,
    *,
    client: anthropic.Anthropic | None = None,
    model: str = MODEL,
    max_tokens: int = MAX_TOKENS,
    use_thinking: bool = True,
    system_prompt: str | None = None,
) -> tuple[str, PrevisaoUsage]:
    """Gera previsao para um analista. Retorna (texto, usage).

    O `bloco_compartilhado` recebe `cache_control: ephemeral` — fica em cache
    por ~5 minutos e e reutilizado nas chamadas seguintes do mesmo trimestre.
    O `bloco_analista` muda a cada chamada e fica fora do cache.
    """
    client = client or anthropic.Anthropic()

    from .prompt import SYSTEM_PROMPT
    sys_prompt = system_prompt if system_prompt is not None else SYSTEM_PROMPT

    kwargs: dict = {
        "model": model,
        "max_tokens": max_tokens,
        "system": [
            {
                "type": "text",
                "text": sys_prompt,
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": bloco_compartilhado,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {"type": "text", "text": bloco_analista},
                ],
            }
        ],
    }
    if use_thinking:
        kwargs["thinking"] = {"type": "adaptive"}
        kwargs["output_config"] = {"effort": "high"}

    msg = client.messages.create(**kwargs)

    texto = ""
    for block in msg.content:
        if block.type == "text":
            texto += block.text

    usage = PrevisaoUsage(
        input_tokens=msg.usage.input_tokens,
        output_tokens=msg.usage.output_tokens,
        cache_creation_input_tokens=getattr(msg.usage, "cache_creation_input_tokens", 0) or 0,
        cache_read_input_tokens=getattr(msg.usage, "cache_read_input_tokens", 0) or 0,
    )
    return texto, usage

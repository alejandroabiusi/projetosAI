"""Previsao de perguntas em earnings calls de incorporadoras (Claude Opus 4.7).

Pacote refatorado em 2026-04-29 a partir do script monolitico prever_perguntas.py,
com as 5 licoes do backtest Tenda 4T25 incorporadas (themes.py + pool.py).
"""
from .cli import gerar_previsoes, main

__all__ = ["gerar_previsoes", "main"]

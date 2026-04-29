"""Shim de compatibilidade: a logica vive em prever_perguntas/ (pacote).

Mantido para que os comandos antigos continuem funcionando:
    python prever_perguntas.py tenda downloads/tenda/releases/Tenda_Release_4T2025.pdf

A migracao para Opus 4.7 e a refatoracao em modulos ocorreram em 2026-04-29.
"""
from prever_perguntas.cli import main

if __name__ == "__main__":
    main()

# Correção de geoloc — Baliza

**Data:** 2026-04-29

## Bug
8 produtos com coord exata `(-29.945, -50.99218)` — região Porto Alegre/Vale dos Sinos.

## Resultado
- **3 atualizados** via iframe `q=ENDEREÇO` + base de CEPs
- **5 NULLificados** (rua não casou ou bbox falhou)

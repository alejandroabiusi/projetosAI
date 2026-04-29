# Correção de geoloc — ACL Incorporadora

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_acl_*`

## Bug
8 produtos com coord exata `(-23.52903, -46.5497)` — sede ACL Tatuapé.

## Resultado
- **0 atualizados** (Selenium provavelmente bloqueado pelo site)
- **8 NULLificados** (coord falsa removida)

## Pendência
Investigar com fonte alternativa quando bloqueio do site for resolvido.

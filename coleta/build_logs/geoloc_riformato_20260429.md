# Correção de geoloc — Riformato — BLOQUEADA

**Data:** 2026-04-29
**Status:** Site bloqueando todas as requisições (urllib + Selenium retornam `ERR_CONNECTION_RESET`).

## Bug
18 produtos com coord exata `(-23.54987, -46.53858)` (sede ou ponto fixo). Não foi possível inspecionar páginas para corrigir.

## Próximos passos
- Tentar de outra rede/IP (talvez bloqueio por região/datacenter)
- Considerar API/cache se existir
- Aguardar; revisitar

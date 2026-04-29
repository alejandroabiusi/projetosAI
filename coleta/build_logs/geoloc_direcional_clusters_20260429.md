# Correção de geoloc — Direcional (clusters)

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_direcional2_*`

## Bug
Direcional tem ~64 produtos. 3 clusters de coord chumbada:
- 9x (-23.59785, -46.68073) — SP
- 4x (-3.0003, -60.00271) — Manaus
- 3x (-3.74431, -38.47874) — Caucaia/Fortaleza

Total 16 produtos com coord falsa.

## Resultado
- **15 produtos atualizados** via Selenium + iframe `!2d!3d` ou logradouro/CEP da página + bbox QA
- **1 NULLificado**

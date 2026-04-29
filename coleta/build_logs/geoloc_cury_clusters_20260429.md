# Correção de geoloc — Cury (top 3 clusters)

**Data:** 2026-04-29

## Bug
Cury tem 271 produtos. 16 clusters com 3+ produtos em coord exata. Os 3 maiores:
- 6x (-23.69088, -46.68647) SP
- 6x (-23.52635, -46.38502) SP
- 5x (-22.81146, -43.36456) RJ

Total: **17 produtos** atacados.

## Resultado
- **5 atualizados**
- **12 NULLificados** (Selenium parcial; ruas não casaram na base)

## Pendência
Restantes 13 clusters com 3-4 produtos cada (~46 produtos) ainda preservam coord chumbada. Atacar em próximo ciclo se confirmado bug.

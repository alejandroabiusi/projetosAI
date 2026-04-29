# Correção de geoloc — Tenda (clusters chumbados)

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_tenda2_*`

## Bug
Tenda tem 440 produtos no total. 4 clusters com coord exata repetida (sinal de coord chumbada por região):
- 11x (-22.90295, -43.55913) — RJ
- 10x (-12.84699, -38.35593) — Salvador
- 5x (-23.58154, -46.79135) — SP
- 5x (-23.41212, -46.39387) — SP

Total: **31 produtos** com coord falsa de "centro de cluster".

## Resultado
Apenas os 31 produtos com coord chumbada foram tocados.
- **5 produtos atualizados** com coord real (via Selenium + extração rua "X, NN" + base local de CEPs + bbox QA)
- **26 NULLificados** (rua não casou na base de CEPs OU coord caiu fora do bbox da cidade)

## Pendência
- Restantes 409 produtos Tenda com coord específica preservados (NÃO tocados)
- 26 NULL precisam fonte alternativa

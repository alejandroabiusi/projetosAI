# Diagnóstico geral de geoloc — todas as empresas

**Data:** 2026-04-29
**Total empresas:** 205 | **Total produtos:** 6.197 | **Mapa:** 5.644 pins (com coord)

## Achados

### A) Empresas com coord EXATAMENTE duplicada (sede/centroide) — caso grave

> "Várias linhas com a mesma lat/lon (5 casas decimais)" = sinal direto de coord chumbada/herdada.

| Empresa | Coord | Produtos no ponto |
|---------|-------|---|
| Eztec | (-23.59548, -46.63841) | **34** |
| MBigucci | (-23.66936, -46.56279) | **27** |
| Solum | (-31.76499, -52.31703) | **25** |
| Riformato | (-23.54987, -46.53858) | 18 |
| Estação 1 | (-12.22498, -38.90677) | 16 |
| Longitude | (-22.88244, -47.19772) | 15 |
| Prohidro | (-23.48963, -47.4504) | 14 |
| Vega | (-16.71802, -49.27674) | 14 |
| Tenda | (-22.90295, -43.55913) | 11 |
| Tenda | (-12.84699, -38.35593) | 10 |
| DMC, Direcional, Habras | varia | 9 cada |
| ACL Incorporadora, Baliza, Estação 1, MP | varia | 8 cada |
| ART, Cury (3 pontos), Exata, Citâ, Geratriz, Longitude, Objeto, SOL, SUGOI, Smart | varia | 5–6 cada |

### B) Empresas com raio máximo < 2 km dentro da mesma cidade (suspeita de coord aproximada)

68 clusters em 34 empresas. Destaques pelo volume:

| Empresa | Cidade | Produtos | Raio máx |
|---------|--------|---------:|---------:|
| Vitta Residencial | Ribeirão Preto | 35 | 1.340 m |
| VIC Engenharia | Contagem | 30 | 449 m |
| Solum | Pelotas | 25 | 0 m |
| Quartzo | Belo Horizonte | 22 | 1.306 m |
| MBigucci | São Bernardo do Campo | 21 | 0 m |
| SR Engenharia | Caucaia | 21 | 1.312 m |
| VIC Engenharia | Votorantim | 11 | 476 m |
| FYP Engenharia | São Paulo | 13 | 1.386 m |
| Cosbat | Salvador | 12 | 1.206 m |
| VIC Engenharia | Indaiatuba | 10 | 456 m |
| ... | | | |

VIC Engenharia aparece em **18 cidades** com raio < 500 m — bug claramente sistêmico no parser/empresa.

### C) Empresas com volume alto e parecem OK (raio > 2 km)
MRV, Tenda (geral), Cury (geral), Plano&Plano, Magik JC, Metrocasa, Helbor, Mitre, Trisul, NVR — distribuição parece razoável, mas validar amostra ainda assim.

## Priorização recomendada

| Prioridade | Critério | Empresas alvo |
|---:|---|---|
| **P0** | Coord exata fixa, >=15 produtos | Eztec, MBigucci, Solum, Riformato, Estação 1, Longitude, Prohidro, Vega |
| **P1** | Cluster <2km com >=10 produtos OU coord exata fixa em 8–14 produtos | VIC Engenharia, Vitta Residencial, Quartzo, SR Engenharia, FYP, Cosbat, Tenda (RJ/Salvador), Direcional, Habras, DMC |
| **P2** | Cluster <2km com 3–9 produtos | demais 34 empresas suspeitas |
| **P3** | Volume alto sem suspeita aparente | MRV, Cury, Plano&Plano, Magik JC, Metrocasa, Helbor, Mitre, Trisul (validar amostra) |

## Próximo passo
Aplicar a hierarquia de 4 níveis (`feedback_hierarquia_geoloc.md`) empresa por empresa, começando pela P0. Cada empresa: inspeção do HTML, ajuste do parser, re-extração das coords, regenerar mapa.

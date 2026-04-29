# Auditoria de geoloc — Quartzo — FALSO POSITIVO

**Data:** 2026-04-29
**Status:** Não houve alteração. Diagnóstico geral marcou cluster de 1.3 km em 22 produtos BH como suspeito.

## Investigação
Coords distintas (26 coords únicas em 5 casas decimais entre os produtos BH). Não há coord chumbada. A Quartzo apenas atua predominantemente num bairro específico de Belo Horizonte (lat ~-19.89 a -19.91, lon ~-43.95 a -43.97).

## Conclusão
Cluster pequeno é reflexo da concentração geográfica real da empresa, não bug de parser. Coord do BD está OK.

## Lição
Diagnóstico de "raio < 2 km" pode produzir falsos positivos para empresas que atuam num bairro só. Validar coord-única-repetida (5 casas) antes de mexer.

# Auditoria de geoloc — FYP Engenharia — FALSO POSITIVO

**Data:** 2026-04-29
**Status:** Não houve alteração.

## Investigação
13 produtos em São Paulo com cluster de 1.4 km. Coords todas DISTINTAS (5 casas únicas). FYP atua na região Zona Norte (Santana/Mandaqui), o cluster pequeno reflete concentração geográfica real, não bug.

## Observação adicional
Existe inconsistência entre nome do produto e cidade declarada (ex: "MAXY São Bernardo" tem cidade=Campinas; "FLEXMED Campinas" tem cidade=São Paulo). Isso é problema de **nome/cidade**, não geoloc. Investigar separadamente.

## Conclusão
Coord original preservada. Site bloqueia urllib (ERR_CONNECTION_RESET); investigação mais profunda exige Selenium dedicado.

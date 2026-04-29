# Correção de geoloc — Prohidro

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_prohidro_20260429_133559`

## Bug
14 produtos com coord exata `(-23.4896335, -47.4504025)` = sede Prohidro (R. João Mercado 260, Jd. Santa Rosália, Sorocaba/SP).

## Particularidade
Prohidro é construtora-empreiteira (faz obras pra outras incorporadoras). Páginas têm apenas descrição textual da localização ("Vizinho do terminal João Dias e da estação Giovanni Gronchi"), sem mapa, sem rua específica, sem CEP.

## Resultado
- **14 produtos NULLificados** (coord da sede removida)
- **8 com cidade corrigida** pelo texto da página (estavam com cidade errada)

## Pendência
Esses 14 NULL precisam fonte externa pra coord. Considerar reatribuir esses empreendimentos à **incorporadora original** (Prohidro só constrói; quem incorpora é outra empresa).

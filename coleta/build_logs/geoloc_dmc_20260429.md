# Correção de geoloc — DMC

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_dmc_*`

## Bug
9 produtos DMC com coord exata `(-18.5884, -46.5094)` = sede DMC em Patos de Minas.

## Fonte real
HTML tem endereços específicos por produto (ex: "Rua Eufrásio Rodrigues, 303"). Iframe gmaps usa coord da sede; texto livre tem rua específica.

## Resultado
- **7 produtos atualizados** com coord do logradouro extraído + base local de CEPs (com bbox QA Patos de Minas)
- **2 NULLificados** (não casou rua na base)

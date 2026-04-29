# Correção de geoloc — Eztec

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_eztec_20260429_130726`

## Bug encontrado
- 39 produtos no total (todos SP)
- **34 produtos com a mesma coord exata** `(-23.5954834, -46.6384071)` = **sede da Eztec** (R. Domingos de Morais 2187, Vila Mariana). O parser anterior pegava o link do footer "Como chegar" da sede.

## Fonte real (nível 1 da hierarquia)
Cada página `/imovel/<slug>/` tem atributos no DOM:
```html
<... data-lat="-23.5816471" data-lng="-46.5649363" ...>
```
Esse é o ponto do produto. Adicionalmente há `!3d!2d` no link do Google Maps do footer (que era o que estava sendo capturado errado).

## Resultado
- **32 produtos atualizados** com coord real do `data-lat/data-lng`
- **2 NULLificados** (estavam com SEDE e a página não tinha `data-lat`): `Lançamento` e `Narciso Sturlini`
- **5 ficaram sem coord** (já estavam NULL no BD ou ficaram NULL agora):
  - EZ PARQUE DA CIDADE
  - GranResort Reserva São Caetano
  - Reserva São Caetano Bosque
  - Reserva São Caetano Parque
  - Lançamento, Narciso Sturlini
- **Endereço** preenchido onde estava vazio (campo `endereco`).
- Raio máximo Eztec: **0 m → 22 km** (distribuição real agora).
- Zero coords duplicadas restantes.

## Pendência arquitetural
**Parser Eztec** precisa ser corrigido pra:
1. Ler `data-lat`/`data-lng` do DOM (esses são do produto)
2. Ignorar o link `<a href="...maps/place/R.+Domingos+de+Morais...">` do footer (sede)
3. Quando não houver `data-lat/lng`, NULLificar — não chumbar a sede
4. Aplicar mesma extração para `endereco`

# Correção de geoloc — Vitta Residencial

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_vitta_*`

## Bug
155 produtos. Várias cidades com cluster < 2 km (centroide municipal).

## Fonte real
Cada página tem `<iframe src="https://maps.google.com/maps?q=LAT,LNG&output=embed">` — coord direta no parâmetro `q=`.

## Resultado
- **78 produtos atualizados** com coord do iframe.
- Distribuição realista agora:
  - Ribeirão Preto: 14 km (era 1.3 km)
  - Araraquara: 6.4 km, Bauru: 8 km, Franca: 11 km
  - São Paulo: 15 km, Piracicaba: 14.5 km, Uberlândia: 18.9 km
- 77 ficaram inalterados (já tinham coord ou página sem iframe q=)

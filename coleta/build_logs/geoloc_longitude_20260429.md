# Correção de geoloc — Longitude — PENDENTE

**Data:** 2026-04-29
**Status:** Investigada mas não corrigida nesta sessão.

## Bug
39 produtos. Maioria com coord exata (-22.88243770, -47.19771530). Vários produtos em cidades distintas (Hortolândia, São José do Rio Preto, Sumaré, Salto) com a mesma coord — claramente sede ou ponto fixo herdado.

## Investigação
- urllib bloqueado (`ERR_CONNECTION_RESET`)
- Selenium funcionou mas página tem 4.8 MB sem `iframe gmaps`, `data-lat/lng` ou endereço extraível em padrões usuais
- Coord provavelmente em JS state — precisa investigação mais profunda

## DB intacto (rollback aplicado)
Backup `data/empreendimentos.db.bak_longitude_20260429_133341` foi restaurado. Coords originais preservadas.

## Próximo passo
Investigar em sessão dedicada: rodar Selenium com waits mais longos, navegar até seção de mapa da página, extrair de evento/state JS.

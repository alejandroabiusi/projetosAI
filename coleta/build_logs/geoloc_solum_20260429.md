# Correção de geoloc — Solum

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_solum_20260429_132721`

## Bug
25 produtos, **todos** com coord exata `(-31.7649901, -52.317033)` = sede Solum (Pelotas/RS). Site Wix sem mapa estruturado.

## Resultado
- **2 produtos** com coord real (Selenium + extração de logradouro + base CEPs)
  - Residencial Cristal → Av. Duque de Caxias
  - Residencial Parque Primavera → Rua Manoel
- **23 produtos NULLificados** — páginas Wix com pouco conteúdo de endereço (apenas "bairro Fragata" sem rua específica)

## Pendência
Esses 23 NULL precisam fonte externa (Instagram Solum, Pelotas GIS, ZAP). Site oficial não disponibiliza.

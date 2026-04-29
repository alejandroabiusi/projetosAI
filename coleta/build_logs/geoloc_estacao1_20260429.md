# Correção de geoloc — Estação 1

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_estacao1_20260429_133210`

## Bug
53 produtos. Várias clusters de coord duplicada — o maior com 16 produtos em (-12.22498, -38.90677).

## Fonte real
Página tem `<iframe src="https://www.google.com/maps/embed?...!2d{lng}!3d{lat}...">`. O site da Estação 1 cadastra cada produto com seu próprio iframe.

## Resultado
- **53 produtos atualizados** com coord do iframe embed.
- Raio máximo: 0 → **10,3 km** (era 0 antes pra alguns clusters).
- Ainda há clusters residuais de coord exata (16 + 8 + 6 + ...). Isso é porque o **próprio site** apresenta a mesma coord para vários produtos — provavelmente fases/torres do mesmo loteamento ou cadastro incompleto no site da empresa. Não é bug nosso, é característica da fonte.

## Pendência
Verificar com a Estação 1 se esses clusters são reais (mesmo loteamento) ou cadastro pendente. Por ora, aceito como nível 1 (fonte autoritativa = a própria página do site).

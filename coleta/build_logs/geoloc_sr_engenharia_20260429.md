# Auditoria de geoloc — SR Engenharia — FALSO POSITIVO

**Data:** 2026-04-29
**Status:** Não houve alteração. Diagnóstico geral marcou cluster de 1.3 km em 21 produtos Caucaia.

## Investigação
Coords no BD são distintas (todas únicas em 5 casas). 1ª tentativa de re-extrair via iframe `q=` da página resultou em todos os 21 caindo no MESMO ponto (iframe usa endereço da sede/central) — pior que original. **Rollback** aplicado.

## Conclusão
SR Engenharia atua concentradamente num bairro de Caucaia (Camurupim/Padre Romualdo). Coords originais (provavelmente coletadas com fonte melhor que iframe) são preservadas.

## Lição
Mesmo iframe Google Maps na página pode apontar pra sede em vez do produto. Validar antes de re-extrair.

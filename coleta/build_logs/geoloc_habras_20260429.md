# Correção de geoloc — Habras

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_habras_*`

## Bug
9 produtos com coord exata `(-23.5658389, -46.686811)` em SP — sede Habras. Mas produtos estão em Mogi das Cruzes, Atibaia, Ferraz de Vasconcelos, etc.

## Resultado
- **2 atualizados** via texto "do empreendimento: ENDEREÇO"
- **7 NULLificados** (rua não casou na base de CEPs ou bbox falhou)

## Pendência
Páginas têm endereço explícito, mas as ruas não estão na base local de CEPs. Considerar Selenium dirigido ou geocoding API.

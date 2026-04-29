# Correção de geoloc — Vega (VC Inc)

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_vega_20260429_133709` (rollback aplicado uma vez), depois `data/empreendimentos.db.bak_vega2_*`

## Bug
30 produtos. 14 com coord exata `(-16.718019867, -49.276738224)` = sede Vega (Av. Goiás Norte 501). Outros 16 já tinham coords distintas (preservadas).

## Tentativa frustrada
1ª tentativa usou CEP da página → todos pegaram CEP do footer da sede `74093-060` e ficaram com a coord errada. **Rollback** imediato.

## Resultado final
- **14 produtos NULLificados** (coord da sede removida)
- **16 produtos preservados** com coords reais que já existiam
- 0 atualizados (páginas Vega têm muito pouco texto estruturado de endereço)

## Pendência
Esses 14 NULL precisam fonte externa. Mesmo o endereço escrito ("Av. Goiás Norte e a poucos minutos da Avenida Perimetral") é genérico — provavelmente é o discurso de marketing falando da sede de obras / showroom, não do produto.

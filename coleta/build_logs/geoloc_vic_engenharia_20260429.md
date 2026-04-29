# Correção de geoloc — VIC Engenharia

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_vic_*`

## Bug
185 produtos no total, em **18 cidades com cluster < 500 m** cada. Sinal de coord chumbada por cidade (centroide municipal/sede regional). Av. Álvares Cabral 1777, BH, é a sede que o parser pegava como fallback.

## Fonte real
Página tem texto explícito tipo `Rua dos Otoni, 98 - Bairro Santa Efigênia - Belo Horizonte/MG` em uma classe `mb_30`. Endereço próprio do produto.

## Resultado
- **48 produtos atualizados** com coord real (Selenium + extração + base local de CEPs)
- **26 outliers NULLificados** (coord caiu fora do bbox da cidade declarada — geocodificação pegou rua homônima)
- Resto preservado (já tinha coord específica)

## Pendência
- Refinar matcher de logradouro pra dar prioridade exata a cidade+UF do BD
- Re-investigar os 26 NULL com fonte alternativa

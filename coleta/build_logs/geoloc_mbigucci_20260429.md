# Correção de geoloc — MBigucci

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_mbigucci_20260429_132402`

## Bug
27 produtos, **todos** com coord exata `(-23.6693561, -46.5627858)` = sede MBigucci (S. Bernardo do Campo). Parser pegava o footer.

## Fonte real
Página do produto tem `<iframe src="maps.google.com/maps?q=ENDEREÇO_COMPLETO">`. Endereço sai do parâmetro `q=`, geocodificado via base local de CEPs (`data/ceps_brasil.db`).

## Resultado
- **14 produtos** com coord real (raio máx 0 m → 33 km)
- **5 outliers NULLificados** (geocodificação pegou logradouro homônimo em cidade errada — fora do bbox do município declarado)
- **8 ficaram NULL** (parse de logradouro não bateu na base de CEPs ou iframe ausente)
- Total 13 sem coord

## Pendência
Parser MBigucci: extrair iframe `q=` e cidade dele (não confiar em cidade do BD pra geocodificar). Re-extrair os 13 NULL com fallback de Selenium ou outras fontes.

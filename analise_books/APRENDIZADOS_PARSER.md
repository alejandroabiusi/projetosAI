# Aprendizados da Comparação: Parser vs Correção Manual

Baseado na comparação de 248 linhas (175 PDFs) entre a extração automática e a correção manual.

## Resumo Quantitativo

| Tipo de diferença | Qtd | Descrição |
|---|---|---|
| **Preenchido** | 132 | Parser deu N/D, usuário encontrou o valor no PDF |
| **Correção** | 204 | Parser extraiu valor errado (incl. 149 de cenário) |
| **Ajuste fino** | 56 | Valor próximo mas impreciso (±5%) |
| **Erro grave** | 21 | Valor absurdamente errado (>5x ou <0.2x) |
| **Apagado** | 1 | Parser extraiu valor que não deveria existir |
| **TOTAL** | 414 | |

## 1. Cenário (149 correções) — PRIORIDADE ALTA

O parser de cenário é o campo com MAIS erros. Padrões de falha:

- **"Cenário Real" / "Cenário Aprovação"**: parser captura o prefixo "Cenário" junto → deve extrair só a palavra final
- **Texto lixo como cenário**: ex: `"Sul de Goiania. Trata-se de um terreno destinado..."`, `"Comitê 26"`, `"3 - verde"` — o parser pega a próxima linha após o label quando não há match, mas essa linha é texto descritivo
- **N/D quando deveria ser "Aprovacao"** (13x): o cenário está no título do One Page mas o parser não consegue extraí-lo
- **"Consolidado" deveria ser "Aprovacao"** (4x): o parser lê literalmente mas o significado real é Aprovação
- **Encoding de acentos**: "Aprovação" vs "Aprovacao" — normalizar

### Correção sugerida
- Normalizar cenário para enum: `{Aprovacao, Real, Fundamento, Alternativo}`
- Remover prefixo "Cenário " antes de classificar
- Se o valor capturado não for um dos enums, tentar extrair do título do One Page
- "Consolidado", "Aprovação e Real" → mapear para Aprovacao

## 2. Margem Bruta Econômica (24 diffs) — PRIORIDADE ALTA

Muitos casos onde o parser lê valor COMPLETAMENTE errado:
- Vila Recife: 0.200 → 0.398 (parser leu ~metade)
- Dália: 0.203 → 0.374
- Brumatti Santos: 0.205 → 0.393
- Solar Trindade: 0.249 → 0.460
- Furacão: 0.183 → 0.381

**Padrão**: o parser está lendo o valor de um campo ADJACENTE (possivelmente a margem rasa ou outro percentual próximo no layout). A margem bruta deve estar tipicamente entre 35-50%.

### Correção sugerida
- Adicionar validação: se margem_bruta < 0.30, marcar como suspeito
- Melhorar o regex para garantir que está na linha correta (usar contexto de 2 campos: "Margem Bruta" + "Econômica")
- Considerar OCR posicional (bounding boxes) em vez de texto sequencial

## 3. Valores Numéricos Truncados ou Multiplicados (21 erros graves)

### 3a. Avaliação unitária lida como milhares truncados
- Estância Dourados: 2.347 → 197.323 (parser leu "2.347" como número, mas era parte de "197.323" com OCR cortando)
- Botanique: 226 → 226.149
- Parque Hortênsia: 2.712 → 210.690

**Padrão**: OCR ou texto extrai só os últimos dígitos após o ponto de milhar.

### 3b. Custo construção multiplicado
- Botanique: 125.403.044 → 101.294 (parser concatenou dois números adjacentes)
- Acqua Viena: 61.094.040 → 61.094 (idem)

**Padrão**: quando há formato "XX.XXX (YY,Z%)" o parser às vezes captura "XX.XXXYYZ" como número único.

### 3c. Prazo da obra com valores absurdos
- Parque Hortênsia: 16.831 → 11 meses
- Solar Trindade: 42.497 → 8 meses
- Porto Valência: 20.694 → 8 meses

**Padrão**: o parser captura o valor de um campo numérico adjacente (custo total?) em vez do prazo. A validação de range (1-40) no `find_field_validated` faz fallback para `find_field` sem validação — esse fallback é perigoso.

### Correção sugerida
- **NUNCA** fazer fallback sem validação para campos com range conhecido
- avaliacao_unit: validar range 100.000 - 500.000
- prazo_obra_meses: se fora de 1-40, retornar N/D em vez de fallback
- custo_construcao_rs: validar range 20.000 - 200.000

## 4. Campos N/D Preenchidos Manualmente (132 ocorrências)

Campos que o parser não encontrou mas existiam no PDF:

| Coluna | N/D preenchidos |
|---|---|
| exposicao_rs_mil | 12 |
| exposicao_pct_vgv | 12 |
| custo_construcao_rs | 11 |
| custo_construcao_pct_vgv | 11 |
| fracao_terreno_pct | 10 |
| li_pct | 9 |
| li_regua_pct | 9 |
| avaliacao_unit | 8 |
| renda | 8 |
| pct_ret1 | 8 |
| unidades | 7 |
| cenario | 5 |
| custo_total_unit | 4 |
| preco_raso_unit | 4 |
| valor_rs_m2 | 3 |
| margem_bruta_economica_pct | 5 |
| preco_nominal_unit | 2 |
| margem_rasa_economica_pct | 2 |
| prazo_obra_meses | 2 |

**Causa provável**: OCR não reconhece o label do campo (variações de fonte, resolução baixa, layout diferente) ou o campo está em uma posição inesperada no slide.

### Correção sugerida
- Ampliar os patterns de regex para cobrir mais variações de OCR
- Tentar busca por POSIÇÃO no layout (campo à esquerda → valor à direita) em vez de só texto sequencial
- Para campos faltantes, tentar extrair da segunda maior imagem da página (tabela alternativa)

## 5. Ajustes Finos (56 ocorrências)

Valores próximos mas com diferença < 5%. Causas:
- OCR lê "47,6%" como "47.6%" ou "476%" → conversão imprecisa
- Arredondamento de percentuais: 0.42 vs 0.419
- Ponto vs vírgula no OCR

**Baixa prioridade** — estes são aceitáveis na maioria dos casos.

## Priorização de Melhorias

1. **Cenário**: reescrever parser com normalização + enum + fallback ao título
2. **Fallback perigoso**: remover fallback sem validação em `find_field_validated`
3. **Margem bruta**: adicionar validação de range (0.30-0.55) e log quando fora
4. **Avaliação unitária**: adicionar validação de range (100k-500k)
5. **Parse de números compostos**: melhorar separação de "número (pct%)" para evitar concatenação
6. **Ampliar regex patterns**: mais variações de OCR para reduzir N/D

# Auditoria de geolocalização — EBM e Somos (foco GO)

**Data:** 2026-04-29
**Snapshot:** `data/empreendimentos.db` (mtime 2026-04-26)
**Escopo:** todos os registros de EBM e Somos.

## Volumes

| Empresa | GO  | SP | DF | Outros | Total |
|---------|-----|----|----|--------|-------|
| EBM     | 59  | 14 | 3  | 0      | 76    |
| Somos   | 15  | 1  | 0  | 0      | 16    |

Nenhum registro com coord fora do bounding box do estado *declarado*. Isso é enganoso — o erro é em outra dimensão.

## Bug 1 — EBM: URLs duplicadas, registro "fantasma" reaproveita coords da sede

O scraper grava o produto duas vezes: uma com `/` final na URL e outra sem. As duas variantes viraram **registros distintos**. Para 5 dos 7 pares, a variante "sem barra" perdeu cidade/estado corretos e ficou com **`cidade=Goiânia, estado=GO`** + **lat/lon coladas no eixo da sede de EBM (raio < 200m do ponto -16.707, -49.293)**.

Pares duplicados encontrados (EBM):

| URL base | Pares | Anomalia |
|----------|-------|----------|
| `/sp/sao-paulo/wish/wish-675` | 2 | "sem barra" caiu em GO |
| `/sp/campinas/wide/nova-campinas` | 2 | "sem barra" caiu em GO |
| `/df/brasilia/now` | 2 | "sem barra" caiu em GO |
| `/df/brasilia/vida/samambaia` | 2 | "sem barra" caiu em GO |
| `/go/goiania/smart/smart-36` | 2 | duplicata benigna (ambas em GO) |
| `/go/goiania/vinhas/parque-evora` | 2 | duplicata benigna |
| `/go/goiania/mais-empreendimentos/the-sun-luxury-style` | 2 | duplicata benigna |
| `/go/goiania/metropolitan/bueno` | 2 | duplicata benigna |

**Caso confirmando o report do user:** `Wish 675` (SP/SP, lat/lon corretas perto de Vila Sônia) tem fantasma com `nome="Wish"`, cidade=Goiânia/GO, lat -16.7093, lon -49.2950 — coordenada da sede em Goiânia.

## Bug 2 — Somos: geoloc genérica para todo o portfólio GO

Os 15 produtos Somos em "Goiânia" estão dentro de um círculo de **raio máximo 1.086 m** (Setor Marista). O par mais distante é `SENSE KASA DESIGN ↔ Loteamento Nova Aliança` — em prédios reais, esses empreendimentos não estão a < 1km um do outro.

Para comparação, **EBM em Goiânia (48 produtos): raio máximo 16.066 m** — distribuição normal.

Hipótese: o scraper Somos não extrai endereço por produto (`endereco=NULL` em quase todos) e está geocodificando com um endereço-default (sede ou centroide do Setor Marista). Confirma o feedback `feedback_no_generic_coords.md` — coordenada genérica é proibida.

## Bug 3 — Somos: classificação errada de UF para "Elev Jundiaí Design"

- Slug: `elev-jundiai-design-anapolis`
- BD: cidade="Jundiaí", estado="SP", lat=-23.1797, lon=-46.8977 (centro de Jundiaí-SP)
- Real: empreendimento no **bairro Jundiaí de Anápolis-GO** (o slug deixa claro)

O parser pegou o token "jundiai" como cidade e ignorou o sufixo "-anapolis", gerando geoloc num estado errado (e cidade errada).

## Severidade e impacto

- **Bug 1** distorce análises por estado (4 produtos SP/DF aparecem como GO no mapa) e infla EBM em Goiânia.
- **Bug 2** torna o mapa de Somos inutilizável para análise espacial — todos os pins ficam empilhados.
- **Bug 3** põe um empreendimento de Anápolis no centro de Jundiaí-SP.

## Próximos passos sugeridos

1. **Limpar duplicatas EBM**: dedup por URL normalizada (rstrip `/`), preservando o registro com mais campos preenchidos / coords coerentes com a UF da URL.
2. **Re-geocodificar Somos GO**: forçar nova coleta de endereço por produto (Selenium se for SPA) ou marcar todos como `latitude=NULL, longitude=NULL` até existir endereço.
3. **Corrigir parser Somos** para entender slugs `*-anapolis`, `*-goiania`, etc., como sufixo de cidade.
4. **Adicionar invariante no scraper EBM**: a UF declarada precisa bater com o primeiro segmento da URL (`/sp/`, `/go/`, `/df/`) ou o registro é rejeitado.

## Execução (2026-04-29)

### EBM: dedup + fix UF
- 9 registros deletados (URLs duplicadas com/sem trailing slash)
- 1 registro corrigido: `id=2956 (Now)` movido de `GO/Goiânia` → `DF/Brasília` com coord NULL
- Pós-execução: EBM = 49 GO + 14 SP + 4 DF (era 76, agora 67)

### Somos: extração de `<div id="map" data-lat data-lng>` direto da página
Fonte ignorada pelo parser anterior. 8 produtos atualizados com coords reais via re-fetch:

| Produto | Mudança |
|---------|---------|
| ALT-65 | +2.4 km (Setor Aeroporto) |
| Arte Square | +5.9 km (Goiânia leste) |
| Liv Urban Marista | +800 m (Setor Marista) |
| **Loteamento Nova Aliança** | **+217 km — cidade mudou de Goiânia → Rio Verde** |
| Natto Bueno Design | +1.2 km (Setor Bueno) |
| New Way Aeroporto | +4.6 km |
| Orby Flamboyant | +5.8 km |
| **Elev Jundiaí Design** | **+790 km — cidade mudou de Jundiaí/SP → Anápolis/GO** |

### Somos: 8 produtos sem `data-lat/lng` (template alternativo)
HTML estático e Selenium confirmam ausência de `div#map`. Mesmo após render JS, sem coord. Investigação textual + cruzamento com `data/ceps_brasil.db` (Base dos Dados):

| Produto | Pista textual | Resolução |
|---------|--------------|-----------|
| **URBANITY** | "Mall Av José Walter" + "alameda Nestor Fonseca" | **Cidade mudou Goiânia → Rio Verde-GO**, coord = cruzamento das vias (-17.784, -50.946) |
| **Heritage Kasa Signature** | "EM FRENTE AO PARQUE INTERLAGOS" | **Cidade mudou Goiânia → Anápolis-GO**, coord da Rua Interlagos (-16.319, -48.917) |
| LEVEL | sem pista | **NULL** (era falsa do Setor Marista) |
| Mediterrane | sem pista | **NULL** |
| NOMAD MODERN LIFE | sem pista | **NULL** |
| NURBAN | sem pista | **NULL** |
| SENSE KASA DESIGN | sem pista (mesmo "Pronto para Morar") | **NULL** |
| AZUS | sem pista | **NULL** |

Query para revisitar quando houver fonte alternativa:
```sql
SELECT id, nome, cidade, url_fonte FROM empreendimentos
WHERE empresa='Somos' AND latitude IS NULL ORDER BY nome;
```

### Padrão emergente
A Somos atua além de Goiânia — também em **Rio Verde-GO** e **Anápolis-GO**. O parser jogava todos como Goiânia. Entre os 16 produtos da Somos, 3 estavam em cidade errada (URBANITY, Heritage, Loteamento Nova Aliança, mais o caso SP→GO Elev Jundiaí). Os 6 PENDENTES podem estar em qualquer dessas três cidades.

### Backup
`coleta/data/empreendimentos.db.bak_20260429_100104` (antes de qualquer alteração).

## Comandos úteis para a remediação

```sql
-- Listar duplicatas por URL normalizada (EBM e Somos)
SELECT
  RTRIM(url_fonte, '/') AS base,
  GROUP_CONCAT(id) AS ids,
  GROUP_CONCAT(estado) AS estados,
  COUNT(*) n
FROM empreendimentos
WHERE empresa IN ('EBM','Somos')
GROUP BY base
HAVING n > 1;

-- Marcar Somos GO com geoloc suspeita pra re-geocodificar
UPDATE empreendimentos
SET latitude=NULL, longitude=NULL
WHERE empresa='Somos' AND estado='GO';
```

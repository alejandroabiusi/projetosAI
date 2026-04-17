# Enriquecimento Autônomo — Sessão 17/04/2026

## Resumo Executivo
- **218/218 empresas** processadas (100%)
- **5831/6206 produtos** visitados (94%)
- **2545 campos atualizados** no banco
- **375 erros** (11 sites inacessíveis = 358 produtos)
- **Tempo total**: ~2.5 horas de processamento

## Completude Antes vs Depois

| Campo | Antes | Depois | Delta | % Antes | % Depois |
|-------|-------|--------|-------|---------|----------|
| tipologias_detalhadas | 871 | 2236 | **+1365** | 14.0% | **36.0%** |
| numero_vagas | 1339 | 2435 | **+1096** | 21.6% | **39.2%** |
| imagem_url | 5401 | 5875 | **+474** | 87.0% | **94.7%** |
| fase | 5767 | 5813 | **+46** | 92.9% | 93.7% |
| endereco | 5904 | 5946 | **+42** | 95.1% | 95.8% |
| itens_lazer | 5444 | 5485 | **+41** | 87.7% | 88.4% |
| area_min_m2 | 4076 | 4091 | +15 | 65.7% | 65.9% |
| dormitorios_descricao | 5304 | 5315 | +11 | 85.5% | 85.6% |
| preco_a_partir | 1285 | 1294 | +9 | 20.7% | 20.9% |
| itens_marketizaveis | 1900 | 1909 | +9 | 30.6% | 30.8% |
| itens_lazer_raw | 5852 | 5858 | +6 | 94.3% | 94.4% |
| total_unidades | 2412 | 2417 | +5 | 38.9% | 38.9% |
| latitude | 5620 | 5620 | 0 | 90.6% | 90.6% |

## Melhorias Implementadas

### 1. Conversor metragens_descricao → tipologias_detalhadas
- Converte formatos como "2 Quartos (PCD): 42.02m²" → "2D 42m²"
- Impacto: **+357 MRV** + 17 Tenda + outras empresas
- Arquivo: `scripts/enriquecimento_autonomo.py` → `converter_metragens_para_tipologias()`

### 2. Extrator de imagens expandido
- Antes: apenas 3 keywords (fachada, hero, banner)
- Depois: 14 keywords + scan 80KB + fallback para primeira imagem grande
- Impacto: **+474 imagens** (87% → 95%)

### 3. Parser __NEXT_DATA__ para sites Next.js
- Extrai dados estruturados de props JSON
- Suporte para: projectStatus, initialRooms, imagemDestaque, localizacao (Google Maps embed)
- Impacto: **Pride +115** (40 fases, 40 imagens, 33 tipologias)

### 4. Patterns de tipologia melhorados
- Ranges: "Studio - 21 m² a 24 m²" → "Studio 21m² | Studio 24m²"
- Near-proximity: "3 dorms. 69 e 71 m²" → "3D 69m² | 3D 71m²"
- Impacto: Eztec +48, One Innovation +35, e outras

## Top 30 Empresas por Atualizações

| # | Empresa | Updates |
|---|---------|---------|
| 1 | Vitta Residencial | +143 |
| 2 | Pride | +115 |
| 3 | Kallas | +95 |
| 4 | Vitacon | +87 |
| 5 | EBM | +81 |
| 6 | Plano&Plano | +80 |
| 7 | ADN | +76 |
| 8 | RSF | +75 |
| 9 | Estação 1 | +62 |
| 10 | Novolar | +56 |
| 11 | Eztec | +48 |
| 12 | Yticon | +46 |
| 13 | Mitre | +42 |
| 14 | CAC | +41 |
| 15 | SR Engenharia | +40 |
| 16 | Graal Engenharia | +38 |
| 17 | Econ Construtora | +37 |
| 18 | One Innovation | +35 |
| 19 | Pro Domo | +34 |
| 20 | Conx | +31 |
| 21 | Vital | +30 |
| 22 | Vega | +30 |
| 23 | Vila Brasil | +30 |
| 24 | Catagua | +25 |
| 25 | Vita Urbana | +22 |
| 26 | Kazzas | +21 |
| 27 | Jerônimo da Veiga | +21 |
| 28 | Pafil | +21 |
| 29 | Vinx | +21 |
| 30 | Riformato | +21 |

## Sites Inacessíveis (precisam Selenium)

| Empresa | Produtos | Motivo |
|---------|----------|--------|
| Direcional | 111 | HTTP 403 (WAF/Cloudflare) |
| Trisul | 65 | HTTP 403 (Cloudflare) |
| Campos Gouveia | 44 | Site inacessível |
| Tibério | 36 | Site inacessível |
| Canopus Construcoes | 21 | Site inacessível |
| BRN | 18 | Site inacessível |
| Prestes | 15 | Site inacessível |
| Girollar | 13 | Site inacessível |
| Riva | 12 | Site inacessível |
| Cosbat | 12 | SSL expirado |
| Lyx | 11 | Site inacessível |

**Total inacessível: 358 produtos (5.8%)**

## Próximos Passos
1. Rodar Selenium para sites inacessíveis (Direcional, Trisul, Tibério, etc.)
2. Selenium para SPAs que retornam HTML vazio (Helbor, HSM, MRV, Vivaz, Cury)
3. Investigar por que coordenadas não aumentaram (possível falta de Google Maps embed)
4. Reprocessar tipologias restantes (4000 produtos sem tipologia)

## Scripts Criados
- `scripts/enriquecimento_autonomo.py` — Script principal (requests, todas as empresas)
- `scripts/enriquecimento_selenium.py` — Script Selenium (preparado, não executado)

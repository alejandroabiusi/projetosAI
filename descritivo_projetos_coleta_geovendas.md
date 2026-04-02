# Descritivo Completo: Projetos Coleta de Empreendimentos e GeoVendas

**Autor:** Alejandro Abiusi — Diretor de Estratégia e Gestão de Carteira, Construtora Tenda  
**Data:** Abril 2026  
**Objetivo deste documento:** Fornecer contexto completo para geração de apresentações executivas (PPT) sobre cada projeto.

---

# PARTE 1 — COLETA DE EMPREENDIMENTOS IMOBILIÁRIOS

## 1.1 Visão Geral

**Nome:** Coleta — Plataforma de Inteligência Competitiva Imobiliária  
**Propósito:** Coleta automatizada, enriquecimento e monitoramento de empreendimentos residenciais de 40+ construtoras brasileiras. Acompanha lançamentos, mudanças de status, preços e amenidades para apoiar decisões estratégicas da Tenda.

**Números-chave:**
- 2.290 empreendimentos no banco de dados
- 43 empresas ativas monitoradas
- ~90 colunas de dados por empreendimento
- 99,6% de cobertura de geocodificação
- 44 empresas com download automatizado de imagens
- 7 regiões geográficas analisadas

## 1.2 Problema que Resolve

A Tenda precisa monitorar o mercado imobiliário residencial popular (MCMV e adjacências) em todo o Brasil. Antes deste projeto:
- O acompanhamento de concorrentes era manual, via visitas a sites individuais
- Não havia histórico estruturado de lançamentos, preços e fases de obra
- A análise geográfica dependia de relatórios esparsos e desatualizados
- Mudanças de preço, fase ou cancelamento de empreendimentos passavam despercebidas

O Coleta automatiza 100% desse processo: coleta dados de 43 construtoras semanalmente, enriquece com geocodificação e classificação de produto, detecta mudanças, e gera relatórios regionais e mapas interativos.

## 1.3 Arquitetura e Pipeline de Dados

### Fase 1 — Coleta (Scraping)

Dois modos de operação:
- **`run_coleta.py`** — Reset completo: apaga o banco e recria do zero (uso inicial ou recovery)
- **`run_atualizacao.py`** — Atualização incremental: compara snapshot anterior, detecta mudanças, registra histórico

**Técnicas de scraping por tipo de site:**

| Técnica | Empresas | Como funciona |
|---------|----------|---------------|
| Requests + BeautifulSoup (SSR) | Plano&Plano, Magik JC, Pacaembu, Vibra, Kazzas, Conx, Novolar, Árbore, SUGOI, Emccamp, EPH, Ampla e +15 | Requisições HTTP diretas a sites server-side rendered |
| Selenium (SPA / Cloudflare) | Cury, Vivaz, Direcional, Metrocasa | Navegador headless para sites com rendering JavaScript ou proteção Cloudflare |
| API GraphQL/REST | MRV | Chamadas diretas ao endpoint GraphQL da MRV (sem navegador) |
| WordPress REST API | ACL, VIC Engenharia, Vasco, Carrilho, Graal, Grafico | Consulta endpoints `/wp-json/wp/v2/` |
| Sitemap XML | Viva Benx | Parsing do sitemap XML para descobrir URLs de empreendimentos |

### Fase 2 — Enriquecimento

Sequência de scripts que adicionam dados não disponíveis diretamente nos sites:

| Script | O que faz | Dados adicionados |
|--------|-----------|-------------------|
| `enriquecer_dados.py` | Geocodificação, endereço, data de lançamento | latitude, longitude, endereço, CEP, data_lancamento |
| `enriquecer_unidades.py` | Total de unidades, vagas, terraços | total_unidades, numero_vagas, lazer_varanda |
| `qualificar_produto.py` | Tipologias, modelo de vaga, amenidades, imagens | tipologias_detalhadas, modelo_vaga, itens_marketizaveis |
| `corrigir_nomes.py` | Correção de nomes via `<title>` e APIs | nome corrigido |
| `validar_coordenadas.py` | Validação de coordenadas (Haversine vs cidade) | flags de coordenada válida |
| `baixar_imagens.py` | Download e categorização de imagens | plantas, fachada, áreas_comuns (6 categorias) |

**Estratégia de geocodificação (3 camadas de fallback):**
1. **CEP direto** → Base local com 905 mil CEPs (Base dos Dados / BigQuery)
2. **Logradouro + cidade** → Busca contextual no banco de CEPs
3. **Centroide do bairro + offset ~100m** → Evita pins genéricos no "centro da cidade"
4. **NULL se tudo falhar** — Nunca usa coordenada imprecisa

### Fase 3 — Detecção de Mudanças e Histórico

O `run_atualizacao.py` mantém histórico completo no banco `movimentacoes.db`:

| Tipo de evento | Descrição |
|----------------|-----------|
| `novo` | Empreendimento apareceu pela primeira vez |
| `fase_mudou` | Status mudou (ex: "Lançamento" → "Em Construção") |
| `preco_mudou` | Preço a partir de foi alterado |
| `campo_mudou` | Qualquer campo monitorado mudou (unidades, área, etc.) |
| `renomeado` | URL redireciona para novo slug (detectado automaticamente) |
| `relancado` | Novo empreendimento a <200m do anterior (mesmo terreno, novo produto) |
| `cancelado` | Empreendimento sumiu do site sem substituto próximo |

**Campos monitorados:** fase, preco_a_partir, total_unidades, evolucao_obra_pct, area_min_m2, area_max_m2, dormitorios

### Fase 4 — Visualização e Relatórios

**A. Mapa Interativo (`mapa_empreendimentos.html`)**
- Leaflet.js + MarkerCluster com 2.290+ pins
- Cores por empresa (43 cores únicas)
- Filtros dinâmicos: status × empresa (cross-filtering)
- 12 overlays KML de clusters regionais MPR (BA, CE, CPS, GO, JP, MG, PE, PR, RJ, RMSP, RS, SP)
- Popup com nome, empresa, fases e link direto ao site
- Estatísticas em tempo real por área visível

**B. Relatório Regional PPT (`gerar_relatorio_pptx.py`)**
- Formato A4 paisagem
- 7 seções regionais: SP_SPRM, SP_Interior, RJ_CO, NE, SUL, MG_N, Outros
- Por região: tabela-resumo, lançamentos recentes (<30 dias), destaques (outliers)
- Detecção de outliers: preço, tamanho e amenidades raras (IQR × 1,5)

**C. Email de Notificação (`notificar_email.py`)**
- Via Outlook COM (Windows)
- HTML formatado com tabelas de novos, mudanças de fase e outros
- Disparado automaticamente após cada ciclo de atualização

## 1.4 Empresas Monitoradas (43 ativas)

| # | Empresa | Qtd | Técnica |
|---|---------|----:|---------|
| 1 | MRV | 441 | API GraphQL |
| 2 | Cury | 272 | Selenium (Cloudflare) |
| 3 | VIC Engenharia | 185 | WP REST API |
| 4 | Plano&Plano | 157 | Requests (SSR) |
| 5 | Magik JC | 112 | Requests (genérico) |
| 6 | Direcional | 111 | Selenium + reconciliação |
| 7 | Metrocasa | 109 | API Next.js |
| 8 | Vivaz (Cyrela) | 106 | Selenium (SPA) |
| 9 | HM Engenharia | 62 | Requests (genérico) |
| 10 | Grafico | 60 | WP REST API |
| 11 | Pacaembu | 54 | Requests (genérico) |
| 12 | Econ Construtora | 51 | Requests (genérico) |
| 13 | Vibra Residencial | 50 | Requests (genérico) |
| 14 | Kazzas (Kallas) | 40 | Requests (genérico) |
| 15 | Conx | 39 | Requests (genérico) |
| 16 | Novolar (Patrimar) | 30 | Requests (genérico) |
| 17 | Graal Engenharia | 30 | WP REST API |
| 18 | Árbore | 29 | Requests (genérico) |
| 19 | Viva Benx | 27 | Sitemap XML |
| 20 | Vasco Construtora | 25 | WP REST API |
| 21 | Mundo Apto (Setin) | 23 | Requests (genérico) |
| 22 | Vinx | 22 | Requests (genérico) |
| 23 | Riformato | 21 | Requests (genérico) |
| 24 | SUGOI | 20 | Requests (genérico) |
| 25 | ACLF | 20 | Requests (genérico) |
| 26 | Emccamp | 17 | Requests (genérico) |
| 27 | BM7 | 17 | Requests (genérico) |
| 28 | FYP Engenharia | 16 | Requests (genérico) |
| 29 | Stanza | 15 | Requests (genérico) |
| 30 | Sousa Araujo | 15 | Requests (genérico) |
| 31 | Smart Construtora | 13 | Requests (genérico) |
| 32 | EPH | 13 | Requests (genérico) |
| 33 | Jotanunes | 12 | Requests (genérico) |
| 34 | Cavazani | 11 | Requests (genérico) |
| 35 | Rev3 | 10 | Requests (genérico) |
| 36 | Carrilho | 10 | WP REST API |
| 37 | BP8 | 10 | Requests (genérico) |
| 38 | ACL Incorporadora | 10 | WP REST API |
| 39 | Ampla | 8 | Requests (genérico) |
| 40 | Novvo | 5 | Requests (genérico) |
| 41 | M.Lar | 5 | Requests (genérico) |
| 42 | Construtora Open | 5 | Requests (genérico) |
| 43 | Ún1ca | 2 | Requests (genérico) |
| | **TOTAL** | **2.290** | |

**Pipeline de expansão adicional:** Empresas mapeadas tecnicamente (análise em `docs/mapeamento_sites_concorrentes.md`):
- **Fáceis** (Requests/SSR): Realiza (66), MPD (50+), ADN (35+)
- **Médias** (Cloudflare/paginação): BRNPAR (80+), Piacentini, CMO (100+)
- **Difíceis** (SPA/frameworks): Vitta (Angular, 40k+ lançadas), Bild (AngularJS, 24k+ unidades), Moura Dubeux (Next.js, 7 estados NE), BRZ (Blazor/.NET)

## 1.5 Modelo de Dados

**Banco:** SQLite (`empreendimentos.db`, 5,6 MB)

**Campos descritivos (61):** empresa, nome, slug, url_fonte, cidade, estado, bairro, endereço, fase, preço_a_partir, renda_mínima, área_terreno_m2, número_torres, total_unidades, unidades_por_andar, número_andares, número_vagas, dormitórios_descrição, metragens_descrição, área_min_m2, área_max_m2, evolução_obra_pct, arquitetura, paisagismo, decoração, itens_lazer, registro_incorporação, latitude, longitude, CEP, data_coleta, data_atualização, imagem_url, data_lançamento, tipologias_detalhadas, modelo_vaga, itens_marketizáveis...

**Atributos binários de lazer (25):** piscina, churrasqueira, fitness, playground, brinquedoteca, salão_festas, pet_care, coworking, bicicletário, quadra, delivery, horta, lavanderia, redário, rooftop, sauna, spa, piquenique, sport_bar, cine, easy_market, espaço_beleza, sala_estudos, espaço_gourmet, praça, solarium, sala_jogos

**Atributos binários de apartamento (8):** 1_dorm, 2_dorms, 3_dorms, 4_dorms, suíte, terraço, giardino, duplex, cobertura, vaga_garagem

**Atributos de programa habitacional (6):** mcmv, his1, his2, hmp, pode_entrar, casa_paulista

**Tabelas de controle:** runs (log de execução), changelog (mudanças), reconciliação (URLs)

**Completude dos dados:**
| Campo | Preenchimento |
|-------|--------------|
| Coordenadas (lat/lon) | 99,6% |
| Dormitórios | 98% |
| Cidade | 96% |
| Fase | 95% |
| Endereço | 91% |
| Total de unidades | 56% |
| Vagas | 39% |

## 1.6 Fluxo de Execução Típico

```
1. Cron semanal dispara run_atualizacao.py
   ├── Snapshot do banco atual
   ├── Executa 11 scrapers em sequência (timeout 30min cada)
   ├── Compara resultado com snapshot
   ├── Registra mudanças no changelog e movimentacoes.db
   └── Executa enriquecimento (geocoding, unidades, qualificação)

2. Pós-coleta
   ├── gerar_mapa.py → mapa_empreendimentos.html atualizado
   ├── detectar_destaques.py → outliers por região
   ├── gerar_relatorio_pptx.py → PPT regional
   └── notificar_email.py → email HTML com resumo das mudanças

3. Sob demanda
   ├── baixar_imagens.py → download de galleries (44 empresas)
   └── qualificar_produto.py → classificação profunda
```

## 1.7 Diferenciais Técnicos

1. **Multi-técnica de scraping:** Combina Requests, Selenium, GraphQL, WP API e Sitemap XML conforme cada site
2. **Atualização incremental com histórico:** Preserva toda a história de mudanças (não reseta o banco a cada execução)
3. **Geocodificação em 3 camadas:** Nunca usa coordenada genérica de "centro da cidade"
4. **Detecção automática de atributos:** 39 flags binárias extraídas por keyword scanning do texto das páginas
5. **Reconciliação de URLs:** Detecta renomeações, relançamentos e cancelamentos automaticamente
6. **Mapa com KML regional:** 12 overlays de clusters MPR para análise geográfica corporativa
7. **Categorização de imagens:** 6 categorias (plantas, fachada, áreas comuns, decorado, geral, obra) para 44 empresas

---

# PARTE 2 — ANÁLISE DE VENDAS POR GEOLOCALIZAÇÃO (GEOVENDAS)

## 2.1 Visão Geral

**Nome:** Análise GeoVendas — Dashboard de Inteligência Geográfica de Vendas  
**Propósito:** Aplicação Streamlit para análise de vendas imobiliárias por raio geográfico, usando dados SIOPI da Caixa Econômica Federal. Permite identificar zonas de oportunidade onde há alta demanda de mercado mas baixa penetração da Tenda.

**Números-chave:**
- ~876 mil registros de vendas no banco
- 587 MB de dados (SQLite)
- 36 cidades cobertas (interior SP, PR, GO, MT)
- 20+ gráficos interativos Plotly
- Dashboard com 6 abas de análise + dashboard de oportunidades
- ~4.200 linhas de código Python

## 2.2 Problema que Resolve

A Tenda precisa entender onde o mercado está comprando imóveis populares e como se posicionar geograficamente. Antes deste projeto:
- Dados do SIOPI (financiamentos pela Caixa) existiam em planilhas brutas, sem análise geográfica
- Não havia forma de visualizar vendas por raio ao redor de um ponto específico
- A comparação geográfica Tenda vs. concorrentes era manual
- Identificação de zonas de oportunidade dependia de intuição, não de dados

O GeoVendas transforma dados de financiamento da Caixa em inteligência geográfica acionável: mostra onde estão as vendas, quem está comprando, que produto está vendendo, e onde existem "vazios" de mercado que a Tenda pode explorar.

## 2.3 Fonte de Dados: SIOPI

**O que é:** O SIOPI (Sistema de Informações de Operações Imobiliárias) é o sistema da Caixa Econômica Federal que registra todas as operações de financiamento habitacional. A base contém informações detalhadas de cada venda financiada.

**Campos disponíveis (~60 por transação):**

| Categoria | Campos |
|-----------|--------|
| **Empreendimento** | incorporadora, empreendimento, município, estado, endereço, CEP |
| **Produto** | tipologia, dormitórios, vagas_garagem, metragem |
| **Preço** | preço (avaliação do imóvel), recursos_próprios, valor_financiado, FGTS, subsídio |
| **Financiamento** | taxa_juros, prazo_meses, encargo_mensal, sistema_amortização |
| **Comprador** | renda_comprovada, profissão, sexo, estado_civil, CEP_cliente |
| **Temporal** | data_venda (data_cadastro) |

**Dataset atual:** ~47 mil registros da Pacaembu em 36 cidades (SP interior, PR, GO, MT)

## 2.4 Arquitetura da Aplicação

```
┌──────────────────────────────────────────────────────┐
│ IMPORTAÇÃO                                            │
│ Excel/CSV SIOPI → database.py (mapeamento automático  │
│ de 26+ colunas SIOPI → schema interno) → vendas.db   │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│ GEOCODIFICAÇÃO                                        │
│ geocodificar_empreendimentos.py (3 níveis de fallback)│
│ mapear_ceps_paralelo.py (CEPs dos clientes)           │
│ precalcular_zonas.py (greedy set-cover algorithm)     │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│ DASHBOARD STREAMLIT (app.py)                          │
│ ├── Sidebar: Upload, Período, Geo, Raio, Filtros     │
│ ├── Mapa interativo (pydeck + deck.gl)                │
│ └── 6 abas de análise:                                │
│     ├── Volume de Vendas                              │
│     ├── Perfil do Comprador                           │
│     ├── Perfil do Produto                             │
│     ├── Análise Financeira                            │
│     ├── Velocidade de Vendas                          │
│     └── Origem dos Clientes                           │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│ DASHBOARD OPORTUNIDADES (pages/oportunidades.py)      │
│ ├── Ranking de cidades por score de oportunidade      │
│ ├── Mapa de calor por zonas geográficas               │
│ ├── Comparativo Tenda vs. concorrência                │
│ └── Drill-down por raio                               │
└──────────────────────────────────────────────────────┘
```

## 2.5 Funcionalidades do Dashboard Principal

### Controles na Sidebar
- **Upload de arquivo:** Detecção automática Excel/CSV, mapeamento de colunas SIOPI
- **Filtro de período:** Range de datas de venda
- **Filtros geográficos:** UF → Cidade → Empreendimento (dependências dinâmicas)
- **Filtro por raio:** 3 modos:
  - Por coordenadas (lat/lon manual)
  - Por endereço (geocodificação on-the-fly)
  - Por empreendimento (seleciona e usa suas coordenadas)
- **Filtros adicionais:** incorporadora, faixa de renda, faixa de preço, tipologia

### Mapa Interativo (pydeck/deck.gl)
- Pontos coloridos por incorporadora
- Overlay de raio (círculo) quando filtro ativo
- Click-to-filter (modal de seleção)
- Zoom automático para dados filtrados
- Tooltips com informações do empreendimento

### 6 Abas de Análise

**1. Volume de Vendas**
- Ranking de vendas por incorporadora
- Evolução temporal de vendas (série histórica)
- Distribuição por faixa de preço

**2. Perfil do Comprador**
- Distribuição de renda dos compradores
- Faixa etária e gênero
- Top profissões
- Ratio de financiamento sobre renda

**3. Perfil do Produto**
- Tipologias vendidas (dormitórios, vagas)
- Distribuição de metragens
- Mix de produto por incorporadora

**4. Análise Financeira**
- Composição: recursos próprios vs. financiamento vs. FGTS vs. subsídio
- Encargo mensal médio
- Prazo de financiamento
- Taxa de juros praticada
- Comprometimento de renda

**5. Velocidade de Vendas**
- Curva de absorção mensal por incorporadora
- Sazonalidade de vendas

**6. Origem dos Clientes**
- Mapa de calor por bairro de origem
- Distribuição geográfica dos compradores
- Distância média comprador → empreendimento

## 2.6 Dashboard de Oportunidades

Painel separado (`pages/oportunidades.py`) que identifica zonas geográficas com alta demanda mas baixa penetração da Tenda.

**Algoritmo de zonas (greedy set-cover):**
1. Para cada combinação cidade × raio (2km, 3km, 5km, 7km, 10km):
   - Trata cada empreendimento como centro de zona potencial
   - Iterativamente seleciona centros que cobrem mais vendas não cobertas
   - Mínimo de 3 vendas por zona
2. Calcula score de oportunidade: (volume de mercado) × (1 - penetração Tenda)
3. Armazena resultado na tabela `zonas_precalc`

**Visualizações:**
- Ranking de cidades por score de oportunidade
- Mapa de calor das zonas identificadas
- Comparativo Tenda vs. concorrência por zona
- Drill-down por raio selecionado

## 2.7 Módulos Técnicos

### src/database.py — Camada de dados
- Suporta SQLite (local) e MySQL (AWS, configurável via .env)
- Mapeamento automático de 26+ colunas SIOPI → schema interno
- Import de Excel e CSV com detecção de formato

### src/spatial.py — Cálculos espaciais
- Fórmula de Haversine vetorizada (NumPy) para cálculo de distância
- Filtro por raio: retorna todas as vendas dentro de N km de um ponto
- Geração de polígonos circulares para overlay no mapa

### src/charts.py — Biblioteca de visualização
- 20+ funções Plotly para diferentes análises
- Gráficos de barras, pizza, linha, dispersão, treemap, sunburst
- Tema escuro profissional consistente para apresentações
- Exportável para imagem estática

### src/geocoding.py — Geocodificação
- Google Maps API (se configurada) ou Nominatim (OpenStreetMap)
- Rate limiting automático
- Cache persistente em JSON

## 2.8 Scripts Auxiliares

| Script | Propósito |
|--------|-----------|
| `geocodificar_empreendimentos.py` | Geocodifica endereços em lote (3 níveis de fallback) |
| `mapear_ceps_paralelo.py` | Enriquece CEPs de clientes com bairro/cidade (multi-thread) |
| `precalcular_zonas.py` | Pré-calcula zonas de oportunidade (set-cover) |
| `baixar_cep_basedosdados.py` | Baixa base de CEPs do BigQuery (905k registros) |
| `geocodificar_por_cep.py` | Geocodifica empreendimentos por CEP |
| `corrigir_geocode.py` | Identifica e corrige coordenadas incorretas |
| `aplicar_geocode_cache.py` | Aplica cache de geocodificação ao banco |

## 2.9 Stack Técnica

| Componente | Tecnologia |
|------------|------------|
| Frontend | Streamlit 1.30+ |
| Gráficos | Plotly 5.18+ |
| Mapa | pydeck 0.8+ (deck.gl) |
| Backend | pandas, SQLAlchemy |
| Banco de dados | SQLite (local), MySQL (AWS, opcional) |
| Geocodificação | geopy (Nominatim/Google Maps), Base dos Dados (BigQuery) |
| Estilo | CSS customizado (tema escuro profissional) |

---

# PARTE 3 — COMO OS DOIS PROJETOS SE COMPLEMENTAM

## 3.1 Oferta vs. Demanda

Os dois projetos formam uma visão completa do mercado imobiliário popular:

| Dimensão | Coleta | GeoVendas |
|----------|--------|-----------|
| **Perspectiva** | Oferta (o que as construtoras oferecem) | Demanda (o que o mercado está comprando) |
| **Fonte** | Sites das construtoras (scraping) | SIOPI / Caixa (financiamentos realizados) |
| **Granularidade** | Empreendimento (produto disponível) | Transação (venda individual) |
| **Atualização** | Semanal (scraping recorrente) | Sob demanda (importação de base SIOPI) |
| **Geografia** | Nacional (43 construtoras, 2.290 empreendimentos) | Regional (36 cidades, foco interior SP/PR/GO/MT) |
| **Análise** | O que existe: preço, fase, amenidades, localização | Quem compra: renda, profissão, financiamento, origem |

## 3.2 Casos de Uso Combinados

1. **Identificação de oportunidades:** GeoVendas mostra zonas com alta demanda → Coleta mostra se já existe oferta concorrente nessa zona
2. **Benchmark de produto:** Coleta mostra amenidades e tipologias dos concorrentes → GeoVendas mostra quais produtos o mercado está absorvendo
3. **Precificação:** Coleta mostra preços praticados pelos concorrentes → GeoVendas mostra a renda e capacidade de pagamento dos compradores reais
4. **Análise de velocidade:** Coleta detecta mudanças de fase (lançamento → em construção → pronto) → GeoVendas mostra velocidade de absorção real via financiamentos
5. **Entrada em novos mercados:** Coleta mapeia presença de concorrentes por cidade → GeoVendas identifica cidades com demanda comprovada mas pouca oferta Tenda

## 3.3 Visão Integrada para a Tenda

```
┌─────────────────────────────────────────────┐
│           INTELIGÊNCIA DE MERCADO            │
├──────────────────┬──────────────────────────┤
│     COLETA       │       GEOVENDAS          │
│                  │                          │
│  "O que os       │  "O que o mercado        │
│   concorrentes   │   está comprando?"       │
│   estão          │                          │
│   oferecendo?"   │  - Vendas por região     │
│                  │  - Perfil do comprador   │
│  - 2.290 empr.   │  - Financiamento usado   │
│  - Preços        │  - Renda real            │
│  - Fases de obra │  - Velocidade absorção   │
│  - Amenidades    │  - Zonas de oportunidade │
│  - Localização   │  - Origem dos clientes   │
│  - Mudanças      │                          │
├──────────────────┴──────────────────────────┤
│                                             │
│  DECISÕES ESTRATÉGICAS DA TENDA:            │
│  • Onde lançar?                             │
│  • Que produto oferecer?                    │
│  • A que preço?                             │
│  • Contra quem competir?                    │
│  • Quais amenidades priorizar?              │
│                                             │
└─────────────────────────────────────────────┘
```

---

# PARTE 4 — EVOLUÇÃO E HISTÓRICO

## 4.1 Timeline do Coleta

| Data | Marco |
|------|-------|
| Mar/2026 | Projeto criado: scrapers iniciais (Plano&Plano, Cury, MRV) |
| Mar/2026 | Expansão: Direcional, Vivaz, Metrocasa, genérico (8+ empresas) |
| Mar/2026 | Geocodificação com Base dos Dados (BigQuery), mapa Leaflet.js |
| Mar/2026 | Sistema de atualização recorrente com change tracking |
| Mar/2026 | Dashboard de oportunidades com zonas geográficas |
| Mar/2026 | Overlays KML regionais (12 clusters MPR) |
| Mar/2026 | Verificação de status e reconciliação de URLs |
| Mar/2026 | 8 novas incorporadoras: Open, Grafico, Stanza, Graal, Rev3, HM, Sousa, Econ |
| Mar/2026 | Qualificação profunda: tipologias, vagas, amenidades, imagens |
| Mar/2026 | Download de imagens: 44 empresas com categorização |
| Mar/2026 | Extração de área do terreno e coeficiente de adensamento |
| Mar/2026 | Relatório PPT regional automatizado |

## 4.2 Timeline do GeoVendas

| Data | Marco |
|------|-------|
| 09/Mar/2026 | Projeto criado: importação SIOPI, dashboard básico |
| 09/Mar/2026 | Otimização para 876k registros, operações vetorizadas |
| 10/Mar/2026 | Dashboard de oportunidades com zonas pré-calculadas |
| 10/Mar/2026 | Integração Base dos Dados para enriquecimento de CEPs |
| 12/Mar/2026 | Pré-cálculo de zonas otimizado (greedy set-cover) |

## 4.3 Próximos Passos Planejados

**Coleta:**
- Expandir para 40+ empresas do pipeline (225+ empreendimentos adicionais só nas "fáceis")
- Automatizar agendamento via Windows Task Scheduler (agendar_tarefa.bat)
- Integrar com BI corporativo (export para PowerBI/Looker)
- Adicionar transcrições de conference calls de resultados (já existe protótipo)

**GeoVendas:**
- Testar com múltiplas incorporadoras (atualmente só Pacaembu)
- Configurar MySQL AWS para acesso multi-usuário
- Completar mapeamento de CEPs de clientes (~300 de 20k mapeados)
- Cruzar dados com Coleta para visão oferta × demanda integrada

---

# PARTE 5 — INFORMAÇÕES PARA MONTAGEM DO PPT

## 5.1 Público-alvo das apresentações

- Diretoria executiva da Tenda
- Gerência de inteligência de mercado
- Equipe de estratégia e gestão de carteira

## 5.2 Mensagens-chave para o PPT do Coleta

1. **Automação total:** Substituímos monitoramento manual por coleta automatizada de 43 construtoras
2. **Cobertura nacional:** 2.290 empreendimentos de 43 construtoras com 99,6% geocodificados
3. **Inteligência de mudanças:** Detectamos automaticamente lançamentos, mudanças de preço, fases de obra e cancelamentos
4. **Visão regional:** 7 regiões com análise de outliers e relatórios automatizados
5. **Escalável:** 40+ empresas no pipeline de expansão (potencial de 2x a cobertura)
6. **Profundidade de dados:** 90 colunas por empreendimento, incluindo amenidades, tipologias e programas habitacionais

## 5.3 Mensagens-chave para o PPT do GeoVendas

1. **Dados reais de transação:** Baseado em financiamentos reais da Caixa (SIOPI), não estimativas
2. **Análise por raio geográfico:** Permite estudar qualquer ponto e raio no mapa
3. **6 dimensões de análise:** Volume, comprador, produto, financiamento, velocidade, origem
4. **Zonas de oportunidade:** Algoritmo identifica automaticamente onde há demanda sem presença Tenda
5. **Dashboard interativo:** Filtros dinâmicos, mapa deck.gl, gráficos Plotly — apresentável para executivos
6. **Perfil real do comprador:** Renda, profissão, comprometimento de renda, uso de FGTS — dados que só existem no SIOPI

## 5.4 Sugestão de Estrutura dos PPTs

**PPT Coleta (sugestão ~15 slides):**
1. Capa
2. Problema: como era o monitoramento antes
3. Solução: visão geral da plataforma
4. Pipeline de dados (diagrama)
5. Cobertura: mapa com pins das 43 empresas
6. Profundidade: exemplo de ficha de empreendimento (90 colunas)
7. Change tracking: exemplo de detecção de mudança
8. Mapa interativo: screenshot do mapa Leaflet
9. Relatório regional: exemplo de slide do PPT gerado
10. Métricas: 2.290 empreendimentos, 43 empresas, 99,6% geocodificação
11. Pipeline de expansão: 40+ empresas mapeadas
12. Casos de uso: 3-4 exemplos de decisões informadas pela plataforma
13. Stack técnica (1 slide)
14. Roadmap
15. Q&A

**PPT GeoVendas (sugestão ~12 slides):**
1. Capa
2. Problema: dados SIOPI brutos sem análise geográfica
3. Solução: dashboard interativo com análise por raio
4. Fonte de dados: o que é o SIOPI e o que ele contém
5. Mapa + filtro por raio: screenshot do dashboard
6. Volume de vendas: gráficos de exemplo
7. Perfil do comprador: distribuição de renda, profissões
8. Análise financeira: composição do financiamento
9. Zonas de oportunidade: como o algoritmo funciona + exemplo visual
10. Caso de uso: identificação de oportunidade real
11. Próximos passos
12. Q&A

# Projeto Coleta — Scrapers de Empreendimentos Imobiliários

## Objetivo
Coletar dados de empreendimentos imobiliários de 77 incorporadoras brasileiras, enriquecer com geocodificação e dados adicionais, gerar mapa interativo HTML e dashboard Streamlit, e manter a base atualizada automaticamente com detecção de mudanças.

## Banco de Dados
- **Arquivo**: `data/empreendimentos.db` (SQLite)
- **Tabela principal**: `empreendimentos` (~120 colunas)
- **~3.470 registros** de **77 empresas**, **~98% com coordenadas**
- **Base de CEPs local**: `data/ceps_brasil.db` (905k CEPs com lat/lon, fonte: basedosdados.org)
- Tenda (469), MRV (441), Cury (272), VIC Engenharia (185), Plano&Plano (157), Vitta Residencial (155), Magik JC (112), Direcional (111), Metrocasa (109), Vivaz (106), EBM (81), Trisul (65), HM Engenharia (62), Grafico (60), Pacaembu (54), Econ Construtora (51), Vibra Residencial (50), CAC (42), Kazzas (40), Conx (39), MGF (36), Vega (30), Canopus (30), Novolar (30), Graal Engenharia (30), Árbore (29), Viva Benx (27), Quartzo (26), Vasco Construtora (25), Mundo Apto (23), Vinx (22), SR Engenharia (21), Canopus Construções (21), Riformato (21), AP Ponto (20), SUGOI (20), ACLF (20), Exata (19), Somos (16), Emccamp (17), BM7 (17), FYP Engenharia (16), Stanza (15), Sousa Araujo (15), Vila Brasil (14), Smart Construtora (13), EPH (13), Cosbat (12), Belmais (12), Jotanunes (12), Cavazani (11), Victa Engenharia (10), SOL Construtora (10), Morana (10), Lotus (10), House Inc (10), ART Construtora (10), Rev3 (10), Carrilho (10), BP8 (10), ACL Incorporadora (10), Maccris (9), Ampla (8), Tenório Simões (8), Grupo Delta (6), Domma (6), Dimensional (5), Mirantes (5), Torreão Villarim (5), Bora (5), Novvo (5), Você (5), M.Lar (5), Construtora Open (5), Ún1ca (2), Versati (1), Sertenge (1)

### Tabelas de controle (change tracking)
- **`runs`** — registro de cada execução (id, inicio, fim, status, contadores)
- **`changelog`** — cada mudança detectada (empresa, nome, tipo, campo, valor_anterior, valor_novo)
- Tipos de mudança: `novo`, `fase_mudou`, `preco_mudou`, `campo_mudou`
- Campos rastreados: fase, preco_a_partir, total_unidades, evolucao_obra_pct, dormitorios_descricao, area_min_m2, area_max_m2
- **`reconciliacao`** — rastreamento de URLs que sumiram do sitemap (Direcional)
- Tipos: `renomeado` (URL redirecionou), `relancado` (novo produto nas mesmas coordenadas <200m), `cancelado` (sem substituto)
- Colunas: nome_anterior, nome_novo, url_anterior, url_nova, fase_anterior, fase_nova, distancia_metros, observacao

### Banco de movimentações: `data/movimentacoes.db` (separado)
- **`movimentacoes`** — histórico completo do ciclo de vida de cada produto
- Tipos: `novo`, `removido`, `fase_mudou`, `preco_mudou`, `campo_mudou`, `renomeado`, `relancado`, `cancelado`
- Colunas: data, empresa, nome, url_fonte, tipo, campo, valor_anterior, valor_novo, observacao, origem
- Alimentado automaticamente por `inserir_empreendimento()`, `registrar_mudanca()` e `registrar_reconciliacao()`
- Módulo: `data/movimentacoes.py` — funções: `registrar_movimentacao()`, `consultar_movimentacoes()`, `resumo_movimentacoes()`, `historico_empreendimento()`

## Arquitetura dos Scrapers

### Scrapers de empreendimentos (em `scrapers/`)
- `mrv_empreendimentos.py` — MRV via API REST (`/api/search`)
- `mrv_detalhes.py` — MRV detalhes por empreendimento
- `cury_empreendimentos.py` — Cury via API GraphQL interna
- `vivaz_empreendimentos.py` — Vivaz via API REST (`/imovel/informacoes/`)
- `direcional_empreendimentos.py` — Direcional via requests + BeautifulSoup (dedup por url_fonte, reconciliação de URLs órfãs)
- `planoeplano_empreendimentos.py` — Plano&Plano via requests (páginas SSR)
- `metrocasa_empreendimentos.py` — Metrocasa via Next.js API (`/api/properties`)
- `vivabenx_empreendimentos.py` — Viva Benx via sitemap XML + BeautifulSoup (empreendimentos MCMV em SP)
- `generico_empreendimentos.py` — Scraper genérico para: Pacaembu, Conx, Vibra, Magik JC, Kazzas, Mundo Apto, Novvo, Novolar, Emccamp, SUGOI, Árbore, Ampla, M.Lar, EPH, Ún1ca, Construtora Open, Stanza, Rev3, HM Engenharia, Sousa Araujo, Econ Construtora + mais
- `wpapi_empreendimentos.py` — Scraper WP REST API para: ACL Incorporadora, VIC Engenharia, Vasco Construtora, Carrilho, Graal Engenharia, Grafico
- `verificar_status.py` — Verificação de status de ~2017 URLs existentes: re-detecta fases, reconcilia URLs mortas (redirect → nome → localização → marca "Removido"). Batch para MRV/Vivaz (API), Direcional/VivaBenx (sitemap), individual para genéricos. Flag `--sem-selenium` pula Cury/Metrocasa. Resumível via JSON de progresso.

**NOTA**: Os scrapers de RI (mzgroup_ri.py, tenda_ri.py, planoeplano_ri.py) foram movidos para `../analise_releases/scrapers/`. Os downloads de docs de RI (releases, ITRs, apresentações, transcrições) estão em `../analise_releases/downloads/`. A pasta `downloads/` deste projeto contém apenas imagens de empreendimentos.

### Problema conhecido (root cause corrigido parcialmente)
- `generico_empreendimentos.py` linha 331 hardcodava `cidade="São Paulo"` para TODAS as empresas genéricas
- Pacaembu atua no interior de SP, MG, PR, MT — foi corrigido via extração de cidade do `<title>` das páginas
- `planoeplano_empreendimentos.py` usava `_nome_da_url()` (slug como nome) em vez de extrair do HTML

## Scripts de Enriquecimento

| Script | Função |
|--------|--------|
| `enriquecer_dados.py` | Geocodificação via base local de CEPs, APIs (Vivaz, MRV), extração de dados das páginas |
| `enriquecer_unidades.py` | Extração de total_unidades, vagas, amenidades via requests/Selenium/APIs |
| `qualificar_produto.py` | Qualificação profunda: tipologias detalhadas, modelo de vaga, itens lazer raw, marketizáveis, classificação de imagens |
| `enriquecer_selenium_batch.py` | Selenium headless para Kazzas, Conx (SPAs) |
| `corrigir_nomes.py` | Correção de nomes via `<title>` das páginas e APIs |
| `validar_coordenadas.py` | Validação de coords vs cidade cadastrada (distância haversine) |
| `gerar_mapa.py` | Gera `mapa_empreendimentos.html` com Leaflet.js + MarkerCluster |

## Sistema de Atualização Recorrente

### Orquestrador: `run_atualizacao.py`
Substitui `run_coleta.py` para uso recorrente. **NÃO apaga o banco**. Fluxo:
1. Backup do banco atual (`data/backups/`)
2. Snapshot do estado atual (empresa+nome → campos rastreados)
3. Roda todos os 9 scrapers + verificação de status (com timeout de 30min cada, retry 1x se falhar)
4. Roda enriquecimento (enriquecer_dados, enriquecer_unidades, qualificar_produto, corrigir_nomes, validar_coordenadas)
5. Snapshot novo → compara → popula `changelog`
6. Gera mapa atualizado
7. Envia email com resumo (se destinatários configurados)
8. Registra execução na tabela `runs`

```bash
python run_atualizacao.py              # Ciclo completo
python run_atualizacao.py --sem-email  # Sem envio de email
python run_atualizacao.py --sem-mapa   # Sem regenerar mapa
```

### Notificação: `notificar_email.py`
- Envia via Outlook COM (`win32com.client`)
- HTML formatado com tabelas: novos, mudanças de fase, outras mudanças, erros
- Configurar destinatários em `config/settings.py` → `EMAIL["destinatarios"]`
- Teste: `python notificar_email.py --teste`

### Agendamento: `agendar_tarefa.bat`
- Entry point para Windows Task Scheduler
- Configurar: toda segunda-feira às 6h (ou horário desejado)

## Relatório Executivo PPT

### Regionais: `config/regionais.py`
Classificação de cidades em 7 regionais:
- **SP_SPRM**: SP + Região Metropolitana
- **SP_INTERIOR**: SP Interior + Litoral
- **RJ_CO**: RJ + Centro-Oeste
- **NE**: Nordeste
- **SUL**: Sul
- **MG_N**: MG + Norte
- **OUTROS**: demais

### Detecção de destaques: `detectar_destaques.py`
Heurísticas por regional:
- Preço outlier (fora de 1.5x IQR)
- Metragem atípica
- Amenidade rara (< 5% na regional)
- Breves lançamentos

```bash
python detectar_destaques.py                 # Todas as regionais
python detectar_destaques.py --regional SP_SPRM
```

### Gerador PPT: `gerar_relatorio_pptx.py`
- Formato A4 retrato, usa template se existir em `config/template_relatorio.pptx`
- Slides por regional: capa, resumo, breves lançamentos, destaques

```bash
python gerar_relatorio_pptx.py                        # Todas as regionais
python gerar_relatorio_pptx.py --regional SP_SPRM     # Uma regional
python gerar_relatorio_pptx.py --saida custom.pptx    # Caminho customizado
```

## APIs descobertas e utilizadas
- **Vivaz**: POST `https://www.meuvivaz.com.br/imovel/informacoes/` com `{"Url": slug}` → vagas, quartos, varanda, bairro
- **Metrocasa**: GET `https://www.metrocasa.com.br/api/properties?page=X&perPage=50` → amenidades, endereço, tipologia
- **Kazzas**: GET `https://kazzas.com.br/wp-json/wp/v2/empreendimento?per_page=50&_embed` → amenidades via taxonomia WP
- **MRV**: API interna de busca com detalhes por empreendimento
- **Vibra Residencial**: Status extraído da home (`section#produtos`, `section#produtos-em-obras`, `section#produtos-prontos`)

## Completude atual dos dados
- latitude/longitude: ~99.6%
- dormitorios_descricao: ~98%
- cidade: ~96%
- endereco: ~91%
- fase (status): ~95% — detectar_fase() com 3 camadas: HTML status elements > conteúdo principal > fallback texto
- total_unidades: ~56% (limitado — SPAs como Cury, Vivaz, Metrocasa não expõem essa info)
- numero_vagas: ~39%

## Geocodificação
- **Base local de CEPs**: `data/ceps_brasil.db` (SQLite, 905k CEPs com lat/lon)
- Fonte: basedosdados.org (BigQuery `basedosdados.br_bd_diretorios_brasil.cep`)
- Estratégia: CEP direto > logradouro+cidade > centroide de bairro (com offset ~100m) > NULL (nunca usar coordenada genérica)
- **NUNCA usar Nominatim como primeira opção** — sempre a base local de CEPs primeiro
- Validação por bounding box do estado para evitar coordenadas em estados errados

## Mapa HTML
- `mapa_empreendimentos.html` (gerado por `gerar_mapa.py`, não versionado)
- Leaflet.js, ~3.400 pins, 77 empresas
- Filtros dinâmicos cruzados por status, empresa e cluster MPR
- Popup com link "Ver no site" para cada empreendimento
- Resumo dinâmico da área visível (contagem por empresa + total unidades)
- Clusters MPR via KML (12 arquivos, 61 clusters: SP, RMSP, BA, CE, CPS, GO, JP, MG, PE, PR, RJ, RS)
- Campo `cluster_mpr` no banco — recalcular sempre que coordenadas mudam

## Dashboard Streamlit
- `dashboard/app.py` — plataforma de inteligência competitiva
- Rodar: `cd coleta && python -m streamlit run dashboard/app.py`
- **Módulos**: Mapa, Visão Geral, Tipologias, Lazer, Preços, Lançamentos
- **Filtros globais**: Regional, UF, Cidade, Empresa, Fase + checkboxes MCMV e Tenda
- **Regionais**: SP+SPRM, Nordeste, Sul+MG, RJ+CO+CPS, SP Interior, Outros
- Mapa gera HTML dinâmico no mesmo formato do mapa estático, com filtro de cluster MPR
- Tabela resumo por empresa: Produtos, Lçtos, Breve Lçtos, Área média, Tipologia frequente, %1D, %3D
- Tabela resumo por cluster MPR (quando regional filtrada)

## Regras de negócio

### MCMV
- Todo empreendimento novo é **MCMV por default** (prog_mcmv=1)
- Só marcar prog_mcmv=0 se preço > R$600k ou informado pelo Alejandro
- Empresas NÃO-MCMV conhecidas: Canopus, Trisul, Cosbat, Sertenge, ART Construtora

### Extração de dados das páginas
- Atributos binários (tipologia, lazer, vaga) devem ser extraídos de **seções relevantes** da página, não do texto completo (evitar falso positivo de menus/footers)
- **Metragens**: priorizar seções de "planta"/"tipologia"/"ficha técnica". Excluir contextos de terreno/garagem. MCMV: 15-80m²
- **Tipologias** (dormitórios): basear no `dormitorios_descricao` extraído da ficha, não do texto completo. Menus/filtros do site contaminam os dados
- **Lazer**: varrer página sem nav/footer. Cada item é distinto — **NÃO unificar sem validação do Alejandro** (Pet Place ≠ Pet Care, Beach Tennis ≠ Quadra, Academia ≠ Fitness ≠ Fitness Externo)
- **Cobertura, Duplex, Garden, Suíte**: basear no `dormitorios_descricao` ou seções de tipologia, não no texto completo
- **Vaga**: buscar em seções relevantes. "Edifício garagem" é item específico e importante

### Geocodificação
- **SEMPRE usar base local de CEPs** (`data/ceps_brasil.db`) — NUNCA Nominatim como primeira opção
- Validar coordenadas por **bounding box do estado** após geocodificar
- Recalcular `cluster_mpr` sempre que coordenadas mudarem

### Clusters MPR
- Campo `cluster_mpr` no banco — calculado via point-in-polygon com KMLs de `data/*.kml`
- Ao inserir empreendimento com coordenadas → calcular cluster_mpr
- Ao alterar latitude/longitude → recalcular cluster_mpr

### Regionais (lógica Tenda)
- Ordem: SP+SPRM, Nordeste, Sul+MG, RJ+CO+CPS (inclui Campinas), SP Interior, Outros
- Campinas (Campinas, Hortolândia, Valinhos, Vinhedo) está dentro de RJ+CO+CPS
- DF está em "Outros"
- SP sem cidade reconhecida → SP Interior (não SPRM)

### Qualidade de dados — 3 frentes
1. **Coleta massificada** — scrapers capturam todos os produtos de uma vez
2. **Qualificação individual** — entrar página por página para corrigir dados específicos quando a coleta massificada gera falsos positivos
3. **Auditoria amostral periódica** — 5-10 empreendimentos aleatórios por player, verificar inconsistências, gerar relatório para decisão conjunta

### Postura esperada do Claude
- Agir como **dono do projeto**: resolver lacunas óbvias sem perguntar, não apenas reportar
- Se algo "não está configurado" e eu tenho a informação para configurar → **configurar**
- Processos autônomos devem ser **à prova de falha**: cada etapa independente, testar timeout antes de lançar para madrugada
- Processar em **batches de 200** para evitar timeouts
- **Uma task de cada vez**, com calma

## Como rodar

```bash
# === Coleta do zero (apaga banco) ===
python run_coleta.py

# === Atualização recorrente (mantém banco, detecta mudanças) ===
python run_atualizacao.py
python run_atualizacao.py --sem-email --sem-mapa  # teste rápido

# === Verificação de status (URLs existentes) ===
python scrapers/verificar_status.py                     # Verificar tudo
python scrapers/verificar_status.py --empresa MRV       # Só uma empresa
python scrapers/verificar_status.py --sem-selenium      # Pular Cury/Metrocasa
python scrapers/verificar_status.py --dry-run            # Só verificar, não atualizar
python scrapers/verificar_status.py --limite 5           # Max 5 por empresa (teste)

# === Enriquecimento manual ===
python enriquecer_dados.py
python enriquecer_unidades.py tudo
python qualificar_produto.py
python corrigir_nomes.py
python validar_coordenadas.py

# === Mapa ===
python gerar_mapa.py

# === Relatório PPT ===
python gerar_relatorio_pptx.py

# === Destaques ===
python detectar_destaques.py
```

## Configuração
- `config/settings.py` — paths, timeouts, user-agents, ATUALIZACAO, EMAIL
- `config/regionais.py` — mapeamento de cidades → regionais
- Dependências: requests, beautifulsoup4, selenium, webdriver-manager, lxml, python-pptx, pywin32

## Fase 3 (pendente): Integração KMZ
- Quando `data/mpr_clusters.kmz` for fornecido, modificar `gerar_mapa.py` para overlay com clusters MPR
- Dependências adicionais: fastkml, shapely

# Projeto Coleta — Scrapers de Empreendimentos Imobiliários

## Objetivo
Coletar dados de empreendimentos imobiliários de 22+ incorporadoras brasileiras, enriquecer com geocodificação e dados adicionais, gerar mapa interativo HTML, e manter a base atualizada automaticamente com detecção de mudanças.

## Banco de Dados
- **Arquivo**: `data/empreendimentos.db` (SQLite, ~3MB)
- **Tabela principal**: `empreendimentos` (~90 colunas)
- **~2017 registros** de 22 empresas, **~99,6% com coordenadas**
- MRV (435), Cury (272), Plano&Plano (157), Magik JC (112), Direcional (110), Metrocasa (109), Vivaz (106), Pacaembu (54), Vibra Residencial (50), Kazzas (40), Conx (35), Novolar (30), Árbore (29), Viva Benx (27), Mundo Apto (23), SUGOI (20), Emccamp (17), EPH (13), Ampla (8), Novvo (5), M.Lar (5), Ún1ca (2)

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

### Scrapers específicos (em `scrapers/`)
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

### Problema conhecido (root cause corrigido parcialmente)
- `generico_empreendimentos.py` linha 331 hardcodava `cidade="São Paulo"` para TODAS as empresas genéricas
- Pacaembu atua no interior de SP, MG, PR, MT — foi corrigido via extração de cidade do `<title>` das páginas
- `planoeplano_empreendimentos.py` usava `_nome_da_url()` (slug como nome) em vez de extrair do HTML

## Scripts de Enriquecimento

| Script | Função |
|--------|--------|
| `enriquecer_dados.py` | Geocodificação via base local de CEPs, APIs (Vivaz, MRV), extração de dados das páginas |
| `enriquecer_unidades.py` | Extração de total_unidades, vagas, amenidades via requests/Selenium/APIs |
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
4. Roda enriquecimento (enriquecer_dados, enriquecer_unidades, corrigir_nomes, validar_coordenadas)
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
- **Base local de CEPs**: `C:\Users\aabiusi\ProjetosAI\data\ceps_brasil.db` (SQLite, 905k CEPs com lat/lon)
- Fonte: basedosdados.org (BigQuery `basedosdados.br_bd_diretorios_brasil.cep`)
- Estratégia: CEP direto > logradouro+cidade > centroide de bairro (com offset ~100m) > NULL (nunca usar coordenada genérica)

## Mapa HTML
- `mapa_empreendimentos.html` (gerado por `gerar_mapa.py`, não versionado)
- Leaflet.js + MarkerCluster, ~2000 pins, 22 empresas
- Filtros dinâmicos cruzados por status e empresa (status desabilita empresas sem match e vice-versa)
- Popup com link "Ver no site" para cada empreendimento
- Resumo dinâmico da área visível (contagem por empresa + total unidades)

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

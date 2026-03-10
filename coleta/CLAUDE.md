# Projeto Coleta — Scrapers de Empreendimentos Imobiliários

## Objetivo
Coletar dados de empreendimentos imobiliários de 13 incorporadoras brasileiras, enriquecer com geocodificação e dados adicionais, e gerar um mapa interativo HTML.

## Banco de Dados
- **Arquivo**: `data/empreendimentos.db` (SQLite, ~3MB)
- **Tabela principal**: `empreendimentos` (~90 colunas)
- **1510 registros** de 13 empresas
- Empresas: MRV (435), Cury (272), Plano&Plano (157), Magik JC (112), Direcional (110), Metrocasa (109), Vivaz (106), Pacaembu (54), Vibra Residencial (50), Kazzas (40), Conx (35), Mundo Apto (23), Benx (7)

## Arquitetura dos Scrapers

### Scrapers específicos (em `scrapers/`)
- `mrv_empreendimentos.py` — MRV via API REST (`/api/search`)
- `cury_empreendimentos.py` — Cury via API GraphQL interna
- `vivaz_empreendimentos.py` — Vivaz via API REST (`/imovel/informacoes/`)
- `direcional_empreendimentos.py` — Direcional via requests + BeautifulSoup
- `planoeplano_empreendimentos.py` — Plano&Plano via requests (páginas SSR)
- `metrocasa_empreendimentos.py` — Metrocasa via Next.js API (`/api/properties`)
- `generico_empreendimentos.py` — Scraper genérico para: Pacaembu, Conx, Vibra, Magik JC, Kazzas, Benx, Mundo Apto

### Problema conhecido (root cause corrigido parcialmente)
- `generico_empreendimentos.py` linha 331 hardcodava `cidade="São Paulo"` para TODAS as empresas genéricas
- Pacaembu atua no interior de SP, MG, PR, MT — foi corrigido via extração de cidade do `<title>` das páginas
- `planoeplano_empreendimentos.py` usava `_nome_da_url()` (slug como nome) em vez de extrair do HTML

## Scripts de Enriquecimento

| Script | Função |
|--------|--------|
| `enriquecer_dados.py` | Geocodificação Nominatim, APIs (Vivaz, MRV), extração de dados das páginas |
| `enriquecer_unidades.py` | Extração de total_unidades, vagas, amenidades via requests/Selenium/APIs |
| `enriquecer_selenium_batch.py` | Selenium headless para Kazzas, Conx, Benx (SPAs) |
| `corrigir_nomes.py` | Correção de nomes via `<title>` das páginas e APIs |
| `validar_coordenadas.py` | Validação de coords vs cidade cadastrada (distância haversine) |
| `gerar_mapa.py` | Gera `mapa_empreendimentos.html` com Leaflet.js + MarkerCluster |

## APIs descobertas e utilizadas
- **Vivaz**: POST `https://www.meuvivaz.com.br/imovel/informacoes/` com `{"Url": slug}` → vagas, quartos, varanda, bairro
- **Metrocasa**: GET `https://www.metrocasa.com.br/api/properties?page=X&perPage=50` → amenidades, endereço, tipologia
- **Kazzas**: GET `https://kazzas.com.br/wp-json/wp/v2/empreendimento?per_page=50&_embed` → amenidades via taxonomia WP
- **MRV**: API interna de busca com detalhes por empreendimento
- **Vibra Residencial**: Status extraído da home (`section#produtos`, `section#produtos-em-obras`, `section#produtos-prontos`)

## Completude atual dos dados
- dormitorios_descricao: 98.6%
- cidade: 95.8%
- endereco: 90.7%
- latitude/longitude: 84.3%
- fase (status): ~90% (Vibra e Plano&Plano corrigidos recentemente)
- total_unidades: 55.6% (limitado — SPAs como Cury, Vivaz, Metrocasa não expõem essa info)
- numero_vagas: 39.3%

## Mapa HTML
- `mapa_empreendimentos.html` (gerado por `gerar_mapa.py`, não versionado)
- Leaflet.js + MarkerCluster, ~1307 pins
- Filtros por empresa e status com "Marcar/Desmarcar todos"
- Resumo dinâmico da área visível (contagem por empresa + total unidades)

## Como rodar
```bash
# Coleta inicial
python run_coleta.py

# Enriquecimento
python enriquecer_dados.py
python enriquecer_unidades.py tudo
python corrigir_nomes.py

# Mapa
python gerar_mapa.py
```

## Configuração
- `config/settings.py` — paths, timeouts, user-agents
- Dependências: requests, beautifulsoup4, selenium, webdriver-manager, lxml

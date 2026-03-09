# Análise de Vendas por Geolocalização

## Visão Geral
Ferramenta web Streamlit para análise de vendas imobiliárias da concorrência por raio geográfico. Usada em apresentações à diretoria da Tenda. Permite importar dados SIOPI (Caixa Econômica Federal) em Excel/CSV, geocodificar empreendimentos, e analisar vendas por raio geográfico com dashboards interativos.

## Stack
- **Frontend/Backend:** Streamlit 1.55 + Plotly + pydeck
- **Banco local:** SQLite (importado de Excel/CSV)
- **Geocodificação:** Nominatim (OpenStreetMap) via geopy + script batch
- **Dados:** MySQL AWS (quando acessível) ou CSV/Excel fallback
- **Python:** 3.14 (testado no Windows 11 Enterprise)

## Estrutura
```
analise-geo-vendas/
├── app.py                          # App principal Streamlit
├── geocodificar_empreendimentos.py # Script batch para geocodificar empreendimentos
├── mapear_ceps_bairros.py          # Script batch para mapear CEPs → bairros (ViaCEP)
├── requirements.txt
├── CLAUDE.md                       # Este arquivo (contexto para o Claude)
├── assets/
│   └── style.css                   # CSS customizado (sidebar escura, métricas, spinner)
├── src/
│   ├── database.py                 # Conexão MySQL/SQLite, importação CSV/Excel, mapeamento SIOPI
│   ├── geocoding.py                # Geocodificação de endereços (geopy)
│   ├── spatial.py                  # Filtro por raio (Haversine vetorizado com numpy)
│   └── charts.py                   # ~20 gráficos Plotly organizados por categoria
└── data/                           # Gitignored — dados locais
    ├── vendas.db                   # SQLite com dados importados
    ├── geocode_cache.json          # Cache de geocodificação
    └── cep_bairros.json            # Cache de mapeamento CEP → bairro
```

## Fonte de Dados
- **Base SIOPI** (Caixa Econômica Federal): Excel com ~60 colunas por venda
- Detecção automática do formato SIOPI e mapeamento de colunas (ver `MAPA_COLUNAS_SIOPI` em `database.py`)
- Colunas principais: incorporadora, empreendimento, município, preço, renda, tipologia, endereço, CEP cliente
- Base atual: ~47k registros, 36 cidades (interior SP, PR, GO, MT), incorporadora Pacaembu

## Funcionalidades Implementadas
- **Importação**: Upload de Excel/CSV direto no app, detecção automática SIOPI
- **Mapa interativo**: pydeck com pontos coloridos por incorporadora, tooltip, zoom automático
- **Filtros**: cidade, empreendimento, raio geográfico, período, incorporadora, renda, preço, tipologia
- **Filtro por raio**: define centro via empreendimento selecionado ou endereço, busca todas as vendas no raio
- **Clique no mapa**: modal (`@st.dialog`) perguntando se quer filtrar pelo empreendimento clicado
- **6 abas de análise**: Volume de Vendas, Perfil do Comprador, Perfil do Produto, Análise Financeira, Velocidade de Vendas, Origem dos Clientes
- **Exportação**: Excel e CSV dos dados filtrados
- **Geocodificação batch**: script que geocodifica empreendimentos por endereço (3 níveis de fallback)
- **Mapeamento CEP → bairro**: script que consulta ViaCEP para traduzir CEPs em bairros

## Convenções
- Código e comentários em português
- Streamlit cache (`@st.cache_data`) para queries pesadas
- Dados sensíveis ficam em `.env` e `data/` (gitignored)
- Cada `st.plotly_chart()` tem um `key` único para evitar `StreamlitDuplicateElementId`
- Faixas de renda: R$500 em R$500, de "Menor de 2.000" até "Acima de 8.000"

## Como rodar
```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Rodar o app
streamlit run app.py

# 3. No app, importar o arquivo Excel/CSV via sidebar

# 4. (Opcional) Geocodificar empreendimentos
python geocodificar_empreendimentos.py

# 5. (Opcional) Mapear CEPs dos clientes para bairros
python mapear_ceps_bairros.py
```

## Problemas Conhecidos / Pendências
- `on_select="rerun"` no pydeck pode causar loops em alguns cenários — há guard com `_mapa_sel_processado`
- Mapeamento CEP → bairro incompleto (~300 de ~20k CEPs mapeados) — requer rodar `mapear_ceps_bairros.py` (~2h)
- Testar com base maior contendo múltiplas incorporadoras (dados no PC pessoal)
- Conexão MySQL AWS ainda não testada (necessita `.env` com credenciais)

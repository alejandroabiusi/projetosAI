# Análise de Vendas por Geolocalização

## Visão Geral
Ferramenta web Streamlit para análise de vendas imobiliárias da concorrência por raio geográfico. Usada em apresentações à diretoria da Tenda.

## Stack
- **Frontend/Backend:** Streamlit + Plotly + pydeck
- **Banco local:** SQLite (importado de CSV)
- **Geocodificação:** geopy (Nominatim/Google Maps)
- **Dados:** MySQL AWS (quando acessível) ou CSV fallback

## Estrutura
- `app.py` — app principal Streamlit
- `src/database.py` — conexão MySQL/SQLite, importação CSV
- `src/geocoding.py` — geocodificação de endereços
- `src/spatial.py` — queries de raio (Haversine)
- `src/charts.py` — gráficos Plotly

## Convenções
- Código e comentários em português
- Streamlit cache (`@st.cache_data`) para queries pesadas
- Dados sensíveis ficam em `.env` e `data/` (gitignored)

## Como rodar
```bash
pip install -r requirements.txt
streamlit run app.py
```

# Mapeamento Tecnológico — 37 Novos Sites (2026-04-03)

## Resumo por Tecnologia

| Tecnologia | Qtd | Sites |
|-----------|-----|-------|
| WordPress + Elementor (SSR) | 16 | SOL, Maccris, Versati, Victa, Brava, SR, Tenório Simões, Exata, Bora, Grupo Delta, Você, CAC, Domma, Quartzo, Morana, Lotus |
| WordPress + Elementor + JetEngine | 3 | Belmais, Lotus, MGF |
| WordPress + WPBakery | 1 | Maccris |
| WordPress + Divi | 1 | Versati |
| Next.js (React SSR) | 2 | ART, Canopus (canopus.com.br) |
| Angular SPA | 1 | Trisul |
| ASP.NET + jQuery (SSR) | 1 | Tenda |
| Custom SSR (não-WP) | 3 | Vitta Residencial, Canopus Construções, Sertenge |
| Wix SPA | 2 | Tecol, Bicalho |
| A verificar | 7 | EBM, Somos, Vila Brasil, Vega, Cosbat (cert expirado), Torreão Villarim, Dimensional, House Inc, Mirantes, AP Ponto |

## Detalhamento por Site

### FÁCEIS (WordPress SSR — adicionar ao generico_empreendimentos.py)

| # | Empresa | Site | Tech | Empreend. | URL Pattern | Obs |
|---|---------|------|------|-----------|-------------|-----|
| 1 | SOL | solconstrutora.com | WP+Elementor | ~8 | /[slug]/ | Guarulhos |
| 2 | Versati | grupoversati.com.br | WP+Divi | ~23 | A mapear | SJC |
| 3 | Victa Eng. | victa.eng.br | WP custom | 10 | /nossosimoveis/[slug]/ | Fortaleza/CE |
| 4 | SR Eng. | srengenharia.com | WP+Elementor | 22 | /[slug] | Caucaia/CE |
| 5 | Tenório Simões | tenoriosimoes.com.br | WP | A mapear | A mapear | Recife/PE |
| 6 | Exata | exataengenharia.com.br | WP | A mapear | A mapear | Recife/PE |
| 7 | Bora | borainc.com.br | WP+ACF | A mapear | A mapear | Recife/PE |
| 8 | Grupo Delta | grupodeltapb.com.br | WP | A mapear | A mapear | João Pessoa/PB |
| 9 | Você | construtoravoce.com.br | WP+Elementor | A mapear | A mapear | RJ/MG |
| 10 | CAC | cacengenharia.com.br | WP | A mapear | A mapear | MG/RJ/SP |
| 11 | Domma | dommainc.com.br | WP | A mapear | A mapear | RJ |
| 12 | Quartzo | vivaquartzo.com.br | WP+Elementor | ~15 | /empreendimentos/[slug]/ | MG/SP |
| 13 | AP Ponto | apponto.com.br | WP+Elementor+Jet | ~54 | A mapear | MG/SP |
| 14 | Morana | morana.net | WP+Elementor | 5 | /empreendimentos/ | POA/RS |
| 15 | Belmais | belmais.com.br | WP+Elementor+Jet | 12 | /empreendimento/[slug]/ | RS |
| 16 | Lotus | lotusincorporadora.com.br | WP+Elementor+Jet | 10 | /empreendimentos/ | POA/RS |
| 17 | MGF | mgfincorporadora.com.br | WP+Elementor+Jet | 22+ | /imovel/[slug]/ | RS |
| 18 | Maccris | maccrisconstrutora.com.br | WP+WPBakery | 12+ | /portfolio/[slug]/ | ABC/SP |
| 19 | Brava | somosbrava.com.br | WP+Elementor | 3 (incorp.) | /incorporacao/ | Fortaleza/CE |
| 20 | House Inc | houseincorporacoes.com.br | WP | 14 | /empreendimento/house-[slug]/ | Salvador/BA |

### MÉDIOS (SSR não-WP ou Next.js)

| # | Empresa | Site | Tech | Empreend. | URL Pattern | Obs |
|---|---------|------|------|-----------|-------------|-----|
| 21 | Tenda | tenda.com | ASP.NET+jQuery | 100+ | /apartamentos-a-venda/[uf]/[cidade]/[slug] | Nacional, precisa API interna |
| 22 | Canopus | canopus.com.br | Next.js | 31 | /[slug] | SP/RJ/MG |
| 23 | Canopus Constr. | canopusconstrucoes.com.br | Custom SSR | A mapear | /[cidade]/imoveis/[slug] | MA/CE/PA/PI |
| 24 | Vitta Resid. | vittaresidencial.com.br | Custom SSR | Muitos | /[cidade]/imoveis/[slug] | SP int/MG |
| 25 | ART | artconstrutora.com.br | Next.js | 5 | /empreendimento/[slug] | ABC/SP |
| 26 | Sertenge | sertenge.com.br | Custom (CRM) | 30+ | A mapear | Salvador/BA |

### DIFÍCEIS (SPA/Angular/Wix)

| # | Empresa | Site | Tech | Empreend. | Obs |
|---|---------|------|------|-----------|-----|
| 27 | Trisul | trisul-sa.com.br | Angular | Muitos | SPA, precisa Selenium ou API interna |
| 28 | Tecol | tecolengenharia.com.br | Wix | A mapear | Wix SPA, difícil scraping |
| 29 | Bicalho | bicalhoempreendimentos.com.br | Wix | A mapear | Wix SPA, difícil scraping |

### A VERIFICAR

| # | Empresa | Site | Problema |
|---|---------|------|---------|
| 30 | Cosbat | cosbat.com.br | Certificado SSL expirado |
| 31 | Torreão Villarim | torreaovillarim.com.br | 404 no wp-json |
| 32 | Dimensional | dimensionaljp.com.br | DNS não resolve |
| 33 | Mirantes | soumirantes.com.br | 404 no wp-json |
| 34 | EBM | ebm.com.br | WP API sem custom post types |
| 35 | Somos | somosdi.com.br | WP API sem custom post types |
| 36 | Vila Brasil | vilabr.com.br | WP API sem custom post types |
| 37 | Vega | vcinc.com.br | WP API sem custom post types |

## Ordem de Implementação Sugerida

### Batch 1 — Genérico SSR (adicionar configs ao generico_empreendimentos.py)
SOL, Victa, SR, Maccris, House Inc, Belmais, Morana, Lotus, MGF, Quartzo, Brava

### Batch 2 — Genérico SSR (mais complexos)
Tenório Simões, Exata, Bora, Grupo Delta, Você, CAC, Domma, Versati, AP Ponto

### Batch 3 — SSR custom / Next.js
Canopus (Next.js), ART (Next.js), Vitta Residencial, Canopus Construções, Sertenge

### Batch 4 — Complexos (API interna / SPA)
Tenda (ASP.NET, API interna), Trisul (Angular), EBM, Somos, Vila Brasil, Vega

### Batch 5 — Wix / Problemas
Tecol, Bicalho, Cosbat, Torreão, Dimensional, Mirantes

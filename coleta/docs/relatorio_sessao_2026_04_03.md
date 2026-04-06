# Relatório de Sessão — 03 de Abril de 2026

## Resumo Executivo

Sessão de trabalho intensiva que cobriu: pesquisa de 38 incorporadoras, mapeamento tecnológico de 37 sites, construção de scrapers, coleta de empreendimentos, e auditoria geral dos dados.

**Resultado principal:** Base passou de **2.290** para **3.476 empreendimentos** (+1.186) de **43** para **78 empresas** (+35 novas).

> **NOTA**: Relatório atualizado em 04/04/2026 com números finais após enriquecimento.

### Enriquecimento realizado (04/04):
- Geocodificação via base local de CEPs: **+556 coordenadas** (89.4% com coords)
- Mapa regenerado com **3.107 pins** de **66 empresas** (era 1.307)
- Download de imagens: **36.068+** imagens em disco
- Flags de dormitórios e varanda atualizados
- Correção de nomes via APIs

---

## 1. Pesquisa de Sites das Incorporadoras

### 38 incorporadoras pesquisadas:

| # | Empresa | Site | UF | Status |
|---|---------|------|----|--------|
| 1 | Tenda | tenda.com | Nacional | **COLETADA** (63 empreend.) |
| 2 | Canopus | canopus.com.br | SP/RJ/MG | **COLETADA** (25) |
| 3 | Canopus Construções | canopusconstrucoes.com.br | MA/CE/PA | Pendente |
| 4 | Victa Engenharia | victa.eng.br | CE | **COLETADA** (10) |
| 5 | Vitta Residencial | vittaresidencial.com.br | SP int/MG | Pendente (SSR custom) |
| 6 | Trisul | trisul-sa.com.br | SP | Pendente (Angular SPA) |
| 7 | SOL Construtora | solconstrutora.com | SP/Guarulhos | **COLETADA** (10) |
| 8 | Smart | smartincorporadora.com.br | SP/Guarulhos | Já na base (13) |
| 9 | Maccris | maccrisconstrutora.com.br | SP/ABC | **COLETADA** (9) |
| 10 | ART | artconstrutora.com.br | SP/ABC | Pendente (Next.js) |
| 11 | Versati | grupoversati.com.br | SP/SJC | Pendente (SPA Divi) |
| 12 | Cosbat | cosbat.com.br | BA | Pendente (cert SSL expirado) |
| 13 | House Inc | houseincorporacoes.com.br | BA | **COLETADA** (10) |
| 14 | Sertenge | sertenge.com.br | BA | **COLETADA** (via genérico) |
| 15 | Brava | somosbrava.com.br | CE | Não viável (dados inline, sem páginas individuais) |
| 16 | SR Engenharia | srengenharia.com | CE | **COLETADA** (21) |
| 17 | Tenório Simões | tenoriosimoes.com.br | PE | Pendente (SPA, links não visíveis) |
| 18 | Carrilho | carrilho.com.br | PE | Já na base (10) |
| 19 | Exata | exataengenharia.com.br | PE | **COLETADA** (12) |
| 20 | Bora | borainc.com.br | PE | **COLETADA** (2) |
| 21 | Grupo Delta | grupodeltapb.com.br | PB | Pendente (links não encontrados) |
| 22 | Torreão Villarim | torreaovillarim.com.br | PB | Pendente (não é WP) |
| 23 | Mirantes | soumirantes.com.br | PB/RN | Pendente (não é WP) |
| 24 | Dimensional | dimensionaljp.com.br | PB | Pendente (DNS não resolve) |
| 25 | Você | construtoravoce.com.br | RJ/MG | **COLETADA** (5) |
| 26 | CAC | cacengenharia.com.br | MG/RJ/SP | **COLETADA** (4) — problemas nos nomes |
| 27 | Domma | dommainc.com.br | RJ | **COLETADA** (6) |
| 28 | EBM | ebm.com.br | GO/DF/SP | Pendente (WP sem custom posts) |
| 29 | Somos | somosdi.com.br | GO | Pendente (WP sem custom posts) |
| 30 | Vila Brasil | vilabr.com.br | GO/SP/RJ | Pendente (WP sem custom posts) |
| 31 | Vega | vcinc.com.br | GO/SP/DF | Pendente (WP sem custom posts) |
| 32 | Tecol | tecolengenharia.com.br | MS | Pendente (Wix SPA) |
| 33 | Quartzo | vivaquartzo.com.br | MG/SP | **COLETADA** (26) |
| 34 | AP Ponto | apponto.com.br | MG/SP | **COLETADA** (20) |
| 35 | Bicalho | bicalhoempreendimentos.com.br | PR | Pendente (Wix SPA) |
| 36 | Morana | morana.net | RS | **COLETADA** (10) |
| 37 | Belmais | belmais.com.br | RS | **COLETADA** (12) |
| 38 | Lotus | lotusincorporadora.com.br | RS | **COLETADA** (10) |
| 39 | MGF | mgfincorporadora.com.br | RS | **COLETADA** (36) |

---

## 2. Mapeamento Tecnológico

Documento completo em: `docs/mapeamento_novos_sites.md`

### Classificação por tecnologia:
- **WordPress + Elementor (SSR)**: 16 sites — mais fáceis, adicionar ao `generico_empreendimentos.py`
- **WordPress + JetEngine**: 3 sites (Belmais, Lotus, MGF)
- **Next.js (React SSR)**: 2 sites (ART, Canopus)
- **Angular SPA**: 1 site (Trisul) — requer Selenium
- **ASP.NET + jQuery**: 1 site (Tenda) — scraper específico criado
- **Custom SSR**: 3 sites (Vitta, Canopus Construções, Sertenge)
- **Wix SPA**: 2 sites (Tecol, Bicalho) — difícil scraping
- **Problemas**: 4 sites (Cosbat cert expirado, Dimensional DNS, Torreão/Mirantes sem WP)

---

## 3. Scrapers Criados/Modificados

### Novo scraper dedicado:
- **`scrapers/tenda_empreendimentos.py`** — Navega por estado/cidade para coletar todos os empreendimentos da Tenda (ASP.NET)

### Adições ao `scrapers/generico_empreendimentos.py` (22 novas configs):
sol, victa, sr_engenharia, maccris, house_inc, belmais, morana, lotus, mgf, quartzo, brava, tenorio_simoes, exata, bora_inc, grupo_delta, voce, cac, domma, ap_ponto, versati, canopus, sertenge

---

## 4. Coleta Realizada

### Novas empresas coletadas (18):
| Empresa | Empreendimentos | UF |
|---------|-----------------|-----|
| Tenda | 63 | Nacional |
| MGF | 36 | RS |
| Quartzo | 26 | MG/SP |
| Canopus | 25 | SP/RJ/MG |
| SR Engenharia | 21 | CE |
| AP Ponto | 20 | MG/SP |
| Exata | 12 | PE |
| Belmais | 12 | RS |
| Victa Engenharia | 10 | CE |
| SOL Construtora | 10 | SP |
| Morana | 10 | RS |
| Lotus | 10 | RS |
| House Inc | 10 | BA |
| Maccris | 9 | SP |
| Domma | 6 | RJ |
| Você | 5 | RJ |
| CAC | 4 | MG |
| Bora | 2 | PE |
| **TOTAL** | **291** | |

### Empresas via WP API (adicionadas ao wpapi_empreendimentos.py):
| Empresa | Empreendimentos | UF | Post Type |
|---------|-----------------|-----|-----------|
| CAC | 42 | MG/RJ/SP | empreendimento |
| EBM | 81 | GO/DF/SP | produtos |
| Vega | 30 | GO/SP/DF | imoveis |
| Exata (WP) | 19 | PE | portfolio |
| Bora (WP) | 5 | PE | empreendimentos |
| Domma (WP) | 6 | RJ | empreendimento |

### Estado do banco:
- **Antes**: 2.290 empreendimentos, 43 empresas
- **Depois**: 3.189 empreendimentos, 66 empresas (+39,3%)

---

## 5. Auditoria dos Dados

### 5.1 Problemas CRÍTICOS encontrados nos dados antigos:

#### VIC Engenharia: 100/185 empreendimentos como "Breve Lançamento" (54%)
- **Muito provavelmente erro** — impossível ter 100 breves lançamentos
- O scraper WP API está detectando a fase incorretamente
- **Ação recomendada**: Reprocessar a detecção de fase de todos os 100 registros da VIC

#### Smart Construtora: 9/13 como "Breve Lançamento" (69%)
- Mesmo problema — fase detectada incorretamente
- **Ação recomendada**: Revalidar fases da Smart

### 5.2 Problemas nos dados novos:

#### Nomes incorretos (CAC):
- 4 registros da CAC têm como nome o título da home page ("Imóveis à venda em MG, RJ e SP")
- **Ação recomendada**: Apagar e re-coletar com parser de nome corrigido

#### Falta de cidade (86 registros novos):
| Empresa | Sem cidade |
|---------|-----------|
| Quartzo | 22 |
| SR Engenharia | 21 |
| Morana | 10 |
| House Inc | 10 |
| Maccris | 9 |
| Belmais | 9 |
| Lotus | 3 |
| Exata | 1 |
| Bora | 1 |

**Ação recomendada**: Rodar `enriquecer_dados.py` para geocodificar + preencher cidades

#### Falta de fase (47 registros novos):
| Empresa | Sem fase |
|---------|---------|
| MGF | 22 |
| House Inc | 10 |
| Morana | 8 |
| Lotus | 5 |
| Bora | 2 |

#### Empreendimentos com metragem >60m² (possivelmente não MCMV):
| Empresa | Empreendimento | Área máx | Cidade |
|---------|----------------|---------|--------|
| Exata | Terrazza Beira Mar | 140m² | Jaboatão |
| Exata | Terrazza Apipucos | 139m² | Recife |
| Exata | Terrazza Beira Rio | 132m² | Recife |
| Victa | Vista Costeira | 121m² | Fortaleza |
| Victa | Atlântico | 100m² | Fortaleza |
| CAC | (vários) | 116m² | MG |
| Domma | Unic Primavera | 93m² | RJ |
| Lotus | Sky/Riviera/Gardens | 73m² | POA |

**Nota**: Estes provavelmente NÃO são MCMV — são segmento médio/alto. Não é erro, mas vale sinalizar.

---

## 6. Reorganização de Pastas

Migração concluída de `C:\Projetos_AI\projetosAI_repo\` → `C:\ProjetosAI\`:
- Pasta única com git, eliminando duplicação
- Todos os dados exclusivos preservados (downloads 23GB, backups, movimentacoes.db, transcricoes.db)
- Instruções gravadas na memória do Claude para evitar recriação de pastas soltas

---

## 7. Empresas Pendentes (19)

### Alta prioridade (sites viáveis, aguardando ajuste):
1. **Canopus Construções** (MA/CE/PA) — SSR custom, precisa mapear padrão de URL
2. **Vitta Residencial** (SP int) — SSR custom, muitos empreendimentos
3. **ART** (SP/ABC) — Next.js, apenas 5 empreendimentos
4. **Grupo Delta** (PB) — WP mas links não encontrados via listagem
5. **Tenório Simões** (PE) — SPA, links não visíveis via requests

### Média prioridade (requerem Selenium ou investigação):
6. **Trisul** (SP) — Angular SPA, requer Selenium
7. **EBM** (GO) — WP sem custom posts expostos
8. **Somos** (GO) — WP sem custom posts
9. **Vila Brasil** (GO) — WP sem custom posts
10. **Vega** (GO) — WP sem custom posts
11. **Versati** (SP/SJC) — SPA Divi, requer Selenium

### Baixa prioridade (problemas técnicos):
12. **Tecol** (MS) — Wix SPA
13. **Bicalho** (PR) — Wix SPA
14. **Cosbat** (BA) — certificado SSL expirado
15. **Torreão Villarim** (PB) — não é WP
16. **Mirantes** (PB/RN) — não é WP
17. **Dimensional** (PB) — DNS não resolve
18. **Brava** (CE) — dados inline sem páginas individuais (apenas 3 empreend.)
19. **Sertenge** — scraper criado mas resultado a verificar

---

## 8. Próximos Passos Recomendados

1. **Rodar `enriquecer_dados.py`** para geocodificar os 291 novos empreendimentos
2. **Corrigir VIC Engenharia** — reprocessar detecção de fase (100 breves lançamentos é erro)
3. **Corrigir Smart Construtora** — revalidar fases (9/13 breves é erro)
4. **Corrigir CAC** — refazer coleta com parser de nome ajustado
5. **Implementar scrapers Selenium** para: Trisul, Versati, Tenório Simões
6. **Investigar WP sites** (EBM, Somos, Vila Brasil, Vega) — podem ter API escondida
7. **Regenerar mapa** — estava com 1.307 marcadores, agora deveria ter ~2.500+
8. **Adicionar novas empresas ao `run_atualizacao.py`** e ao `verificar_status.py`

---

## Arquivos criados/modificados nesta sessão:

| Arquivo | Ação |
|---------|------|
| `scrapers/tenda_empreendimentos.py` | Novo scraper |
| `scrapers/generico_empreendimentos.py` | +22 novas configs de empresas |
| `docs/mapeamento_novos_sites.md` | Mapeamento tecnológico completo |
| `docs/relatorio_sessao_2026_04_03.md` | Este relatório |

---

*Relatório gerado em 03/04/2026 por Claude Code durante sessão de trabalho autônomo.*

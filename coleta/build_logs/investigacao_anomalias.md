# Investigacao de Anomalias por Amostragem

Data: 2026-04-08

## 1. Enderecos Ausentes

### SR Engenharia (19/21 sem endereco)

- URL 1: https://srengenharia.com/essenza-rooftop
  - Endereco na pagina? NAO
  - A pagina nao exibe endereco, bairro, mapa ou coordenadas
  - Apenas WhatsApp de contato e specs do produto (57m2, 2-3 quartos)
  - Nenhum schema.org com localizacao

- URL 2: https://srengenharia.com/gran-vitoria-residence-3
  - Endereco na pagina? NAO (parcial)
  - Bairro mencionado: Cumurupim (na secao "Sobre a regiao do empreendimento")
  - Cidade mencionada: Caucaia (no texto descritivo)
  - Sem endereco de rua, sem mapa, sem coordenadas

- URL 3: https://srengenharia.com/natureresidence
  - Endereco na pagina? NAO
  - Cidade mencionada: Caucaia (no texto promocional)
  - Sem rua, sem bairro, sem mapa

**Diagnostico:** Site nao publica enderecos. Modelo de negocios via WhatsApp -- o visitante pede a localizacao.
**Recomendacao:** Aceitar como limitacao do site. O scraper ja extrai a cidade corretamente. Endereco nao esta disponivel para extracao automatica.

---

### Cosbat (12/12 sem endereco)

- URL 1: https://cosbat.com.br/empreendimentos/mansao-goes-mascarenhas/
  - ERRO: Certificado SSL expirado (site inacessivel)
  
- URL 2: https://cosbat.com.br/empreendimentos/terrazzo-rio-vermelho/
  - ERRO: Certificado SSL expirado (site inacessivel)

**Diagnostico:** Site com certificado SSL expirado -- impossivel acessar. Possivelmente os dados foram coletados quando o site estava acessivel.
**Recomendacao:** Aguardar renovacao do certificado ou remover empresa se o site permanecer offline.

---

### Maccris (8/9 sem endereco)

- URL 1: https://www.maccrisconstrutora.com.br/portfolio/switch
  - Endereco na pagina? NAO
  - Apenas "Santo Andre - Sao Paulo" no footer (endereco da empresa, nao do empreendimento)
  - Sem secao de localizacao, sem mapa, sem schema.org

- URL 2: https://www.maccrisconstrutora.com.br/portfolio/cube-ipanema
  - Endereco na pagina? NAO
  - Mesma situacao: footer com endereco da sede

- URL 3: https://www.maccrisconstrutora.com.br/portfolio/cube-itavuvu
  - Endereco na pagina? NAO
  - Cidade mencionada: Sorocaba (no texto promocional "melhor de Sorocaba")
  - Sem rua, sem mapa

**Diagnostico:** Site tipo portfolio/vitrine. Paginas de empreendimento sao showcases visuais sem informacao de localizacao. Nao publicam endereco.
**Recomendacao:** Aceitar como limitacao do site. Cidade ja esta no banco.

---

### Engelux (8/8 sem endereco)

- URL 1: https://www.engelux.com.br/liber-jacana
  - Endereco na pagina? SIM - COMPLETO
  - Rua: "R. Freire Bastos, 280 - Jardim Modelo, Sao Paulo - SP, 02261-020"
  - Coordenadas: lat=-23.466375, lon=-46.585213
  - Google Maps link embutido
  - Showroom separado tambem listado

- URL 2: https://www.engelux.com.br/golden-life-tatuape
  - Endereco na pagina? SIM - COMPLETO
  - Rua: "Rua Cel. Gustavo Santiago, 220"
  - Coordenadas: lat=-23.535797, lon=-46.565629
  - Links Google Maps e Waze

- URL 3: https://www.engelux.com.br/ayna-ipiranga
  - Endereco na pagina? SIM - COMPLETO
  - Rua: "Rua Costa Aguiar, 1650 - Ipiranga"
  - Coordenadas: lat=-23.588492, lon=-46.606377
  - Google Maps e Waze embutidos

**Diagnostico:** FALHA DE EXTRACAO. O site Engelux publica endereco completo com coordenadas, Google Maps e Waze em TODAS as paginas. O scraper nao esta capturando nenhum desses dados. Provavel que a estrutura HTML/JS da Engelux seja diferente do esperado pelo generico.
**Recomendacao:** PRIORIDADE ALTA. Ajustar scraper para Engelux -- site tem dados ricos (endereco + CEP + coordenadas + links de mapa). Possivelmente precisa de parser especifico ou ajuste no generico para detectar a estrutura da Engelux.

---

### MP Incorporadora (11/14 sem endereco)

- URL 1: https://mpincorporadora.com.br/encantos-do-fonseca
  - Endereco na pagina? NAO (apenas bairro "Fonseca" e cidade "Niteroi" no contexto)
  - O campo endereco no banco contem lixo: texto promocional capturado por engano

- URL 2: https://mpincorporadora.com.br/jardim-central_3
  - Endereco na pagina? NAO
  - Apenas "Regiao Central de Sao Goncalo" no texto

- URL 3: https://mpincorporadora.com.br/monumentalokara
  - Endereco na pagina? NAO
  - Bairro: Sao Lourenco mencionado no texto
  - Cidade: Niteroi

**Diagnostico:** Site nao publica enderecos de rua. Apenas menciona bairro/regiao no texto descritivo. O unico registro COM endereco (id=4360) na verdade capturou texto promocional como endereco.
**Recomendacao:** Aceitar como limitacao. O caso id=4360 com endereco-lixo deve ser limpo -- o campo capturou texto de marketing.

---

### Due (9/12 sem endereco)

- URL 1: https://www.dueinc.com.br/empreendimento/costa-dos-coqueiros/
  - Endereco na pagina? NAO
  - Localizacao: "Praia dos Carneiros, Pernambuco" (texto descritivo)
  - Referencia juridica: matricula em cartorio de Tamandare

- URL 2: https://www.dueinc.com.br/empreendimento/cais-eco-residencia/
  - Endereco na pagina? NAO
  - Localizacao: "Muro Alto, Pernambuco" (praia/regiao)
  - Matricula registrada em Ipojuca

**Diagnostico:** Empreendimentos de praia/resort -- modelo diferente de MCMV urbano. Nao publicam endereco de rua (faz sentido para loteamentos na praia).
**Recomendacao:** Aceitar como limitacao. Sao empreendimentos turisticos/resort, nao residencias urbanas.

---

### HSM (30 sem fase E sem endereco)

- URL 1: https://www.hsmconstrutora.com/empreendimento/mirante-de-porto
  - Conteudo da pagina? VAZIO
  - Apenas Google Tag Manager, sem conteudo renderizado
  - Pagina eh JS-rendered (SPA)

- URL 2: https://www.hsmconstrutora.com/empreendimento/porto-ibiza
  - Conteudo da pagina? VAZIO
  - Mesma situacao: apenas tracking scripts, sem conteudo estatico

**Diagnostico:** Site da HSM eh 100% renderizado via JavaScript (SPA). O scraper HTTP nao consegue capturar nenhum dado. Todo o conteudo (endereco, fase, tipologia) depende de JS para carregar.
**Recomendacao:** HSM precisa de Selenium para qualquer extracao. Sem Selenium, os 30 registros ficarao vazios. Avaliar se vale o investimento (30 empreendimentos).

---

### Usina de Obras (11 sem fase E sem endereco)

- URL 1: https://www.usinadeobras.com.br/empreendimento/engenho-planalto
  - Conteudo da pagina? VAZIO (apenas Facebook pixel + GTM)
  - Site JS-rendered

- URL 2: https://www.usinadeobras.com.br/empreendimento/residencial-suassuna-quadra-1
  - Conteudo da pagina? VAZIO
  - Mesma situacao: SPA sem conteudo estatico

**Diagnostico:** Identico a HSM -- site 100% SPA, requer Selenium.
**Recomendacao:** Precisa Selenium. 11 empreendimentos -- baixa prioridade.

---

### EBM (19/78 sem endereco)

- URL COM endereco: https://ebm.com.br/go/goiania/now/now111/
  - Endereco na pagina? SIM
  - "Avenida Paranaiba e Rua 72, Quadra 111, Lote 4-117-6, Setor Central. Goiania-GO"
  - Secao "Localizacao Privilegiada"
  - Link "Ver no Maps" presente

- URL COM endereco: https://ebm.com.br/df/brasilia/now/now-29/
  - Endereco na pagina? SIM
  - "QNM-29, Area Especial Lote G, Ceilandia-DF"
  - Coordenadas no link Waze: -16.738111, -49.281333
  - Google Maps + Waze links

- URL SEM endereco (id=3025): https://ebm.com.br/df/
  - NAO eh empreendimento! Eh pagina de LISTAGEM regional (DF)
  - Lista empreendimentos da regiao DF com filtros

- URL SEM endereco (id=3027): https://ebm.com.br/go/
  - NAO eh empreendimento! Pagina de listagem regional (GO)

- URL SEM endereco (id=3029): https://ebm.com.br/sp/
  - NAO eh empreendimento! Pagina de listagem regional (SP)

**Diagnostico:** Os 19 sem endereco incluem PAGINAS QUE NAO SAO EMPREENDIMENTOS -- sao paginas de listagem regional (/df/, /go/, /sp/) e paginas de categoria (/mais-empreendimentos/). O scraper esta capturando URLs de navegacao do site como se fossem produtos.
**Recomendacao:** PRIORIDADE ALTA. Limpar registros que sao paginas de listagem/categoria, nao empreendimentos reais. Ajustar scraper EBM para filtrar URLs de navegacao (ex: URLs sem 4+ segmentos de path).

---

## 2. Cidades Suspeitas

### Cidades que sao bairros (confirmados)

- id=5260 **Abiatar** "Innovare Aricanduva" - cidade="Aricanduva"
  - URL: https://abiatar.com/empreendimentos/innovare-aricanduva
  - Pagina diz: "Av. Aricanduva, Aricanduva - SP"
  - CONFIRMADO: Aricanduva eh BAIRRO de Sao Paulo, nao cidade
  - Cidade correta: Sao Paulo

- id=5351 **Habras** "Arte Campo Limpo" - cidade="Campo Limpo"
  - URL: https://habrasconstrutora.com.br/arte-campo-limpo/
  - Pagina diz: "Januario Zingaro, 411 - Campo Limpo, SP"
  - CONFIRMADO: Campo Limpo eh BAIRRO de Sao Paulo
  - Cidade correta: Sao Paulo

- id=5345 **Reitzfeld** "Breeze Bosque da Saude" - cidade="Saude"
  - URL: https://reitzfeld.com.br/breeze-bosque-da-saude.html
  - Pagina diz: "Rua Guararema, 694 - Bosque da Saude - SP"
  - Developer em: "Itaim Bibi / Sao Paulo - SP"
  - CONFIRMADO: Saude eh BAIRRO de Sao Paulo (Zona Sul)
  - Cidade correta: Sao Paulo

- id=7796 **Prohidro** "Eleva Tatuape" - cidade="Tatuape"
  - Endereco: "R. Joao Mercado, 260"
  - CONFIRMADO: Tatuape eh BAIRRO de Sao Paulo (Zona Leste)
  - Cidade correta: Sao Paulo

- id=7821 **MBigucci** "Station MBigucci" - cidade="Jabaquara"
  - Endereco: "Rua Conduru, 49, no Jabaquara/SP"
  - CONFIRMADO: Jabaquara eh BAIRRO de Sao Paulo (Zona Sul)
  - Cidade correta: Sao Paulo

**Diagnostico:** Todos os 29 casos de "cidade_e_bairro" sao bairros de grandes cidades (SP, RJ, Salvador) confundidos com cidades. O scraper esta extraindo o bairro do titulo/URL e usando como cidade.
**Recomendacao:** Criar lista de bairros conhecidos de SP/RJ/Salvador e, quando detectado, substituir pela cidade-mae. Muitos scrapers extraem bairro do titulo (ex: "Eleva Tatuape" -> cidade="Tatuape") sem verificar se eh cidade real.

---

## 3. Fases Suspeitas (Contaminacao)

### Dialogo (186/186 = "Lancamento")

- URL 1: https://www.dialogo.com.br/imoveis/alto-do-ipiranga/apartamentos/bosque-santa-cruz-by-dialogo-residences
  - Fase REAL: "Em construcao"
  - Menu do site mostra: Em construcao, Breve lancamento, Lancamento, Pronto para morar, Portfolio

- URL 2: https://www.dialogo.com.br/imoveis/analia-franco/apartamentos/analia-franco-station
  - Fase REAL: "Breve lancamento"

- URL 3: https://www.dialogo.com.br/imoveis/vila-prudente/apartamentos/panoramico-home-club
  - Fase REAL: "Pronto para morar"

**Diagnostico:** CONTAMINACAO CONFIRMADA. O site Dialogo tem multiplas fases reais. O scraper provavelmente esta pegando o filtro da pagina de listagem (que estava em "Lancamento") e aplicando a todos os empreendimentos.
**Recomendacao:** PRIORIDADE ALTA. Ajustar scraper para extrair a fase de CADA pagina individual, nao do filtro/menu de listagem.

---

### You,inc (101/101 = "Em Construcao")

- URL 1: https://www.youinc.com.br/imovel/apartamentos-venda-centro-sao-paulo-sp-openlife-frei-caneca
  - Fase REAL: "Lancamento" (estagio=8)

- URL 2: https://www.youinc.com.br/imovel/apartamentos-venda-vila-olimpia-sao-paulo-sp-qg-cardoso-de-melo-by-youinc
  - Fase REAL: "Em Obra" (estagio=11)

- URL 3: https://www.youinc.com.br/imovel/apartamentos-venda-pinheiros-sao-paulo-sp-qg-ferreira-de-araujo-by-youinc
  - Fase REAL: "Em Obra" (estagio=11, 12.39% concluido em fev/2026)

**Diagnostico:** CONTAMINACAO CONFIRMADA. O site You,inc tem campo `estagio` com valores distintos (8=Lancamento, 10=Breve Lancamento, 11=Em Obra, 12=Pronto). O scraper esta aplicando uma fase unica a todos.
**Recomendacao:** PRIORIDADE ALTA. O site tem API/dados estruturados com o campo `estagio` -- extrair esse campo individualmente.

---

### HM Engenharia (62/62 = "Em Construcao")

- URL 1: https://eme.maishm.com.br/imoveis/hm-campos-eliseos
  - Fase REAL: "Lancamento"

- URL 2: https://eme.maishm.com.br/imoveis/hm-smart-freguesia-do-o
  - Fase REAL: "Lancamento"

- URL 3: https://eme.maishm.com.br/imoveis/hm-smart-marques
  - Fase REAL: "Lancamento"

**Diagnostico:** CONTAMINACAO CONFIRMADA. Todas as amostras sao "Lancamento", nao "Em Construcao". O site exibe badges de fase em cada pagina. Scraper provavelmente pegou a fase de um filtro ou de uma pagina de listagem.
**Recomendacao:** Ajustar extracao de fase para usar o badge/selo individual de cada empreendimento.

---

### Calper (28/28 = "Pronto para Morar")

- URL 1: https://www.calper.com.br/empreendimentos/arte-design/
  - Fase REAL: "Grupo Fechado" (Entrega 100%)

- URL 2: https://www.calper.com.br/empreendimentos/arte-botanica/
  - Fase REAL: "Em Construcao" (campo "Obras")

- URL 3: https://www.calper.com.br/empreendimentos/arte-jardim-residencial/
  - Fase REAL: "Em Construcao"

**Diagnostico:** CONTAMINACAO CONFIRMADA. Fases reais variam entre "Grupo Fechado", "Em Construcao", etc. O banco tem todos como "Pronto para Morar".
**Recomendacao:** Ajustar scraper Calper. O site tem campo de status por empreendimento + barra de progresso de obra.

---

### MBigucci (26/27 = "Breve Lancamento")

- URL 1: https://mbigucci.com.br/mbiguccijoy/
  - Fase REAL: "Lancamento" (CSS da pagina: `.fase_da_obra-lancamento`)

**Diagnostico:** CONTAMINACAO CONFIRMADA. Pelo menos este empreendimento eh "Lancamento", nao "Breve Lancamento".
**Recomendacao:** Ajustar scraper. O site usa classes CSS especificas para cada fase (ex: `fase_da_obra-lancamento`).

---

### Pafil (26/26 = "Breve Lancamento")

- URL 1: https://www.pafil.com.br/parc-das-artes-complexo-comercial/
  - Fase REAL: "Em Obras"

- URL 2: https://www.pafil.com.br/vertice-novo-urbanismo/
  - Fase REAL: "Futuro Lancamento"

**Diagnostico:** CONTAMINACAO CONFIRMADA. Fases reais incluem "Em Obras" e "Futuro Lancamento", mas o banco tem todos como "Breve Lancamento".
**Recomendacao:** Ajustar scraper. O site exibe a fase no cabecalho de cada empreendimento.

---

## 4. Tipologias Suspeitas

### Eztec (39/39 com apto_studio=1)

- URL 1: https://www.eztec.com.br/imovel/lume-house-vila-prudente/
  - Tipologias REAIS: 2 dorms (38m2) e 3 dorms (69-71m2)
  - Studio? NAO - nao tem studios neste empreendimento
  - O `dormitorios_descricao` no banco inclui "Studios" mas a pagina nao tem

- URL 2: https://www.eztec.com.br/imovel/east-blue/
  - Tipologias REAIS: 3 dorms 1 suite (105m2), 4 dorms 2 suites (140m2), Garden (237-258m2)
  - Studio? NAO - menor unidade eh 105m2

**Diagnostico:** CONTAMINACAO CONFIRMADA. O site Eztec provavelmente lista "Studios" no menu/filtro do site como opcao de busca, e o scraper captura isso como se fosse tipologia do empreendimento especifico. O `dormitorios_descricao` contem strings do menu ("Studios | 3 dorms. | 2 dorms. | Studio | 2 e 3 dorm | 4 dorms. | 3 e 4 Dorms.") que sao claramente filtros do site, nao tipologias do produto.
**Recomendacao:** PRIORIDADE ALTA. Ajustar scraper Eztec para extrair tipologias da ficha tecnica do empreendimento, nao dos filtros/menus do site. O campo `dormitorios_descricao` esta capturando lixo do menu.

---

### Vita Urbana (19/19 com apto_studio=1)

- URL 1: https://vitaurbana.com.br/empreendimentos/nurban-praca-da-arvore
  - Tipologias REAIS: Studios (18-24m2), 1 dorm (27-28m2), 2 dorms (33-37m2), 3 dorms (42m2)
  - Studio? SIM - LEGITIMO, este empreendimento realmente tem studios

**Diagnostico:** FALSO POSITIVO na auditoria. Vita Urbana ("Nurban") realmente constroi studios. A flag de 100% studio pode ser correta se todos os empreendimentos da marca incluem studios.
**Recomendacao:** Verificar mais empreendimentos, mas provavelmente eh dado correto para esta empresa.

---

### Hacasa (12/12 com apto_studio=1)

- URL: https://www.hacasa.com.br/para-comprar/duo-residence/
  - ERRO 403 (acesso bloqueado)

**Diagnostico:** Nao foi possivel verificar (403 Forbidden).
**Recomendacao:** Verificar manualmente via browser.

---

## 5. Nomes Suspeitos

### EBM - Nomes curtos e sujos

- id=3025, nome="DF" - URL: https://ebm.com.br/df/
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem regional do Distrito Federal.

- id=3027, nome="GO" - URL: https://ebm.com.br/go/
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem regional de Goias.

- id=3029, nome="SP" - URL: https://ebm.com.br/sp/
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem regional de Sao Paulo.

- id=2959, nome="Mais empreendimentos" - URL: https://ebm.com.br/sp/sao-carlos/mais-empreendimentos/
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem/categoria.

- id=2968, nome="Mais Empreendimentos" - URL: https://ebm.com.br/go/anapolis/mais-empreendimentos/
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem/categoria.

**Diagnostico:** Todos os nomes suspeitos da EBM sao FALSOS EMPREENDIMENTOS -- paginas de navegacao do site capturadas como produtos pelo scraper.
**Recomendacao:** PRIORIDADE ALTA. Remover estes registros e ajustar filtro do scraper EBM para ignorar URLs de listagem/categoria.

---

### Yonder

- id=4862, nome="Empreendimentos His" - URL: https://www.yonderincorporadora.com.br/empreendimentos-his
  - NAO EH EMPREENDIMENTO. Eh pagina de listagem de empreendimentos HIS (Habitacao de Interesse Social).

- id=4864, nome="YONDER" - URL: https://www.yonderincorporadora.com.br/listadecorretores
  - NAO EH EMPREENDIMENTO. Eh pagina de lista de corretores da empresa.

**Diagnostico:** Scraper capturou paginas institucionais/de listagem como empreendimentos.
**Recomendacao:** Remover estes registros. Ajustar filtro para Yonder.

---

## 6. Metragens e Precos Suspeitos

### Metragem alta: RNI Estacao RNI (area=4292 m2)

- URL: https://rni.com.br/imoveis/apartamentos/go/goiania/estacao-rni
  - Area REAL: Apto 2 dorms de 41,93 m2 e 42,92 m2
  - O valor 4292 m2 eh na verdade 42.92 m2 com o ponto decimal ignorado

**Diagnostico:** ERRO DE PARSING. O parser interpretou "42.92" como "4292" (removeu o ponto decimal ou tratou como separador de milhar).
**Recomendacao:** Corrigir parser de metragem para tratar ponto como separador decimal quando o valor resultante seria absurdo (>500m2 para apartamento).

---

### Metragem baixa: MRV (area=0.0)

- URLs: MRV Residencial Green Park, Epic
  - Provavelmente a API da MRV nao retornou dados de area para estes empreendimentos
  - O valor 0.0 deveria ser NULL

**Diagnostico:** Valor 0.0 usado como placeholder em vez de NULL.
**Recomendacao:** Converter area_min=0 e area_max=0 para NULL no banco.

---

### Preco baixo: MRV Residencial Belgrano (preco=R$11.000)

- URL: https://mrv.com.br/imoveis/sao-paulo/bauru/apartamentos-residencial-belgrano
  - A pagina NAO exibe preco
  - Apartamento de 48.18 m2

**Diagnostico:** Preco R$11.000 para um apartamento de 48m2 eh impossivel (seria ~R$229/m2). Provavelmente houve erro de parsing -- pode ser parcela mensal ou entrada, nao preco total.
**Recomendacao:** Verificar se API da MRV distingue preco total vs parcela. Valores abaixo de R$50k para apartamentos devem ser tratados como suspeitos e investigados.

---

### Precos MRV no RJ (R$400-800)

- ids 731-734: Precos de R$400 a R$800
  - Claramente sao valores de PARCELA (ex: "a partir de R$400/mes"), nao preco total
  - A MRV frequentemente anuncia parcela na busca e preco total na pagina de detalhe

**Diagnostico:** Parser capturando valor de parcela mensal como preco total.
**Recomendacao:** Filtrar precos < R$10.000 como provaveis parcelas e setar como NULL, ou ajustar parser MRV para diferenciar parcela vs preco.

---

## Resumo Executivo

### Anomalias investigadas: ~50 URLs de ~20 empresas

### Falhas de extracao (corrigiveis): 4 empresas
| Empresa | Problema | Impacto | Prioridade |
|---------|----------|---------|------------|
| Engelux | Endereco+coordenadas na pagina, nao capturados | 8 empreendimentos | ALTA |
| EBM | URLs de navegacao capturadas como empreendimentos | ~10 registros falsos | ALTA |
| Yonder | Paginas institucionais capturadas como empreendimentos | 2 registros falsos | MEDIA |
| RNI | Ponto decimal ignorado na metragem (42.92 -> 4292) | 1 registro | BAIXA |

### Contaminacao de fase (corrigivel): 6 empresas confirmadas
| Empresa | Registros | Fase no banco | Fases reais encontradas |
|---------|-----------|---------------|------------------------|
| Dialogo | 186 | Lancamento | Em construcao, Breve lancamento, Pronto para morar |
| You,inc | 101 | Em Construcao | Lancamento, Em Obra |
| HM Engenharia | 62 | Em Construcao | Lancamento |
| Calper | 28 | Pronto para Morar | Em Construcao, Grupo Fechado |
| Pafil | 26 | Breve Lancamento | Em Obras, Futuro Lancamento |
| MBigucci | 26 | Breve Lancamento | Lancamento |

### Contaminacao de tipologia: 1 empresa confirmada
| Empresa | Problema |
|---------|----------|
| Eztec | dormitorios_descricao captura filtros do menu do site, nao tipologias reais |

### Cidade = bairro (corrigivel): ~29 registros
- Todos sao bairros de SP/RJ/Salvador confundidos com cidades
- Correcao: mapear bairros conhecidos para a cidade-mae

### Limitacoes do site (nao publicam dados): 5 empresas
| Empresa | Dado ausente | Motivo |
|---------|-------------|--------|
| SR Engenharia | Endereco | Site nao publica, contato via WhatsApp |
| Maccris | Endereco | Portfolio visual sem localizacao |
| MP | Endereco | Apenas bairro/regiao no texto |
| Due | Endereco | Empreendimentos de praia/resort |
| Cosbat | Tudo | Certificado SSL expirado |

### Precisam Selenium: 2 empresas
| Empresa | Registros | Situacao |
|---------|-----------|---------|
| HSM | 30 | SPA 100% JS-rendered, paginas vazias sem JS |
| Usina de Obras | 11 | SPA 100% JS-rendered, paginas vazias sem JS |

### Dados incorretos no banco (falsos empreendimentos): ~12 registros
- EBM: 5 registros sao paginas de navegacao (/df/, /go/, /sp/, /mais-empreendimentos/)
- Yonder: 2 registros sao paginas institucionais (listagem HIS, lista de corretores)
- MRV: precos de parcela mensal capturados como preco total (~5 registros)

### Metragens incorretas: ~3 registros
- MRV: area=0.0 deveria ser NULL (2 registros)
- RNI: 42.92 m2 parseado como 4292 m2 (1 registro)

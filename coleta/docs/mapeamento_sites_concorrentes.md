# Mapeamento Tecnico - Sites de Empreendimentos dos Concorrentes
# Preparado para orientar o desenvolvimento dos scrapers
# Data: 2026-02-28

## RESUMO DE VIABILIDADE

| Empresa     | URL Base                       | Renderizacao | Anti-bot | Viabilidade    |
|-------------|--------------------------------|-------------|----------|----------------|
| Plano&Plano | planoeplano.com.br             | Server-side | Nenhum   | CONCLUIDO (v3) |
| Cury        | cury.net                       | Server-side | Cloudflare 403 | SELENIUM |
| Vivaz       | meuvivaz.com.br                | SPA (JS)    | N/A      | SELENIUM       |
| Direcional  | direcional.com.br              | SPA + 403   | Cloudflare 403 | SELENIUM |
| MRV         | mrv.com.br                     | SPA (JS)    | N/A      | SELENIUM       |
| Econ        | econconstrutora.com.br         | Server-side | Nenhum   | REQUESTS (facil) |
| Stanza      | stanza.com.br                  | Server-side | Nenhum   | REQUESTS (facil) |
| Pacaembu    | pacaembu.com                   | Server-side | Nenhum   | REQUESTS (facil) |
| Metrocasa   | metrocasa.com.br               | ???         | Cloudflare 403 | SELENIUM |
| Vibra       | vibraresidencial.com.br        | Server-side | Nenhum   | REQUESTS (facil) |
| Benx        | benx.com.br                    | Server-side | Nenhum   | REQUESTS (facil) |
| Conx        | conx.com.br                    | Server-side | Nenhum   | REQUESTS (facil) |
| Magik JC    | magikjc.com.br                 | Server-side | Nenhum   | REQUESTS (facil) |
| Kazzas      | kazzas.com.br                  | Server-side | Nenhum   | REQUESTS (facil) |
| Mundo Apto  | mundoapto.com.br               | Server-side | Nenhum   | REQUESTS (facil) |
| Cyrela      | cyrela.com.br                  | A verificar | A verificar | A VERIFICAR |
| Moura Dubeux| mouradubeux.com.br             | A verificar | A verificar | A VERIFICAR |


## DETALHES POR EMPRESA

### 1. CURY (cury.net)
- **Estrutura de URLs**:
  - Pagina de empreendimento: cury.net/imovel/{UF}/{regiao}/{slug}
    Exemplos:
      cury.net/imovel/SP/centro/arque
      cury.net/imovel/RJ/zona-oeste/completojpa
  - Paginas regionais: cury.net/regiao/SP/zona-sul
  - Site auxiliar: curysp.com.br (corretores, mas tem listagem)
- **Conteudo disponivel na pagina do empreendimento**:
  - Dormitorios (1, 2, 3 dorms)
  - Status da obra com percentuais por etapa (fundacao, estrutura, alvenaria, acabamento)
  - Endereco e localizacao
  - Registro de incorporacao
  - Itens de lazer
  - Imagens (perspectivas, plantas, decorado)
  - Metragens
- **Problema**: Retorna 403 para requests direto (Cloudflare)
- **Solucao**: Selenium com User-Agent real e waits explicitos
- **Prioridade**: ALTA (concorrente direto no MCMV)
- **Estimativa de empreendimentos**: ~40-60 ativos (SP + RJ)
- **Notas**: Dados bastante ricos, similares a Plano&Plano. Ficha tecnica 
  com area terreno, torres, unidades, andares. Lazer bem categorizado.

### 2. VIVAZ (meuvivaz.com.br)
- **Estrutura de URLs**:
  - Landing pages individuais: lp.meuvivaz.com.br/{slug}
    Exemplos:
      lp.meuvivaz.com.br/vivaz-connection-klabin
      lp.meuvivaz.com.br/grand-vivaz-jardim-franca
      lp.meuvivaz.com.br/clube-barra-funda
  - Listagem por estado: meuvivaz.com.br/apartamentos/{estado}
  - Pagina individual: meuvivaz.com.br/apartamento/{slug}-{bairro}-{zona}-{cidade}-{uf}
- **Conteudo disponivel**:
  - 2 dorms, opcao suíte, varanda, vaga
  - Metragens (41 a 64 m2 por exemplo)
  - Localizacao com mapa
  - Itens de lazer
  - Informacoes de financiamento (MCMV)
  - Subsidio, FGTS, entrada
- **Problema**: SPA puro, web_fetch retorna HTML vazio
- **Solucao**: Selenium obrigatorio
- **Prioridade**: MEDIA (Vivaz = marca popular da Cyrela, concorrente direto)
- **Estimativa de empreendimentos**: ~40+ ativos (SP, RJ, RS)
- **Notas**: Empresa do Grupo Cyrela focada em MCMV/popular.
  Atua em SP, RJ e RS. Relevante para comparacao direta com Tenda.

### 3. DIRECIONAL (direcional.com.br)
- **Estrutura de URLs**:
  - Listagem: direcional.com.br/empreendimentos (403)
  - Empreendimento: direcional.com.br/empreendimentos/{slug}/
    Exemplos:
      direcional.com.br/empreendimentos/conquista-clube-sacoma/
      direcional.com.br/empreendimentos/viva-vida-realengo/
      direcional.com.br/empreendimentos/direcional-praia/
  - Paginas regionais: direcional.com.br/{cidade}/
- **Conteudo disponivel**:
  - Dormitorios (1, 2 quartos, studios, coberturas, garden)
  - Metragens (40 a 90 m2)
  - Numero de pavimentos e blocos
  - Vagas (inclusive PCD e rotativas)
  - Endereco completo
  - Itens de lazer
  - Relacao com programa MCMV
- **Problema**: Retorna 403 para requests e para web_fetch
- **Solucao**: Selenium com protecao anti-Cloudflare
- **Prioridade**: ALTA (concorrente direto, 8 estados + DF)
- **Estimativa de empreendimentos**: 50+ ativos em todo o Brasil
- **Notas**: Grupo Direcional inclui Riva (media renda) e Direto (fintech).
  Portfolio muito amplo geograficamente. Dados ricos incluindo vagas 
  detalhadas (PCD, rotativas, moto, bicicleta).

### 4. MRV (mrv.com.br)
- **Estrutura de URLs**:
  - Home: mrv.com.br (SPA, requer JS)
  - Listagem por estado: mrv.com.br/imoveis/{estado}
  - Mapa interativo: mapas.mrv.com.br
  - Pagina individual: mrv.com.br/imoveis/... (estrutura variavel)
- **Conteudo disponivel** (extraido de sites terceiros):
  - Dormitorios (1, 2, 3 quartos)
  - Metragens (36 a 55+ m2)
  - Vagas
  - Lazer (fitness ao ar livre, playground, salao jogos, gourmet, kids, pomar, 
    bicicletario, coworking)
  - Linhas: Class (media-alta), padrao MCMV
  - Localizacao ampla (160+ cidades)
- **Problema**: SPA puro ("You need to enable JavaScript to run this app")
- **Solucao**: Selenium OU investigar API interna (provavel React com API REST)
- **Prioridade**: ALTA (maior construtora do pais por volume)
- **Estimativa de empreendimentos**: 200+ ativos em todo o Brasil
- **Notas**: Volume muito grande. Pode ser mais produtivo investigar se 
  existe uma API interna que o SPA consome (comum em React apps).
  O mapa interativo (mapas.mrv.com.br) pode ter API propria.


### 5. ECON (econconstrutora.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Home com listagem: econconstrutora.com.br/
  - Empreendimento (formato novo): econconstrutora.com.br/imovel/{regiao}/{slug}
    Exemplos:
      econconstrutora.com.br/imovel/zona-oeste/go-lapa
      econconstrutora.com.br/imovel/zona-leste/line-sao-miguel
      econconstrutora.com.br/imovel/zona-sul/fast-interlagos
  - Empreendimento (formato antigo): econconstrutora.com.br/detalhes/{slug}
    Exemplos:
      econconstrutora.com.br/detalhes/app
      econconstrutora.com.br/detalhes/six-santa-marina
      econconstrutora.com.br/detalhes/enter-morumbi
      econconstrutora.com.br/detalhes/fort-aricanduva
- **Conteudo disponivel**:
  - Dormitorios (1, 2, 3 dorms) com metragens por planta
  - Fase (Lancamento, Breve Lancamento, Em Construcao, Pronto para Morar, Futuro Lancamento)
  - Regiao e bairro
  - Lazer detalhado (piscina, churrasqueira, quadra, pet care, bicicletario, coworking, etc.)
  - Programas (HIS1, HIS2, HMP, R2V, MCMV, FGTS)
  - Registro de incorporacao
  - Imagens em S3 da AWS (econ-portal-static.s3.sa-east-1.amazonaws.com)
- **Renderizacao**: Server-side! web_fetch retorna HTML completo com dados
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup (mesmo stack da Plano&Plano)
- **Prioridade**: ALTA (concorrente direto no MCMV em SP, facil de implementar)
- **Estimativa de empreendimentos**: ~30-50 ativos
- **Area de atuacao**: Estado de Sao Paulo (capital e interior)
- **Notas**: Site muito limpo e bem estruturado. Imagens em CDN do S3.
  Dois formatos de URL (/imovel/ e /detalhes/) precisam ser tratados.
  Dados bem ricos, incluindo programas habitacionais e metragens por planta.

### 6. STANZA (stanza.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Listagem: stanza.com.br/empreendimentos
  - Filtro por estado: stanza.com.br/empreendimentos?estado=Sergipe
  - Empreendimento: stanza.com.br/empreendimentos/{slug}/
    Exemplos:
      stanza.com.br/empreendimentos/jardim-patamares-salvador/
      stanza.com.br/empreendimentos/ventura-patamares/
      stanza.com.br/empreendimentos/caete-serigy/
      stanza.com.br/empreendimentos/vista-aruana/
      stanza.com.br/empreendimentos/recanto-santa-maria/
- **Conteudo disponivel**:
  - Dormitorios (2, 3 quartos com suite)
  - Endereco completo com CEP
  - Area de lazer detalhada (piscina adulto e infantil, prainha, churrasqueira,
    quadra, academia, brinquedoteca, pet place, bicicletario, etc.)
  - Registro de incorporacao
  - Imagens
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: MEDIA (portfolio pequeno, atua em Sergipe e Bahia apenas)
- **Estimativa de empreendimentos**: ~8-15 ativos
- **Area de atuacao**: Sergipe (Aracaju) e Bahia (Salvador)
- **Notas**: Site WordPress bem estruturado. Portfolio pequeno mas dados ricos.
  Todos os empreendimentos listados no footer do site. Scraper sera rapido.

### 7. PACAEMBU (pacaembu.com) *** FACIL ***
- **Estrutura de URLs**:
  - Listagem residencial: pacaembu.com/imoveis-residenciais
  - Terrenos comerciais: pacaembu.com/terrenos-comerciais
  - Empreendimento: pacaembu.com/imoveis-residenciais/{slug}
    Exemplos:
      pacaembu.com/imoveis-residenciais/residencial-terras-de-bady
      pacaembu.com/imoveis-residenciais/vida-nova-sao-rogerio-4
      pacaembu.com/imoveis-residenciais/moradas-do-horizonte-condominio-ouro-preto
  - Filtros na listagem: por estado (GO, MG, MT, PR, SP), cidade e fase
- **Conteudo disponivel**:
  - Dormitorios (2 dorms padrao)
  - Fase (Lancamento, Em Obras, Concluida)
  - Parcelas a partir de R$ (quando disponivel)
  - Tipo (casas individuais com quintal, condominio fechado)
  - Vaga de garagem
  - Cidade e estado
  - Lazer (quando condominio fechado)
  - Imagens (lazy loaded, base64 placeholder)
- **Renderizacao**: Server-side! web_fetch retorna HTML completo com listagem
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: MEDIA-ALTA (grande volume, 2a maior construtora de casas do Brasil)
- **Estimativa de empreendimentos**: 50-100+ ativos (cidades no interior de SP, PR, MT, MG, GO)
- **Area de atuacao**: Interior de SP, PR, MT, MG, GO (cidades medias)
- **Notas**: Foco em bairros planejados de CASAS (nao apartamentos).
  Produto muito diferente das demais (horizontal vs vertical).
  Atuacao fortissima no interior, poucas sobreposicoes geograficas com Tenda.
  Imagens usam lazy loading com placeholder base64, precisar extrair data-src.
  Cidades atendidas: SJ Rio Preto, Ribeirao Preto, Marilia, Londrina, Cuiaba, etc.

### 8. METROCASA (metrocasa.com.br)
- **Estrutura de URLs**:
  - Home: metrocasa.com.br
- **Conteudo disponivel**: A verificar (site retorna 403)
- **Renderizacao**: Desconhecida (403 impede verificacao)
- **Anti-bot**: Cloudflare 403
- **Solucao**: Selenium
- **Prioridade**: MEDIA (foco em SP, proximo ao metro, MCMV)
- **Estimativa de empreendimentos**: ~70 em toda SP
- **Area de atuacao**: Cidade de Sao Paulo
- **Notas**: Fundada em 2017. Foco em imoveis proximos a estacoes de metro.
  Segmento HIS/MCMV. Capital aberto. Precisa Selenium para contornar 403.

### 9. VIBRA RESIDENCIAL (vibraresidencial.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Home: vibraresidencial.com.br
  - Empreendimento: vibraresidencial.com.br/produtos/{slug}/
    Exemplos:
      vibraresidencial.com.br/produtos/bios-santo-amaro/
      vibraresidencial.com.br/produtos/vibra-estacao-campo-limpo/
      vibraresidencial.com.br/produtos/vibra-vila-das-belezas/
      vibraresidencial.com.br/produtos/vibra-mooca/
      vibraresidencial.com.br/produtos/vibra-nacoes-unidas/
  - Todos os empreendimentos listados no menu do site
- **Conteudo disponivel**:
  - Dormitorios (1, 2 dorms)
  - Metragem
  - Bairro / Zona
  - Lazer
  - Programas (HIS, HMP, MCMV)
  - Ficha tecnica (tipologia, pavimentos, unidades por andar, total)
  - Imagens
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: ALTA (concorrente direto MCMV/HIS em SP, Grupo Nortis, ~23 empreendimentos)
- **Estimativa de empreendimentos**: ~23 ativos (listados no menu)
- **Area de atuacao**: Cidade de Sao Paulo
- **Notas**: Grupo Nortis (Vibra + Vibe). Dados de ficha tecnica muito ricos.
  Foco em estacoes de metro. Portfolio inteiro listado no nav menu.
  Linhas: Vibra (HIS), Bios (HMP). Concorrente frontal da Tenda em SP.

### 10. BENX (benx.com.br)
- **Estrutura de URLs**:
  - Home: benx.com.br
  - Imoveis: benx.com.br/imoveis, /residencial, /comercial
  - Empreendimento: benx.com.br/empreendimento/{slug}
    Exemplos:
      benx.com.br/empreendimento/autor-jardins
      benx.com.br/empreendimento/vogel-moema
      benx.com.br/empreendimento/1800-oscar-pinheiros
  - Portfolio Viva Benx (economico): vivabenx.com.br
- **Conteudo disponivel**:
  - Dormitorios (studios, 1, 2, 3 suites)
  - Metragens (34 a 263m2)
  - Fase (Breve lancamento, Lancamento, Em obras)
  - Imagens em CDN (conteudo.benx.com.br)
- **Renderizacao**: Server-side! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: BAIXA (foco medio/alto padrao, Moema, Jardins, Pinheiros)
- **Estimativa de empreendimentos**: ~10-15 ativos
- **Area de atuacao**: Sao Paulo (zonas nobres: Jardins, Moema, Pinheiros, Vila Olimpia)
- **Notas**: Grupo Bueno Netto, +46 anos. Foco alto padrao, nao e MCMV.
  Linha Viva Benx e economica (vivabenx.com.br) e pode ser mais relevante.
  Parque Global e o carro-chefe (bairro planejado).

### 11. CONX (conx.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Home: conx.com.br
  - Todos empreendimentos: conx.com.br/produtos/destaque/todos/
  - Por fase: conx.com.br/produtos/categoria/lancamento/
            conx.com.br/produtos/categoria/em-obras/
            conx.com.br/produtos/categoria/pronto-para-morar/
            conx.com.br/produtos/categoria/futuro-lancamento/
            conx.com.br/produtos/categoria/breve-lancamento/
  - Linha select: conx.com.br/produtos/linha/select/
- **Conteudo disponivel**:
  - Dormitorios (studios, 1, 2, 3 dorms)
  - Metragens (27 a 75m2)
  - Fase
  - Lazer (rooftop, piscina)
  - Localizacao
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: MEDIA (35+ anos, +23mil unidades, SP e RJ, economico a alto padrao)
- **Estimativa de empreendimentos**: ~15-25 ativos
- **Area de atuacao**: Sao Paulo e Rio de Janeiro
- **Notas**: Atua em todos os segmentos (economico, medio e alto padrao).
  Linha Welconx e economica. 7 Premios Master Imobiliario.
  MCMV representou historicamente uma parte relevante do portfolio.

### 12. MAGIK JC (magikjc.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Home: magikjc.com.br
  - Empreendimento: magikjc.com.br/empreendimento/{slug}/
    Exemplos:
      magikjc.com.br/empreendimento/bem-viver-paulista/
      magikjc.com.br/empreendimento/bem-viver-angelica/
      magikjc.com.br/empreendimento/bem-viver-albuquerque-lins/
- **Conteudo disponivel**:
  - Dormitorios (1, 2, 3 dorms + office)
  - Programas (HIS1, HMP, R2V, EHMP, MCMV, FGTS)
  - Fase (Breve lancamento, Em Obra)
  - Bairro e endereco
  - Lazer
  - Imagens
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: ALTA (lider MCMV no centro de SP, concorrente direto)
- **Estimativa de empreendimentos**: ~8-15 ativos
- **Area de atuacao**: Centro de Sao Paulo (Vila Buarque, Bela Vista, Santa Cecilia, Barra Funda, Campos Eliseos)
- **Notas**: Desde 1972, serie "Bem Viver" desde 2016 para MCMV no centro de SP.
  Certificada Sistema B. Parceria com Central Capital para 7 projetos Campos Eliseos.
  Margem liquida declarada de 25-27% (Brazil Journal). VGV 2025: ~R$250mi.
  MagikLZ e outra empresa do mesmo grupo focada em medio/alto padrao (magiklz.com.br).

### 13. KAZZAS (kazzas.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Home: kazzas.com.br
  - Imoveis: kazzas.com.br/imoveis
  - Empreendimento: kazzas.com.br/imoveis/{slug}
    Exemplos:
      kazzas.com.br/imoveis/arena-kazzas-itaquera-4
      kazzas.com.br/imoveis/uniko-vila-olimpia
      kazzas.com.br/imoveis/clube-kazzas-tiquatira
      kazzas.com.br/imoveis/kz-carapicuiba-2
- **Conteudo disponivel**:
  - Dormitorios (1, 2 quartos)
  - Metragens (24 a 42m2)
  - Fase (Breve lancamento, Lancamento, Em construcao)
  - Bairro
  - Lazer completo
  - Programas (R2V, HIS, MCMV, FGTS)
  - Imagens
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: ALTA (Grupo Kallas, foco economico/MCMV, concorrente direto)
- **Estimativa de empreendimentos**: ~10-20 ativos
- **Area de atuacao**: Sao Paulo, Osasco, Carapicuiba, Taboao da Serra
- **Notas**: Marca economica do Grupo Kallas (fundado 1983, +8mi m2 construidos).
  Kallas atua em medio/alto padrao, Kazzas e o braco MCMV/economico.
  Linhas: Arena Kazzas, Clube Kazzas, KZ, Gran Kazzas, K, KZ Direct.

### 14. MUNDO APTO (mundoapto.com.br) *** FACIL ***
- **Estrutura de URLs**:
  - Imoveis: mundoapto.com.br/imoveis/
  - Futuros lancamentos: mundoapto.com.br/futuros-lancamentos/
  - Empreendimento: mundoapto.com.br/mundo-apto-{slug}/
    Exemplos:
      mundoapto.com.br/mundo-apto-estacao-sp-morumbi/
      mundoapto.com.br/mundo-apto-elevato-pinheiros/
      mundoapto.com.br/mundo-apto-alto-do-ipiranga/
- **Conteudo disponivel**:
  - Dormitorios (studio, 1, 2 dorms)
  - Fase (Breve Lancamento, Lancamento, Em Obras)
  - Zona (Sul, Oeste, Leste, Norte)
  - Programas (HIS, R2V, MCMV)
  - Proximidade metro
  - Imagens
- **Renderizacao**: Server-side (WordPress)! web_fetch retorna HTML completo
- **Anti-bot**: Nenhum detectado
- **Solucao**: requests + BeautifulSoup
- **Prioridade**: MEDIA-ALTA (MCMV em SP, socios Setin + Grupo Capital)
- **Estimativa de empreendimentos**: ~10-15 ativos
- **Area de atuacao**: Cidade de Sao Paulo (todas as zonas)
- **Notas**: Fundada 2017. Socios: Setin Incorporadora + Grupo Capital.
  Programa Pode Entrar (COHAB) tambem no portfolio.
  Foco forte em proximidade de metro. Sites WordPress bem estruturado.


## ESTRATEGIA DE IMPLEMENTACAO RECOMENDADA

### Ordem de desenvolvimento sugerida:
1. **Cury** - Server-side + Cloudflare 403. Selenium resolve. Dados ricos. PRIORIDADE MAXIMA.
2. **Vivaz** - SPA puro, Selenium obrigatorio. Concorrente direto MCMV. PRIORIDADE ALTA.
3. **Direcional** - Selenium + Cloudflare. Dados muito ricos.
4. **Magik JC** - Server-side, facil. Lider MCMV centro SP. Concorrente direto.
5. **Kazzas** - Server-side, facil. Grupo Kallas, MCMV. Concorrente direto.
6. **Vibra** - Server-side, facil. HIS em SP, Grupo Nortis. Concorrente direto.
7. **Mundo Apto** - Server-side, facil. MCMV em SP.
8. **Econ** - Server-side, facil. MCMV em SP.
9. **Conx** - Server-side, facil. Economico a alto padrao.
10. **Metrocasa** - Selenium para 403.
11. **MRV** - Investigar API interna primeiro (pode poupar Selenium)
12. **Pacaembu** - Server-side, facil. Casas no interior.
13. **Stanza** - Server-side, facil. Portfolio pequeno, SE e BA.
14. **Benx** - Server-side, facil. Alto padrao (exceto Viva Benx).
15. **Cyrela / Moura Dubeux** - A mapear

### Dependencia tecnica:
Todos os proximos scrapers (exceto eventualmente MRV via API) vao precisar
de Selenium. O requirements.txt ja inclui selenium e webdriver_manager.
Precisamos garantir que o ChromeDriver esta instalado e funcional na maquina.

### Checklist pre-implementacao (para cada empresa):
1. [ ] Rodar Selenium e acessar pagina de listagem
2. [ ] Coletar HTML de uma pagina de empreendimento com dados ricos
3. [ ] Extrair texto limpo (get_text) e enviar para calibracao do parser
4. [ ] Mapear seletores CSS/XPath dos dados-chave
5. [ ] Implementar scraper seguindo a mesma arquitetura (database.py + incremental)
6. [ ] Testar com --limite 5, validar dados no banco
7. [ ] Rodar completo

### Padrao de nomes dos scrapers:
- scrapers/planoeplano_empreendimentos.py  (CONCLUIDO)
- scrapers/cury_empreendimentos.py         (PROXIMO - Selenium)
- scrapers/vivaz_empreendimentos.py        (Selenium)
- scrapers/direcional_empreendimentos.py   (Selenium)
- scrapers/magikjc_empreendimentos.py      (facil)
- scrapers/kazzas_empreendimentos.py       (facil)
- scrapers/vibra_empreendimentos.py        (facil)
- scrapers/mundoapto_empreendimentos.py    (facil)
- scrapers/econ_empreendimentos.py         (facil)
- scrapers/conx_empreendimentos.py         (facil)
- scrapers/metrocasa_empreendimentos.py    (Selenium)
- scrapers/mrv_empreendimentos.py          (Selenium ou API)
- scrapers/pacaembu_empreendimentos.py     (facil)
- scrapers/stanza_empreendimentos.py       (facil)
- scrapers/benx_empreendimentos.py         (facil)
- scrapers/cyrela_empreendimentos.py
- scrapers/mouradubeux_empreendimentos.py

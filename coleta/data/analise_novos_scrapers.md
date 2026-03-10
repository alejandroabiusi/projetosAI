# Análise Técnica de Novos Scrapers — Incorporadoras MCMV

**Data da análise:** 2026-03-04
**Contexto:** Pesquisa identificou 42 novas construtoras/incorporadoras MCMV não rastreadas.
**Arquivo de referência:** `data/novas_incorporadoras.json` (dados brutos da pesquisa)

---

## Empresas já rastreadas (13)

MRV, Cury, Plano&Plano, Direcional, Vivaz, Magik JC, Metrocasa, Kazzas,
Vibra Residencial, Pacaembu, Mundo Apto, Conx, Benx

## Empresa descartada

- **Tenda** — empresa onde o usuário trabalha, já possui os dados internamente.
  Futuramente pode importar dados diretamente no banco sem scraper.

---

## GRUPO 1 — FÁCIL (requests + BeautifulSoup)

Todos são sites server-side rendered, sem anti-bot relevante.
Podem usar o padrão `scrapers/generico_empreendimentos.py` como base.

### 1. Realiza Construtora ⭐ MELHOR CASO
- **Site:** https://www.realizaconstrutora.com.br
- **Listagem:** /empreendimentos (ou raiz)
- **Empreendimentos:** ~66
- **Framework:** Next.js com SSG (getStaticProps)
- **Técnica:** Extrair `<script id="__NEXT_DATA__">` → JSON com TODOS os dados estruturados.
  Nem precisa parsear HTML. Um único fetch retorna tudo.
- **Atuação:** MG, GO, RJ, SP
- **Porte:** Grande (25.000+ imóveis entregues)

### 2. MPD Engenharia
- **Site:** https://www.mpd.com.br
- **Listagem:** /empreendimentos
- **Empreendimentos:** ~50+
- **Framework:** PHP/Laravel custom
- **Técnica:** Tudo SSR numa página só. Cards com nome, localização, tipo, m², suítes, vagas, status.
  requests + BS4 direto.
- **Atuação:** 7 estados, 45+ cidades
- **Porte:** Grande (3ª maior INTEC 2025)

### 3. ADN Construtora
- **Site:** https://www.adnconstrutora.com.br
- **Listagem:** /produtos
- **Empreendimentos:** ~35-41
- **Framework:** WordPress (tema custom `adn`)
- **Técnica:** Tudo SSR numa página. Cards com status, cidade, nome, dormitórios, vagas, m².
  Tem `admin-ajax.php?action=buscar_resultados` mas desnecessário pois HTML já tem tudo.
- **Atuação:** Interior SP (25+ cidades)
- **Porte:** Grande (22ª maior do Brasil)

### 4. Econ Construtora
- **Site:** https://www.econconstrutora.com.br
- **Listagem:** /imoveis
- **Empreendimentos:** ~30+
- **Framework:** Next.js (App Router) com SSR excelente
- **Técnica:** Apesar de Next.js, dados totalmente renderizados no HTML. requests + BS4.
  Imagens em AWS S3.
- **Atuação:** SP capital
- **Porte:** Médio (desde 2001, 10+ prêmios)

### 5. Novolar (Grupo Patrimar)
- **Site:** https://www.novolar.com.br
- **Listagem:** /imoveis/
- **Empreendimentos:** ~26
- **Framework:** WordPress (Oxygen Builder)
- **Técnica:** Tudo SSR, cards limpos com nome, localização, features, status. Imagens WebP diretas.
- **Atuação:** MG, RJ, SP
- **Porte:** Grande (capital aberto, opera até Faixa 1)

### 6. Emccamp
- **Site:** https://www.emccamp.com.br
- **Listagem:** /imoveis/
- **Empreendimentos:** ~18
- **Framework:** WordPress (tema custom `emccamp`) + WP Rocket
- **Técnica:** SSR completo. Cards com nome, endereço, m², quartos, banheiros, status.
  Imagens lazy-loaded via `data-lazy-src` (fácil de extrair).
- **Atuação:** MG, RJ, SP
- **Porte:** Grande (48 anos, R$1.5bi/ano, preparando IPO)

### 7. EPH Incorporadora
- **Site:** https://www.ephincorporadora.com.br
- **Listagem:** /empreendimentos
- **Empreendimentos:** ~16
- **Framework:** WordPress
- **Técnica:** Cards limpos com nome, localização, quartos, área, preço (parcelas), status.
  Imagens diretas em `<img src>`.
- **Atuação:** SP capital e região
- **Porte:** Médio (2.088+ entregues)

### 8. Lyx Construtora
- **Site:** https://www.lyx.com.br
- **Listagem:** /empreendimentos/
- **Empreendimentos:** ~10
- **Framework:** WordPress (tema custom)
- **Técnica:** O mais simples de todos. SSR, imagens diretas (sem lazy-load), tudo numa página.
- **Atuação:** PR e RS
- **Porte:** Grande (maior MCMV do Sul, 5.500 unidades a lançar)

### 9. Smart Incorporadora
- **Site:** https://www.smartincorporadora.com.br
- **Listagem:** homepage (ou /empreendimento/)
- **Empreendimentos:** ~10
- **Framework:** WordPress
- **Técnica:** SSR com preços visíveis no HTML. Dados de simulador em JSON inline.
- **Atuação:** SP (Zona Leste/Norte), Guarulhos, Salvador
- **Porte:** Médio (2.200 unidades)

### 10. Habras Construtora
- **Site:** https://www.habrasconstrutora.com.br
- **Listagem:** homepage (404 em /empreendimentos)
- **Empreendimentos:** ~5
- **Framework:** WordPress + Elementor + LiteSpeed
- **Técnica:** SSR, markup verboso (Elementor) mas parseável. Catálogo muito pequeno.
- **Atuação:** SP
- **Porte:** Médio (20.000+ em desenvolvimento)

### 11. Construtora Capital
- **Site:** https://www.construtoracapital.com.br
- **Listagem:** URL a descobrir (não é /empreendimentos)
- **Empreendimentos:** pequeno catálogo
- **Framework:** WordPress + Divi
- **Técnica:** SSR, sem anti-bot. Precisa navegar o site para achar a URL correta.
- **Atuação:** Manaus/AM, Belém/PA
- **Porte:** Grande regional (50 anos, 23.000+ projetos)

### 12. Morar Construtora (não analisado tecnicamente)
- **Site:** https://www.morar.com.br
- **Empreendimentos:** ~10+ (4.620 MCMV)
- **Atuação:** Espírito Santo
- **Porte:** Médio (40+ anos)
- **TODO:** Analisar estrutura do site antes de implementar

---

## GRUPO 2 — MÉDIO (Cloudflare, lazy-load, ou paginação)

Precisam de `cloudscraper` ou tratamento especial, mas dados estão no HTML.

### 13. VIC Engenharia
- **Site:** https://www.vicengenharia.com.br
- **Listagem:** /empreendimentos/
- **Empreendimentos:** ~20-25
- **Framework:** WordPress + WP Rocket
- **Obstáculo:** Cloudflare Turnstile CAPTCHA
- **Técnica:** SSR (dados no HTML), mas precisa `cloudscraper` ou headers adequados.
  Filtro por estado via `?estado=XX&busca=1`. Imagens via `data-lazy-src`.
- **Atuação:** MG, SP, RJ, DF, BA
- **Porte:** Grande (30+ anos, 17.500+ unidades)

### 14. BRNPAR / BRN
- **Site:** https://www.brn.com.br
- **Listagem:** /empreendimentos
- **Empreendimentos:** ~80+ (paginado, 20/página)
- **Framework:** Laravel + Livewire
- **Obstáculo:** Cloudflare ativo + paginação (`?page=N`)
- **Técnica:** SSR (dados na primeira carga), precisa `cloudscraper` + loop de paginação.
  Livewire CSRF token pode ser necessário.
- **Atuação:** Interior SP (47+ cidades)
- **Porte:** Grande (Top 10 INTEC, 8.000 unidades)

### 15. Piacentini
- **Site:** https://www.piacentiniconstrutora.com.br
- **Listagem:** a descobrir
- **Empreendimentos:** ~15 (8.500+ unidades)
- **Framework:** WordPress + Elementor + WP Rocket (lazy-load agressivo)
- **Obstáculo:** RocketLazyLoadScripts adia JS até interação do usuário.
  Pode funcionar com requests direto, mas precisa testar.
  Alternativa: WP REST API (`/wp-json/wp/v2/...`).
- **Atuação:** PR (Curitiba)
- **Porte:** Médio

### Não analisados tecnicamente (médio porte estimado):
- **CMO Construtora** (cmoconstrutora.com.br) — Goiânia, ~100+ empreendimentos
- **Pernambuco/Soft** (pernambucoconstrutora.com.br) — Recife, ~20

---

## GRUPO 3 — DIFÍCIL (SPA, precisa Selenium ou reverse-engineering)

### 16. Vitta Residencial
- **Site:** https://www.vittaresidencial.com.br
- **Listagem:** /imoveis
- **Framework:** Angular SPA
- **Obstáculo:** HTML contém apenas templates (`{{imovel.nome}}`), zero dados.
  Imagens em Azure CDN (`bildvitta.blob.core.windows.net`).
- **Técnica:** Selenium OU interceptar XHR no DevTools para descobrir API oculta.
  `listaDeBairros` com ~130 bairros embutida sugere grande catálogo.
- **Atuação:** SP e MG (14 regionais)
- **Porte:** Grande (40.000+ lançadas)

### 17. Bild Desenvolvimento Imobiliário
- **Site:** https://www.bild.com.br
- **Framework:** AngularJS (v1.x)
- **Obstáculo:** SPA com templates (`{{imovel.nome}}`), sem dados no HTML.
  Variáveis de filtro embutidas: `minValorImovel=0`, `maxValorImovel=1001491`.
- **Técnica:** Interceptar XHR no DevTools → descobrir endpoint REST → chamar com requests.
  Se API protegida, Selenium.
- **Atuação:** Interior SP e MG
- **Porte:** Grande (24.000+ unidades)

### 18. Moura Dubeux
- **Site:** https://www.mouradubeux.com.br
- **Framework:** Next.js (App Router) + React Query (Tanstack)
- **Obstáculo:** Navegação por estado (7 estados NE). Dados carregados via API client-side.
  Não usa `__NEXT_DATA__` (usa streaming `__next_f`).
- **Técnica:** Capturar API call no DevTools quando seleciona estado → chamar direto.
  React Query quase certamente usa REST API limpa.
- **Atuação:** 7 estados do Nordeste
- **Porte:** Grande (capital aberto, maior do NE)
- **Nota:** Poucos empreendimentos MCMV (marca "Unica" recente)

### 19. BRZ Empreendimentos
- **Site:** https://www.brzempreendimentos.com
- **Framework:** Blazor WebAssembly (.NET no browser)
- **Obstáculo:** O mais difícil de todos. Runtime .NET roda no browser (~5-10MB).
  Comunicação pode ser binária (SignalR), não REST. reCAPTCHA Enterprise.
- **Técnica:** Obrigatoriamente Selenium/Playwright. Sem atalhos.
- **Atuação:** MG, SP, RJ
- **Porte:** Médio (VGV R$680mi em MG)

---

## GRUPO 4 — NÃO ANALISADOS (empresas menores, avaliar sob demanda)

| Empresa | Site | Região | Porte |
|---------|------|--------|-------|
| MBigucci | mbigucci.com.br | ABC/SP | Médio |
| Pride | construtorapride.com.br | Curitiba/PR | Médio |
| Construart | construartmcmv.com.br | PR, RS | Médio |
| Vasco | construtoravasco.com.br | RS | Médio |
| Morana | morana.net | POA/RS | Médio |
| Brio | brioincorporadora.com.br | SP | Médio |
| Ampla | amplaincorporadora.com.br | SP | Médio |
| ZIP | meuzip.com.br | DF | Médio |
| Alfa Realty | alfarealty.com.br | SP | Médio |
| Yees | yeesinc.com.br | Sorocaba/SP | Médio |
| CMO | cmoconstrutora.com.br | Goiânia/GO | Médio |
| Pernambuco/Soft | pernambucoconstrutora.com.br | Recife/PE | Médio |
| Construcenter | construcenterurbanismo.com.br | AL, PE, SE | Médio |
| Inter | ri.interconstrutora.net.br | Juiz de Fora/MG | Médio |
| MZM | mzm.com.br | SP e ABC | Médio |
| Pafil | pafil.com.br | Interior SP | Médio |
| Open/Melnick | melnickconstrutora.com.br | POA/RS | Grande |
| Exkalla | exkalla.com.br | ABC/SP | Pequeno |
| Paddan | paddan.com.br | ABC/SP | Pequeno |
| Pro Domo | prodomoconstrutora.com.br | BH/MG | Pequeno |
| Construlike | prohidro.com | Sorocaba/SP | Médio |

---

## Plano de Implementação Recomendado

### Fase 1 — Quick wins (FÁCIL, grande catálogo)
Ordem sugerida, priorizando volume de dados e simplicidade:

1. **Realiza** (66 empreendimentos, JSON pronto no __NEXT_DATA__)
2. **MPD** (50+, tudo SSR numa página)
3. **ADN** (35+, WordPress simples)
4. **Econ** (30+, Next.js com SSR)
5. **Novolar** (26, WordPress)
6. **Emccamp** (18, WordPress)

> Esses 6 scrapers adicionam ~225+ empreendimentos ao banco.
> Todos podem usar o padrão `scrapers/generico_empreendimentos.py` adaptado.
> Estimativa: ~2-3 horas de implementação para os 6.

### Fase 2 — WordPress menores (FÁCIL, catálogo menor)
7. EPH (16)
8. Lyx (10)
9. Smart (10)
10. Habras (5)

### Fase 3 — Cloudflare (MÉDIO)
11. VIC Engenharia (25, cloudscraper)
12. BRNPAR (80+, cloudscraper + paginação)
13. Piacentini (15, testar requests primeiro)

### Fase 4 — SPAs (DIFÍCIL)
14. Vitta (Angular — grande catálogo, vale o esforço)
15. Bild (AngularJS — grande catálogo)
16. Moura Dubeux (Next.js SPA — poucos MCMV, prioridade baixa)
17. BRZ (Blazor — mais difícil de todos)

### Notas para a próxima sessão

- Cada scraper novo deve seguir o padrão dos existentes em `scrapers/`:
  - Usar `data/database.py` (`inserir_empreendimento`, `atualizar_empreendimento`, etc.)
  - Usar `config/settings.py` (REQUESTS headers, LOGS_DIR, DOWNLOADS_DIR)
  - Salvar log em `logs/{empresa}_empreendimentos.log`
  - Salvar progresso em `logs/{empresa}_empreendimentos_progresso.json`
  - Download de imagens em `downloads/{empresa_key}/imagens/{slug}/`
- Após criar cada scraper, rodar `enriquecer_dados.py` e `baixar_imagens.py` para a nova empresa
- Atualizar `EMPRESA_CONFIG` em `enriquecer_dados.py` e `baixar_imagens.py`
- Atualizar `gerar_mapa.py` (cores) para incluir novas empresas
- O scraper genérico (`scrapers/generico_empreendimentos.py`) já suporta múltiplas empresas
  e pode ser estendido — avaliar se compensa adicionar lá ou criar arquivo separado

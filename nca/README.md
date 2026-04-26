# Projeto NCA — Pipeline de Geração de Carrosséis

Repositório técnico do projeto experimental de humor astrológico
@naoculpeosastros. Este repositório hospeda o pipeline de geração
automatizada de carrosséis para Instagram. A frente editorial e
estratégica do projeto (briefing, prompt de sistema, planilha mestre,
resumos executivos, decisões de naming e tom) vive em outro lugar
(projeto Claude web) e não é escopo deste repo.

## Estado atual

Pipeline ponta a ponta funcional, lendo da planilha mestre e
renderizando carrosséis em lote. Decisões tipográficas fechadas
(DM Serif Display, 64 px, contraste validado nas cinco paletas).
Próxima etapa é empacotar como nó executável dentro de workflow
n8n e migrar a leitura do Sheets de CSV público para Service
Account (necessário para implementar fallback de erro com escrita
de status na planilha).

## Stack técnica travada

**Renderizador SVG**: resvg-py. Foi escolhido após cairosvg ter
apresentado bug silencioso de não renderizar grupos com máscara de
opacidade aplicada, o que fazia o handle @naoculpeosastros sumir
do rodapé dos cards mesmo estando presente no SVG. resvg respeita
máscaras e filtros corretamente.

**Composição de texto**: Pillow. Os SVGs base do Canva são
renderizados primeiro (com fundo, ícone do signo e handle já
embutidos), e o texto da tirada é desenhado por cima via Pillow.
Essa separação foi adotada porque o Canva exporta tipografia como
paths vetoriais, o que impede substituição direta de string no XML
do SVG.

**Fonte**: DM Serif Display Regular, fixada em 25/04/2026 após
calibragem visual contra Inter, Manrope, Space Grotesk, Playfair
Display, Bebas Neue, Special Elite e Courier Prime. Critério: humor
editorial inteligente sem soar amador. Empacotada em fonts/ no
formato .ttf para portabilidade (independente do que está instalado
no host n8n).

**Tamanho de fonte**: 64 px nos três cards, fixado em 25/04/2026
após grade comparativa de 56, 64, 72 e 80 px.

## Estrutura de pastas

```
nca/
├── README.md                       (este arquivo)
├── compor_carrossel.py             (composição de um card a partir do template)
├── gerar_de_planilha.py            (lê o Sheets e gera carrosséis em lote)
├── validar_contraste.py            (gera grade de contraste sobre 5 paletas)
├── templates/                      (SVGs base por signo, exportados do Canva)
│   ├── Aries.svg
│   ├── Touro.svg
│   ├── Gemeos.svg
│   ├── Cancer.svg
│   ├── Leao.svg
│   ├── Virgem.svg
│   ├── Libra.svg
│   ├── Escorpiao.svg
│   ├── Sagitario.svg
│   ├── Capricornio.svg
│   ├── Aquario.svg
│   ├── Peixes.svg
│   └── Geral.svg                   (carta coringa para conteúdo não-signo)
├── fonts/
│   └── DMSerifDisplay-Regular.ttf  (fonte definitiva, OFL)
└── out/                            (gerada pelos scripts; PNGs por data agendada)
    └── AAAA_MM_DD/
        ├── card1.png               (Tirada feminina)
        ├── card2.png               (Tirada masculina)
        └── card3.png               (CTA)
```

## Decisões editoriais que afetam o pipeline

Estrutura de carrossel é fixa em três cards. Os três cards usam o
mesmo template visual (mesmo signo, mesmo fundo, mesmo ícone, mesmo
handle). O ícone do signo se repete intencionalmente nos três para
reforçar identidade visual e funcionar para quem entra direto no
card 2 ou 3 via compartilhamento.

Os três cards usam o mesmo tamanho de fonte para o texto. A
hierarquia editorial entre eles (gancho, desenvolvimento,
fechamento) é dada pelo conteúdo, não pela tipografia.

Texto centralizado vertical e horizontalmente no canvas. Cor do
texto é creme #ECE6D8, mesma cor do handle e do ícone do signo, que
funciona como contraste sobre todas as cinco cores de fundo da
paleta (terracota fogo, azul-acinzentado ar, verde-azulado escuro
água, verde-oliva terra, e carvão escuro do template Geral).

## Pendências para a primeira sessão neste repo

~~Calibrar tamanho de fonte.~~ Resolvido em 25/04/2026: 64 px.

~~Decidir e empacotar fonte definitiva.~~ Resolvido em 25/04/2026:
DM Serif Display Regular, empacotada em fonts/.

~~Validar contraste do texto creme sobre as cinco paletas de
fundo.~~ Resolvido em 25/04/2026 via validar_contraste.py. Todas
as paletas aprovadas. A paleta de Ar (azul-acinzentado) é a de
menor contraste relativo, mas ainda dentro do confortável.

~~Integrar leitura da planilha mestre.~~ Resolvido em 25/04/2026.
Leitura via endpoint público de export CSV do Google Sheets, sem
auth (gerar_de_planilha.py). Schema real difere do que o README
original previa: cards 1/2/3 vêm das colunas Tirada feminina,
Tirada masculina e CTA respectivamente. A coluna Legenda é o
caption do post no Instagram, fora do pipeline de renderização.

Empacotar o script como nó de código Python dentro do workflow
n8n. Considerar overhead de cold start e disponibilidade de
dependências no executor Python do n8n Cloud.

Implementar fallback de erro: se a renderização falhar por
qualquer motivo, o pipeline deve registrar o erro na planilha em
vez de publicar carrossel quebrado. Depende de migrar leitura
para Service Account (CSV público é read-only).

## Como rodar o pipeline localmente

Instalação de dependências:

```
pip install resvg-py pillow
```

Geração de carrosséis a partir da planilha mestre (uso normal):

```
python gerar_de_planilha.py            # gera todos os IDs aprovados
python gerar_de_planilha.py NCA0001    # gera IDs específicos
```

A planilha é lida via endpoint público de export CSV do Sheets, sem
auth. Os PNGs caem em `out/AAAA_MM_DD/cardN.png`, onde a data vem
da coluna `Data agendada`. Enquanto essa coluna não estiver populada,
um fallback local em gerar_de_planilha.py calcula a data sequencial
a partir do ID. Esse fallback some assim que `Data agendada` é
preenchida no Sheets.

Sanity check do composer isolado, sem depender da planilha:

```
python compor_carrossel.py
```

Gera três PNGs em out/ usando tirada fictícia de Gêmeos.

## Histórico técnico relevante

Tentativa inicial de pipeline previa Canva Connect API com endpoint
de Autofill para renderização nativa pelo próprio Canva a partir
de Brand Templates. Essa rota foi invalidada em 25 de abril de
2026 ao se descobrir que a API exige plano Canva Enterprise, não
Pro, com precificação por cotação direta em centenas de dólares
por usuário e mínimo de assentos. O caminho atual (SVGs base
exportados manualmente do Canva, renderizados via resvg, com
texto composto via Pillow) foi adotado como substituto de zero
custo recorrente.

Tentativa subsequente de pipeline previa cairosvg como
renderizador. Foi invalidada na sessão de 25 de abril ao se
descobrir bug de renderização silenciosa de grupos com máscara
de opacidade. resvg substituiu cairosvg.

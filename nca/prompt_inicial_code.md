# Prompt inicial para abertura no Claude Code

Cole o conteúdo abaixo como primeira mensagem na nova sessão do Claude Code,
depois de ter rodado o comando inicial (ver no fim deste arquivo).

---

Estou abrindo este repositório como continuidade técnica de um projeto
experimental que conduzo em outro contexto (consultor estratégico em
projeto Claude web). O escopo deste repositório é exclusivamente o pipeline
técnico de geração automatizada de carrosséis para a página de Instagram
@naoculpeosastros, que faz humor astrológico com ângulo editorial específico
(deboche de pessoas que usam astrologia como desculpa, não deboche da
astrologia em si).

A frente editorial e estratégica vive no projeto Claude web e não é escopo
seu aqui. Você não precisa opinar sobre tom, naming, prompt de sistema,
métricas, ou cadência editorial. Sua função é construir, debugar e iterar
o pipeline técnico de composição de imagem e integração com a planilha
Google Sheets e com o orquestrador n8n.

Leia o README.md antes de qualquer coisa para entender o estado atual,
a stack travada, as decisões editoriais que afetam o pipeline, e a lista
de pendências.

Sobre meu perfil: tenho formação em Estatística e Economia, MBA em
Finanças, mais de quinze anos em marketing com foco em tráfego e growth
hacking. Não sou desenvolvedor profissional mas leio código razoavelmente
bem, debugo coisas simples, e quero aprender o que estamos construindo,
não terceirizar como caixa-preta. Explique decisões técnicas quando
houver tradeoff relevante, mas sem entrar em didatismo desnecessário
sobre conceitos básicos de programação.

Não use emojis, travessões longos, ou frases curtas estilo punchline.
Não use construções do tipo "isso não é X, isso é Y". Escreva em prosa
corrida quando estiver explicando decisões. Para código, comentários e
documentação, mantenha o padrão usual.

Antes de fechar qualquer decisão de stack ou de biblioteca nova, valide
via web search ou via documentação oficial que ela atende ao requisito
no plano pretendido (incluindo requisitos de autenticação, limites de
chamada, e dependências cruzadas). Se não validou, marque a decisão
como provisória e pendente de validação técnica.

Primeira tarefa para esta sessão: rodar o protótipo `compor_carrossel.py`
para validar que tudo funciona localmente no meu ambiente, e me ajudar
a calibrar o tamanho de fonte definitivo gerando uma grade comparativa
de 56, 64, 72 e 80 pixels lado a lado para que eu escolha visualmente.

---

# Comando para abrir a sessão no terminal

Abre um novo terminal, navega para a pasta do projeto, e roda claude:

```
cd ~/projetosIA/nca
claude
```

(Ajuste o caminho se a pasta projetosIA não estiver direto no seu home.
No Windows, o equivalente seria abrir o PowerShell ou Terminal e rodar
`cd C:\Users\SEU_USUARIO\projetosIA\nca` antes do `claude`.)

Antes de colar o prompt, garanta que os seguintes arquivos estão na pasta:

- README.md (na raiz)
- compor_carrossel.py (na raiz)
- templates/ (com os 13 SVGs já depositados)

Quando o Claude Code abrir e estiver pronto para receber input, cole o
conteúdo da seção acima como primeira mensagem.

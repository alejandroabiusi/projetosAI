# Prompt para Claude Code: Extracao de Dados de Books de Produto da Construtora Tenda

## Objetivo
Ler uma pasta com aproximadamente 150 arquivos PDF (books de produto/investimento da Construtora Tenda) e extrair dados estruturados dos slides de capa e dos slides "One Page" de cada PDF, gerando um arquivo Excel (.xlsx) consolidado que servira como banco de dados para analises de viabilidade de projetos.

## Estrutura dos PDFs
Cada PDF e uma apresentacao de slides (geralmente entre 20 e 40 paginas). Os dados relevantes estao concentrados nos primeiros slides (entre o slide 1 e o slide 6, tipicamente). A estrutura geral e:

1. **Slide de Capa (sempre slide 1):** contem nome do projeto, data do comite e tipo de comite.
2. **Slide(s) "One Page":** contem os indicadores financeiros e operacionais do projeto. Pode haver 1 ou mais One Pages, cada um representando um cenario diferente (ex: "Aprovacao", "Real"). Cada cenario deve gerar uma linha separada no banco de dados.
3. Slides subsequentes (pareceres, produto, operacoes, financeiro, etc.) nao precisam ser lidos.

## Campos a Extrair

### Bloco 1: Identificacao (extrair da capa, slide 1)

| Campo | Onde encontrar | Como parsear |
|---|---|---|
| nome_projeto | Linha que comeca com "Projeto:" na capa | Texto apos "Projeto:" |
| data_comite | Linha que comeca com "Data Comite:" ou "Data Comitê:" | Texto apos o label. Converter para formato dd/mm/aaaa |
| tipo_comite | Primeira linha descritiva da capa (ex: "Book de Escritura + Lancamento", "Comite de Investimentos - Essencial") | Texto completo da primeira linha descritiva. Fica acima de "Regional:" |

### Bloco 2: Dados do One Page (extrair de cada slide cujo titulo contenha "One Page")

**IMPORTANTE:** Um PDF pode ter multiplos One Pages (ex: "One Page - Torres Marina - Aprovacao" e "One Page - Torres Marina - Real"). Cada One Page gera uma LINHA SEPARADA no banco de dados. A coluna "cenario" identifica qual e qual.

| Campo no banco | Label no slide | Como parsear | Tipo | Nullable |
|---|---|---|---|---|
| cenario | Titulo do slide OU campo "Cenario de Aprovacao" dentro da tabela | Se o titulo do slide contiver o cenario (ex: "- Aprovacao", "- Real"), usar esse. Senao, buscar o campo "Cenario de Aprovacao" na tabela do One Page | Texto | Nao |
| fracao_terreno_pct | "Valor Liquido do Terreno (% VGV)" ou "Valor Líquido do Terreno (% VGV)" | Extrair o percentual entre parenteses. Ex: "4.659 (4,0%)" -> 4,0% | Percentual | Nao |
| valor_rs_m2 | "Valor R$/m²" ou "Valor R$/m2" | Numero inteiro ou decimal | Numerico | Nao |
| custo_construcao_rs | "Custo Construção (%VGV)" ou "Custo Construcao (%VGV)" | Extrair o numero antes do parentese. Ex: "55.446 (47,6%)" -> 55446 | Numerico | Nao |
| custo_construcao_pct_vgv | "Custo Construção (%VGV)" | Extrair o percentual entre parenteses. Ex: "55.446 (47,6%)" -> 47,6% | Percentual | Nao |
| prazo_obra_meses | "Prazo da Obra (meses)" | Numero inteiro | Inteiro | Nao |
| custo_total_unit | "Custo Total Unit. | Corrigido (LPU):" | Extrair APENAS o primeiro numero (antes do pipe). Ex: "104.616 | 105.361 (100)" -> 104616 | Numerico | Nao |
| custo_adc_selling | "Custo Adc. Selling:" | Percentual, pode ser negativo. Ex: "-0,4%" | Percentual | SIM |
| preco_nominal_unit | "Preço Nominal Unit." ou "Preco Nominal Unit." | Numero | Numerico | Nao |
| preco_raso_unit | "Preço Raso Unit." ou "Preco Raso Unit." | Numero | Numerico | Nao |
| li_pct | "LI (Régua):" ou "LI (Regua):" | Extrair o percentual ANTES do parentese. Ex: "18,8% (22,0%)" -> 18,8% | Percentual | Nao |
| li_regua_pct | "LI (Régua):" | Extrair o percentual DENTRO do parentese. Ex: "18,8% (22,0%)" -> 22,0% | Percentual | Nao |
| margem_bruta_economica_pct | "Margem Bruta Econômica:" ou "Margem Bruta Economica:" | Percentual | Percentual | Nao |
| margem_rasa_economica_pct | "Margem Rasa Econômica:" ou "Margem Rasa Economica:" | Percentual | Percentual | Nao |
| exposicao_rs_mil | "Exposição (% VGV):" ou "Exposicao (% VGV):" | Extrair o numero antes do parentese. Ex: "18.137 (15,6%)" -> 18137 | Numerico | Nao |
| exposicao_pct_vgv | "Exposição (% VGV):" | Extrair o percentual entre parenteses. Ex: "18.137 (15,6%)" -> 15,6% | Percentual | Nao |
| renda | "Renda (% em RET 1%)" | Extrair o numero antes do parentese. Ex: "2.506 (76,2%)" -> 2506 | Numerico | Nao |
| pct_ret1 | "Renda (% em RET 1%)" | Extrair o percentual entre parenteses. Ex: "2.506 (76,2%)" -> 76,2% | Percentual | Nao |

### Bloco 3: Campos calculados

| Campo | Formula | Formato |
|---|---|---|
| pct_tcd | (preco_nominal_unit - preco_raso_unit) / preco_nominal_unit | Percentual |
| raso_sobre_custo | preco_raso_unit / custo_total_unit | Decimal (2 casas, ex: 1.90) |

## Regras de Parsing

### Identificacao dos slides One Page
- Buscar slides cujo titulo/texto inicial contenha "One Page" (case insensitive).
- O titulo tipicamente segue o padrao: "One Page – [Nome do Projeto] - [Cenario]" ou "One Page – [Nome do Projeto]" (sem cenario explicito).
- Se nao houver cenario no titulo, buscar o campo "Cenário de Aprovação" dentro da tabela do slide.

### Tratamento de numeros
- Os PDFs usam formatacao brasileira: ponto como separador de milhar, virgula como separador decimal.
- Exemplos: "104.616" = 104616 (inteiro), "47,6%" = 0.476, "4.569" no campo Valor R$/m2 = 4569.
- CUIDADO: "Valor R$/m²: 654" e um numero inteiro 654, mas "Valor R$/m²: 4.569" e 4569 (com ponto de milhar).
- Para diferenciar: se o numero tiver ponto seguido de 3 digitos, tratar como separador de milhar. Se tiver ponto seguido de menos de 3 digitos, pode ser decimal (mas neste contexto, quase todos serao separador de milhar).

### Tratamento de campos compostos
Varios campos aparecem no formato "NUMERO (PERCENTUAL)" ou "NUMERO | NUMERO (NUMERO)". Regras:
- "55.446 (47,6%)": numero = 55446, percentual = 47.6%
- "104.616 | 105.361 (100)": pegar apenas 104616 (primeiro valor)
- "18,8% (22,0%)": primeiro percentual = 18.8%, segundo = 22.0%
- "18.137 (15,6%)": numero = 18137, percentual = 15.6%
- "2.506 (76,2%)": numero = 2506, percentual = 76.2%

### Tolerancia a variacoes
- Labels podem ter ou nao acentos (ex: "Construção" vs "Construcao").
- Pode haver espacos extras, tabs ou quebras de linha entre o label e o valor.
- O campo "Custo Adc. Selling" pode simplesmente nao existir no slide. Neste caso, deixar a celula vazia.

## Output

### Formato
Arquivo Excel (.xlsx) com uma unica aba chamada "dados".

### Colunas (nesta ordem)
1. nome_arquivo (nome do PDF de origem, para rastreabilidade)
2. nome_projeto
3. data_comite
4. tipo_comite
5. cenario
6. fracao_terreno_pct
7. valor_rs_m2
8. custo_construcao_rs
9. custo_construcao_pct_vgv
10. prazo_obra_meses
11. custo_total_unit
12. custo_adc_selling
13. preco_nominal_unit
14. preco_raso_unit
15. li_pct
16. li_regua_pct
17. margem_bruta_economica_pct
18. margem_rasa_economica_pct
19. exposicao_rs_mil
20. exposicao_pct_vgv
21. renda
22. pct_ret1
23. pct_tcd (calculado)
24. raso_sobre_custo (calculado)

### Formatacao
- Primeira linha: cabecalho com os nomes das colunas
- Campos percentuais: gravar como numero decimal (ex: 47.6% -> 0.476) e formatar a coluna como percentual no Excel
- Campos numericos: gravar como numero (sem ponto de milhar, sem R$)
- Campo data_comite: gravar como data no formato dd/mm/aaaa
- Uma linha por combinacao de PDF + cenario

## Estrategia de Leitura dos PDFs

### Biblioteca sugerida
Usar `pdfplumber` ou `PyMuPDF (fitz)` para extrair texto pagina a pagina. O pdfplumber tende a funcionar melhor com tabelas, enquanto o PyMuPDF e mais rapido. Testar com os dois primeiros PDFs e decidir.

### Fluxo por PDF
1. Extrair texto da pagina 1 (capa) -> parsear nome_projeto, data_comite, tipo_comite.
2. Iterar pelas paginas 2 a 8 (no maximo) buscando paginas cujo texto contenha "One Page".
3. Para cada pagina One Page encontrada, extrair todos os campos do Bloco 2.
4. Calcular os campos do Bloco 3.
5. Gerar uma linha de output para cada One Page encontrado, replicando os dados da capa.

### Tratamento de erros
- Se um campo obrigatorio nao for encontrado em um One Page, gravar "ERRO_NAO_ENCONTRADO" na celula correspondente para permitir revisao manual.
- Se nenhum One Page for encontrado no PDF, gerar uma unica linha com os dados da capa preenchidos e todos os campos do One Page como "SEM_ONE_PAGE".
- Ao final, imprimir no console um resumo: total de PDFs processados, total de linhas geradas, total de erros.

## Caminho dos PDFs
A pasta com os PDFs estara em: `[SUBSTITUIR_PELO_CAMINHO_DA_PASTA]`
O arquivo de saida deve ser salvo em: `[SUBSTITUIR_PELO_CAMINHO_DE_SAIDA]/books_database.xlsx`

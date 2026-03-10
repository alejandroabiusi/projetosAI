# INSTRUÇÕES: Projeto de Inteligência Financeira - Releases e Planilhas

## CONTEXTO DO PROJETO

Você está executando a fase de reconhecimento e mapeamento de dados para um sistema de inteligência competitiva no setor imobiliário brasileiro. O objetivo é construir um banco de dados estruturado com dados financeiros e operacionais de incorporadoras de capital aberto, extraídos de releases de resultados (PDFs) e planilhas de dados históricos.

As empresas de interesse são: **Tenda, MRV, Direcional, Cury, Plano&Plano, Cyrela/Vivaz**.

Você tem autonomia total para ler, explorar e analisar arquivos. Não precisa pedir confirmação para abrir arquivos, navegar em diretórios ou executar scripts de leitura. Apenas evite modificar, mover ou deletar arquivos existentes.

---

## PASSO 1 — MAPEAMENTO DA ESTRUTURA DE DIRETÓRIOS

Explore recursivamente a partir de `C:\Projetos_AI\` e localize:
- Todos os diretórios que contenham arquivos `.pdf` relacionados a releases de resultados
- Todos os diretórios que contenham arquivos `.xlsx` relacionados a planilhas de dados financeiros/operacionais

Gere um inventário completo com:
- Caminho completo de cada diretório relevante encontrado
- Quantidade de arquivos por diretório
- Lista de arquivos com nome, extensão e tamanho

Salve o resultado em: `C:\Projetos_AI\analise_releases\inventario_arquivos.txt`

---

## PASSO 2 — AUDITORIA E PADRONIZAÇÃO DE NOMENCLATURA

Para cada arquivo de release (PDF) e planilha (XLSX) encontrado, avalie se o nome do arquivo segue um padrão claro que permita identificar:
- Empresa
- Tipo de documento (release / planilha operacional / demonstração financeira)
- Período de referência (trimestre e ano)

### Padrão de nomenclatura alvo:
```
{EMPRESA}_{TIPO}_{PERIODO}.{ext}

Exemplos:
  Tenda_Release_3T2025.pdf
  MRV_Release_4T2024.pdf
  Cyrela_Planilha_Operacional_3T2025.xlsx
  Direcional_DF_2T2025.xlsx
```

### Empresas — siglas padronizadas:
- Tenda
- MRV
- Direcional
- Cury
- PlanoePlano
- Cyrela

### Tipos de documento:
- `Release` — release de resultados trimestral (PDF)
- `Planilha_Operacional` — planilha com dados operacionais históricos (XLSX)
- `Planilha_DF` — planilha com demonstrações financeiras (XLSX)
- `Planilha_Fundamentos` — planilha consolidada com múltiplos tipos de dados (XLSX)

Gere uma tabela com:
- Nome atual do arquivo
- Nome sugerido padronizado
- Flag indicando se renomeação é necessária (SIM/NÃO)

**Não renomeie os arquivos ainda.** Apenas documente as sugestões.

Salve em: `C:\Projetos_AI\analise_releases\sugestao_nomenclatura.txt`

---

## PASSO 3 — LEITURA DOS RELEASES E MAPEAMENTO DE CAMPOS

Para cada empresa de interesse, identifique o release mais recente disponível (maior trimestre/ano) e leia o PDF completo.

Use a biblioteca `pdfplumber` ou `pymupdf` para extração de texto. Se não estiverem instaladas, instale com pip antes de prosseguir.

Para cada release lido, extraia e documente **todos os campos de dados** que aparecem no documento, incluindo:

### Categorias de campos a mapear:

**Identificação**
- Empresa, segmento reportado (ex: Tenda Core, Alea, MRV Incorporação, Resia, Riva, Vivaz, Total)
- Trimestre e ano de referência
- Data de publicação

**Lançamentos**
- VGV total lançado (R$ milhões)
- Número de empreendimentos lançados
- Número de unidades lançadas
- Preço médio por unidade lançada
- Tamanho médio dos empreendimentos

**Vendas**
- Vendas brutas VGV
- Vendas brutas unidades
- Distratos VGV
- Distratos unidades
- Vendas líquidas VGV
- Vendas líquidas unidades
- VSO bruta (%)
- VSO líquida (%)
- Preço médio por unidade vendida
- % das vendas por faixa de renda (MCMV F1, F2, F3) — quando disponível

**Repasses e Entregas**
- VGV repassado
- Unidades repassadas
- Unidades entregues
- Obras em andamento

**Estoque**
- Estoque a valor de mercado VGV
- Estoque a valor de mercado unidades
- Preço médio do estoque
- % estoque pronto
- Giro do estoque (meses)
- Status de obra do estoque (não iniciado / até 30% / 30-70% / acima 70% / concluído)

**Landbank**
- VGV total do banco de terrenos
- Número de empreendimentos no landbank
- Número de unidades no landbank
- Preço médio por unidade no landbank
- % permuta total
- % permuta financeiro
- % permuta unidades
- Aquisições/ajustes no trimestre

**Resultado a Apropriar (Backlog)**
- Receitas a apropriar
- Custo das unidades vendidas a apropriar
- Resultado a apropriar
- Margem a apropriar (%)
- Margem a apropriar ajustada (%)

**DRE**
- Receita operacional bruta
- Deduções (PDD, distratos, impostos)
- Receita operacional líquida
- Custo dos imóveis vendidos
- Lucro bruto
- Margem bruta (%)
- Custos financeiros capitalizados
- Lucro bruto ajustado
- Margem bruta ajustada (%)
- Despesas com vendas
- Despesas G&A
- Outras receitas/despesas operacionais
- EBITDA
- EBITDA ajustado
- Margem EBITDA ajustada (%)
- Resultado financeiro
- Receitas financeiras
- Despesas financeiras
- Lucro líquido
- Margem líquida (%)
- Lucro por ação (UDM)
- ROE (UDM)
- ROCE (UDM)

**Métricas proprietárias específicas por empresa** (quando presentes)
- Margem REF / Margem de referência de projetos (Tenda)
- Margem bruta das novas vendas (Tenda)
- Margem bruta rasa (Tenda)

**Endividamento**
- Dívida bruta total
- Dívida corporativa
- Financiamento à construção SFH
- Caixa e aplicações financeiras
- Dívida líquida
- Dívida líquida corporativa / PL (%)
- Dívida líquida total / (PL + Minoritários) (%)
- Duration da dívida (meses)
- Custo médio ponderado da dívida (%)
- Saldo de cessão de recebíveis

**Geração de Caixa**
- Fluxo de caixa operacional (metodologia gerencial da empresa)
- Geração/consumo de caixa por segmento

**Recebíveis Financiados pela Companhia** (quando disponível)
- Carteira bruta total
- Carteira bruta pré-chaves
- Carteira bruta pós-chaves
- Carteira líquida de provisão
- Aging da carteira pós-chaves: adimplente, inadimplente < 90d, 90-360d, > 360d
- Índice de cobertura de provisão por bucket
- % TCD/VGV (pró-soluto pós-chaves)
- Dias de contas a receber

**Guidance** (quando disponível)
- Métrica sob guidance, valor mínimo, valor máximo, realizado no período, % de atingimento

Para cada campo, anote:
- Nome exato como aparece no release
- Se está disponível para aquela empresa (SIM/NÃO/PARCIAL)
- Unidade de medida
- Se aparece segregado por segmento

---

## PASSO 4 — LEITURA DAS PLANILHAS E MAPEAMENTO DE CAMPOS ADICIONAIS

Para cada planilha XLSX encontrada (todas as empresas), leia todas as abas e identifique campos adicionais que não aparecem nos releases ou que oferecem granularidade maior (séries históricas mais longas, segregações geográficas, por empreendimento, etc.).

Use `openpyxl` ou `pandas` para leitura. Instale se necessário.

Para cada planilha, documente:
- Nome da aba
- Colunas presentes
- Período histórico coberto (trimestre mais antigo e mais recente)
- Campos relevantes não cobertos pelos releases
- Nível de granularidade (consolidado / por segmento / por região / por empreendimento)

Foque especialmente em:
- Séries históricas de VSO, margem, vendas desde 2018+
- Dados por região geográfica
- Dados por faixa de renda MCMV
- Informações sobre landbank por região
- Dados de SFH e financiamento bancário

---

## PASSO 5 — GERAÇÃO DO ARQUIVO DE SUGESTÃO DE SCHEMA

Consolide tudo que foi mapeado nos passos 3 e 4 em um único arquivo Excel estruturado com as seguintes abas:

### Aba 1: `schema_campos`
Tabela com todas as colunas:

| campo_id | categoria | nome_campo | descricao | unidade | fonte | tenda | mrv | direcional | cury | plano_plano | cyrela | observacoes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|

- `fonte`: "release" / "planilha" / "ambos" / "calculado"
- Colunas por empresa: "SIM" / "NÃO" / "PARCIAL" / "ESTIMADO"

### Aba 2: `schema_segmentos`
Lista de todos os segmentos reportáveis por empresa:

| empresa | segmento | descricao | disponivel_release | disponivel_planilha |
|---|---|---|---|---|

Exemplos: Tenda/Tenda Core, Tenda/Alea, MRV/MRV Incorporação, MRV/Resia, MRV/Urba, MRV/Luggo, Cyrela/Total, Cyrela/Vivaz, etc.

### Aba 3: `releases_inventario`
Lista de todos os releases encontrados:

| empresa | periodo | ano | trimestre | caminho_arquivo | nome_arquivo_atual | nome_sugerido | data_publicacao |
|---|---|---|---|---|---|---|---|

### Aba 4: `planilhas_inventario`
Lista de todas as planilhas encontradas:

| empresa | tipo | periodo_inicio | periodo_fim | caminho_arquivo | nome_arquivo_atual | nome_sugerido | abas_disponiveis |
|---|---|---|---|---|---|---|---|

### Aba 5: `campos_adicionais_planilhas`
Campos encontrados exclusivamente nas planilhas (não nos releases):

| empresa | planilha | aba | campo | descricao | periodo_disponivel | granularidade |
|---|---|---|---|---|---|---|

Salve em: `C:\Projetos_AI\analise_releases\schema_sugestao.xlsx`

---

## INSTRUÇÕES GERAIS DE EXECUÇÃO

- Execute os passos em sequência (1 → 2 → 3 → 4 → 5)
- Para cada passo concluído, imprima no terminal uma linha de status: `[PASSO X CONCLUÍDO] — {resumo do que foi feito}`
- Se encontrar um arquivo que não consegue abrir ou ler, registre o erro no inventário e continue sem interromper a execução
- Ao final de cada passo, salve o arquivo de saída correspondente antes de prosseguir
- Ao final do passo 5, imprima um resumo executivo com: total de releases encontrados, total de planilhas encontradas, total de campos mapeados no schema, empresas cobertas

---

## DEPENDÊNCIAS PYTHON NECESSÁRIAS

```bash
pip install pdfplumber pymupdf pandas openpyxl
```

Instale no início da execução caso não estejam disponíveis.

---

## ARQUIVOS DE SAÍDA ESPERADOS

```
C:\Projetos_AI\analise_releases\
    inventario_arquivos.txt
    sugestao_nomenclatura.txt
    schema_sugestao.xlsx
```

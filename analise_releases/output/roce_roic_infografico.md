# ROCE e ROIC — Incorporadoras Brasileiras (UDM)

> Fonte: Planilhas de RI + ITRs/DFPs CVM | Banco: dados_financeiros.db | Abril 2026

## ROCE UDM (Return on Capital Employed)

ROCE = NOPAT UDM / Capital Empregado (PL + Divida Liquida)

| Empresa | 4T2022 | 1T2023 | 2T2023 | 3T2023 | 4T2023 | 1T2024 | 2T2024 | 3T2024 | 4T2024 | 1T2025 | 2T2025 | 3T2025 |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Cury** | - | 56.2% | 56.5% | 76.1% | 82.1% | 75.8% | 107.5% | 75.8% | 82.6% | 74.0% | 67.8% | 85.5% |
| **MRV** | 36.1% | 30.0% | 27.8% | 33.2% | 28.6% | 31.0% | 39.4% | 21.4% | 33.7% | 42.5% | 41.5% | 59.2% |
| **PlanoePlano** | 40.4% | 36.6% | 48.3% | 46.5% | 101.6% | 67.1% | 76.6% | 68.8% | 33.3% | 55.3% | 58.4% | 49.8% |
| **Direcional** | 13.8% | 14.0% | 23.6% | 15.7% | 13.3% | 12.1% | 7.0% | 14.3% | 22.4% | 28.3% | 39.4% | 36.9% |
| **Tenda** | -19.6% | -16.2% | -10.8% | -0.5% | 6.7% | 9.8% | 9.3% | 14.3% | 18.6% | 19.5% | 19.3% | 20.0% |
| **Cyrela** | 8.5% | 8.3% | 9.3% | 8.8% | 9.1% | - | - | 12.0% | 14.8% | 14.4% | - | 13.6% |

### Destaques ROCE

- **Cury**: lider absoluto, consistente ~80%+. Combinacao de margens altas e baixa alavancagem
- **MRV**: recuperacao forte de 28% (4T23) para 59% (3T25), mas volatil entre trimestres
- **Direcional**: expansao impressionante de 13% (4T23) para 37% (3T25), quase triplicou
- **Tenda**: turnaround espetacular — de -20% (4T22) para +20% (3T25)
- **PlanoePlano**: alto mas volatil (33% a 77%), refletindo ciclo de lancamentos
- **Cyrela**: mais baixo do setor (9-15%), coerente com mix de alta/media renda e maior intensidade de capital

## ROIC UDM (Return on Invested Capital)

ROIC = NOPAT UDM / Capital Investido (Ativo Total - Passivo Circulante)

| Empresa | 4T2022 | 1T2023 | 2T2023 | 3T2023 | 4T2023 | 1T2024 | 2T2024 | 3T2024 | 4T2024 | 1T2025 | 2T2025 | 3T2025 |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Cury** | 21.1% | 20.4% | 21.2% | 20.9% | 25.3% | 25.3% | 23.6% | 24.9% | 23.5% | 23.1% | 24.4% | 25.9% |
| **PlanoePlano** | 20.3% | 22.1% | 29.5% | 32.5% | 44.7% | 44.6% | 33.2% | 28.2% | 17.4% | 20.6% | 19.8% | 15.2% |
| **MRV** | - | 11.7% | 10.5% | 13.2% | - | 11.5% | 14.5% | 7.9% | 11.6% | 14.5% | 12.5% | 17.0% |
| **MouraDubeux** | 5.5% | 5.7% | 6.2% | 6.5% | 8.7% | 9.1% | 9.7% | 9.7% | 11.7% | 12.4% | 12.8% | 10.9% |
| **Direcional** | 4.1% | 4.1% | 5.9% | 4.6% | 3.8% | 3.7% | 1.9% | 3.7% | 5.3% | 6.6% | 8.3% | 8.2% |
| **Cyrela** | 4.7% | 4.6% | 5.2% | 4.9% | 5.3% | 5.9% | 6.3% | 6.8% | 7.5% | 7.4% | 6.8% | 6.8% |

### Destaques ROIC

- **Cury**: ~24-26%, destaque absoluto — retorno excepcional sobre capital investido
- **PlanoePlano**: 15-28%, segundo melhor ROIC, modelo asset-light
- **MRV**: recuperacao de 8% para 17%, reflexo da melhora operacional da MRV Inc
- **MouraDubeux**: estavel em 10-12%, performance solida para o porte
- **Direcional**: de 2% para 8%, acompanhando expansao de margens
- **Cyrela**: 6-7%, mais baixo (alto ativo por empreendimentos de medio/alto padrao)

## Gaps Remanescentes

| Gap | Impacto | Como resolver |
|-----|---------|---------------|
| ROCE MouraDubeux | Sem divida liquida na planilha | Extrair manualmente dos releases ou buscar campo no ITR |
| ROIC Tenda | Sem ativo total para periodos antigos | Extrair dos ITRs da Tenda (formato Sumaq, nao CVM) |
| 4T2025 DRE trimestral | Valor do ITR 4T e anual, nao trimestral | Subtrair acumulado 3T do valor anual para isolar Q4 |
| MRV ITRs | 26 arquivos em formato nao-CVM | MRV usa formato de auditoria propria — extrair via release PDF |
| Cyrela planilha 4T2025 | Economatica ainda nao atualizou | Aguardar atualizacao ou usar ITR (ja processado) |

## Metodologia

| Metrica | Formula | Observacao |
|---------|---------|------------|
| **ROCE** | NOPAT UDM / (PL + Divida Liquida) | Capital Empregado = proxy para capital de terceiros + proprio |
| **ROIC** | NOPAT UDM / (Ativo Total - Passivo Circulante) | Capital Investido = ativos de longo prazo |
| **NOPAT** | EBIT x (1 - aliquota efetiva) | Aliquota efetiva = IR&CSLL / Lucro antes IR |
| **UDM** | Soma dos ultimos 4 trimestres | Excecao: Tenda reporta NOPAT/ROCE ja em base UDM |

**Fontes**: Planilhas interativas de RI (DRE, BP) + 135 ITRs/DFPs CVM processados automaticamente

**Empresas cobertas**: Tenda, Cury, Cyrela, Direcional, MRV, Plano\&Plano, Moura Dubeux

**Periodo**: 1T2020 a 4T2025 (24 trimestres por empresa)
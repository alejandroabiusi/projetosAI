# Relatório Missão 1 Continuação — Datas de Lançamento

**Data de execução:** 2026-04-12 12:43

## Resumo

| Métrica | Valor |
|---|---|
| Total na base | 6221 |
| Total com data (ANTES desta rodada) | 2107 |
| Matches novos nesta rodada | 26 |
|   - Match direto/único | 4 |
|   - Ambíguos resolvidos (fase ou oldest) | 22 |
| Ainda sem data | 4088 |
| **Total com data AGORA** | **2133 (34.3%)** |

## Ponto 1+3: Matches novos

### Matches diretos/únicos (amostra, primeiros 30)

| Empresa | Nome (Base) | Nome (Excel) | Score | Data | Método |
|---|---|---|---|---|---|
| Rottas | Porto Aurora | Porto Aurora - 1 Fase | 100 | 2025-08 | unique_match |
| Rio Branco | Residencial Estação Garden Club | Estação Real Garden Club - 1 Fase | 87 | 2025-08 | unique_match |
| Zuma | Recanto das Pérolas | Recanto Das Pérolas - 1 Fase | 100 | 2024-07 | unique_match |
| Seazone | Jurerê Spot III | Jurerê Spot Ii | 97 | 2025-05 | unique_match |

### Ambíguos resolvidos (todos)

| Empresa | Nome (Base) | Nome (Excel) | Fase DB | Data | Método |
|---|---|---|---|---|---|
| Prestes | VITTACE BOSQUE | Vittace Bosque - Fase 1 - Torres 6 A 15 E 23 A 28 | None | 2023-10 | oldest_multi_phase |
| Prestes | VITTACE SERENA | Vittace Serena - Fase 1 | None | 2022-10 | oldest_multi_phase |
| Embraplan | Ítalo Residencial Clube | Ítalo Residencial Clube - Fase 2 (Torre 5, 6, 7 E 8) | None | 2025-01 | oldest_multi_phase |
| Rottas | MEO Neoville | Meo Neoville - Fase 1 Torres A, B C | None | 2022-10 | oldest_multi_phase |
| Hexagonal | Quinta das Figueiras | Quinta Das Figueiras- 2ª Fase | None | 2021-10 | oldest_multi_phase |
| Access | Parque Vida | Parque Vida | None | 2023-10 | oldest_multi_phase |
| Blendi | Bella Coimbra | Bella Coimbra - 1 Fase | None | 2023-10 | oldest_multi_phase |
| Solum | Baviera | Baviera - Fase 1 | None | 2024-11 | oldest_multi_phase |
| LBX | Terra de Santa Cruz | Terra De Santa Cruz I | None | 2022-08 | oldest_multi_phase |
| LBX | Terra de Santa Cruz I | Terra De Santa Cruz I | None | 2022-08 | oldest_multi_phase |
| Pro Domo | Orb. Residence | Hórus Residence | None | 2024-03 | oldest_multi_phase |
| Pro Domo | Residencial Luna | Residencial Luxus | None | 2021-05 | oldest_multi_phase |
| Eco Vila | Eco Vila Santa Margarida | Eco Vila Santa Margarida - Fase 1 | None | 2022-01 | oldest_multi_phase |
| Visconde | Icaraí Parque Clube | Icaraí Parque Club - Fase 1 ( Torres 1 E 2 ) | None | 2022-12 | oldest_multi_phase |
| Yees | Ipa Club | Ipa Club- 1ª Fase | None | 2023-09 | oldest_multi_phase |
| Mac Lucer | Avelã Vila Residencial | Avelã Vila Residencial - Fase 2 | None | 2023-01 | oldest_multi_phase |
| Zuma | Nosso Paraíso Bio | Nosso Paraíso Bio | None | 2021-01 | oldest_multi_phase |
| You,inc | Studios Alto by You,inc Paraíso | Alto Studios By You,Inc | None | 2021-11 | oldest_multi_phase |
| Kallas | Enseada 360 | Enseada 360 | None | 2022-10 | oldest_multi_phase |
| Kallas | Enseada 360 – Fase 2 | Enseada 360 Fase 2 | 2 | 2023-06 | phase_match_from_name |
| Morar | Vista do Vale | Vista Do Vale - Fase 1 (Torres A / B / C / D) | None | 2021-05 | oldest_multi_phase |
| Hacasa | Duo Residence | Duo Residence - Fase 1 Torre A | None | 2021-01 | oldest_multi_phase |

### Sem match — Breakdown por razão

- **nome_no_match**: 2941
- **cidade_not_found**: 1102
- **empresa_not_found**: 45

### Sem match — Top empresas

- Tenda: 345
- MRV: 217
- VIC Engenharia: 148
- Diálogo: 141
- Magik JC: 100
- You,inc: 86
- Vitta Residencial: 83
- Bild: 79
- Living: 75
- Helbor: 71
- Vitacon: 63
- Grafico: 54
- EBM: 52
- Pacaembu: 49
- HM Engenharia: 48
- One Innovation: 47
- Realiza: 45
- NVR: 45
- Estação 1: 44
- Trisul: 43

## Ponto 4: Match por cidade+UF+nome (análise sem aplicar)

**Potenciais matches encontrados:** 108 (score >= 90, empresa ignorada)
**Empresa diferente:** 108

### Amostra (primeiros 30)

| Empresa (DB) | Nome (DB) | Empresa (Excel) | Nome (Excel) | Cidade/UF | Score | Data |
|---|---|---|---|---|---|---|
| Magik JC | Vert Cidade Universitaria | Vibra | Vibra Cidade Universitária | São Paulo/SP | 90 | 2023-07 |
| Magik JC | Arte Faria Lima | Rezende | Àra Faria Lima | São Paulo/SP | 90 | 2022-07 |
| Vibra Residencial | Vibe Campo Belo | Vibe | Vibe Campo Belo | São Paulo/SP | 100 | 2022-02 |
| Vibra Residencial | Vibe Vila Olímpia | Vibe | Vibe Vila Olímpia | São Paulo/SP | 100 | 2022-06 |
| Vibra Residencial | Vibra Conceicao | OXE | Vibe Conceição | São Paulo/SP | 90 | 2025-10 |
| Vibra Residencial | Vibe Pinheiros | Vibe | Vibe Pinheiros | São Paulo/SP | 100 | 2022-03 |
| Conx | Villa Perdizes Welconx | Welconx | Villa Perdizes Welconx | São Paulo/SP | 100 | 2022-11 |
| MRV | Sensia Galleria | Sensia | Sensia Galleria | Campinas/SP | 100 | 2022-09 |
| MRV | Sensia Swiss Garden | Sensia | Sensia Swiss Garden | Campinas/SP | 100 | 2025-06 |
| MRV | Sensia Taquaral | Sensia | Sensia Taquaral | Campinas/SP | 100 | 2023-06 |
| MRV | Sensia Gran Bosque | Sensia | Sensia Gran Bosque | Belo Horizonte/MG | 100 | 2025-08 |
| MRV | Sensia Pampulha | Sensia | Sensia Pampulha | Belo Horizonte/MG | 100 | 2022-06 |
| MRV | Sensia Paris | Sensia | Sensia Paris | Belo Horizonte/MG | 100 | 2023-09 |
| MRV | Sensia Way | Sensia | Sensia Way | Belo Horizonte/MG | 100 | 2022-10 |
| MRV | Sensia Horizon | Sensia | Sensia Horizon | Curitiba/PR | 100 | 2023-09 |
| MRV | Sensia Aurora | Sensia | Sensia Aurora | Londrina/PR | 100 | 2023-11 |
| MRV | Sensia Jardim | Sensia | Sensia Jardim | Maringá/PR | 100 | 2025-01 |
| MRV | Sensia Patamares | Sensia | Sensia Patamares | Salvador/BA | 100 | 2023-09 |
| MRV | Sensia Urban | Sensia | Sensia Urban | Salvador/BA | 100 | 2023-08 |
| MRV | Reserva da Lagoa | L França | Reserva Da Lagoa | Fortaleza/CE | 100 | 2022-08 |
| MRV | Sensia La Vie | Sensia | Sensia La Vie | Manaus/AM | 100 | 2023-08 |
| MRV | Sensia Ponta Negra | Sensia | Sensia Ponta Negra | Manaus/AM | 100 | 2022-06 |
| Metrocasa | Vila Guilherme | Evo | In Vila Guilherme | São Paulo/SP | 90 | 2022-08 |
| Árbore | Vista Park | Emccamp | Vista Park I | São Paulo/SP | 91 | 2022-02 |
| Árbore | Cais Eco Residência | Due | Cais Eco Residência | Ipojuca/PE | 100 | 2021-03 |
| Árbore | Naturê Eco Residência | Due | Nature Eco Residência | Ipojuca/PE | 100 | 2021-10 |
| M.Lar | M.Lar Cambeba | Marquise | M.lar Cambeba | Fortaleza/CE | 100 | 2025-09 |
| VIC Engenharia | Gran VIC Santana | Gedecon | Gran Vic Santana | Hortolândia/SP | 100 | 2021-12 |
| Vinx | Vinx Tatuapé | Ix. | Ix. Tatuapé | São Paulo/SP | 91 | 2022-02 |
| Vinx | Nurban Carnaubeiras | Vita Urbana | Nurban Carnaubeiras | São Paulo/SP | 100 | 2024-01 |

---

*Relatório gerado automaticamente em 2026-04-12 12:43*
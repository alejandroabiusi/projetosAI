# Auditoria Amostral — 06/04/2026

## Resumo
- **70 empresas** verificadas (5 produtos aleatórios cada)
- **49 inconsistências** encontradas

## Por tipo de inconsistência

| Tipo | Qtd | Descrição |
|------|-----|-----------|
| DORMS_MISMATCH | 35 | Dormitórios no banco não conferem com a página |
| FASE_DESATUALIZADA | 8 | Fase "Breve Lançamento" no banco mas site diz "pronto/entregue" |
| NOME_NAO_ENCONTRADO | 6 | Nome do empreendimento não aparece na página (pode ter mudado de nome ou URL redirecionou) |

## Detalhes

### FASE_DESATUALIZADA (8 casos)
Empreendimentos marcados como "Breve Lançamento" que já estão prontos/entregues:
- **Smart Construtora**: 5/5 amostras com fase desatualizada (Vila Andrade, Parada Inglesa, Cidade Patriarca, Laní Residence, Vila Augusta)
- **SUGOI**: 2 (Mirai Reserva Atlântica, Vida e Alegria Atibaia)
- **Ação recomendada**: Rodar `verificar_status.py` para Smart e SUGOI para atualizar fases

### DORMS_MISMATCH (35 casos)
Muitos são falsos alertas — o site usa "dorms" ou "dormitórios" ao invés de "quartos" e a auditoria buscou apenas "2 quartos". Os casos reais de mismatch precisam ser verificados individualmente:
- **MRV**: 5 amostras com mismatch — site usa "dormitórios" e não "quartos" (falso alerta)
- **Conx**: 4 amostras — site é SPA, texto não renderiza via requests
- **Direcional**: 4 amostras — site usa "quartos" em JS renderizado
- **Ação recomendada**: Melhorar a auditoria para buscar "dorm" além de "quarto"

### NOME_NAO_ENCONTRADO (6 casos)
- **Tenda**: 2 (Renascença Candeias, Parque Real Garden) — URLs redirecionam para página genérica
- **Ação recomendada**: Verificar se esses empreendimentos ainda existem no site

## Ações prioritárias
1. **Smart Construtora**: Atualizar fases (5/5 desatualizadas)
2. **SUGOI**: Atualizar fases (2 desatualizadas)
3. **Tenda**: Verificar URLs que redirecionam

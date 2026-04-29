# Correção de geoloc — batch 4 (5 empresas)

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_batch3b_*`

## Empresas tocadas (cluster específico de 5 produtos cada)
| Empresa | Updates | NULL | Notas |
|---|---:|---:|---|
| **Cittá** | 2 | 3 | iframe q=ENDEREÇO + CEP em texto |
| **Geratriz** | 5 | 0 | logradouro extraído + base CEPs |
| **Objeto** | 5 | 0 | iframe `!2d!3d` perfeito |
| **SOL Construtora** | 2 | 3 | Selenium (urllib bloqueado); ruas casaram pouco |
| **SUGOI** | 5 | 0 | ⚠ todos receberam mesma coord do iframe (provável sede ou stand de vendas) — investigar |

## Pendência
- **SUGOI**: as 5 atualizações ficaram na mesma coord (-23.58203, -46.45157). Iframe da página parece apontar pro mesmo lugar pra todos os produtos. Investigar se é stand de vendas.
- Geratriz ainda tem cluster (-23.48187, -47.43866) com 4 produtos. Não atacado nesta volta.

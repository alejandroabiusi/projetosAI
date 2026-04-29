# Correção de geoloc — batch 5 (5 empresas)

**Data:** 2026-04-29
**Backup:** `data/empreendimentos.db.bak_batch5_*`

| Empresa | Cluster | Updates | NULL | Notas |
|---|---|---:|---:|---|
| **Smart Construtora** | 5 numa coord | 0 | 5 | Selenium + iframe não casou bbox; coords falsas removidas |
| **Yees** | 4 numa coord | 3 | 1 | iframe + base CEPs |
| **Jotanunes** | 4 numa coord | 0 | 4 | Página não tinha geo extraível |
| **Econ Construtora** | 4 numa coord | 4 | 0 | iframe q=ENDEREÇO + base CEPs |
| **MRV** | 4+4 (2 clusters) | 0 | 8 | Site MRV pesado em JS; coords falsas removidas |

## Pendência
MRV requer scraper dedicado (SPA Next.js complexo) — coords reais existem nas demais 366 entradas, só esses 8 falsos foram limpos.

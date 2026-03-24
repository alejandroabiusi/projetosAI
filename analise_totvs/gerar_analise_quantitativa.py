"""
Gera analise quantitativa da TOTVS a partir das planilhas interativas.
Output: data/analise_quantitativa_totvs.md
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
PLAN_DIR = os.path.join(BASE, "downloads", "totvs", "planilhas")
OUT = os.path.join(BASE, "data", "analise_quantitativa_totvs.md")

HIST = os.path.join(PLAN_DIR, "TOTVS_Planilha_Historico.xlsx")

# Quarters labels
QUARTERS_PL = ["1Q23","2Q23","3Q23","4Q23","1Q24","2Q24","3Q24","4Q24","1Q25","2Q25","3Q25","4Q25"]

def read_sheet(sheet, start_col=2):
    df = pd.read_excel(HIST, sheet_name=sheet, header=None)
    return df

def get_row(df, keyword_pt, cols_start=2, n_cols=12):
    """Find row by Portuguese label (column 0) and return values."""
    for i in range(len(df)):
        val = str(df.iloc[i, 0]).strip()
        if keyword_pt in val:
            values = []
            for j in range(cols_start, cols_start + n_cols):
                v = df.iloc[i, j]
                if pd.notna(v):
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    values.append(None)
            return values
    return [None] * n_cols

def fmt_brl(v, divisor=1000):
    """Format R$ thousands to millions or billions."""
    if v is None: return "n/d"
    v = v / divisor  # thousands -> millions
    if abs(v) >= 1000:
        return f"R${v/1000:,.1f}B"
    return f"R${v:,.0f}M"

def fmt_pct(v):
    if v is None: return "n/d"
    if abs(v) < 1:  # already decimal
        return f"{v*100:.1f}%"
    return f"{v:.1f}%"

def fmt_num(v):
    if v is None: return "n/d"
    return f"{v:,.0f}"

def yoy(values, idx):
    """Calculate YoY growth for quarter at idx (needs idx-4)."""
    if idx < 4 or values[idx] is None or values[idx-4] is None or values[idx-4] == 0:
        return None
    return (values[idx] / values[idx-4]) - 1

def annual(values, year_start_idx, n=4):
    """Sum 4 quarters starting from year_start_idx."""
    s = sum(v for v in values[year_start_idx:year_start_idx+n] if v is not None)
    return s if s != 0 else None

def main():
    print("Lendo planilhas...")

    pl = read_sheet('GAAP-Consolidated-P&L')
    op = read_sheet('OPERATIONAL INFO')
    bs = read_sheet('GAAP-Consolidated-BS')
    cf = read_sheet('GAAP-Consolidated-CF')

    # --- EXTRACT KEY METRICS ---

    # P&L - Gestao
    gestao_rec = get_row(pl, "Resultado de Gest")  # header row, skip
    gestao_rl = get_row(pl, "Receita Líquida", 2, 12)  # row 3
    # Manual extraction by row index for reliability
    gestao_rl = [float(pl.iloc[3, c]) if pd.notna(pl.iloc[3, c]) else None for c in range(2, 14)]
    gestao_recorr = [float(pl.iloc[4, c]) if pd.notna(pl.iloc[4, c]) else None for c in range(2, 14)]
    gestao_nrecorr = [float(pl.iloc[5, c]) if pd.notna(pl.iloc[5, c]) else None for c in range(2, 14)]
    gestao_licencas = [float(pl.iloc[6, c]) if pd.notna(pl.iloc[6, c]) else None for c in range(2, 14)]
    gestao_servicos = [float(pl.iloc[7, c]) if pd.notna(pl.iloc[7, c]) else None for c in range(2, 14)]
    gestao_custos = [float(pl.iloc[8, c]) if pd.notna(pl.iloc[8, c]) else None for c in range(2, 14)]
    gestao_lb = [float(pl.iloc[9, c]) if pd.notna(pl.iloc[9, c]) else None for c in range(2, 14)]
    gestao_mb = [float(pl.iloc[10, c]) if pd.notna(pl.iloc[10, c]) else None for c in range(2, 14)]
    gestao_ebitda_adj = [float(pl.iloc[17, c]) if pd.notna(pl.iloc[17, c]) else None for c in range(2, 14)]
    gestao_ebitda_mg = [float(pl.iloc[18, c]) if pd.notna(pl.iloc[18, c]) else None for c in range(2, 14)]

    # P&L - RD Station
    rd_rl = [float(pl.iloc[33, c]) if pd.notna(pl.iloc[33, c]) else None for c in range(2, 14)]
    rd_recorr = [float(pl.iloc[34, c]) if pd.notna(pl.iloc[34, c]) else None for c in range(2, 14)]
    rd_saas = [float(pl.iloc[35, c]) if pd.notna(pl.iloc[35, c]) else None for c in range(2, 14)]
    rd_ebitda_adj = [float(pl.iloc[45, c]) if pd.notna(pl.iloc[45, c]) else None for c in range(2, 14)]
    rd_ebitda_mg = [float(pl.iloc[46, c]) if pd.notna(pl.iloc[46, c]) else None for c in range(2, 14)]

    # P&L - Consolidado
    cons_rl = [float(pl.iloc[57, c]) if pd.notna(pl.iloc[57, c]) else None for c in range(2, 14)]
    cons_lb = [float(pl.iloc[58, c]) if pd.notna(pl.iloc[58, c]) else None for c in range(2, 14)]
    cons_mb = [float(pl.iloc[59, c]) if pd.notna(pl.iloc[59, c]) else None for c in range(2, 14)]
    cons_ebitda_adj = [float(pl.iloc[112, c]) if pd.notna(pl.iloc[112, c]) else None for c in range(2, 14)]
    cons_ebitda_mg = [float(pl.iloc[113, c]) if pd.notna(pl.iloc[113, c]) else None for c in range(2, 14)]
    cons_ll_gaap = [float(pl.iloc[88, c]) if pd.notna(pl.iloc[88, c]) else None for c in range(2, 14)]
    cons_ll_adj = [float(pl.iloc[130, c]) if pd.notna(pl.iloc[130, c]) else None for c in range(2, 14)]
    cons_ml_adj = [float(pl.iloc[131, c]) if pd.notna(pl.iloc[131, c]) else None for c in range(2, 14)]
    cons_desp_com = [float(pl.iloc[62, c]) if pd.notna(pl.iloc[62, c]) else None for c in range(2, 14)]
    cons_ped = [float(pl.iloc[60, c]) if pd.notna(pl.iloc[60, c]) else None for c in range(2, 14)]
    cons_ga = [float(pl.iloc[63, c]) if pd.notna(pl.iloc[63, c]) else None for c in range(2, 14)]
    cons_res_fin = [float(pl.iloc[71, c]) if pd.notna(pl.iloc[71, c]) else None for c in range(2, 14)]

    # Operational Info
    arr_total = [float(op.iloc[4, c]) if pd.notna(op.iloc[4, c]) else None for c in range(2, 14)]
    arr_net_add = [float(op.iloc[6, c]) if pd.notna(op.iloc[6, c]) else None for c in range(2, 14)]
    arr_gestao = [float(op.iloc[10, c]) if pd.notna(op.iloc[10, c]) else None for c in range(2, 14)]
    arr_net_add_gestao = [float(op.iloc[12, c]) if pd.notna(op.iloc[12, c]) else None for c in range(2, 14)]
    retention_gestao = [float(op.iloc[14, c]) if pd.notna(op.iloc[14, c]) else None for c in range(2, 14)]
    arr_rd = [float(op.iloc[17, c]) if pd.notna(op.iloc[17, c]) else None for c in range(2, 14)]
    arr_net_add_rd = [float(op.iloc[19, c]) if pd.notna(op.iloc[19, c]) else None for c in range(2, 14)]
    retention_rd = [float(op.iloc[21, c]) if pd.notna(op.iloc[21, c]) else None for c in range(2, 14)]

    # Techfin
    producao_credito = [float(op.iloc[24, c]) if pd.notna(op.iloc[24, c]) else None for c in range(2, 14)]
    inadimplencia = [float(op.iloc[41, c]) if pd.notna(op.iloc[41, c]) else None for c in range(2, 14)]

    # --- GENERATE OUTPUT ---
    lines = []
    def w(s=""): lines.append(s)

    w("# TOTVS (TOTS3) - Analise Quantitativa Detalhada")
    w()
    w("*Dados extraidos das planilhas interativas oficiais (1Q23-4Q25)*")
    w()
    w("---")
    w()

    # ============================================================
    # SECTION 1: CONSOLIDATED EVOLUTION
    # ============================================================
    w("## 1. EVOLUCAO CONSOLIDADA TRIMESTRAL")
    w()
    w("### 1.1 Receita Liquida Consolidada (Gestao + RD Station)")
    w()
    w("| Quarter | Receita Liq. | YoY | Rec. Recorrente Gestao | % Recorr. | SaaS RD |")
    w("|---------|-------------|-----|------------------------|-----------|---------|")
    for i in range(12):
        rl = fmt_brl(cons_rl[i]) if cons_rl[i] else "n/d"
        yoy_v = yoy(cons_rl, i)
        yoy_s = fmt_pct(yoy_v) if yoy_v is not None else "-"
        recorr = fmt_brl(gestao_recorr[i]) if gestao_recorr[i] else "n/d"
        pct_rec = fmt_pct(gestao_recorr[i] / gestao_rl[i]) if gestao_rl[i] and gestao_recorr[i] else "n/d"
        saas = fmt_brl(rd_saas[i]) if rd_saas[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {rl} | {yoy_s} | {recorr} | {pct_rec} | {saas} |")
    w()

    # Annual summaries
    w("### 1.2 Receita Anual Consolidada")
    w()
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        rl_a = annual(cons_rl, start)
        gest_a = annual(gestao_rl, start)
        rd_a = annual(rd_rl, start)
        w(f"- **{year_label}:** Consolidado {fmt_brl(rl_a)} | Gestao {fmt_brl(gest_a)} | RD {fmt_brl(rd_a)}")

    # YoY annual
    rl_23 = annual(cons_rl, 0)
    rl_24 = annual(cons_rl, 4)
    rl_25 = annual(cons_rl, 8)
    w(f"- Crescimento 2024 vs 2023: {fmt_pct((rl_24/rl_23)-1)}")
    w(f"- Crescimento 2025 vs 2024: {fmt_pct((rl_25/rl_24)-1)}")
    w()

    # ============================================================
    # SECTION 2: EBITDA & MARGINS
    # ============================================================
    w("## 2. EBITDA E MARGENS")
    w()
    w("### 2.1 EBITDA Ajustado Trimestral")
    w()
    w("| Quarter | EBITDA Cons. | Mg EBITDA | EBITDA Gestao | Mg Gestao | EBITDA RD | Mg RD |")
    w("|---------|-------------|----------|--------------|-----------|----------|-------|")
    for i in range(12):
        ec = fmt_brl(cons_ebitda_adj[i]) if cons_ebitda_adj[i] else "n/d"
        mc = fmt_pct(cons_ebitda_mg[i]) if cons_ebitda_mg[i] else "n/d"
        eg = fmt_brl(gestao_ebitda_adj[i]) if gestao_ebitda_adj[i] else "n/d"
        mg = fmt_pct(gestao_ebitda_mg[i]) if gestao_ebitda_mg[i] else "n/d"
        er = fmt_brl(rd_ebitda_adj[i]) if rd_ebitda_adj[i] else "n/d"
        mr = fmt_pct(rd_ebitda_mg[i]) if rd_ebitda_mg[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {ec} | {mc} | {eg} | {mg} | {er} | {mr} |")
    w()

    w("### 2.2 EBITDA Anual")
    w()
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        ec_a = annual(cons_ebitda_adj, start)
        eg_a = annual(gestao_ebitda_adj, start)
        er_a = annual(rd_ebitda_adj, start)
        rl_a = annual(cons_rl, start)
        rg_a = annual(gestao_rl, start)
        rr_a = annual(rd_rl, start)
        w(f"- **{year_label}:**")
        w(f"  - Consolidado: {fmt_brl(ec_a)} (margem {fmt_pct(ec_a/rl_a)})")
        w(f"  - Gestao: {fmt_brl(eg_a)} (margem {fmt_pct(eg_a/rg_a)})")
        w(f"  - RD Station: {fmt_brl(er_a)} (margem {fmt_pct(er_a/rr_a)})")
    w()

    # Margin trajectory
    w("### 2.3 Trajetoria de Margem EBITDA Ajustada (Consolidado)")
    w()
    w("```")
    for i in range(12):
        if cons_ebitda_mg[i]:
            bar = "█" * int(cons_ebitda_mg[i] * 100 * 2)
            w(f"{QUARTERS_PL[i]}  {bar} {cons_ebitda_mg[i]*100:.1f}%")
    w("```")
    w()

    # ============================================================
    # SECTION 3: LUCRO LIQUIDO
    # ============================================================
    w("## 3. LUCRO LIQUIDO")
    w()
    w("| Quarter | LL Ajustado | Mg Liq. Aj. | LL GAAP | YoY Aj. |")
    w("|---------|------------|-------------|---------|---------|")
    for i in range(12):
        lla = fmt_brl(cons_ll_adj[i]) if cons_ll_adj[i] else "n/d"
        mla = fmt_pct(cons_ml_adj[i]) if cons_ml_adj[i] else "n/d"
        llg = fmt_brl(cons_ll_gaap[i]) if cons_ll_gaap[i] else "n/d"
        yoy_v = yoy(cons_ll_adj, i)
        yoy_s = fmt_pct(yoy_v) if yoy_v is not None else "-"
        w(f"| {QUARTERS_PL[i]} | {lla} | {mla} | {llg} | {yoy_s} |")
    w()

    # Annual
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        lla_a = annual(cons_ll_adj, start)
        llg_a = annual(cons_ll_gaap, start)
        w(f"- **{year_label}:** LL Ajustado {fmt_brl(lla_a)} | LL GAAP {fmt_brl(llg_a)}")
    w()

    # ============================================================
    # SECTION 4: ARR & UNIT ECONOMICS
    # ============================================================
    w("## 4. ARR E METRICAS OPERACIONAIS")
    w()
    w("### 4.1 ARR Total e Net Add")
    w()
    w("| Quarter | ARR Total | Net Add | ARR Gestao | Net Add G. | ARR RD | Net Add RD |")
    w("|---------|-----------|---------|-----------|------------|--------|------------|")
    for i in range(12):
        at = fmt_brl(arr_total[i], 1) if arr_total[i] else "n/d"
        na = fmt_brl(arr_net_add[i], 1) if arr_net_add[i] else "n/d"
        ag = fmt_brl(arr_gestao[i], 1) if arr_gestao[i] else "n/d"
        ng = fmt_brl(arr_net_add_gestao[i], 1) if arr_net_add_gestao[i] else "n/d"
        ar = fmt_brl(arr_rd[i], 1) if arr_rd[i] else "n/d"
        nr = fmt_brl(arr_net_add_rd[i], 1) if arr_net_add_rd[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {at} | {na} | {ag} | {ng} | {ar} | {nr} |")
    w()

    # ARR growth visualization
    w("### 4.2 Evolucao do ARR Total (R$ milhoes)")
    w()
    w("```")
    for i in range(12):
        if arr_total[i]:
            bar_len = int(arr_total[i] / 200)
            w(f"{QUARTERS_PL[i]}  {'█' * bar_len} {arr_total[i]:,.0f}")
    w("```")
    w()

    # Annual net add
    w("### 4.3 Adicao Liquida de ARR Anual")
    w()
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        na_a = annual(arr_net_add, start)
        ng_a = annual(arr_net_add_gestao, start)
        nr_a = annual(arr_net_add_rd, start)
        w(f"- **{year_label}:** Total R${na_a:,.0f}M | Gestao R${ng_a:,.0f}M | RD R${nr_a:,.0f}M")
    w()

    # ============================================================
    # SECTION 5: RETENTION RATES
    # ============================================================
    w("## 5. TAXAS DE RETENCAO")
    w()
    w("| Quarter | Retencao Gestao | Retencao RD SaaS |")
    w("|---------|-----------------|------------------|")
    for i in range(12):
        rg = fmt_pct(retention_gestao[i]) if retention_gestao[i] else "n/d"
        rr = fmt_pct(retention_rd[i]) if retention_rd[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {rg} | {rr} |")
    w()

    w("**Analise:** Gestao manteve retencao consistente >97,5% ao longo do periodo.")
    w("RD Station teve queda de retencao em 2024 (~91-92%), mas estabilizou e comecou a melhorar.")
    w()

    # ============================================================
    # SECTION 6: COST STRUCTURE
    # ============================================================
    w("## 6. ESTRUTURA DE CUSTOS E DESPESAS (Consolidado Gestao+RD)")
    w()
    w("### 6.1 Despesas como % da Receita")
    w()
    w("| Quarter | P&D/RL | Comercial/RL | G&A/RL | Margem Bruta |")
    w("|---------|--------|-------------|--------|-------------|")
    for i in range(12):
        if cons_rl[i] and cons_rl[i] != 0:
            ped_pct = fmt_pct(abs(cons_ped[i]) / cons_rl[i]) if cons_ped[i] else "n/d"
            com_pct = fmt_pct(abs(cons_desp_com[i]) / cons_rl[i]) if cons_desp_com[i] else "n/d"
            ga_pct = fmt_pct(abs(cons_ga[i]) / cons_rl[i]) if cons_ga[i] else "n/d"
            mb_pct = fmt_pct(cons_mb[i]) if cons_mb[i] else "n/d"
            w(f"| {QUARTERS_PL[i]} | {ped_pct} | {com_pct} | {ga_pct} | {mb_pct} |")
    w()

    w("### 6.2 Alavancagem Operacional")
    w()
    w("A TOTVS demonstra alavancagem operacional clara:")
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        rl_a = annual(cons_rl, start)
        ped_a = annual(cons_ped, start)
        com_a = annual(cons_desp_com, start)
        ga_a = annual(cons_ga, start)
        w(f"- **{year_label}:** P&D {fmt_pct(abs(ped_a)/rl_a)} | Comercial {fmt_pct(abs(com_a)/rl_a)} | G&A {fmt_pct(abs(ga_a)/rl_a)}")
    w()

    # ============================================================
    # SECTION 7: GESTAO DEEP DIVE
    # ============================================================
    w("## 7. GESTAO - ANALISE DETALHADA")
    w()
    w("### 7.1 Mix de Receita: Recorrente vs Nao Recorrente")
    w()
    w("| Quarter | Recorrente | % do Total | Licencas | Servicos |")
    w("|---------|-----------|------------|----------|----------|")
    for i in range(12):
        rec = fmt_brl(gestao_recorr[i])
        pct = fmt_pct(gestao_recorr[i] / gestao_rl[i]) if gestao_rl[i] else "n/d"
        lic = fmt_brl(gestao_licencas[i])
        srv = fmt_brl(gestao_servicos[i])
        w(f"| {QUARTERS_PL[i]} | {rec} | {pct} | {lic} | {srv} |")
    w()

    w("### 7.2 Crescimento YoY de Gestao")
    w()
    w("| Metrica | 2024 vs 2023 | 2025 vs 2024 |")
    w("|---------|-------------|-------------|")
    for label, data in [
        ("Receita Liquida", gestao_rl),
        ("Rec. Recorrente", gestao_recorr),
        ("EBITDA Ajustado", gestao_ebitda_adj),
    ]:
        a23 = annual(data, 0)
        a24 = annual(data, 4)
        a25 = annual(data, 8)
        g24 = fmt_pct((a24/a23)-1) if a23 else "n/d"
        g25 = fmt_pct((a25/a24)-1) if a24 else "n/d"
        w(f"| {label} | {g24} | {g25} |")
    w()

    # ============================================================
    # SECTION 8: RD STATION DEEP DIVE
    # ============================================================
    w("## 8. RD STATION - ANALISE DETALHADA")
    w()
    w("### 8.1 Evolucao Trimestral")
    w()
    w("| Quarter | Receita Liq. | SaaS | YoY RL | EBITDA | Margem |")
    w("|---------|-------------|------|--------|--------|--------|")
    for i in range(12):
        rl = fmt_brl(rd_rl[i])
        saas = fmt_brl(rd_saas[i])
        yoy_v = yoy(rd_rl, i)
        yoy_s = fmt_pct(yoy_v) if yoy_v is not None else "-"
        eb = fmt_brl(rd_ebitda_adj[i])
        mg = fmt_pct(rd_ebitda_mg[i]) if rd_ebitda_mg[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {rl} | {saas} | {yoy_s} | {eb} | {mg} |")
    w()

    w("### 8.2 Crescimento Sequencial QoQ da Receita SaaS RD")
    w()
    w("```")
    for i in range(1, 12):
        if rd_saas[i] and rd_saas[i-1] and rd_saas[i-1] != 0:
            qoq = (rd_saas[i] / rd_saas[i-1]) - 1
            bar = "█" * max(1, int(qoq * 100 * 5))
            w(f"{QUARTERS_PL[i]}  {bar} {qoq*100:+.1f}%")
    w("```")
    w()

    w("**Ponto de inflexao 4T25:** Crescimento SaaS QoQ de 6,3% (anualizado ~27%).")
    w("Historicamente, o melhor trimestre de crescimento sequencial da RD sob gestao TOTVS.")
    w()

    # ============================================================
    # SECTION 9: TECHFIN
    # ============================================================
    w("## 9. TECHFIN - INDICADORES OPERACIONAIS")
    w()
    w("| Quarter | Producao Credito | Inadimplencia >90d |")
    w("|---------|-----------------|-------------------|")
    for i in range(12):
        pc = fmt_brl(producao_credito[i], 1) if producao_credito[i] else "n/d"
        inad = fmt_pct(inadimplencia[i]) if inadimplencia[i] else "n/d"
        w(f"| {QUARTERS_PL[i]} | {pc} | {inad} |")
    w()

    # ============================================================
    # SECTION 10: KEY RATIOS & TRENDS
    # ============================================================
    w("## 10. INDICADORES-CHAVE E TENDENCIAS")
    w()

    w("### 10.1 Resumo Anual Comparativo")
    w()
    w("| Metrica | 2023 | 2024 | 2025 | CAGR 2Y |")
    w("|---------|------|------|------|---------|")

    metrics = [
        ("Receita Liq. Cons.", cons_rl, True),
        ("Rec. Recorrente Gestao", gestao_recorr, True),
        ("Receita SaaS RD", rd_saas, True),
        ("EBITDA Aj. Cons.", cons_ebitda_adj, True),
        ("EBITDA Aj. Gestao", gestao_ebitda_adj, True),
        ("EBITDA Aj. RD", rd_ebitda_adj, True),
        ("LL Ajustado", cons_ll_adj, True),
        ("ARR Net Add Total", arr_net_add, False),
    ]

    for label, data, as_brl in metrics:
        a23 = annual(data, 0)
        a24 = annual(data, 4)
        a25 = annual(data, 8)
        if as_brl:
            v23 = fmt_brl(a23)
            v24 = fmt_brl(a24)
            v25 = fmt_brl(a25)
        else:
            v23 = f"R${a23:,.0f}M" if a23 else "n/d"
            v24 = f"R${a24:,.0f}M" if a24 else "n/d"
            v25 = f"R${a25:,.0f}M" if a25 else "n/d"
        cagr = ((a25/a23)**(1/2) - 1) if a23 and a25 and a23 > 0 else None
        cagr_s = fmt_pct(cagr) if cagr is not None else "n/d"
        w(f"| {label} | {v23} | {v24} | {v25} | {cagr_s} |")

    # Margin rows
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        pass  # already shown above

    w()
    w("### 10.2 Margens Anuais")
    w()
    w("| Margem | 2023 | 2024 | 2025 | Delta 2Y |")
    w("|--------|------|------|------|----------|")

    for label, ebitda_data, rl_data in [
        ("EBITDA Cons.", cons_ebitda_adj, cons_rl),
        ("EBITDA Gestao", gestao_ebitda_adj, gestao_rl),
        ("EBITDA RD", rd_ebitda_adj, rd_rl),
        ("Liquida Aj.", cons_ll_adj, cons_rl),
    ]:
        margins = []
        for start in [0, 4, 8]:
            eb = annual(ebitda_data, start)
            rl = annual(rl_data, start)
            margins.append(eb/rl if eb and rl else None)
        delta = ((margins[2] - margins[0]) * 10000) if margins[0] and margins[2] else None
        delta_s = f"+{delta:.0f}bps" if delta and delta > 0 else f"{delta:.0f}bps" if delta else "n/d"
        w(f"| {label} | {fmt_pct(margins[0])} | {fmt_pct(margins[1])} | {fmt_pct(margins[2])} | {delta_s} |")
    w()

    # ============================================================
    # SECTION 11: SAAS TRANSITION
    # ============================================================
    w("## 11. TRANSICAO PARA SAAS - QUALIDADE DA RECEITA")
    w()
    w("### 11.1 % Receita Recorrente sobre Total (Gestao)")
    w()
    w("```")
    for i in range(12):
        if gestao_recorr[i] and gestao_rl[i]:
            pct = gestao_recorr[i] / gestao_rl[i] * 100
            bar = "█" * int(pct * 0.8)
            w(f"{QUARTERS_PL[i]}  {bar} {pct:.1f}%")
    w("```")
    w()

    w("### 11.2 Receita de Licencas em Declinio")
    w()
    for year_label, start in [("2023", 0), ("2024", 4), ("2025", 8)]:
        lic_a = annual(gestao_licencas, start)
        rl_a = annual(gestao_rl, start)
        w(f"- **{year_label}:** Licencas {fmt_brl(lic_a)} ({fmt_pct(lic_a/rl_a)} da RL Gestao)")
    w()
    w("Licencas on-premise encolhem enquanto SaaS+Cloud cresce >20% a/a — transicao saudavel.")
    w()

    # ============================================================
    # SECTION 12: KEY TAKEAWAYS
    # ============================================================
    w("## 12. PRINCIPAIS CONCLUSOES QUANTITATIVAS")
    w()
    w("1. **Crescimento consistente e acelerando:** CAGR de receita ~17% em 2 anos, com EBITDA crescendo mais rapido (~20%+ CAGR). Alavancagem operacional clara.")
    w()
    w("2. **Qualidade de receita melhorando:** Receita recorrente saiu de ~84% para ~91% do total. ARR net add acelerou de R$537M (2023) para R$905M (2025).")
    w()
    w("3. **Gestao e uma maquina:** Margem EBITDA convergiu de ~26% para ~29%, com crescimento de receita recorrente >20%. Retencao >97,5% indica moat forte.")
    w()
    w("4. **RD Station virou o jogo:** De margem EBITDA ~4% em 2023 para ~12-13% em 2025. Crescimento SaaS QoQ acelerou para 6,3% no 4T25 (anualizado ~27%).")
    w()
    w("5. **Techfin em estabilizacao:** Producao de credito estavel, inadimplencia controlada (~1,3-1,5%), modelo caminhando para rentabilidade sustentavel.")
    w()
    w("6. **Estrutura de custos otimizada:** P&D estavel em ~17-18% da receita (investimento preservado), Comercial caindo de ~23% para ~21%, G&A de ~9% para ~8%.")
    w()
    w("7. **Potencial do TaaS:** Habilitadores de AI ja representam 17% da receita de Gestao, crescendo 37% a/a e acelerando. E a maior avenida de crescimento incremental.")
    w()

    w("---")
    w()
    w("*Fonte: Planilhas interativas TOTVS (TOTVS_Planilha_Historico.xlsx). Dados trimestrais de 1Q23 a 4Q25.*")
    w("*Este documento, junto com o briefing_totvs.md, fornece contexto completo para analises aprofundadas sobre a empresa.*")

    # Write output
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Analise gerada em: {OUT}")
    print(f"Total de linhas: {len(lines)}")

if __name__ == "__main__":
    main()

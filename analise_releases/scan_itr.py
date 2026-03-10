import pdfplumber
import sys

pdf = pdfplumber.open('C:/Projetos_AI/coleta/downloads/planoeplano/itr_dfp/PlanoePlano_ITR_2022_1T.pdf')
for i in range(30, 100):
    text = pdf.pages[i].extract_text() or ''
    tl = text.lower()
    hits = []
    if 'contas a receber' in tl: hits.append('receb')
    if 'vencid' in tl or 'aging' in tl: hits.append('aging')
    if 'pecld' in tl or 'pdd' in tl or 'perda espera' in tl: hits.append('pecld')
    if 'debenture' in tl or 'financiamento' in tl: hits.append('debt')
    if 'garantia' in tl: hits.append('gar')
    if 'contingencia' in tl or 'contingência' in tl or 'conting' in tl: hits.append('conting')
    if 'resultado financeiro' in tl: hits.append('resfin')
    if 'caixa' in tl and 'aplica' in tl: hits.append('caixa')
    if hits:
        print(f'P{i+1}: {",".join(hits)}')
pdf.close()
sys.stdout.flush()

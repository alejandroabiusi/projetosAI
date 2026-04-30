import os
import pdfplumber
import sys

_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
pdf = pdfplumber.open(os.path.join(_BASE, 'planoeplano', 'itr_dfp', 'PlanoePlano_ITR_2025_3T.pdf'))
output = []
for i in range(72, 145):
    page = pdf.pages[i]
    text = page.extract_text()
    if text:
        clean = text.encode('ascii', 'replace').decode('ascii')
        output.append(f'PAGE {i+1}')
        output.append(clean[:3000])
        output.append('---')
pdf.close()
with open(r'C:/Projetos_AI/analise_releases/planoeplano_notes.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))
print('Done')

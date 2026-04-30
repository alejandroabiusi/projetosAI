"""
Batch ITR extractor using pdfplumber.
Scans ITR PDFs for key financial data sections and extracts text.
"""
import pdfplumber
import re
import os
import sys

def scan_pdf(pdf_path):
    """Scan PDF and find pages with key sections."""
    pdf = pdfplumber.open(pdf_path)
    sections = {
        'clientes': [],      # Nota de Clientes/Contas a receber
        'aging': [],          # Aging/vencimento
        'pecld': [],          # PECLD/perda esperada
        'divida': [],         # Empréstimos/financiamentos/debêntures
        'resultado_fin': [],  # Resultado financeiro
        'provisoes': [],      # Provisões
        'cessao': [],         # Cessão de recebíveis
    }

    for i, page in enumerate(pdf.pages):
        text = (page.extract_text() or '').lower()
        pn = i + 1

        # Clientes/Recebíveis (look for note header, not just any mention)
        if re.search(r'nota\s*\d+.*client|client.*nota\s*\d+|contas\s*a\s*receber.*incorpora', text):
            sections['clientes'].append(pn)
        elif 'circulante' in text and ('client' in text or 'receb' in text) and ('conclu' in text or 'constru' in text or 'pro soluto' in text):
            sections['clientes'].append(pn)

        # Aging
        if ('a vencer' in text or 'aging' in text) and ('vencid' in text or '180' in text or '360' in text):
            sections['aging'].append(pn)

        # PECLD
        if 'pecld' in text or ('perda esperada' in text and 'credito' in text):
            sections['pecld'].append(pn)
        elif 'provisao para credito' in text or 'provisão para crédito' in text:
            sections['pecld'].append(pn)

        # Dívida
        if re.search(r'nota\s*\d+.*empr[eé]stimo|empr[eé]stimo.*nota\s*\d+', text):
            sections['divida'].append(pn)
        elif ('debenture' in text or 'debênture' in text) and ('cri' in text or 'financiamento' in text):
            sections['divida'].append(pn)

        # Resultado financeiro
        if 'resultado financeiro' in text and ('receita' in text or 'despesa' in text) and ('juros' in text or 'capitaliza' in text or 'rendiment' in text):
            sections['resultado_fin'].append(pn)

        # Provisões
        if re.search(r'nota\s*\d+.*provis[oõ]|provis[oõ].*nota\s*\d+', text):
            sections['provisoes'].append(pn)
        elif ('garantia' in text or 'civel' in text or 'cíveis' in text) and 'provisao' in text:
            sections['provisoes'].append(pn)

        # Cessão
        if 'cessao' in text or 'cessão' in text:
            sections['cessao'].append(pn)

    pdf.close()
    return sections


def extract_pages(pdf_path, pages):
    """Extract text from specific pages."""
    pdf = pdfplumber.open(pdf_path)
    results = {}
    for pn in pages:
        if 0 < pn <= len(pdf.pages):
            text = pdf.pages[pn-1].extract_text() or ''
            results[pn] = text
    pdf.close()
    return results


def extract_tables(pdf_path, pages):
    """Extract tables from specific pages."""
    pdf = pdfplumber.open(pdf_path)
    results = {}
    for pn in pages:
        if 0 < pn <= len(pdf.pages):
            tables = pdf.pages[pn-1].extract_tables()
            if tables:
                results[pn] = tables
    pdf.close()
    return results


def parse_number(s):
    """Parse Brazilian number format: 1.234.567 or (1.234) for negative."""
    if not s:
        return None
    s = s.strip()
    negative = False
    if s.startswith('(') and s.endswith(')'):
        negative = True
        s = s[1:-1]
    s = s.replace('.', '').replace(',', '.')
    s = re.sub(r'[^\d.\-]', '', s)
    if not s:
        return None
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def process_itr(empresa, itr_name, pdf_path):
    """Process a single ITR and extract key data."""
    print(f"\n{'='*70}")
    print(f"  {empresa} - {itr_name}")
    print(f"{'='*70}")

    if not os.path.exists(pdf_path):
        print(f"  FILE NOT FOUND: {pdf_path}")
        return

    sections = scan_pdf(pdf_path)

    for section_name, pages in sections.items():
        if pages:
            print(f"  {section_name}: pages {pages}")

    # Extract text from key pages
    all_pages = set()
    for pages in sections.values():
        all_pages.update(pages)

    if not all_pages:
        print("  No relevant sections found!")
        return

    # Get the most relevant pages for each section
    # For clientes, get first 2-3 pages
    key_pages = set()
    for section_name, pages in sections.items():
        key_pages.update(pages[:3])  # First 3 pages of each section

    texts = extract_pages(pdf_path, sorted(key_pages))
    tables = extract_tables(pdf_path, sorted(key_pages))

    for pn in sorted(key_pages):
        print(f"\n--- Page {pn} ---")
        if pn in texts:
            # Print first 80 lines max
            lines = texts[pn].split('\n')
            for line in lines[:80]:
                print(f"  {line}")
        if pn in tables:
            for ti, table in enumerate(tables[pn]):
                print(f"\n  [Table {ti+1}]")
                for row in table:
                    print(f"  {row}")


# =============================================
# Main - specify which ITRs to process
# =============================================
if __name__ == '__main__':
    BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')

    # ITRs to process - edit this list as needed
    itrs = [
        # ('Empresa', 'ITR_name', 'path')
        ('Cury', '3T2023', os.path.join(BASE, 'cury', 'itr_dfp', 'Cury_ITR_3T2023.pdf')),
        ('Tenda', '3T2023', os.path.join(BASE, 'tenda', 'itr_dfp', 'Tenda_ITR_3T2023.pdf')),
        ('MRV', '3T2023', os.path.join(BASE, 'mrv', 'itr_dfp', 'MRV_ITR_3T2023.pdf')),
        ('Cyrela', '3T2023', os.path.join(BASE, 'cyrela', 'itr_dfp', 'Cyrela_ITR_3T2023.pdf')),
        ('PlanoePlano', '3T2023', os.path.join(BASE, 'planoeplano', 'itr_dfp', 'PlanoePlano_ITR_2023_3T.pdf')),
    ]

    if len(sys.argv) > 1:
        # Filter to specific companies
        companies = [a.lower() for a in sys.argv[1:]]
        itrs = [t for t in itrs if t[0].lower() in companies]

    for empresa, itr_name, path in itrs:
        process_itr(empresa, itr_name, path)

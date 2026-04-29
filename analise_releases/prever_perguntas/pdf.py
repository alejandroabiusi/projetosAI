"""Extracao de texto de releases PDF."""
from __future__ import annotations

from pathlib import Path


def extrair_texto_pdf(pdf_path: str | Path) -> str:
    """Extrai texto de todas as paginas de um PDF.

    Importacao tardia de pypdf para nao exigir a dependencia em testes que
    nao tocam essa funcao.
    """
    import pypdf

    reader = pypdf.PdfReader(str(pdf_path))
    parts: list[str] = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            parts.append(text)
    return "\n".join(parts)

"""Indexador de PDFs de releases trimestrais no ChromaDB."""

import re
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from pypdf import PdfReader

from config import RELEASES_BASE, CHROMA_DIR, EMPRESAS_RELEASES

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
COLLECTION_NAME = "releases"

# Mapa de pasta → nome padronizado da empresa
FOLDER_TO_EMPRESA = {
    "cury": "Cury",
    "cyrela": "Cyrela",
    "direcional": "Direcional",
    "mrv": "MRV",
    "planoeplano": "PlanoePlano",
    "tenda": "Tenda",
}


def _extract_text(pdf_path: Path) -> str:
    """Extrai texto de um PDF."""
    try:
        reader = PdfReader(str(pdf_path))
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text.strip()
    except Exception as e:
        print(f"  ERRO ao ler {pdf_path.name}: {e}")
        return ""


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Divide texto em chunks com overlap."""
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start += chunk_size - overlap
    return chunks


def _extract_periodo(filename: str) -> str:
    """Extrai período do nome do arquivo (ex: 'Cury_ITR_3T2024.pdf' → '3T2024')."""
    match = re.search(r"(\d[Tt]\d{4})", filename)
    return match.group(1).upper() if match else ""


def index_all_pdfs(verbose: bool = True):
    """Indexa todos os PDFs das empresas cobertas."""
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)

    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )

    chroma_client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    # Recriar collection
    try:
        chroma_client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass

    collection = chroma_client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    total_chunks = 0
    total_pdfs = 0

    for folder in EMPRESAS_RELEASES:
        empresa = FOLDER_TO_EMPRESA.get(folder, folder)
        pdf_dir = RELEASES_BASE / folder
        if not pdf_dir.exists():
            if verbose:
                print(f"[SKIP] Pasta não encontrada: {pdf_dir}")
            continue

        pdfs = sorted(pdf_dir.rglob("*.pdf"))
        if verbose:
            print(f"\n[{empresa}] {len(pdfs)} PDFs encontrados")

        for pdf_path in pdfs:
            text = _extract_text(pdf_path)
            if not text:
                continue

            chunks = _chunk_text(text)
            if not chunks:
                continue

            periodo = _extract_periodo(pdf_path.name)

            ids = [f"{empresa}_{pdf_path.stem}_chunk{i}" for i in range(len(chunks))]
            metadatas = [
                {
                    "empresa": empresa,
                    "filename": pdf_path.name,
                    "periodo": periodo,
                    "chunk_index": i,
                }
                for i in range(len(chunks))
            ]

            # ChromaDB aceita batches de até ~5000
            batch_size = 500
            for start in range(0, len(chunks), batch_size):
                end = start + batch_size
                collection.add(
                    ids=ids[start:end],
                    documents=chunks[start:end],
                    metadatas=metadatas[start:end],
                )

            total_chunks += len(chunks)
            total_pdfs += 1
            if verbose:
                print(f"  {pdf_path.name}: {len(chunks)} chunks")

    if verbose:
        print(f"\nIndexação concluída: {total_pdfs} PDFs, {total_chunks} chunks")

    return {"pdfs": total_pdfs, "chunks": total_chunks}

"""Script CLI para indexar os PDFs de releases no ChromaDB."""

import sys
import time

# Adicionar diretório do assistant ao path
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from rag.indexer import index_all_pdfs


def main():
    print("=" * 60)
    print("Financial Intelligence — Indexação de Releases PDF")
    print("=" * 60)

    start = time.time()
    stats = index_all_pdfs(verbose=True)
    elapsed = time.time() - start

    print(f"\nTempo total: {elapsed:.1f}s")
    print(f"PDFs indexados: {stats['pdfs']}")
    print(f"Chunks criados: {stats['chunks']}")
    print("\nPronto! O índice está em assistant/chroma_db/")


if __name__ == "__main__":
    main()

"""Interface de busca vetorial nos releases indexados."""

import chromadb
from chromadb.utils import embedding_functions

from config import CHROMA_DIR

COLLECTION_NAME = "releases"


def _get_collection():
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    return client.get_collection(name=COLLECTION_NAME, embedding_function=ef)


def search(query: str, empresa: str = None, n_results: int = 5) -> list[dict]:
    """Busca semântica nos releases.

    Args:
        query: Texto de busca
        empresa: Filtro opcional por empresa (Cury, Cyrela, etc.)
        n_results: Número de resultados

    Returns:
        Lista de dicts com {text, empresa, filename, periodo, distance}
    """
    try:
        collection = _get_collection()
    except Exception:
        return []

    where_filter = {"empresa": empresa} if empresa else None

    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter,
    )

    output = []
    if results and results["documents"]:
        for i, doc in enumerate(results["documents"][0]):
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            dist = results["distances"][0][i] if results["distances"] else None
            output.append({
                "text": doc,
                "empresa": meta.get("empresa", ""),
                "filename": meta.get("filename", ""),
                "periodo": meta.get("periodo", ""),
                "distance": round(dist, 4) if dist is not None else None,
            })

    return output

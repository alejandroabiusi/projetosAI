"""Tool: busca semântica nos releases PDF das incorporadoras."""

from rag.searcher import search

TOOL_DEFINITION = {
    "name": "search_releases",
    "description": (
        "Busca semântica nos releases trimestrais (PDFs) das incorporadoras. "
        "Útil para encontrar contexto qualitativo, comentários da administração, "
        "estratégias, guidance e informações que não estão no banco de dados numérico. "
        "Empresas: Cury, Cyrela, Direcional, MRV, PlanoePlano, Tenda."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Texto de busca (em português). Ex: 'estratégia de lançamentos Resia 2024'",
            },
            "empresa": {
                "type": "string",
                "description": "Filtro por empresa (opcional). Ex: 'MRV', 'Cury'.",
                "enum": ["Cury", "Cyrela", "Direcional", "MRV", "PlanoePlano", "Tenda"],
            },
            "n_results": {
                "type": "integer",
                "description": "Número de resultados (padrão: 5, máximo: 10).",
            },
        },
        "required": ["query"],
    },
}


def execute(query: str, empresa: str = None, n_results: int = 5) -> dict:
    n_results = min(n_results or 5, 10)

    results = search(query=query, empresa=empresa, n_results=n_results)

    if not results:
        return {
            "results": [],
            "message": "Nenhum resultado encontrado. O índice pode não estar criado — execute 'python index_pdfs.py' primeiro.",
        }

    return {
        "results": results,
        "result_count": len(results),
    }

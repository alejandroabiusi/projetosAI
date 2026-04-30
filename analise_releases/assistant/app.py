"""Financial Intelligence — App Chainlit com Groq (Llama 3.3 70B) + tool use."""

import json
import os

import chainlit as cl
import plotly.io as pio
from dotenv import load_dotenv
from groq import AsyncGroq

from config import MODEL, MAX_TOKENS
from system_prompt import SYSTEM_PROMPT
from tools.sql_query import execute as sql_execute
from tools.spreadsheet_reader import execute as sheet_execute
from tools.chart_generator import execute as chart_execute

load_dotenv()

# PDF search é opcional (requer chromadb + sentence-transformers)
try:
    from tools.pdf_search import execute as pdf_execute
    _pdf_available = True
except ImportError:
    _pdf_available = False

client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))

# --- Tool definitions (formato OpenAI) ---

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": (
                "Executa uma consulta SQL SELECT no banco de dados de indicadores financeiros "
                "trimestrais de incorporadoras brasileiras. Tabela principal: dados_trimestrais. "
                "Colunas-chave: empresa, segmento, periodo, ano, trimestre, e ~130 métricas. "
                "Use segmento='Consolidado' para comparar empresas."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "Consulta SQL SELECT. Apenas SELECT é permitido. "
                            "Ex: SELECT empresa, receita_liquida FROM dados_trimestrais "
                            "WHERE periodo='3T2024' AND segmento='Consolidado'"
                        ),
                    }
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_spreadsheet",
            "description": (
                "Lê planilhas Excel das incorporadoras. Ações: "
                "'list_spreadsheets', 'list_sheets', 'read_sheet'. "
                "Planilhas: Cury, Cyrela_Operacionais, Cyrela_DFs, Cyrela_Lancamentos, "
                "Cyrela_Indicadores, Direcional, MouraDubeux, MRV, PlanoePlano, Tenda."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_spreadsheets", "list_sheets", "read_sheet"],
                        "description": "Ação a executar.",
                    },
                    "spreadsheet": {
                        "type": "string",
                        "description": "Nome da planilha (ex: 'Cury', 'MRV').",
                    },
                    "sheet_name": {
                        "type": "string",
                        "description": "Nome da aba. Necessário para read_sheet.",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Máximo de linhas (padrão: 50).",
                    },
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_chart",
            "description": (
                "Cria gráficos Plotly (bar, line, waterfall, pie) para visualização de dados. "
                "Envie os dados já processados da query SQL ou planilha."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {
                        "type": "string",
                        "enum": ["bar", "line", "waterfall", "pie"],
                        "description": "Tipo de gráfico.",
                    },
                    "title": {"type": "string", "description": "Título do gráfico."},
                    "x_label": {"type": "string", "description": "Rótulo do eixo X."},
                    "y_label": {"type": "string", "description": "Rótulo do eixo Y."},
                    "series": {
                        "type": "array",
                        "description": "Séries: [{name, x:[...], y:[...]}]. Pie: [{name, labels:[...], values:[...]}].",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "x": {"type": "array", "items": {"type": "string"}},
                                "y": {"type": "array", "items": {"type": "number"}},
                                "labels": {"type": "array", "items": {"type": "string"}},
                                "values": {"type": "array", "items": {"type": "number"}},
                            },
                            "required": ["name"],
                        },
                    },
                },
                "required": ["chart_type", "title", "series"],
            },
        },
    },
]

PDF_TOOL = {
    "type": "function",
    "function": {
        "name": "search_releases",
        "description": (
            "Busca semântica nos releases trimestrais (PDFs) das incorporadoras. "
            "Útil para contexto qualitativo, comentários da administração, estratégias."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Texto de busca em português."},
                "empresa": {"type": "string", "description": "Filtro por empresa (opcional)."},
                "n_results": {"type": "integer", "description": "Número de resultados (padrão: 5)."},
            },
            "required": ["query"],
        },
    },
}

if _pdf_available:
    TOOLS.append(PDF_TOOL)

TOOL_EXECUTORS = {
    "query_database": sql_execute,
    "read_spreadsheet": sheet_execute,
    "create_chart": chart_execute,
}
if _pdf_available:
    TOOL_EXECUTORS["search_releases"] = pdf_execute


# --- Chainlit handlers ---


@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="Comparativo de Receita",
            message="Compare a receita líquida de todas as empresas no 3T2024 e mostre em gráfico de barras.",
        ),
        cl.Starter(
            label="Evolução de Margens",
            message="Mostre a evolução da margem bruta e margem líquida da Cury de 2020 a 2024 em gráfico de linha.",
        ),
        cl.Starter(
            label="Análise de Endividamento",
            message="Qual empresa tem o menor nível de endividamento (dívida líquida/PL) atualmente? Analise a tendência.",
        ),
        cl.Starter(
            label="VSO e Eficiência",
            message="Quais empresas tiveram VSO líquida trimestral acima de 40% em 2024? O que isso indica?",
        ),
    ]


@cl.on_chat_start
async def on_chat_start():
    cl.user_session.set("history", [])


@cl.on_message
async def on_message(message: cl.Message):
    history = cl.user_session.get("history", [])
    history.append({"role": "user", "content": message.content})

    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history

    # Tool use loop
    while True:
        response = await client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            max_tokens=MAX_TOKENS,
        )

        choice = response.choices[0]

        # Se não há tool calls, temos a resposta final
        if not choice.message.tool_calls:
            final_text = choice.message.content or ""
            history.append({"role": "assistant", "content": final_text})
            cl.user_session.set("history", history)
            await cl.Message(content=final_text).send()
            return

        # Processar tool calls
        assistant_msg = {
            "role": "assistant",
            "content": choice.message.content or "",
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in choice.message.tool_calls
            ],
        }
        messages.append(assistant_msg)

        for tc in choice.message.tool_calls:
            tool_name = tc.function.name
            tool_args = json.loads(tc.function.arguments)
            executor = TOOL_EXECUTORS.get(tool_name)

            if executor is None:
                result = {"error": f"Tool '{tool_name}' não encontrada."}
            else:
                try:
                    result = executor(**tool_args)
                except Exception as e:
                    result = {"error": f"Erro ao executar {tool_name}: {str(e)}"}

            # Renderizar charts inline
            if tool_name == "create_chart" and "plotly_json" in result:
                fig = pio.from_json(result["plotly_json"])
                await cl.Message(content="", elements=[
                    cl.Plotly(name="chart", figure=fig, display="inline")
                ]).send()
                result = {"status": "Gráfico gerado e exibido com sucesso", "title": result.get("title", "")}

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, ensure_ascii=False, default=str),
            })

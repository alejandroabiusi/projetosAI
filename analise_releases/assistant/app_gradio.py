"""Financial Intelligence — App Gradio com Groq (Llama 3.3 70B) + tool use."""

import json
import os

import gradio as gr
import plotly.io as pio
from dotenv import load_dotenv
from groq import Groq

from config import MODEL, MAX_TOKENS
from system_prompt import SYSTEM_PROMPT
from tools.sql_query import execute as sql_execute
from tools.spreadsheet_reader import execute as sheet_execute
from tools.chart_generator import execute as chart_execute

load_dotenv()

# PDF search é opcional
try:
    from tools.pdf_search import execute as pdf_execute
    _pdf_available = True
except ImportError:
    _pdf_available = False

client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# --- Tool definitions ---

SQL_TOOL_DESC = """Executa SQL SELECT no banco dados_trimestrais (279 registros, 6 empresas, 1T2020-3T2025).
Chave única: (empresa, segmento, periodo). Use segmento='Consolidado' para comparar empresas.

Colunas principais:
- empresa, segmento, periodo, ano, trimestre
- Lançamentos: vgv_lancado, unidades_lancadas, preco_medio_lancamento
- Vendas: vendas_brutas_vgv, vendas_liquidas_vgv, vendas_liquidas_unidades, vso_bruta_trimestral, vso_liquida_trimestral, vso_liquida_12m
- Estoque: estoque_vgv, estoque_unidades, pct_estoque_pronto
- Landbank: landbank_vgv, landbank_unidades
- Backlog: receitas_apropriar, margem_apropriar
- DRE: receita_bruta, receita_liquida, lucro_bruto, margem_bruta, ebitda, ebitda_ajustado, margem_ebitda, lucro_liquido, margem_liquida, roe
- SG&A: despesas_comerciais, despesas_ga, pct_sga_receita_liquida
- Dívida: divida_bruta, caixa_aplicacoes, divida_liquida, divida_liquida_pl, patrimonio_liquido
- Caixa: geracao_caixa
- Recebíveis: carteira_recebiveis_total, inadimplencia_total_pct

Valores monetários em R$ milhões. Percentuais já formatados (35.2 = 35.2%)."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": SQL_TOOL_DESC,
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query. Ex: SELECT empresa, receita_liquida FROM dados_trimestrais WHERE periodo='3T2024' AND segmento='Consolidado'",
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
            "description": "Cria gráficos Plotly. SEMPRE chame esta function para gráficos, nunca escreva JSON como texto. Para comparar múltiplas empresas, inclua todas no mesmo labels/values. Para comparar duas métricas diferentes, chame esta function duas vezes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {
                        "type": "string",
                        "enum": ["bar", "line", "waterfall", "pie"],
                        "description": "Tipo de gráfico.",
                    },
                    "title": {"type": "string", "description": "Título do gráfico."},
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Rótulos do eixo X (nomes das empresas, períodos, etc).",
                    },
                    "values": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Valores numéricos correspondentes aos labels.",
                    },
                    "series_name": {"type": "string", "description": "Nome da série (ex: 'Receita Líquida')."},
                    "x_label": {"type": "string", "description": "Rótulo do eixo X."},
                    "y_label": {"type": "string", "description": "Rótulo do eixo Y."},
                },
                "required": ["chart_type", "title", "labels", "values"],
            },
        },
    },
]

if _pdf_available:
    TOOLS.append({
        "type": "function",
        "function": {
            "name": "search_releases",
            "description": "Busca semântica nos releases PDF das incorporadoras. Útil para contexto qualitativo.",
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
    })

TOOL_EXECUTORS = {
    "query_database": sql_execute,
    "read_spreadsheet": sheet_execute,
    "create_chart": chart_execute,
}
if _pdf_available:
    TOOL_EXECUTORS["search_releases"] = pdf_execute


# --- Core: tool use loop ---

def chat(user_message: str, history: list):
    """Processa mensagem com tool use loop. Retorna (texto, fig_or_None)."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_message})

    chart_fig = None
    max_iterations = 5

    for _ in range(max_iterations):
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            max_tokens=MAX_TOKENS,
        )

        choice = response.choices[0]

        if not choice.message.tool_calls:
            return choice.message.content or "", chart_fig

        # Montar assistant msg com tool_calls
        messages.append({
            "role": "assistant",
            "content": choice.message.content or "",
            "tool_calls": [
                {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in choice.message.tool_calls
            ],
        })

        for tc in choice.message.tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}
            executor = TOOL_EXECUTORS.get(tool_name)

            if executor is None:
                result = {"error": f"Tool '{tool_name}' não encontrada."}
            else:
                try:
                    result = executor(**tool_args)
                except Exception as e:
                    result = {"error": str(e)}

            if tool_name == "create_chart" and "plotly_json" in result:
                chart_fig = pio.from_json(result["plotly_json"])
                result = {"status": "Gráfico gerado com sucesso", "title": result.get("title", "")}

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, ensure_ascii=False, default=str),
            })

    return "Desculpe, não consegui processar a consulta. Tente reformular.", chart_fig


def respond(user_message, chat_history):
    if not user_message or not user_message.strip():
        return "", chat_history, None
    try:
        text, fig = chat(user_message, chat_history)
    except Exception as e:
        text = f"Erro: {str(e)}"
        fig = None
    chat_history = chat_history + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": text},
    ]
    return "", chat_history, fig


def click_example(example_text, chat_history):
    return respond(example_text, chat_history)


# --- UI ---

DARK_CSS = """
body, .gradio-container { background-color: #0f1117 !important; color: #e0e0e0 !important; }
.chatbot { background-color: #1a1b26 !important; border: 1px solid #2a2b3d !important; border-radius: 12px !important; }
.message-wrap .message { border-radius: 12px !important; }
.user .message-content { background-color: #1e3a5f !important; }
.bot .message-content { background-color: #1a1b26 !important; }
footer { display: none !important; }
.contain { max-width: 1100px !important; margin: auto !important; }
h1 { color: #4ecdc4 !important; }
.example-btn { border: 1px solid #2a2b3d !important; background: #1a1b26 !important; color: #a0a0b0 !important; }
.example-btn:hover { border-color: #4ecdc4 !important; color: #4ecdc4 !important; }
"""

EXAMPLES = [
    "Qual foi a receita líquida da Cury no 3T2024?",
    "Compare o lucro líquido de todas as empresas no 3T2024",
    "Faça um gráfico de barras da receita líquida das empresas no 3T2024",
    "Quais empresas tiveram VSO acima de 40% em 2024?",
    "Evolução da margem bruta da Direcional de 2022 a 2024 em gráfico de linha",
]

with gr.Blocks(title="Financial Intelligence") as demo:
    gr.Markdown("# Financial Intelligence")
    gr.Markdown("Análise financeira de incorporadoras brasileiras — **Cury · Cyrela · Direcional · MRV · PlanoePlano · Tenda** — 1T2020 a 3T2025")

    with gr.Row():
        with gr.Column(scale=2):
            chatbot = gr.Chatbot(height=520, show_label=False, placeholder="Faça uma pergunta...")
            with gr.Row():
                msg = gr.Textbox(
                    placeholder="Ex: Qual o EBITDA da MRV no 3T2024?",
                    show_label=False,
                    scale=8,
                    container=False,
                )
                send_btn = gr.Button("Enviar", scale=1, variant="primary", min_width=80)

        with gr.Column(scale=1):
            chart_output = gr.Plot(label="Gráfico")

    gr.Markdown("**Exemplos:**")
    with gr.Row():
        for i, ex in enumerate(EXAMPLES):
            btn = gr.Button(ex, size="sm", elem_classes=["example-btn"])
            btn.click(fn=click_example, inputs=[gr.State(ex), chatbot], outputs=[msg, chatbot, chart_output])

    msg.submit(respond, [msg, chatbot], [msg, chatbot, chart_output])
    send_btn.click(respond, [msg, chatbot], [msg, chatbot, chart_output])

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=8000, share=False)

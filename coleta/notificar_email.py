"""
Notificacao por email via Outlook (win32com).
=============================================
Envia resumo da atualizacao com novos empreendimentos,
mudancas de status e erros.

Uso:
    Chamado automaticamente por run_atualizacao.py.
    Para teste manual:
        python notificar_email.py --teste
"""

import os
import sys
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.settings import EMAIL


def gerar_html(stats, changelog, falhas=None):
    """Gera corpo HTML do email com resumo da atualizacao."""
    data = datetime.now().strftime("%d/%m/%Y")
    novos = [c for c in changelog if c["tipo"] == "novo"]
    fases = [c for c in changelog if c["tipo"] == "fase_mudou"]
    outras = [c for c in changelog if c["tipo"] in ("preco_mudou", "campo_mudou")]

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Calibri, Arial, sans-serif; font-size: 11pt; color: #333; }}
        h2 {{ color: #1a5276; border-bottom: 2px solid #1a5276; padding-bottom: 5px; }}
        h3 {{ color: #2c3e50; margin-top: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th {{ background-color: #1a5276; color: white; padding: 8px 12px; text-align: left; font-size: 10pt; }}
        td {{ padding: 6px 12px; border-bottom: 1px solid #ddd; font-size: 10pt; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .resumo {{ background-color: #eaf2f8; padding: 15px; border-radius: 5px; margin: 15px 0; }}
        .resumo strong {{ color: #1a5276; }}
        .erro {{ color: #c0392b; }}
        .novo {{ color: #27ae60; font-weight: bold; }}
        .mudanca {{ color: #e67e22; }}
    </style>
    </head>
    <body>
    <h2>Atualizacao de Empreendimentos - {data}</h2>

    <div class="resumo">
        <strong>Total de empreendimentos:</strong> {stats.get('total_apos', 0)}<br>
        <strong>Etapas OK:</strong> {stats.get('etapas_ok', 0)} |
        <strong>Etapas com erro:</strong> {stats.get('etapas_erro', 0)}<br>
        <strong>Novos:</strong> <span class="novo">{stats.get('novos', 0)}</span> |
        <strong>Mudancas:</strong> <span class="mudanca">{stats.get('mudancas', 0)}</span>
    </div>
    """

    # Novos empreendimentos
    if novos:
        html += """
    <h3>Novos Empreendimentos</h3>
    <table>
    <tr><th>Empresa</th><th>Nome</th></tr>
    """
        for c in novos:
            html += f"    <tr><td>{c['empresa']}</td><td>{c['nome']}</td></tr>\n"
        html += "    </table>\n"

    # Mudancas de fase
    if fases:
        html += """
    <h3>Mudancas de Status/Fase</h3>
    <table>
    <tr><th>Empresa</th><th>Nome</th><th>Anterior</th><th>Novo</th></tr>
    """
        for c in fases:
            html += (f"    <tr><td>{c['empresa']}</td><td>{c['nome']}</td>"
                     f"<td>{c.get('valor_anterior', '')}</td><td>{c.get('valor_novo', '')}</td></tr>\n")
        html += "    </table>\n"

    # Outras mudancas
    if outras:
        html += """
    <h3>Outras Mudancas</h3>
    <table>
    <tr><th>Empresa</th><th>Nome</th><th>Campo</th><th>Anterior</th><th>Novo</th></tr>
    """
        for c in outras:
            html += (f"    <tr><td>{c['empresa']}</td><td>{c['nome']}</td>"
                     f"<td>{c.get('campo', '')}</td>"
                     f"<td>{c.get('valor_anterior', '')}</td><td>{c.get('valor_novo', '')}</td></tr>\n")
        html += "    </table>\n"

    # Sem novidades
    if not novos and not fases and not outras:
        html += "    <p>Nenhuma novidade detectada nesta atualizacao.</p>\n"

    # Erros
    if falhas:
        html += '    <h3 class="erro">Scrapers com Falha</h3>\n    <ul>\n'
        for f in falhas:
            html += f'    <li class="erro">{f["nome"]}</li>\n'
        html += "    </ul>\n"

    html += """
    <hr style="margin-top: 30px;">
    <p style="font-size: 9pt; color: #999;">
        Gerado automaticamente pelo sistema de coleta de empreendimentos.
    </p>
    </body>
    </html>
    """
    return html


def enviar_email_atualizacao(stats, changelog, falhas=None):
    """Envia email via Outlook COM."""
    import win32com.client

    destinatarios = EMAIL.get("destinatarios", [])
    if not destinatarios:
        print("AVISO: Nenhum destinatario configurado em EMAIL['destinatarios']")
        return False

    prefixo = EMAIL.get("assunto_prefixo", "[Coleta IM]")
    data = datetime.now().strftime("%d/%m/%Y")
    n_novos = stats.get("novos", 0)
    n_mudancas = stats.get("mudancas", 0)
    assunto = f"{prefixo} Atualizacao {data} - {n_novos} novos, {n_mudancas} mudancas"

    html = gerar_html(stats, changelog, falhas)

    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = "; ".join(destinatarios)
    mail.Subject = assunto
    mail.HTMLBody = html
    mail.Send()

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--teste", action="store_true", help="Envia email de teste")
    args = parser.parse_args()

    if args.teste:
        stats = {"total_apos": 2031, "etapas_ok": 9, "etapas_erro": 0, "novos": 3, "mudancas": 5}
        changelog = [
            {"tipo": "novo", "empresa": "Cury", "nome": "Teste Novo 1"},
            {"tipo": "novo", "empresa": "MRV", "nome": "Teste Novo 2"},
            {"tipo": "novo", "empresa": "Vivaz", "nome": "Teste Novo 3"},
            {"tipo": "fase_mudou", "empresa": "Plano&Plano", "nome": "Residencial X",
             "campo": "fase", "valor_anterior": "Em Construção", "valor_novo": "Imóvel Pronto"},
            {"tipo": "preco_mudou", "empresa": "Cury", "nome": "Residencial Y",
             "campo": "preco_a_partir", "valor_anterior": "250000", "valor_novo": "265000"},
        ]
        if EMAIL.get("destinatarios"):
            enviar_email_atualizacao(stats, changelog)
            print("Email de teste enviado!")
        else:
            # Sem destinatarios, apenas mostra o HTML
            print(gerar_html(stats, changelog))

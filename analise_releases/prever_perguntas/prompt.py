"""Montagem do prompt de previsao de perguntas.

O prompt e dividido em dois blocos por motivos de prompt caching:

- bloco_compartilhado: system + release + analise de temas + pool — identico
  em todas as chamadas do mesmo trimestre, candidato a cache_control.
- bloco_analista: pergunta especifica sobre UM analista/banco — varia por chamada.

Ver client.py para a aplicacao do cache.
"""
from __future__ import annotations

from .pool import CandidatoBanco
from .themes import AnaliseTemas


SYSTEM_PROMPT = (
    "Voce e um especialista em earnings calls de construtoras brasileiras "
    "listadas na B3 (MCMV e media-alta renda). Sua tarefa e prever as 2-3 "
    "perguntas que cada analista fara no proximo call, baseado no release de "
    "resultados, no historico individual do analista e nos temas dominantes "
    "do trimestre.\n\n"
    "PRINCIPIOS DE PREVISAO (calibrados via backtest):\n"
    "1. TEMA DOMINANTE: quando o release tem surpresa negativa forte (desvio de "
    "custo grande, provisao elevada, revisao de guidance), 60%+ das perguntas "
    "do call concentram-se nesse tema unico. Concentre suas previsoes nele.\n"
    "2. PREVISAO POR BANCO: bancos rotacionam analistas (mesmo banco, pessoa "
    "diferente). Quando indicado um analista alternativo, considere ambos os "
    "estilos possiveis.\n"
    "3. CURTO PRAZO > ESTRUTURAL: surpresas imediatas do release pesam mais "
    "que temas estruturais (reforma tributaria, IVA, longo prazo). Estes so "
    "aparecem se nao houver bomba de curto prazo.\n"
    "4. POOL CROSS-SETOR: analistas de outros bancos podem aparecer pela primeira "
    "vez. Use o estilo de perguntas do analista em outras empresas como base.\n"
    "5. TEMAS OBRIGATORIOS: numeros que divergem fortemente do guidance "
    "previo SAO QUASE CERTAMENTE perguntados. Atribua um deles a pelo menos "
    "metade dos analistas previstos.\n"
)


def montar_bloco_compartilhado(
    empresa: str,
    periodo: str,
    release_text: str,
    analise: AnaliseTemas,
    pool: list[CandidatoBanco],
    release_max_chars: int = 12000,
) -> str:
    """Bloco identico em todas as chamadas do trimestre: candidato a cache."""
    release_resumido = release_text[:release_max_chars]

    flags_txt = "\n".join(
        f"- [{f.severidade.upper()}] {f.rotulo}: \"{f.trecho[:300]}...\""
        for f in analise.flags
    ) or "(nenhum sinal de surpresa detectado automaticamente)"

    pool_txt = "\n".join(
        (
            f"- {c.banco}: {c.primario}"
            + (f" (alternativo: {', '.join(c.alternativos)})" if c.alternativos else "")
            + (" [SEM historico na empresa]" if not c.tem_historico_empresa else "")
        )
        for c in pool
    ) or "(pool vazio)"

    obrigatorios = (
        "\n".join(f"- {t}" for t in analise.temas_obrigatorios)
        or "(nenhum tema obrigatorio identificado)"
    )

    return (
        f"## Release de Resultados - {empresa.upper()} {periodo}\n"
        f"{release_resumido}\n\n"
        f"## Analise automatica de surpresas no release\n"
        f"Surpresa negativa forte detectada: "
        f"{'SIM' if analise.surpresa_negativa_forte else 'NAO'}\n"
        f"Tema dominante: {analise.tema_dominante or '(nao identificado)'}\n\n"
        f"### Flags detectados:\n{flags_txt}\n\n"
        f"### Temas obrigatorios (devem aparecer em pelo menos metade dos analistas):\n"
        f"{obrigatorios}\n\n"
        f"## Pool de analistas candidatos\n{pool_txt}\n"
    )


def montar_bloco_analista(
    empresa: str,
    periodo: str,
    analista: str,
    banco: str,
    historico_empresa: list[dict],
    historico_setor: list[dict],
    alternativos: list[str] | None = None,
    sem_historico_empresa: bool = False,
) -> str:
    """Bloco especifico por analista — varia em cada chamada."""
    hist_emp = ""
    for h in historico_empresa[-4:]:
        hist_emp += f"\n--- {h['periodo']} ---\n{h['texto'][:1200]}\n"
    hist_set = ""
    for h in historico_setor[:6]:
        hist_set += f"\n--- {h['empresa'].upper()} {h['periodo']} ---\n{h['texto'][:800]}\n"

    alt_txt = (
        f"\nALERTA: o banco {banco} pode mandar um analista alternativo "
        f"({', '.join(alternativos)}). Considere ambos os estilos possiveis.\n"
        if alternativos else ""
    )
    sem_hist_txt = (
        f"\nALERTA: {analista} ({banco}) NAO tem historico nesta empresa, "
        f"mas tem cobertura cross-setor. Use o estilo dele em outras "
        f"construtoras como base de previsao.\n"
        if sem_historico_empresa else ""
    )

    return (
        f"## Tarefa: prever perguntas de {analista} ({banco}) em {empresa.upper()} {periodo}\n"
        f"{alt_txt}{sem_hist_txt}\n"
        f"### Historico de {analista} em calls da {empresa.upper()}\n"
        f"{hist_emp or '(sem historico nesta empresa)'}\n\n"
        f"### Perguntas recentes de {analista} em OUTRAS construtoras\n"
        f"{hist_set or '(sem historico em outras construtoras)'}\n\n"
        f"### Instrucoes\n"
        f"Gere 2-3 perguntas que {analista} provavelmente fara. Para cada uma:\n"
        f"- Escreva a pergunta no estilo do analista (em portugues, profissional)\n"
        f"- Justificativa em 1-2 frases (por que e provavel)\n"
        f"- Confianca: Alta/Media/Baixa\n"
        f"- Se atribuida a um TEMA OBRIGATORIO, marque [OBRIGATORIO]\n\n"
        f"Formato:\n"
        f"### Pergunta 1\n"
        f"**Pergunta:** ...\n"
        f"**Justificativa:** ...\n"
        f"**Confianca:** ...\n"
    )

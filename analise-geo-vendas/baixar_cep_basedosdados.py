"""
Baixa a tabela CEP do dataset "Diretórios Brasileiros" da Base dos Dados.

A tabela está em: basedosdados.br_bd_diretorios_brasil.cep (BigQuery público)
- 905.210 linhas, 8 colunas, ~378 MB descomprimido
- Colunas: cep, logradouro, localidade, id_municipio, nome_municipio,
           sigla_uf, estabelecimentos, centroide

REQUISITOS:
  - pip install basedosdados (já instalado)
  - Conta Google (autenticação OAuth — abre o navegador na 1ª vez)
  - Um projeto Google Cloud (pode ser free tier) para billing_project_id
    Crie um em: https://console.cloud.google.com/projectselector2/home/dashboard
    O custo de query no BigQuery free tier é ZERO para os primeiros 1 TB/mês.

COMO USAR:
  python baixar_cep_basedosdados.py

  Na primeira execução, o script vai:
  1. Abrir o navegador para autenticação Google
  2. Pedir o ID do projeto Google Cloud (billing)
  3. Baixar a tabela inteira via BigQuery
  4. Salvar como data/cep_basedosdados.csv

ALTERNATIVA SEM GOOGLE CLOUD (read_sql com query limitada):
  Se não tiver projeto Google Cloud, use a opção B abaixo que
  baixa apenas as colunas necessárias (cep, localidade, id_municipio,
  nome_municipio, sigla_uf) — sem a coluna GEOGRAPHY que é pesada.
"""

import json
import sys
from pathlib import Path

import pandas as pd

OUTPUT_CSV = Path("data") / "cep_basedosdados.csv"
OUTPUT_JSON = Path("data") / "cep_bairros_basedosdados.json"


def opcao_a_read_table():
    """
    Opção A: Baixa a tabela inteira via bd.read_table.
    Requer billing_project_id (projeto Google Cloud).
    """
    import basedosdados as bd

    print("Baixando tabela completa via bd.read_table...")
    print("(Vai abrir o navegador para autenticação na 1ª vez)")

    df = bd.read_table(
        dataset_id="br_bd_diretorios_brasil",
        table_id="cep",
        billing_project_id=None,  # Vai pedir interativamente
    )

    print(f"Baixado: {len(df)} linhas, {len(df.columns)} colunas")
    print(f"Colunas: {list(df.columns)}")
    return df


def opcao_b_read_sql():
    """
    Opção B: Baixa apenas colunas úteis via SQL (sem GEOGRAPHY).
    Mais rápido e mais leve. Também requer billing_project_id.
    """
    import basedosdados as bd

    query = """
    SELECT
        cep,
        logradouro,
        localidade,
        id_municipio,
        nome_municipio,
        sigla_uf
    FROM `basedosdados.br_bd_diretorios_brasil.cep`
    """

    print("Baixando via SQL (sem coluna GEOGRAPHY)...")
    print("(Vai abrir o navegador para autenticação na 1ª vez)")

    df = bd.read_sql(query, billing_project_id=None)

    print(f"Baixado: {len(df)} linhas, {len(df.columns)} colunas")
    return df


def opcao_c_download_csv():
    """
    Opção C: Baixa e salva direto como CSV comprimido.
    """
    import basedosdados as bd

    print("Baixando e salvando como CSV...")

    bd.download(
        savepath=str(OUTPUT_CSV),
        dataset_id="br_bd_diretorios_brasil",
        table_id="cep",
        billing_project_id=None,
    )

    print(f"Salvo em {OUTPUT_CSV}")


def converter_para_json_bairros(df: pd.DataFrame):
    """
    Converte o DataFrame para o formato JSON esperado pelo app:
    { "12345678": {"bairro": "...", "cidade": "...", "uf": "..."} }

    A coluna 'localidade' da base dos dados representa o bairro na maioria
    dos casos (conforme documentação: "na maior parte dos casos, representa
    o bairro").
    """
    cache = {}
    for _, row in df.iterrows():
        cep = str(row["cep"]).strip()
        if len(cep) == 8:
            cache[cep] = {
                "bairro": str(row.get("localidade", "")).strip(),
                "cidade": str(row.get("nome_municipio", "")).strip(),
                "uf": str(row.get("sigla_uf", "")).strip(),
            }

    OUTPUT_JSON.write_text(
        json.dumps(cache, ensure_ascii=False, indent=None),
        encoding="utf-8",
    )
    com_bairro = sum(1 for v in cache.values() if v["bairro"])
    print(f"\nJSON salvo em {OUTPUT_JSON}")
    print(f"  Total: {len(cache)} CEPs ({com_bairro} com bairro/localidade)")
    return cache


def main():
    print("=" * 60)
    print("Baixar tabela CEP — Base dos Dados (BigQuery publico)")
    print("=" * 60)

    import basedosdados as bd

    BILLING_PROJECT = "gen-lang-client-0366404057"

    query = """
    SELECT
        cep,
        logradouro,
        localidade,
        id_municipio,
        nome_municipio,
        sigla_uf
    FROM `basedosdados.br_bd_diretorios_brasil.cep`
    """

    print("Baixando via BigQuery (sem coluna GEOGRAPHY)...")
    print("Se abrir o navegador, autorize com sua conta Google.")

    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT)

    print(f"Baixado: {len(df)} linhas, {len(df.columns)} colunas")

    # Salva CSV
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"CSV salvo em {OUTPUT_CSV}")

    # Converte para JSON no formato do app
    converter_para_json_bairros(df)


if __name__ == "__main__":
    main()

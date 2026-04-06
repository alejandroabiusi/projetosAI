"""Módulo Consulta — Interface conversacional (placeholder)."""
import streamlit as st
import pandas as pd


def render(df):
    st.header("Consulta Inteligente")

    st.info(
        "**Em breve:** Interface conversacional com IA para perguntas em linguagem natural.\n\n"
        "Exemplos do que poderá perguntar:\n"
        '- "Me dê 10 exemplos de produtos em São Paulo com plantas acima de 40m²"\n'
        '- "Quais empresas lançaram em Guarulhos nos últimos 3 meses?"\n'
        '- "Produtos com piscina e churrasqueira abaixo de R$300k"\n'
        '- "Compare a Tenda com a MRV em número de amenidades"\n\n'
        "A interface usará um cache de perguntas frequentes para respostas instantâneas, "
        "e a API Claude para perguntas novas (~R$0,03 por pergunta)."
    )

    st.divider()

    # Enquanto isso: consulta SQL simplificada
    st.subheader("Consulta rápida")

    col1, col2 = st.columns(2)
    with col1:
        campo = st.selectbox("Buscar por", ["nome", "empresa", "cidade", "bairro", "endereco"])
    with col2:
        termo = st.text_input("Contém...")

    if termo:
        resultado = df[df[campo].astype(str).str.contains(termo, case=False, na=False)]
        st.caption(f"{len(resultado)} resultados")

        if not resultado.empty:
            cols_exibir = ["empresa", "nome", "cidade", "estado", "fase",
                          "dormitorios_descricao", "area_min_m2", "preco_a_partir", "url_fonte"]
            cols_exibir = [c for c in cols_exibir if c in resultado.columns]
            st.dataframe(resultado[cols_exibir].head(50), use_container_width=True, hide_index=True)

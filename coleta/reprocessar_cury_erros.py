import sys
import time

sys.path.insert(0, ".")
from scrapers.cury_empreendimentos import criar_driver, carregar_pagina, extrair_dados_empreendimento, extrair_e_baixar_imagens
from data.database import inserir_empreendimento, empreendimento_existe, contar_empreendimentos

URLS = [
    "https://cury.net/imovel/RJ/centro/vargas-1140",
    "https://cury.net/imovel/RJ/zona-portuaria/luzes-do-rio",
    "https://cury.net/imovel/SP/santo-andre/tutto-jardim",
    "https://cury.net/imovel/SP/zona-leste/merito-penha",
    "https://cury.net/imovel/SP/zona-norte/marco-freguesia",
    "https://cury.net/imovel/SP/zona-oeste/cidade-lapa-agua-branca",
    "https://cury.net/imovel/SP/zona-oeste/cidade-villa-lobos-maestro",
  ]

driver = criar_driver()
ok = 0
erros = []

try:
    for url in URLS:
        slug = url.split("/")[-1]
        print(f"Processando: {slug}")
        try:
            soup = carregar_pagina(driver, url, espera=12)
            dados = extrair_dados_empreendimento(soup, url)
            nome = dados.get("nome", "")
            if not nome:
                print("  ERRO: nome nao encontrado")
                erros.append(url)
                continue
            if not empreendimento_existe("Cury", nome):
                inserir_empreendimento(dados)
                extrair_e_baixar_imagens(soup, nome)
                print(f"  NOVO: {nome}")
                ok += 1
            else:
                print(f"  Ja existe: {nome}")
                ok += 1
            time.sleep(5)
        except Exception as e:
            print(f"  ERRO: {e}")
            erros.append(url)
finally:
    driver.quit()

print(f"\nConcluido: {ok} ok, {len(erros)} erros")
print(f"Total Cury no banco: {contar_empreendimentos('Cury')}")
if erros:
    print("Ainda com erro:")
    for u in erros:
        print(f"  {u}")
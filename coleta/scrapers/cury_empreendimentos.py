"""
Scraper de empreendimentos da Cury Construtora.
===============================================
Usa Selenium para contornar protecao Cloudflare.
Coleta dados de todos os empreendimentos a partir da home (cury.net).
Grava no banco SQLite compartilhado (data/empreendimentos.db).

Uso:
    python scrapers/cury_empreendimentos.py
    python scrapers/cury_empreendimentos.py --atualizar
    python scrapers/cury_empreendimentos.py --limite 3
"""

import os
import sys
import re
import time
import json
import argparse
import hashlib
import requests
from datetime import datetime
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# Ajusta path para importar modulos do projeto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.database import (
    inserir_empreendimento,
    atualizar_empreendimento,
    empreendimento_existe,
    detectar_atributos_binarios,
    contar_empreendimentos,
)
from config.settings import DOWNLOADS_DIR, LOGS_DIR

EMPRESA = "Cury"
BASE_URL = "https://cury.net"
PROGRESSO_FILE = os.path.join(LOGS_DIR, "cury_empreendimentos_progresso.json")
IMAGENS_DIR = os.path.join(DOWNLOADS_DIR, "cury", "imagens")


# ============================================================
# SELENIUM HELPERS
# ============================================================

def criar_driver():
    """Cria e retorna instancia do Chrome via Selenium."""
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    driver.set_page_load_timeout(60)
    return driver


def carregar_pagina(driver, url, scroll=True, espera=5):
    """Carrega pagina e opcionalmente faz scroll para ativar lazy loading."""
    driver.get(url)
    time.sleep(espera)
    if scroll:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
    return BeautifulSoup(driver.page_source, "html.parser")


# ============================================================
# COLETA DE LINKS
# ============================================================

def coletar_links_empreendimentos(driver):
    """
    Coleta URLs de todos os empreendimentos da Cury.
    Usa a URL de busca com parametro state para SP e RJ.
    Itera por todas as paginas de resultados (div.pages).
    """
    all_links = set()
    
    estados = [
        ("SP", "https://cury.net/busca?state=SP&regions=&bedrooms="),
        ("RJ", "https://cury.net/busca?state=RJ&regions=&bedrooms="),
    ]
    
    for uf, url_busca in estados:
        print(f"  Coletando links {uf}...")
        pagina = 1
        total_uf = 0
        
        while True:
            url_pagina = f"{url_busca}&page={pagina}" if pagina > 1 else url_busca
            soup = carregar_pagina(driver, url_pagina, espera=5)
            
            # Coletar links desta pagina
            count_antes = len(all_links)
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/imovel/" in href and href.count("/") >= 4:
                    if href.startswith("/"):
                        href = BASE_URL + href
                    all_links.add(href)
            
            novos_pagina = len(all_links) - count_antes
            total_uf += novos_pagina
            print(f"    {uf} pagina {pagina}: {novos_pagina} empreendimentos")
            
            # Verificar se existe proxima pagina
            div_pages = soup.find("div", class_="pages")
            if not div_pages:
                break
            
            # Procurar link para proxima pagina
            tem_proxima = False
            for a in div_pages.find_all("a", href=True):
                # Procurar link com numero da proxima pagina ou seta ">"
                href = a.get("href", "")
                texto = a.get_text(strip=True)
                if texto == str(pagina + 1) or "next" in href.lower() or texto in [">", "»"]:
                    tem_proxima = True
                    break
            
            if not tem_proxima or novos_pagina == 0:
                break
            
            pagina += 1
            time.sleep(2)
        
        print(f"    {uf} total: {total_uf} empreendimentos em {pagina} pagina(s)")
    
    return sorted(all_links)
    
    return sorted(all_links)


# ============================================================
# PARSING DE EMPREENDIMENTO
# ============================================================

def extrair_dados_empreendimento(soup, url):
    """Extrai todos os dados estruturados de uma pagina de empreendimento."""
    dados = {
        "empresa": EMPRESA,
        "url_fonte": url,
    }
    
    # Nome do empreendimento
    nome_tag = soup.find(class_="name-imovel")
    if nome_tag:
        dados["nome"] = nome_tag.get_text(strip=True)
    
    # Slug (extraido da URL)
    partes_url = url.rstrip("/").split("/")
    dados["slug"] = partes_url[-1] if partes_url else ""
    
    # Estado e regiao (extraidos da URL: /imovel/SP/zona-leste/slug)
    if len(partes_url) >= 5:
        dados["estado"] = partes_url[-3].upper()  # SP ou RJ
        regiao = partes_url[-2].replace("-", " ").title()  # Zona Leste
    else:
        dados["estado"] = ""
        regiao = ""
    
    # Bairro
    region_tag = soup.find(class_="region")
    if region_tag:
        dados["bairro"] = region_tag.get_text(strip=True)
    
    # Cidade (inferida do estado)
    estado = dados.get("estado", "")
    if estado == "SP":
        dados["cidade"] = "Sao Paulo"
    elif estado == "RJ":
        dados["cidade"] = "Rio de Janeiro"
    
    # Fase (Lancamento, Em Obras, etc.)
    fase_tag = soup.find(class_="tag-house")
    if fase_tag:
        fase_text = fase_tag.get_text(strip=True)
        dados["fase"] = fase_text
    
    # Dormitorios
    about_tag = soup.find(class_="about-imovel")
    if about_tag:
        p_tag = about_tag.find("p")
        if p_tag:
            dorm_text = p_tag.get_text(strip=True)
            # Limpar: "1 dorm. , 2 dorms." -> "1 e 2 dorms."
            dorm_text = re.sub(r'\s*,\s*', ', ', dorm_text)
            dorm_text = re.sub(r'\s+', ' ', dorm_text)
            dados["dormitorios_descricao"] = dorm_text
    
    # Endereco do empreendimento
    # Estrutura: div com "Empreendimento" seguido de h4.color-white com endereco
    enderecos_h4 = soup.find_all("h4", class_="color-white")
    for h4 in enderecos_h4:
        texto = h4.get_text(strip=True)
        # O endereco do empreendimento geralmente e o que NAO tem numero de stand
        # Procurar pelo que tem S/N ou pelo segundo endereco
        parent_section = h4.find_parent("div")
        if parent_section:
            section_text = parent_section.get_text()
            if "Empreendimento" in section_text and ("S/N" in texto or "s/n" in texto):
                dados["endereco"] = texto
                break
            elif "Stand" not in section_text and texto:
                dados["endereco"] = texto
    
    # Itens de lazer (via box-option)
    itens_lazer = []
    seen_lazer = set()
    for box in soup.find_all(class_="box-option"):
        p = box.find("p")
        if p:
            item = p.get_text(strip=True)
            # Limpar sufixos
            item = re.sub(r'\s*-\s*(Perspectiva|Foto|Ilustra).*$', '', item, flags=re.IGNORECASE)
            item_lower = item.lower()
            if item and item_lower not in seen_lazer and item_lower != "ver todos":
                itens_lazer.append(item)
                seen_lazer.add(item_lower)
    
    # Tambem pegar do detail-hidden (itens expandidos)
    for div in soup.find_all(class_="detail-hidden"):
        p = div.find("p")
        if p:
            item = p.get_text(strip=True)
            item_lower = item.lower()
            if item and item_lower not in seen_lazer and item_lower != "ver todos":
                itens_lazer.append(item)
                seen_lazer.add(item_lower)
    
    if itens_lazer:
        dados["itens_lazer"] = " | ".join(itens_lazer)
    
    # Registro de incorporacao
    for tag in soup.find_all(string=lambda s: s and "INCORPORA" in str(s).upper()):
        parent = tag.find_parent("p")
        if parent:
            texto_incorp = parent.get_text(strip=True)
            dados["registro_incorporacao"] = texto_incorp
            
            # Extrair total de unidades do registro
            # Padrao: "COM 312 UNIDADES HIS-1 ... 768 UNIDADES HIS-2 ... 168 UNIDADES R2V"
            unidades_match = re.findall(r'(\d+)\s+UNIDADES?\s+(?:HIS|HMP|R2V|HABITACION)', texto_incorp, re.IGNORECASE)
            if unidades_match:
                total = sum(int(n) for n in unidades_match)
                dados["total_unidades"] = total
            
            # Detectar programas do registro
            if "HIS-1" in texto_incorp or "HIS 1" in texto_incorp:
                dados["prog_his1"] = 1
            if "HIS-2" in texto_incorp or "HIS 2" in texto_incorp:
                dados["prog_his2"] = 1
            if "R2V" in texto_incorp:
                # R2V nao e programa, mas vale registrar
                pass
            if "HMP" in texto_incorp:
                dados["prog_hmp"] = 1
            break
    
    # Texto completo para deteccao de atributos binarios
    texto_completo = soup.get_text(separator=" ", strip=True)
    atributos = detectar_atributos_binarios(texto_completo)
    dados.update(atributos)
    
    return dados


# ============================================================
# DOWNLOAD DE IMAGENS
# ============================================================

def _slug_seguro(nome):
    """Converte nome para slug seguro para filesystem."""
    import unicodedata
    nome = unicodedata.normalize('NFKD', nome).encode('ascii', 'ignore').decode('ascii')
    nome = re.sub(r'[^\w\s-]', '', nome.lower())
    nome = re.sub(r'[\s_]+', '-', nome)
    return nome.strip('-')


def _categorizar_imagem(src, alt):
    """Categoriza imagem baseado na URL e alt text."""
    texto = (alt + " " + src).lower()
    if "plant" in texto or "planta" in texto:
        return "plantas"
    elif "fachada" in texto:
        return "fachada"
    elif "decorado" in texto:
        return "decorado"
    elif "obra" in texto:
        return "obra"
    elif "area" in texto or "comum" in texto or "lazer" in texto:
        return "areas_comuns"
    else:
        return "geral"


def extrair_e_baixar_imagens(soup, nome_empreendimento):
    """Baixa imagens do empreendimento organizadas por categoria."""
    slug = _slug_seguro(nome_empreendimento)
    dominio_cury = "cury.net/storage/"
    
    imagens_baixadas = 0
    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        alt = img.get("alt", "")
        
        if not src or dominio_cury not in src:
            continue
        if len(src) < 30:
            continue
        # Pular icones pequenos
        if "/icons/" in src:
            continue
        
        categoria = _categorizar_imagem(src, alt)
        pasta = os.path.join(IMAGENS_DIR, slug, categoria)
        os.makedirs(pasta, exist_ok=True)
        
        # Nome do arquivo baseado no hash da URL
        ext = os.path.splitext(src.split("?")[0])[1] or ".jpg"
        nome_arquivo = hashlib.md5(src.encode()).hexdigest()[:12] + ext
        caminho = os.path.join(pasta, nome_arquivo)
        
        if os.path.exists(caminho):
            continue
        
        try:
            resp = requests.get(src, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 5000:
                with open(caminho, "wb") as f:
                    f.write(resp.content)
                imagens_baixadas += 1
        except Exception:
            pass
    
    return imagens_baixadas


# ============================================================
# PROGRESSO
# ============================================================

def carregar_progresso():
    """Carrega progresso de execucao anterior."""
    if os.path.exists(PROGRESSO_FILE):
        with open(PROGRESSO_FILE, "r") as f:
            return json.load(f)
    return {"processados": [], "erros": []}


def salvar_progresso(progresso):
    """Salva progresso da execucao."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(PROGRESSO_FILE, "w") as f:
        json.dump(progresso, f, indent=2)


# ============================================================
# MAIN
# ============================================================

def _input_com_timeout(prompt, timeout=60, default="10"):
    """Input com timeout. Retorna default se o usuario nao responder."""
    import sys
    import threading
    
    resultado = [default]
    
    def ler_input():
        try:
            resultado[0] = input(prompt).strip()
        except EOFError:
            pass
    
    thread = threading.Thread(target=ler_input, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    
    if thread.is_alive():
        print(f"\n  [Timeout {timeout}s] Usando default: {default}")
    
    return resultado[0]


def main():
    parser = argparse.ArgumentParser(description="Scraper de empreendimentos Cury")
    parser.add_argument("--atualizar", action="store_true", help="Atualizar empreendimentos existentes")
    parser.add_argument("--limite", type=int, default=0, help="Limitar numero de empreendimentos (0=todos)")
    parser.add_argument("--reset-progresso", action="store_true", help="Resetar progresso e reprocessar tudo")
    parser.add_argument("--lote", type=int, default=None, help="Tamanho fixo do lote (sem perguntar)")
    parser.add_argument("--reprocessar-erros", action="store_true", help="Reprocessar apenas URLs que deram erro")
    args = parser.parse_args()
    
    if args.reset_progresso:
        if os.path.exists(PROGRESSO_FILE):
            os.remove(PROGRESSO_FILE)
            print("Progresso resetado.")
        return

    progresso = carregar_progresso()

    # Modo reprocessar erros: move erros para pendentes
    if args.reprocessar_erros:
        erros = progresso.get("erros", [])
        if not erros:
            print("Nenhum erro registrado no progresso.")
            return
        progresso["processados"] = [u for u in progresso["processados"] if u not in erros]
        progresso["erros"] = []
        salvar_progresso(progresso)
        print(f"{len(erros)} URLs com erro movidas para reprocessamento.")

    print(f"\n{'='*60}")
    print(f"  SCRAPER CURY CONSTRUTORA")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Empreendimentos ja no banco: {contar_empreendimentos(EMPRESA)}")
    print(f"{'='*60}")
    
    driver = criar_driver()
    
    try:
        # FASE 1: Coletar links
        print("\n[FASE 1] Coletando links de empreendimentos...")
        links = coletar_links_empreendimentos(driver)
        print(f"\n  Total de links coletados: {len(links)}")
        
        if args.limite > 0:
            links = links[:args.limite]
            print(f"  Limitado a {args.limite} empreendimentos")
        
        # Filtrar links ja processados
        links_pendentes = [url for url in links if url not in progresso["processados"] or args.atualizar]
        print(f"  Pendentes de processamento: {len(links_pendentes)}")
        
        if not links_pendentes:
            print("\n  Todos os empreendimentos ja foram processados.")
            print("  Use --reset-progresso para reprocessar tudo.")
            return
        
        # FASE 2: Processar em lotes
        total_pendentes = len(links_pendentes)
        indice_global = 0
        total_novos = 0
        total_atualizados = 0
        total_erros = 0
        REINICIAR_CHROME_A_CADA = 40  # reinicia o Chrome a cada N paginas

        while indice_global < total_pendentes:
            restantes = total_pendentes - indice_global
            
            # Determinar tamanho do lote
            if args.lote:
                tamanho_lote = args.lote
            else:
                print(f"\n  Restam {restantes} empreendimentos.")
                resposta = _input_com_timeout(
                    f"  Quantos processar neste lote? [Enter ou 60s = 10, 0 = parar]: ",
                    timeout=60,
                    default="10"
                )
                if resposta == "0":
                    print("  Parando por solicitacao do usuario.")
                    break
                try:
                    tamanho_lote = int(resposta) if resposta else 10
                except ValueError:
                    tamanho_lote = 10
            
            fim = min(indice_global + tamanho_lote, total_pendentes)
            lote = links_pendentes[indice_global:fim]
            
            print(f"\n[LOTE] Processando {indice_global + 1} a {fim} de {total_pendentes}")
            print(f"{'='*60}")
            
            novos = 0
            atualizados = 0
            erros = 0
            
            for i, url in enumerate(lote, 1):
                slug = url.rstrip("/").split("/")[-1]
                num_global = indice_global + i

                # Reinicia o Chrome a cada N empreendimentos para liberar memoria
                if (num_global - 1) > 0 and (num_global - 1) % REINICIAR_CHROME_A_CADA == 0:
                    print(f"\n  [MANUTENCAO] Reiniciando Chrome apos {REINICIAR_CHROME_A_CADA} paginas...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(3)
                    driver = criar_driver()
                    print("  Chrome reiniciado.")

                try:
                    print(f"\n  [{num_global}/{total_pendentes}] {slug}")
                    soup = carregar_pagina(driver, url, espera=4)
                    
                    # Verificar se a pagina carregou
                    if "404" in driver.title or "não encontrad" in driver.page_source[:3000].lower():
                        print(f"    ERRO: Pagina 404")
                        erros += 1
                        progresso["erros"].append(url)
                        continue
                    
                    # Extrair dados
                    dados = extrair_dados_empreendimento(soup, url)
                    nome = dados.get("nome", "")
                    
                    if not nome:
                        print(f"    ERRO: Nome nao encontrado")
                        erros += 1
                        progresso["erros"].append(url)
                        continue
                    
                    # Verificar existencia no banco
                    existe = empreendimento_existe(EMPRESA, nome)
                    
                    if existe and not args.atualizar:
                        print(f"    {nome} - ja no banco (pulando)")
                        progresso["processados"].append(url)
                        salvar_progresso(progresso)
                        continue
                    
                    # Baixar imagens
                    imgs = extrair_e_baixar_imagens(soup, nome)
                    
                    # Inserir ou atualizar
                    if existe:
                        atualizar_empreendimento(EMPRESA, nome, dados)
                        atualizados += 1
                        print(f"    {nome} - ATUALIZADO | {dados.get('fase', '?')} | {dados.get('bairro', '?')} | lazer: {len(dados.get('itens_lazer', '').split('|'))} itens | {imgs} imgs")
                    else:
                        inserir_empreendimento(dados)
                        novos += 1
                        print(f"    {nome} - NOVO | {dados.get('fase', '?')} | {dados.get('bairro', '?')} | lazer: {len(dados.get('itens_lazer', '').split('|'))} itens | {imgs} imgs")
                        if dados.get("total_unidades"):
                            print(f"    Unidades: {dados['total_unidades']}")
                    
                    progresso["processados"].append(url)
                    salvar_progresso(progresso)
                    
                    # Pausa entre requests
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"    ERRO: {e}")
                    erros += 1
                    progresso["erros"].append(url)
                    salvar_progresso(progresso)
            
            # Resumo do lote
            total_novos += novos
            total_atualizados += atualizados
            total_erros += erros
            indice_global = fim
            
            print(f"\n  Lote concluido: +{novos} novos, {atualizados} atualizados, {erros} erros")
            print(f"  Total no banco (Cury): {contar_empreendimentos(EMPRESA)}")
        
        # REPROCESSAMENTO DE ERROS
        erros_pendentes = [u for u in progresso.get("erros", []) if u not in progresso["processados"]]
        if erros_pendentes:
            print(f"\n{'='*60}")
            print(f"  REPROCESSANDO {len(erros_pendentes)} URLs COM ERRO")
            print(f"{'='*60}")
            progresso["erros"] = []
            salvar_progresso(progresso)

            for i, url in enumerate(erros_pendentes, 1):
                slug = url.rstrip("/").split("/")[-1]

                if i > 1 and (i - 1) % REINICIAR_CHROME_A_CADA == 0:
                    print(f"\n  [MANUTENCAO] Reiniciando Chrome...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(3)
                    driver = criar_driver()

                try:
                    print(f"\n  [ERRO {i}/{len(erros_pendentes)}] {slug}")
                    soup = carregar_pagina(driver, url, espera=4)

                    if "404" in driver.title or "não encontrad" in driver.page_source[:3000].lower():
                        print(f"    ERRO: Pagina 404")
                        progresso["erros"].append(url)
                        salvar_progresso(progresso)
                        continue

                    dados = extrair_dados_empreendimento(soup, url)
                    nome = dados.get("nome", "")

                    if not nome:
                        print(f"    ERRO: Nome nao encontrado")
                        progresso["erros"].append(url)
                        salvar_progresso(progresso)
                        continue

                    existe = empreendimento_existe(EMPRESA, nome)
                    imgs = extrair_e_baixar_imagens(soup, nome)

                    if existe:
                        atualizar_empreendimento(EMPRESA, nome, dados)
                        print(f"    {nome} - ATUALIZADO")
                    else:
                        inserir_empreendimento(dados)
                        total_novos += 1
                        print(f"    {nome} - NOVO")

                    progresso["processados"].append(url)
                    salvar_progresso(progresso)
                    time.sleep(2)

                except Exception as e:
                    print(f"    ERRO: {e}")
                    progresso["erros"].append(url)
                    salvar_progresso(progresso)

        # RESUMO FINAL
        print(f"\n{'='*60}")
        print(f"  RESUMO FINAL")
        print(f"  Novos: {total_novos}")
        print(f"  Atualizados: {total_atualizados}")
        print(f"  Erros: {total_erros}")
        print(f"  Total no banco (Cury): {contar_empreendimentos(EMPRESA)}")
        print(f"  Total geral no banco: {contar_empreendimentos()}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\nERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        print("Chrome fechado.")


if __name__ == "__main__":
    main()

"""
Mapeamento automatico de sites concorrentes.
=============================================
Recebe uma lista de URLs de incorporadoras e faz o mapeamento tecnico:
  - Testa acesso via requests (server-side) vs necessidade de Selenium
  - Extrai classes CSS, tags semanticas, links de empreendimentos
  - Salva HTML e texto extraido para calibracao dos parsers

Uso:
    python scrapers/mapear_concorrentes.py
    python scrapers/mapear_concorrentes.py --selenium   (usa Selenium para todas)
    python scrapers/mapear_concorrentes.py --empresa "Magik JC"

Gera outputs em: docs/mapeamento_auto/
"""

import os
import sys
import re
import time
import json
import argparse
from datetime import datetime
from collections import Counter

import requests
from bs4 import BeautifulSoup

# Tenta importar Selenium (opcional)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_DISPONIVEL = True
except ImportError:
    SELENIUM_DISPONIVEL = False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "mapeamento_auto")
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ============================================================
# CONSTRUTORAS A MAPEAR
# ============================================================
# Cada entrada: (nome, url_listagem, url_exemplo_empreendimento)
# url_exemplo_empreendimento pode ser None (sera descoberta automaticamente)

CONSTRUTORAS = [
    {
        "nome": "Magik JC",
        "urls_listagem": [
            "https://www.magikjc.com.br/empreendimentos",
            "https://www.magikjc.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Kazzas",
        "urls_listagem": [
            "https://www.kazzas.com.br/empreendimentos",
            "https://www.kazzas.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Vibra",
        "urls_listagem": [
            "https://www.vibra.com.br/empreendimentos",
            "https://www.vibra.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Econ",
        "urls_listagem": [
            "https://www.econ.com.br/empreendimentos",
            "https://www.econ.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Stanza",
        "urls_listagem": [
            "https://www.stanza.com.br/empreendimentos",
            "https://www.stanza.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Pacaembu",
        "urls_listagem": [
            "https://www.pacaembuconstrutora.com.br/empreendimentos",
            "https://www.pacaembuconstrutora.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Benx",
        "urls_listagem": [
            "https://www.benx.com.br/empreendimentos",
            "https://www.benx.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Conx",
        "urls_listagem": [
            "https://www.conx.com.br/empreendimentos",
            "https://www.conx.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Mundo Apto",
        "urls_listagem": [
            "https://www.mundoapto.com.br/empreendimentos",
            "https://www.mundoapto.com.br/",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/empreendimento",
    },
    {
        "nome": "Direcional",
        "urls_listagem": [
            "https://www.direcional.com.br/empreendimentos",
            "https://www.direcional.com.br/",
        ],
        "url_exemplo": "https://www.direcional.com.br/empreendimentos/conquista-clube-sacoma/",
        "padrao_link_empreendimento": r"/empreendimentos/",
    },
    {
        "nome": "Vivaz",
        "urls_listagem": [
            "https://www.meuvivaz.com.br/",
            "https://www.meuvivaz.com.br/apartamentos/sao-paulo",
        ],
        "url_exemplo": None,
        "padrao_link_empreendimento": r"/apartamento",
    },
]


# ============================================================
# FUNCOES DE MAPEAMENTO
# ============================================================

def tentar_requests(url):
    """Tenta acessar URL via requests. Retorna (status_code, html, erro)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        return resp.status_code, resp.text, None
    except Exception as e:
        return None, None, str(e)


def tentar_selenium(url, driver=None):
    """Tenta acessar URL via Selenium. Retorna (html, erro)."""
    if not SELENIUM_DISPONIVEL:
        return None, "Selenium nao instalado"
    
    fechar = False
    if driver is None:
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        driver = webdriver.Chrome(options=options)
        fechar = True
    
    try:
        driver.get(url)
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        html = driver.page_source
        return html, None
    except Exception as e:
        return None, str(e)
    finally:
        if fechar:
            driver.quit()


def analisar_html(html, url_base):
    """Analisa HTML e retorna dict com informacoes estruturais."""
    soup = BeautifulSoup(html, "html.parser")
    
    resultado = {
        "tamanho_html": len(html),
        "titulo": soup.title.string.strip() if soup.title and soup.title.string else "",
        "meta_description": "",
        "total_links": 0,
        "links_internos": [],
        "links_empreendimentos": [],
        "classes_frequentes": {},
        "tags_semanticas": {},
        "tem_conteudo": False,
        "sinais_spa": False,
        "frameworks_detectados": [],
    }
    
    # Meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta:
        resultado["meta_description"] = meta.get("content", "")[:200]
    
    # Detectar SPA
    body_text = soup.body.get_text(strip=True) if soup.body else ""
    if len(body_text) < 200:
        resultado["sinais_spa"] = True
    if "enable JavaScript" in html or "noscript" in html[:5000]:
        resultado["sinais_spa"] = True
    
    # Frameworks
    if "react" in html.lower()[:10000] or "__NEXT_DATA__" in html:
        resultado["frameworks_detectados"].append("React/Next.js")
    if "angular" in html.lower()[:10000] or "ng-app" in html:
        resultado["frameworks_detectados"].append("Angular")
    if "vue" in html.lower()[:10000] or "__vue__" in html:
        resultado["frameworks_detectados"].append("Vue.js")
    if "nuxt" in html.lower()[:10000]:
        resultado["frameworks_detectados"].append("Nuxt.js")
    
    # Links
    dominio_base = re.search(r"https?://([^/]+)", url_base)
    dominio = dominio_base.group(1) if dominio_base else ""
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        resultado["total_links"] += 1
        
        # Links internos
        if href.startswith("/") or dominio in href:
            url_completa = href if href.startswith("http") else url_base.rstrip("/") + href
            texto = a.get_text(strip=True)[:60]
            resultado["links_internos"].append({"url": url_completa, "texto": texto})
    
    # Classes CSS mais frequentes
    classes = []
    for tag in soup.find_all(True, class_=True):
        for cls in tag.get("class", []):
            classes.append(cls)
    resultado["classes_frequentes"] = dict(Counter(classes).most_common(30))
    
    # Tags semanticas
    for tag_name in ["h1", "h2", "h3", "h4", "article", "section", "main", "nav", "header", "footer"]:
        tags = soup.find_all(tag_name)
        if tags:
            textos = [t.get_text(strip=True)[:80] for t in tags[:5]]
            resultado["tags_semanticas"][tag_name] = textos
    
    # Tem conteudo real?
    resultado["tem_conteudo"] = len(body_text) > 500
    
    return resultado


def identificar_links_empreendimentos(analise, padrao):
    """Filtra links que parecem ser de empreendimentos."""
    empreendimentos = []
    seen = set()
    
    for link in analise["links_internos"]:
        url = link["url"]
        if re.search(padrao, url, re.IGNORECASE) and url not in seen:
            # Evitar links de filtro, paginacao, etc
            if "page=" not in url and "filter=" not in url and "#" not in url:
                empreendimentos.append(link)
                seen.add(url)
    
    return empreendimentos


def mapear_construtora(construtora, usar_selenium=False, driver=None):
    """Mapeia uma construtora completa e retorna relatorio."""
    nome = construtora["nome"]
    print(f"\n{'='*60}")
    print(f"  Mapeando: {nome}")
    print(f"{'='*60}")
    
    relatorio = {
        "nome": nome,
        "data_mapeamento": datetime.now().isoformat(),
        "urls_testadas": [],
        "acesso_requests": False,
        "acesso_selenium": False,
        "precisa_selenium": False,
        "sinais_spa": False,
        "links_empreendimentos": [],
        "analise_listagem": None,
        "analise_detalhe": None,
        "recomendacao": "",
    }
    
    # Testar cada URL de listagem
    html_listagem = None
    url_listagem_ok = None
    
    for url in construtora["urls_listagem"]:
        print(f"\n  Testando (requests): {url}")
        status, html, erro = tentar_requests(url)
        
        resultado_url = {"url": url, "metodo": "requests", "status": status, "erro": erro}
        
        if status == 200 and html:
            analise = analisar_html(html, url)
            resultado_url["tamanho"] = analise["tamanho_html"]
            resultado_url["tem_conteudo"] = analise["tem_conteudo"]
            resultado_url["sinais_spa"] = analise["sinais_spa"]
            
            print(f"    Status: {status} | HTML: {analise['tamanho_html']} chars | Conteudo: {'SIM' if analise['tem_conteudo'] else 'NAO'} | SPA: {'SIM' if analise['sinais_spa'] else 'NAO'}")
            
            if analise["tem_conteudo"] and not analise["sinais_spa"]:
                relatorio["acesso_requests"] = True
                html_listagem = html
                url_listagem_ok = url
                
                # Salvar HTML
                slug = re.sub(r'[^a-z0-9]+', '_', nome.lower())
                html_path = os.path.join(OUTPUT_DIR, f"{slug}_listagem.html")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html)
                
                # Salvar texto
                texto_path = os.path.join(OUTPUT_DIR, f"{slug}_listagem_texto.txt")
                soup = BeautifulSoup(html, "html.parser")
                with open(texto_path, "w", encoding="utf-8") as f:
                    f.write(soup.get_text(separator="\n", strip=True))
                
                # Salvar analise
                analise_path = os.path.join(OUTPUT_DIR, f"{slug}_listagem_analise.json")
                with open(analise_path, "w", encoding="utf-8") as f:
                    json.dump(analise, f, ensure_ascii=False, indent=2)
                
                relatorio["analise_listagem"] = analise
                break
            elif analise["sinais_spa"]:
                relatorio["sinais_spa"] = True
        elif status == 403:
            print(f"    Status: 403 (Cloudflare/WAF bloqueou)")
            resultado_url["bloqueado"] = True
        else:
            print(f"    Status: {status} | Erro: {erro}")
        
        relatorio["urls_testadas"].append(resultado_url)
    
    # Se requests falhou, tentar Selenium
    if not relatorio["acesso_requests"] and (usar_selenium or SELENIUM_DISPONIVEL):
        for url in construtora["urls_listagem"]:
            print(f"\n  Testando (Selenium): {url}")
            html, erro = tentar_selenium(url, driver)
            
            if html and len(html) > 1000:
                analise = analisar_html(html, url)
                print(f"    HTML: {analise['tamanho_html']} chars | Conteudo: {'SIM' if analise['tem_conteudo'] else 'NAO'}")
                
                if analise["tem_conteudo"]:
                    relatorio["acesso_selenium"] = True
                    relatorio["precisa_selenium"] = True
                    html_listagem = html
                    url_listagem_ok = url
                    
                    slug = re.sub(r'[^a-z0-9]+', '_', nome.lower())
                    html_path = os.path.join(OUTPUT_DIR, f"{slug}_listagem_selenium.html")
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(html)
                    
                    relatorio["analise_listagem"] = analise
                    break
            else:
                print(f"    Falhou: {erro}")
    
    # Identificar links de empreendimentos
    if html_listagem:
        analise = relatorio["analise_listagem"]
        links_emp = identificar_links_empreendimentos(analise, construtora["padrao_link_empreendimento"])
        relatorio["links_empreendimentos"] = links_emp[:20]  # Top 20
        print(f"\n  Links de empreendimentos encontrados: {len(links_emp)}")
        for link in links_emp[:5]:
            print(f"    {link['texto'][:40]:40s} -> {link['url'][:80]}")
    
    # Tentar acessar pagina de detalhe de um empreendimento
    url_detalhe = construtora.get("url_exemplo")
    if not url_detalhe and relatorio["links_empreendimentos"]:
        url_detalhe = relatorio["links_empreendimentos"][0]["url"]
    
    if url_detalhe:
        print(f"\n  Testando detalhe: {url_detalhe}")
        
        if relatorio["precisa_selenium"]:
            html_det, erro = tentar_selenium(url_detalhe, driver)
        else:
            status, html_det, erro = tentar_requests(url_detalhe)
        
        if html_det and len(html_det) > 1000:
            analise_det = analisar_html(html_det, url_detalhe)
            relatorio["analise_detalhe"] = analise_det
            
            slug = re.sub(r'[^a-z0-9]+', '_', nome.lower())
            html_path = os.path.join(OUTPUT_DIR, f"{slug}_detalhe.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_det)
            
            texto_path = os.path.join(OUTPUT_DIR, f"{slug}_detalhe_texto.txt")
            soup = BeautifulSoup(html_det, "html.parser")
            with open(texto_path, "w", encoding="utf-8") as f:
                f.write(soup.get_text(separator="\n", strip=True))
            
            print(f"    Detalhe OK: {analise_det['tamanho_html']} chars, titulo: {analise_det['titulo'][:60]}")
    
    # Recomendacao
    if relatorio["acesso_requests"]:
        relatorio["recomendacao"] = "SERVER-SIDE: requests simples, sem Selenium"
    elif relatorio["acesso_selenium"]:
        relatorio["recomendacao"] = "SELENIUM: precisa de browser para acessar"
    elif relatorio["sinais_spa"]:
        relatorio["recomendacao"] = "SPA: precisa Selenium + waits para JS renderizar"
    else:
        relatorio["recomendacao"] = "INACESSIVEL: verificar URLs manualmente"
    
    print(f"\n  RESULTADO: {relatorio['recomendacao']}")
    return relatorio


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Mapeamento automatico de sites concorrentes")
    parser.add_argument("--selenium", action="store_true", help="Usar Selenium para todos os testes")
    parser.add_argument("--empresa", type=str, default=None, help="Mapear apenas uma empresa")
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"  MAPEAMENTO AUTOMATICO DE CONCORRENTES")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  Selenium: {'disponivel' if SELENIUM_DISPONIVEL else 'NAO disponivel'}")
    print(f"{'='*60}")
    
    # Filtrar por empresa se solicitado
    construtoras = CONSTRUTORAS
    if args.empresa:
        construtoras = [c for c in CONSTRUTORAS if args.empresa.lower() in c["nome"].lower()]
        if not construtoras:
            print(f"\nEmpresa '{args.empresa}' nao encontrada. Disponiveis:")
            for c in CONSTRUTORAS:
                print(f"  - {c['nome']}")
            return
    
    # Criar driver Selenium se necessario
    driver = None
    if args.selenium and SELENIUM_DISPONIVEL:
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        driver = webdriver.Chrome(options=options)
    
    relatorios = []
    
    try:
        for construtora in construtoras:
            relatorio = mapear_construtora(construtora, usar_selenium=args.selenium, driver=driver)
            relatorios.append(relatorio)
            time.sleep(2)
    finally:
        if driver:
            driver.quit()
    
    # Salvar relatorio consolidado
    consolidado_path = os.path.join(OUTPUT_DIR, "mapeamento_consolidado.json")
    with open(consolidado_path, "w", encoding="utf-8") as f:
        json.dump(relatorios, f, ensure_ascii=False, indent=2, default=str)
    
    # Resumo
    print(f"\n\n{'='*60}")
    print(f"  RESUMO DO MAPEAMENTO")
    print(f"{'='*60}")
    print(f"  {'Empresa':<20s} {'Metodo':<15s} {'Links':<8s} {'Recomendacao'}")
    print(f"  {'-'*70}")
    for r in relatorios:
        metodo = "requests" if r["acesso_requests"] else "Selenium" if r["acesso_selenium"] else "FALHOU"
        links = str(len(r["links_empreendimentos"]))
        print(f"  {r['nome']:<20s} {metodo:<15s} {links:<8s} {r['recomendacao']}")
    
    print(f"\n  Relatorio salvo em: {consolidado_path}")
    print(f"  HTMLs e textos em: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

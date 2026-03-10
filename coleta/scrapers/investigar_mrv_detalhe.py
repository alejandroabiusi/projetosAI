"""
Investiga requisicoes de rede na pagina de detalhe da MRV.
Captura todas as chamadas XHR/Fetch e exibe as relevantes.

Uso:
    python investigar_mrv_detalhe.py
"""

import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

URL = "https://mrv.com.br/imoveis/sao-paulo/campinas/apartamentos-residencial-canoas"

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--log-level=3")

# Habilita captura de rede via DevTools Protocol
options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

print(f"Abrindo: {URL}")
driver = webdriver.Chrome(options=options)

try:
    driver.get(URL)
    time.sleep(5)  # aguarda carregamento do JS

    # Coleta todos os eventos de rede
    perf_logs = driver.get_log("performance")

    requisicoes = []
    for entry in perf_logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg.get("method") != "Network.responseReceived":
                continue

            params = msg.get("params", {})
            resp = params.get("response", {})
            url = resp.get("url", "")
            mime = resp.get("mimeType", "")
            status = resp.get("status", 0)

            # Filtra apenas XHR/Fetch e JSON
            tipo = params.get("type", "")
            if tipo not in ("XHR", "Fetch"):
                continue

            requisicoes.append({
                "url": url,
                "status": status,
                "mime": mime,
                "tipo": tipo,
            })
        except Exception:
            continue

    print(f"\n{len(requisicoes)} requisicoes XHR/Fetch capturadas:\n")
    for r in requisicoes:
        print(f"  [{r['status']}] {r['tipo']} | {r['mime']}")
        print(f"         {r['url']}")
        print()

finally:
    driver.quit()

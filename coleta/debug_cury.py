import sys
sys.path.insert(0, ".")
from scrapers.cury_empreendimentos import criar_driver, carregar_pagina, extrair_dados_empreendimento

driver = criar_driver()
url = "https://cury.net/imovel/RJ/zona-portuaria/luzes-do-rio"
soup = carregar_pagina(driver, url, espera=8)
dados = extrair_dados_empreendimento(soup, url)
print(dados)
driver.quit()

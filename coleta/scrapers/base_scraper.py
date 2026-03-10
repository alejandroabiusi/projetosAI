"""
Classe base para todos os scrapers do projeto.
Concentra a logica compartilhada: configuracao do Selenium, logging,
download de arquivos e verificacao de duplicatas.
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime

# Adiciona o diretorio raiz ao path para permitir imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SELENIUM, REQUESTS, LOGS_DIR


class BaseScraper:
    """Classe base com funcionalidades comuns a todos os scrapers."""

    def __init__(self, nome_empresa, diretorio_downloads):
        self.nome_empresa = nome_empresa
        self.diretorio_downloads = diretorio_downloads
        self.driver = None
        self.logger = self._setup_logger()

        # Garante que os diretorios existem
        os.makedirs(self.diretorio_downloads, exist_ok=True)
        os.makedirs(LOGS_DIR, exist_ok=True)

    def _setup_logger(self):
        """Configura logger com saida para arquivo e console."""
        logger = logging.getLogger(f"scraper.{self.nome_empresa}")
        logger.setLevel(logging.INFO)

        # Evita duplicar handlers se o logger ja existir
        if logger.handlers:
            return logger

        # Formato do log
        fmt = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler para arquivo
        log_file = os.path.join(
            LOGS_DIR,
            f"{self.nome_empresa.lower().replace('&', '').replace(' ', '_')}.log",
        )
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        # Handler para console
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        return logger

    def iniciar_navegador(self):
        """Inicializa o Chrome via Selenium com as configuracoes do projeto."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        self.logger.info("Iniciando navegador Chrome...")

        options = Options()
        if SELENIUM["headless"]:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--user-agent={SELENIUM['user_agent']}")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # Tenta usar webdriver-manager se disponivel; senao, assume ChromeDriver no PATH
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.logger.info("ChromeDriver configurado via webdriver-manager.")
        except ImportError:
            self.logger.info(
                "webdriver-manager nao encontrado. Usando ChromeDriver do PATH. "
                "Se falhar, instale com: pip install webdriver-manager"
            )
            service = Service()

        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(SELENIUM["timeout"])
        self.logger.info("Navegador iniciado com sucesso.")

    def fechar_navegador(self):
        """Fecha o navegador de forma segura."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.logger.info("Navegador fechado.")

    def aguardar(self, segundos=None):
        """Pausa entre requisicoes para nao sobrecarregar o site."""
        t = segundos or SELENIUM["wait_between_requests"]
        time.sleep(t)

    def arquivo_ja_existe(self, nome_arquivo):
        """Verifica se um arquivo ja foi baixado anteriormente."""
        caminho = os.path.join(self.diretorio_downloads, nome_arquivo)
        return os.path.exists(caminho)

    def listar_arquivos_baixados(self):
        """Retorna lista de arquivos ja presentes no diretorio de downloads."""
        if not os.path.exists(self.diretorio_downloads):
            return []
        return os.listdir(self.diretorio_downloads)

    def baixar_pdf(self, url, nome_arquivo):
        """
        Baixa um PDF a partir de uma URL e salva no diretorio de downloads.
        Retorna True se o download foi bem-sucedido, False caso contrario.
        """
        if self.arquivo_ja_existe(nome_arquivo):
            self.logger.info(f"Arquivo ja existe, pulando: {nome_arquivo}")
            return False

        caminho_destino = os.path.join(self.diretorio_downloads, nome_arquivo)
        self.logger.info(f"Baixando: {nome_arquivo}")
        self.logger.info(f"  URL: {url}")

        try:
            response = requests.get(
                url,
                headers=REQUESTS["headers"],
                timeout=REQUESTS["timeout"],
                stream=True,
            )
            response.raise_for_status()

            with open(caminho_destino, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            tamanho = os.path.getsize(caminho_destino)
            self.logger.info(
                f"  Download concluido: {tamanho / 1024:.1f} KB"
            )
            return True

        except requests.RequestException as e:
            self.logger.error(f"  Erro no download: {e}")
            # Remove arquivo parcial se existir
            if os.path.exists(caminho_destino):
                os.remove(caminho_destino)
            return False

    def gerar_relatorio(self, novos, ja_existentes, erros):
        """Imprime um resumo da execucao no log."""
        self.logger.info("=" * 60)
        self.logger.info(f"RELATORIO DE EXECUCAO - {self.nome_empresa}")
        self.logger.info(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Novos downloads: {novos}")
        self.logger.info(f"Ja existentes (pulados): {ja_existentes}")
        self.logger.info(f"Erros: {erros}")
        self.logger.info("=" * 60)

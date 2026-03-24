"""
Classe base para todos os scrapers do projeto.
Concentra a logica compartilhada: configuracao do Selenium, logging,
download de arquivos e verificacao de duplicatas.

Adaptado do projeto coleta (incorporadoras).
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SELENIUM, REQUESTS, LOGS_DIR


class BaseScraper:
    """Classe base com funcionalidades comuns a todos os scrapers."""

    def __init__(self, nome_empresa, diretorio_downloads):
        self.nome_empresa = nome_empresa
        self.diretorio_downloads = diretorio_downloads
        self.driver = None
        self.logger = self._setup_logger()

        os.makedirs(self.diretorio_downloads, exist_ok=True)
        os.makedirs(LOGS_DIR, exist_ok=True)

    def _setup_logger(self):
        """Configura logger com saida para arquivo e console."""
        logger = logging.getLogger(f"scraper.{self.nome_empresa}")
        logger.setLevel(logging.INFO)

        if logger.handlers:
            return logger

        fmt = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        log_file = os.path.join(
            LOGS_DIR,
            f"{self.nome_empresa.lower().replace('&', '').replace(' ', '_')}.log",
        )
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        return logger

    def iniciar_navegador(self):
        """Inicializa o Chrome via Selenium."""
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

        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except ImportError:
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
        """Pausa entre requisicoes."""
        t = segundos or SELENIUM["wait_between_requests"]
        time.sleep(t)

    def arquivo_ja_existe(self, nome_arquivo):
        """Verifica se um arquivo ja foi baixado."""
        caminho = os.path.join(self.diretorio_downloads, nome_arquivo)
        return os.path.exists(caminho)

    def listar_arquivos_baixados(self):
        """Retorna lista de arquivos presentes no diretorio de downloads."""
        if not os.path.exists(self.diretorio_downloads):
            return []
        return os.listdir(self.diretorio_downloads)

    def baixar_pdf(self, url, nome_arquivo):
        """Baixa um arquivo (PDF, XLSX, etc.) a partir de uma URL."""
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
            self.logger.info(f"  Download concluido: {tamanho / 1024:.1f} KB")
            return True

        except requests.RequestException as e:
            self.logger.error(f"  Erro no download: {e}")
            if os.path.exists(caminho_destino):
                os.remove(caminho_destino)
            return False

    def baixar_audio_youtube(self, url, nome_arquivo):
        """Baixa audio de uma URL do YouTube usando yt-dlp."""
        import subprocess

        if self.arquivo_ja_existe(nome_arquivo):
            self.logger.info(f"Arquivo ja existe, pulando: {nome_arquivo}")
            return False

        caminho_destino = os.path.join(self.diretorio_downloads, nome_arquivo)
        self.logger.info(f"Baixando audio do YouTube: {nome_arquivo}")
        self.logger.info(f"  URL: {url}")

        try:
            ffmpeg_loc = None
            localappdata = os.environ.get("LOCALAPPDATA", "")
            winget_pkgs = os.path.join(localappdata, "Microsoft", "WinGet", "Packages")
            if os.path.isdir(winget_pkgs):
                for d in os.listdir(winget_pkgs):
                    if "FFmpeg" in d:
                        pkg_dir = os.path.join(winget_pkgs, d)
                        for root, dirs, files in os.walk(pkg_dir):
                            if "ffmpeg.exe" in files:
                                ffmpeg_loc = root
                                break
                        if ffmpeg_loc:
                            break
            if not ffmpeg_loc:
                for p in [
                    os.path.join(os.environ.get("ProgramFiles", ""), "FFmpeg", "bin"),
                    "C:\\ffmpeg\\bin",
                ]:
                    if os.path.isdir(p):
                        ffmpeg_loc = p
                        break

            cmd = [
                sys.executable, "-m", "yt_dlp",
                "-x", "--audio-format", "mp3",
                "--audio-quality", "0",
                "-o", caminho_destino.replace(".mp3", ".%(ext)s"),
                "--no-playlist",
                "--quiet",
                url,
            ]
            if ffmpeg_loc:
                cmd.insert(-1, "--ffmpeg-location")
                cmd.insert(-1, ffmpeg_loc)
            resultado = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300
            )
            if resultado.returncode != 0:
                self.logger.error(f"  Erro yt-dlp: {resultado.stderr[:500]}")
                return False

            if os.path.exists(caminho_destino):
                tamanho = os.path.getsize(caminho_destino)
                self.logger.info(f"  Download concluido: {tamanho / 1024 / 1024:.1f} MB")
                return True
            else:
                self.logger.error(f"  Arquivo nao encontrado apos download: {caminho_destino}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("  Timeout no download do YouTube (5 min)")
            return False
        except Exception as e:
            self.logger.error(f"  Erro no download YouTube: {e}")
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

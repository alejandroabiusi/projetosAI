"""
Scraper de Documentos de RI - TOTVS
====================================
Baixa documentos da central de resultados da TOTVS (plataforma MZ Group).

Tipos de documentos suportados:
  - release: Release de Resultados
  - itr_dfp: DFP e ITR
  - demonstracoes: Demonstracoes Financeiras em Padroes Internacionais (PT)
  - apresentacao: Apresentacao de Resultados
  - transcricao: Transcricao da Videoconferencia
  - audio: Audio da Videoconferencia
  - planilha: Planilhas Interativas

Uso:
    python scrapers/totvs_ri.py release
    python scrapers/totvs_ri.py release 2025 2024 2023 2022
    python scrapers/totvs_ri.py transcricao
    python scrapers/totvs_ri.py itr_dfp 2025 2024 2023 2022
    python scrapers/totvs_ri.py demonstracoes
    python scrapers/totvs_ri.py planilha
    python scrapers/totvs_ri.py audio
    python scrapers/totvs_ri.py todos          # baixa todos os tipos
    python scrapers/totvs_ri.py todos 2025 2024 2023 2022
"""

import os
import sys
import re
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.base_scraper import BaseScraper
from config.settings import TOTVS


# Filtros de inclusao/exclusao por tipo de documento
FILTROS_POR_TIPO = {
    "release": {
        "incluir": [
            "release de resultado", "release resultado",
            "press release", "earnings release", "release",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "apresenta", "presentation", "planilha",
            "spreadsheet", "demonstra", "financial statement", "dfp", "itr",
            "formulario", "form", "informe", "comentario de desempenho",
            "earnings call", "conference call", "transcri", "transcript",
        ],
        "prefixo_arquivo": "Release",
        "config_dir": "downloads_releases",
    },
    "itr_dfp": {
        "incluir": [
            "itr", "dfp",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "apresenta", "presentation", "planilha",
            "spreadsheet", "release", "press release", "earnings release",
            "formulario", "form", "informe", "comentario de desempenho",
            "earnings call", "conference call", "demonstra", "financial statement",
            "dfs consolidadas", "transcri", "transcript",
        ],
        "prefixo_arquivo": "ITR",
        "config_dir": "downloads_itr_dfp",
    },
    "demonstracoes": {
        "incluir": [
            "demonstra", "dfs consolidadas", "financial statement",
            "padrões internacionais", "padroes internacionais",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "apresenta", "presentation", "planilha",
            "spreadsheet", "release", "press release", "earnings release",
            "formulario", "form", "itr", "dfp", "transcri", "transcript",
            "inglês", "ingles", "english", "versão ing", "versao ing",
            "(ing)", "- ing",
        ],
        "prefixo_arquivo": "DF",
        "config_dir": "downloads_demonstracoes",
    },
    "apresentacao": {
        "incluir": [
            "apresenta", "presentation", "slide",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "release", "press release", "earnings release",
            "itr", "dfp", "demonstra", "planilha", "spreadsheet",
            "formulario", "form", "transcri", "transcript",
        ],
        "prefixo_arquivo": "Apresentacao",
        "config_dir": "downloads_apresentacoes",
    },
    "transcricao": {
        "incluir": [
            "transcri", "transcript",
        ],
        "excluir": [
            "release", "press release", "itr", "dfp", "demonstra",
            "planilha", "spreadsheet", "formulario", "apresenta",
            "presentation", "slide", "prévia", "previa",
            "audio", "áudio", "webcast",
        ],
        "prefixo_arquivo": "Transcricao",
        "config_dir": "downloads_transcricoes_ri",
    },
    "audio": {
        "incluir": [
            "audio", "áudio", "teleconferencia", "teleconferência",
            "conference call", "earnings call", "webcast",
            "videoconferência", "videoconferencia",
        ],
        "excluir": [
            "release", "press release", "itr", "dfp", "demonstra",
            "planilha", "spreadsheet", "formulario", "apresenta",
            "presentation", "slide", "transcri", "transcript",
        ],
        "prefixo_arquivo": "Audio",
        "config_dir": "downloads_audios",
        "aceitar_midia": True,
    },
    "planilha": {
        "incluir": [
            "planilha", "spreadsheet", "interativ",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "release", "press release", "itr", "dfp",
            "demonstra", "apresenta", "presentation", "formulario",
            "transcri", "transcript",
        ],
        "prefixo_arquivo": "Planilha",
        "config_dir": "downloads_planilhas",
        "aceitar_xlsx": True,
    },
}


class TotvsRI(BaseScraper):
    """Scraper para documentos de RI da TOTVS (plataforma MZ Group)."""

    def __init__(self, tipo_documento="release"):
        self.config = TOTVS
        self.tipo_documento = tipo_documento
        self.prefixo = "TOTVS"

        filtros = FILTROS_POR_TIPO[tipo_documento]
        self.termos_incluir = filtros["incluir"]
        self.termos_excluir = filtros["excluir"]
        self.prefixo_arquivo = filtros["prefixo_arquivo"]

        # Planilhas usam URL diferente
        if tipo_documento == "planilha":
            self.url_central = TOTVS["ri_planilhas_url"]
        else:
            self.url_central = TOTVS["ri_url"]

        diretorio = TOTVS[filtros["config_dir"]]

        super().__init__(
            nome_empresa="TOTVS",
            diretorio_downloads=diretorio,
        )
        self._usa_select = False

    def _obter_anos_disponiveis(self):
        """Identifica os anos disponiveis no seletor da pagina."""
        from selenium.webdriver.common.by import By

        # Estrategia 1: <select id="fano">
        try:
            from selenium.webdriver.support.ui import Select
            select_elem = self.driver.find_element(By.ID, "fano")
            anos = []
            for option in Select(select_elem).options:
                val = option.get_attribute("value").strip()
                if val.isdigit() and len(val) == 4:
                    anos.append(int(val))
                    self.logger.info(f"  Ano disponivel: {val}")
            if anos:
                self.logger.info(f"Select #fano: {len(anos)} anos encontrados.")
                self._usa_select = True
                return anos
        except Exception:
            pass

        # Estrategia 2: formulario lAnoLink com <a>
        anos_encontrados = []
        try:
            form_ano = self.driver.find_element(By.ID, "lAnoLink")
            links_ano = form_ano.find_elements(By.TAG_NAME, "a")
            for link in links_ano:
                texto = link.text.strip()
                if texto.isdigit() and len(texto) == 4:
                    anos_encontrados.append((int(texto), link))
                    self.logger.info(f"  Ano disponivel: {texto}")
            if anos_encontrados:
                self.logger.info(
                    f"Seletor lAnoLink encontrado com {len(anos_encontrados)} anos."
                )
                return anos_encontrados
        except Exception:
            self.logger.info("lAnoLink nao encontrado, tentando alternativas...")

        # Estrategia 3: fallbacks CSS
        for seletor_css in [
            "#divAnoResultado a",
            ".mz-years a",
            ".year-selector a",
            "[class*='ano'] a",
            "[class*='year'] a",
        ]:
            try:
                links = self.driver.find_elements(By.CSS_SELECTOR, seletor_css)
                for link in links:
                    texto = link.text.strip()
                    if texto.isdigit() and len(texto) == 4:
                        anos_encontrados.append((int(texto), link))
            except Exception:
                continue

        if anos_encontrados:
            self.logger.info(f"Seletor alternativo: {len(anos_encontrados)} anos.")
        else:
            self.logger.warning("Nenhum seletor de ano encontrado.")

        return anos_encontrados

    def _selecionar_ano(self, ano):
        """Seleciona o ano via <select id='fano'>."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select, WebDriverWait

        try:
            select_elem = self.driver.find_element(By.ID, "fano")
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", select_elem
            )
            time.sleep(0.3)
            Select(select_elem).select_by_value(str(ano))
            self.logger.info(f"Ano {ano} selecionado via Select #fano.")
            time.sleep(3)
            WebDriverWait(self.driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except Exception as e:
            self.logger.warning(f"Erro ao selecionar ano {ano}: {e}")
            return False

    def _clicar_ano(self, elemento_ano):
        """Clica no elemento <a> do ano e espera carregamento."""
        from selenium.webdriver.support.ui import WebDriverWait

        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", elemento_ano
            )
            time.sleep(0.5)
            try:
                elemento_ano.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", elemento_ano)
            self.logger.info("Clique no ano realizado, aguardando...")
            time.sleep(3)
            WebDriverWait(self.driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except Exception as e:
            self.logger.warning(f"Erro ao clicar no ano: {e}")
            return False

    def _eh_arquivo_valido(self, url):
        """Verifica se a URL aponta para um arquivo baixavel."""
        url_lower = url.lower()
        filtros = FILTROS_POR_TIPO.get(self.tipo_documento, {})

        # Audio: aceita mp3, mp4, links de streaming MZ
        if filtros.get("aceitar_midia"):
            for ext in (".mp3", ".mp4", ".m4a", ".wav", ".ogg"):
                if url_lower.endswith(ext):
                    return True
            if "mzfilemanager" in url_lower or "mziq.com" in url_lower:
                return True
            return False

        # Planilha: aceita xlsx, xls
        if filtros.get("aceitar_xlsx"):
            for ext in (".xlsx", ".xls", ".xlsm"):
                if url_lower.endswith(ext):
                    return True
            if "mzfilemanager" in url_lower or "mziq.com" in url_lower:
                for termo in ["audio", "video", "stream", "webcast", "mp3", "mp4"]:
                    if termo in url_lower:
                        return False
                return True
            return False

        # PDF
        if url_lower.endswith(".pdf"):
            return True
        if "mzfilemanager" in url_lower or "mziq.com" in url_lower:
            for termo in ["audio", "video", "stream", "webcast", "mp3", "mp4"]:
                if termo in url_lower:
                    return False
            return True
        return False

    def _extrair_da_tabela(self):
        """Extrai links da estrutura table.tb-central (MZ Group padrao)."""
        from selenium.webdriver.common.by import By

        links_encontrados = []
        tabelas = self.driver.find_elements(By.CSS_SELECTOR, "table.tb-central")
        if not tabelas:
            return links_encontrados

        self.logger.info(f"Tabelas tb-central encontradas: {len(tabelas)}")

        for tabela in tabelas:
            trimestres = []
            try:
                ths = tabela.find_elements(By.CSS_SELECTOR, "th.tabelatt.icone")
                trimestres = [th.text.strip() for th in ths]
            except Exception:
                pass

            linhas = tabela.find_elements(
                By.XPATH, ".//tr[td[contains(@class,'titulo')]]"
            )
            for linha in linhas:
                try:
                    tipo_doc = linha.find_element(
                        By.CSS_SELECTOR, "td.first.titulo"
                    ).text.strip()
                except Exception:
                    continue

                tipo_lower = tipo_doc.lower()
                eh_alvo = any(termo in tipo_lower for termo in self.termos_incluir)
                if not eh_alvo:
                    self.logger.info(f"  Linha ignorada: '{tipo_doc}'")
                    continue

                self.logger.info(f"  Linha de {self.tipo_documento}: '{tipo_doc}'")

                celulas = linha.find_elements(By.CSS_SELECTOR, "td.icone")
                for i, celula in enumerate(celulas):
                    try:
                        link = celula.find_element(By.TAG_NAME, "a")
                        href = link.get_attribute("href") or ""
                        if not href:
                            continue
                        if not self._eh_arquivo_valido(href):
                            continue

                        tri = trimestres[i] if i < len(trimestres) else f"T{i+1}"
                        img_alt = ""
                        try:
                            img_alt = (
                                link.find_element(By.TAG_NAME, "img")
                                .get_attribute("alt") or ""
                            )
                        except Exception:
                            pass

                        texto_id = tri
                        if img_alt and re.search(r"\d[Tt]\d{2,4}", img_alt):
                            texto_id = img_alt
                        links_encontrados.append((texto_id, href))
                        self.logger.info(f"    ACEITO: {tri} | alt='{img_alt}'")
                    except Exception:
                        continue

        return links_encontrados

    def _extrair_links_documento(self):
        """Extrai links de documentos do tipo configurado."""
        from selenium.webdriver.common.by import By

        links_tabela = self._extrair_da_tabela()
        if links_tabela:
            return links_tabela

        self.logger.info("Tabela tb-central nao encontrada, usando scan de links...")
        links_encontrados = []
        todos_links = self.driver.find_elements(By.TAG_NAME, "a")
        self.logger.info(f"Total de links na pagina: {len(todos_links)}")

        for link in todos_links:
            href = link.get_attribute("href") or ""
            texto = link.text.strip()
            if not href or not texto:
                continue
            if not self._eh_arquivo_valido(href):
                continue
            texto_lower = texto.lower()
            if any(termo in texto_lower for termo in self.termos_excluir):
                continue
            if any(termo in texto_lower for termo in self.termos_incluir):
                links_encontrados.append((texto, href))
                self.logger.info(f"  ACEITO: {texto}")

        vistos = set()
        return [(t, u) for t, u in links_encontrados if u not in vistos and not vistos.add(u)]

    def _detectar_extensao(self, url):
        """Detecta a extensao do arquivo pela URL."""
        url_lower = url.lower().split("?")[0]
        for ext in (".pdf", ".mp3", ".mp4", ".m4a", ".wav", ".ogg", ".xlsx", ".xls"):
            if url_lower.endswith(ext):
                return ext
        filtros = FILTROS_POR_TIPO.get(self.tipo_documento, {})
        if filtros.get("aceitar_midia"):
            return ".mp3"
        if filtros.get("aceitar_xlsx"):
            return ".xlsx"
        return ".pdf"

    def _gerar_nome_arquivo(self, texto, url, ano):
        """Gera nome de arquivo no padrao TOTVS_{Prefixo}_XTAnno.ext"""
        ext = self._detectar_extensao(url)
        padrao_tri = re.search(r"(\d)[Tt](\d{2,4})", texto)
        if padrao_tri:
            trimestre = padrao_tri.group(1)
            ano_tri = padrao_tri.group(2)
            if len(ano_tri) == 2:
                ano_tri = f"20{ano_tri}"
            return f"TOTVS_{self.prefixo_arquivo}_{trimestre}T{ano_tri}{ext}"

        url_hash = abs(hash(url)) % 100000
        return f"TOTVS_{self.prefixo_arquivo}_{ano}_{url_hash}{ext}"

    def _verificar_e_corrigir_audio(self, caminho, url_original):
        """Verifica se arquivo baixado e audio real ou HTML (YouTube embed do MZ)."""
        try:
            with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                inicio = f.read(500)
        except Exception:
            return True

        if "<html" not in inicio.lower() and "<!doctype" not in inicio.lower():
            return True

        self.logger.info(f"  Arquivo e HTML (player MZ), extraindo YouTube ID...")

        try:
            with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                conteudo = f.read()
        except Exception:
            return False

        video_ids = re.findall(r'"videoId":"([^"]+)"', conteudo)
        if not video_ids:
            video_ids = re.findall(r'(?:watch\?v=|embed/)([a-zA-Z0-9_-]{11})', conteudo)
        if not video_ids:
            self.logger.error("  Nao encontrou YouTube video ID no HTML")
            os.remove(caminho)
            return False

        video_id = video_ids[0]
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        self.logger.info(f"  YouTube encontrado: {youtube_url}")

        os.remove(caminho)
        return self.baixar_audio_youtube(youtube_url, os.path.basename(caminho))

    def _baixar_e_verificar(self, url, nome):
        """Baixa arquivo e verifica integridade."""
        if not self.baixar_pdf(url, nome):
            return False
        filtros = FILTROS_POR_TIPO.get(self.tipo_documento, {})
        if filtros.get("aceitar_midia"):
            caminho = os.path.join(self.diretorio_downloads, nome)
            return self._verificar_e_corrigir_audio(caminho, url)
        return True

    def executar(self, anos=None):
        """Executa o scraper para o tipo de documento configurado."""
        tipo_label = self.tipo_documento.upper().replace("_", "/")
        self.logger.info("=" * 60)
        self.logger.info(f"SCRAPER TOTVS - {tipo_label}")
        self.logger.info(f"URL: {self.url_central}")
        self.logger.info("=" * 60)

        contadores = {"novos": 0, "existentes": 0, "erros": 0}

        try:
            self.iniciar_navegador()
            self.driver.get(self.url_central)
            self.aguardar(5)
            self.logger.info(f"Titulo: {self.driver.title}")

            anos_disponiveis = self._obter_anos_disponiveis()

            if not anos_disponiveis:
                self.logger.warning("Sem seletor de ano. Extraindo pagina atual...")
                links = self._extrair_links_documento()
                for texto, url_doc in links:
                    nome = self._gerar_nome_arquivo(texto, url_doc, 2025)
                    if self.arquivo_ja_existe(nome):
                        contadores["existentes"] += 1
                    elif self._baixar_e_verificar(url_doc, nome):
                        contadores["novos"] += 1
                    else:
                        contadores["erros"] += 1
                    self.aguardar()

            elif self._usa_select:
                if anos:
                    anos_disponiveis = [a for a in anos_disponiveis if a in anos]

                for ano in anos_disponiveis:
                    self.logger.info(f"\n--- TOTVS - Ano {ano} ---")

                    if not self._selecionar_ano(ano):
                        self.logger.warning(f"Falha ao selecionar ano {ano}, pulando.")
                        continue

                    links = self._extrair_links_documento()
                    self.logger.info(f"Documentos para {ano}: {len(links)}")

                    for texto, url_doc in links:
                        nome = self._gerar_nome_arquivo(texto, url_doc, ano)
                        if self.arquivo_ja_existe(nome):
                            self.logger.info(f"Ja existe: {nome}")
                            contadores["existentes"] += 1
                            continue
                        if self._baixar_e_verificar(url_doc, nome):
                            contadores["novos"] += 1
                        else:
                            contadores["erros"] += 1
                        self.aguardar()

            else:
                if anos:
                    anos_disponiveis = [
                        (a, elem) for a, elem in anos_disponiveis if a in anos
                    ]

                for idx, (ano, elemento) in enumerate(anos_disponiveis):
                    self.logger.info(f"\n--- TOTVS - Ano {ano} ---")

                    if not self._clicar_ano(elemento):
                        self.logger.warning(f"Falha ao clicar ano {ano}, pulando.")
                        continue

                    links = self._extrair_links_documento()
                    self.logger.info(f"Documentos para {ano}: {len(links)}")

                    for texto, url_doc in links:
                        nome = self._gerar_nome_arquivo(texto, url_doc, ano)
                        if self.arquivo_ja_existe(nome):
                            self.logger.info(f"Ja existe: {nome}")
                            contadores["existentes"] += 1
                            continue
                        if self._baixar_e_verificar(url_doc, nome):
                            contadores["novos"] += 1
                        else:
                            contadores["erros"] += 1
                        self.aguardar()

                    # Re-obtem seletor (DOM pode ter sido atualizado)
                    if idx < len(anos_disponiveis) - 1:
                        novos = self._obter_anos_disponiveis()
                        if novos and not self._usa_select:
                            ja_feitos = {a for a, _ in anos_disponiveis[:idx + 1]}
                            restantes = [
                                (a, e) for a, e in novos
                                if a not in ja_feitos and (not anos or a in anos)
                            ]
                            anos_disponiveis = anos_disponiveis[:idx + 1] + restantes

        except Exception as e:
            self.logger.error(f"Erro: {e}", exc_info=True)
            contadores["erros"] += 1
        finally:
            self.fechar_navegador()

        self.gerar_relatorio(contadores["novos"], contadores["existentes"], contadores["erros"])
        return contadores


def main():
    tipos_validos = list(FILTROS_POR_TIPO.keys()) + ["todos"]

    if len(sys.argv) < 2:
        print("Uso: python scrapers/totvs_ri.py <tipo> [ano1] [ano2] ...")
        print(f"Tipos disponiveis: {', '.join(tipos_validos)}")
        sys.exit(1)

    tipo = sys.argv[1].lower()
    anos = [int(a) for a in sys.argv[2:]] if len(sys.argv) > 2 else None

    if tipo == "todos":
        tipos_executar = ["release", "itr_dfp", "demonstracoes", "apresentacao",
                          "transcricao", "planilha"]
    elif tipo in FILTROS_POR_TIPO:
        tipos_executar = [tipo]
    else:
        print(f"Tipo '{tipo}' invalido. Opcoes: {', '.join(tipos_validos)}")
        sys.exit(1)

    for t in tipos_executar:
        print(f"\n{'='*60}")
        print(f"  Baixando: {t.upper()}")
        print(f"{'='*60}")
        scraper = TotvsRI(tipo_documento=t)
        resultado = scraper.executar(anos=anos)
        print(
            f"Resumo TOTVS ({t}): "
            f"{resultado['novos']} novos, "
            f"{resultado['existentes']} existentes, "
            f"{resultado['erros']} erros."
        )


if __name__ == "__main__":
    main()

"""
Scraper Generico de Documentos de RI - Plataforma MZ Group
==========================================================
Todas as incorporadoras monitoradas usam a plataforma MZ Group para
seus sites de RI. Este scraper generico recebe a configuracao da empresa
como parametro e aplica a mesma logica para todas:

  - Seletor de ano via formulario id="lAnoLink"
  - Filtro por tipo de documento: release, itr_dfp ou demonstracoes
  - Ignora audios, videos, apresentacoes, webcasts

Uso direto (qualquer empresa):
    python scrapers/mzgroup_ri.py cury
    python scrapers/mzgroup_ri.py mrv 2025 2024
    python scrapers/mzgroup_ri.py cury --tipo itr_dfp
    python scrapers/mzgroup_ri.py mrv --tipo demonstracoes
"""

import os
import sys
import re
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.base_scraper import BaseScraper
from config import settings


# Mapeamento de nomes curtos para configuracoes
EMPRESAS = {
    "planoeplano": settings.PLANOEPLANO,
    "plano": settings.PLANOEPLANO,
    "cury": settings.CURY,
    "mrv": settings.MRV,
    "direcional": settings.DIRECIONAL,
    "cyrela": settings.CYRELA,
    "mouradubeux": settings.MOURADUBEUX,
    "moura": settings.MOURADUBEUX,
}

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
            "earnings call", "conference call",
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
            "dfs consolidadas",
        ],
        "prefixo_arquivo": "ITR",
        "config_dir": "downloads_itr_dfp",
    },
    "demonstracoes": {
        "incluir": [
            "demonstra", "dfs consolidadas", "financial statement",
        ],
        "excluir": [
            "audio", "áudio", "vídeo", "video", "webcast", "teleconferencia",
            "teleconferência", "apresenta", "presentation", "planilha",
            "spreadsheet", "release", "press release", "earnings release",
            "formulario", "form", "informe", "comentario de desempenho",
            "earnings call", "conference call", "itr", "dfp",
        ],
        "prefixo_arquivo": "DF",
        "config_dir": "downloads_demonstracoes",
    },
}


class MZGroupRI(BaseScraper):
    """Scraper generico para documentos de RI em sites da plataforma MZ Group."""

    def __init__(self, config_empresa, tipo_documento="release"):
        """
        Recebe o dict de configuracao da empresa (ex: settings.CURY)
        e o tipo de documento a baixar: "release", "itr_dfp" ou "demonstracoes".
        """
        self.config = config_empresa
        self.tipo_documento = tipo_documento
        self.prefixo = config_empresa["nome"].replace("&", "").replace(" ", "")

        filtros = FILTROS_POR_TIPO[tipo_documento]
        self.termos_incluir = filtros["incluir"]
        self.termos_excluir = filtros["excluir"]
        self.prefixo_arquivo = filtros["prefixo_arquivo"]

        diretorio = config_empresa[filtros["config_dir"]]

        super().__init__(
            nome_empresa=config_empresa["nome"],
            diretorio_downloads=diretorio,
        )
        self.url_central = config_empresa["ri_url"]
        self._usa_select = False  # Determina se o site usa <select> ou <a> para anos

    def _obter_anos_disponiveis(self):
        """
        Identifica os anos disponiveis no seletor da pagina MZ Group.
        Tenta <select id="fano"> primeiro, depois lAnoLink com <a>, depois fallbacks CSS.
        Retorna lista de inteiros (anos) se usar select, ou lista de (int, element) se usar links.
        """
        from selenium.webdriver.common.by import By

        # Estrategia 1: <select id="fano"> (Cury, PlanoePlano, e outros)
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
        """Seleciona o ano via <select id='fano'>. Retorna True se OK."""
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
            elemento_ano.click()
            self.logger.info("Clique no ano realizado, aguardando...")
            time.sleep(3)
            WebDriverWait(self.driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except Exception as e:
            self.logger.warning(f"Erro ao clicar no ano: {e}")
            return False

    def _eh_documento_alvo(self, texto, url):
        """Verifica se o documento corresponde ao tipo_documento configurado."""
        texto_lower = texto.lower()
        url_lower = url.lower()

        for termo in self.termos_excluir:
            if termo in texto_lower:
                return False

        for termo in self.termos_incluir:
            if termo in texto_lower:
                return True

        # Para releases: fallback por padrao trimestral no texto
        if self.tipo_documento == "release":
            if re.search(r"\d[Tt]\d{2,4}", texto):
                for termo in self.termos_excluir:
                    if termo in url_lower:
                        return False
                return True

        return False

    def _eh_pdf_valido(self, url):
        """Verifica se a URL aponta para PDF real (nao audio/video)."""
        url_lower = url.lower()
        if url_lower.endswith(".pdf"):
            return True
        if "mzfilemanager" in url_lower or "mziq.com" in url_lower:
            for termo in ["audio", "video", "stream", "webcast", "mp3", "mp4"]:
                if termo in url_lower:
                    return False
            return True
        return False

    def _extrair_da_tabela(self):
        """
        Extrai links da estrutura table.tb-central (usada por todos os sites MZ Group).
        A tabela tem linhas com <td class="first titulo"> indicando o tipo de documento
        e celulas <td class="icone"> com links (que tem texto vazio, so icone).
        Retorna lista de (texto_identificador, url).
        """
        from selenium.webdriver.common.by import By

        links_encontrados = []
        tabelas = self.driver.find_elements(By.CSS_SELECTOR, "table.tb-central")
        if not tabelas:
            return links_encontrados

        self.logger.info(f"Tabelas tb-central encontradas: {len(tabelas)}")

        for tabela in tabelas:
            # Trimestres do cabecalho (ex: ["1T25", "2T25", "3T25", "4T25"])
            trimestres = []
            try:
                ths = tabela.find_elements(By.CSS_SELECTOR, "th.tabelatt.icone")
                trimestres = [th.text.strip() for th in ths]
            except Exception:
                pass

            # Linhas com tipo de documento
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
                # Verificar se esta linha corresponde ao tipo desejado
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
                        if not href or not self._eh_pdf_valido(href):
                            continue

                        # Texto identificador: usa trimestre do cabecalho ou img alt
                        tri = trimestres[i] if i < len(trimestres) else f"T{i+1}"
                        img_alt = ""
                        try:
                            img_alt = (
                                link.find_element(By.TAG_NAME, "img")
                                .get_attribute("alt") or ""
                            )
                        except Exception:
                            pass

                        # Preferir img_alt se tiver padrao trimestral, senao usar tri do cabecalho
                        texto_id = tri
                        if img_alt and re.search(r"\d[Tt]\d{2,4}", img_alt):
                            texto_id = img_alt
                        links_encontrados.append((texto_id, href))
                        self.logger.info(f"    ACEITO: {tri} | alt='{img_alt}'")
                    except Exception:
                        continue

        return links_encontrados

    def _extrair_links_documento(self):
        """
        Extrai links de PDFs do tipo_documento configurado.
        Tenta primeiro via tabela tb-central (estrutura MZ Group padrao),
        e usa scan de todos os <a> como fallback.
        """
        from selenium.webdriver.common.by import By

        # Estrategia 1: tabela tb-central (links tem texto vazio, tipo no <td>)
        links_tabela = self._extrair_da_tabela()
        if links_tabela:
            return links_tabela

        # Estrategia 2: fallback - scan de todos os <a> pelo texto
        self.logger.info("Tabela tb-central nao encontrada, usando scan de links...")
        links_encontrados = []
        todos_links = self.driver.find_elements(By.TAG_NAME, "a")
        self.logger.info(f"Total de links na pagina: {len(todos_links)}")

        for link in todos_links:
            href = link.get_attribute("href") or ""
            texto = link.text.strip()
            if not href or not texto:
                continue
            if not self._eh_pdf_valido(href):
                continue
            if self._eh_documento_alvo(texto, href):
                links_encontrados.append((texto, href))
                self.logger.info(f"  ACEITO: {texto}")
            else:
                self.logger.info(f"  Ignorado: {texto}")

        vistos = set()
        return [(t, u) for t, u in links_encontrados if u not in vistos and not vistos.add(u)]

    def _gerar_nome_arquivo(self, texto, url, ano):
        """Gera nome de arquivo no padrao Empresa_{Prefixo}_XTAnno.pdf"""
        padrao_tri = re.search(r"(\d)[Tt](\d{2,4})", texto)
        if padrao_tri:
            trimestre = padrao_tri.group(1)
            ano_tri = padrao_tri.group(2)
            if len(ano_tri) == 2:
                ano_tri = f"20{ano_tri}"
            return f"{self.prefixo}_{self.prefixo_arquivo}_{trimestre}T{ano_tri}.pdf"

        url_hash = abs(hash(url)) % 100000
        return f"{self.prefixo}_{self.prefixo_arquivo}_{ano}_{url_hash}.pdf"

    def executar(self, anos=None):
        """Executa o scraper para a empresa configurada."""
        tipo_label = self.tipo_documento.upper().replace("_", "/")
        self.logger.info("=" * 60)
        self.logger.info(
            f"SCRAPER DE {tipo_label} - {self.config['nome']} ({self.config['ticker']})"
        )
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
                for texto, url_pdf in links:
                    nome = self._gerar_nome_arquivo(texto, url_pdf, 2025)
                    if self.arquivo_ja_existe(nome):
                        contadores["existentes"] += 1
                    elif self.baixar_pdf(url_pdf, nome):
                        contadores["novos"] += 1
                    else:
                        contadores["erros"] += 1
                    self.aguardar()

            elif self._usa_select:
                # Modo <select>: anos_disponiveis e lista de ints
                if anos:
                    anos_disponiveis = [a for a in anos_disponiveis if a in anos]

                for ano in anos_disponiveis:
                    self.logger.info(f"\n--- {self.config['nome']} - Ano {ano} ---")

                    if not self._selecionar_ano(ano):
                        self.logger.warning(f"Falha ao selecionar ano {ano}, pulando.")
                        continue

                    links = self._extrair_links_documento()
                    self.logger.info(f"Documentos para {ano}: {len(links)}")

                    for texto, url_pdf in links:
                        nome = self._gerar_nome_arquivo(texto, url_pdf, ano)
                        if self.arquivo_ja_existe(nome):
                            self.logger.info(f"Ja existe: {nome}")
                            contadores["existentes"] += 1
                            continue
                        if self.baixar_pdf(url_pdf, nome):
                            contadores["novos"] += 1
                        else:
                            contadores["erros"] += 1
                        self.aguardar()

            else:
                # Modo <a> links: anos_disponiveis e lista de (int, element)
                if anos:
                    anos_disponiveis = [
                        (a, elem) for a, elem in anos_disponiveis if a in anos
                    ]

                for idx, (ano, elemento) in enumerate(anos_disponiveis):
                    self.logger.info(f"\n--- {self.config['nome']} - Ano {ano} ---")

                    if not self._clicar_ano(elemento):
                        continue

                    links = self._extrair_links_documento()
                    self.logger.info(f"Documentos para {ano}: {len(links)}")

                    for texto, url_pdf in links:
                        nome = self._gerar_nome_arquivo(texto, url_pdf, ano)
                        if self.arquivo_ja_existe(nome):
                            self.logger.info(f"Ja existe: {nome}")
                            contadores["existentes"] += 1
                            continue
                        if self.baixar_pdf(url_pdf, nome):
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
    if len(sys.argv) < 2:
        print("Uso: python scrapers/mzgroup_ri.py <empresa> [ano1] [ano2] ... [--tipo release|itr_dfp|demonstracoes]")
        print(f"Empresas disponiveis: {', '.join(EMPRESAS.keys())}")
        sys.exit(1)

    # Parsear argumentos
    args = sys.argv[1:]
    tipo_documento = "release"
    if "--tipo" in args:
        idx = args.index("--tipo")
        if idx + 1 < len(args):
            tipo_documento = args[idx + 1]
            args = args[:idx] + args[idx + 2:]
        else:
            print("Erro: --tipo requer um valor (release, itr_dfp, demonstracoes)")
            sys.exit(1)

    nome = args[0].lower()
    if nome not in EMPRESAS:
        print(f"Empresa '{nome}' nao encontrada.")
        print(f"Opcoes: {', '.join(EMPRESAS.keys())}")
        sys.exit(1)

    anos = [int(a) for a in args[1:]] if len(args) > 1 else None

    scraper = MZGroupRI(EMPRESAS[nome], tipo_documento=tipo_documento)
    resultado = scraper.executar(anos=anos)
    print(
        f"\nResumo {EMPRESAS[nome]['nome']} ({tipo_documento}): "
        f"{resultado['novos']} novos, "
        f"{resultado['existentes']} existentes, "
        f"{resultado['erros']} erros."
    )


if __name__ == "__main__":
    main()

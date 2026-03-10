"""
Scraper de Documentos de RI - Plano&Plano (v3)
==============================================
Correcoes aplicadas sobre a v2:
  1) Seletor de ano: usa <select id="fano"> via Selenium Select, em vez
     de buscar <a> dentro do form lAnoLink (que nao existem).
  2) Extracao de documentos: le a estrutura real da tabela (table.tb-central)
     em vez de varrer todos os <a> da pagina. O tipo do documento fica em
     <td class="first titulo"> e o trimestre no <th class="tabelatt icone">.
  3) Filtro de documentos: aplicado no tipo da linha da tabela, nao no texto
     do <a> (que e sempre vazio — o link contem apenas um <img>).
  4) Stale element eliminado: Select re-encontra o elemento a cada ano,
     sem necessidade de re-obter a lista de elementos apos cada clique.

Estrutura da tabela na pagina:
  <table class="tb-central">
    <thead>
      <tr>
        <th class="tabelatt ano">2025</th>
        <th class="tabelatt icone">1T25</th>
        <th class="tabelatt icone">2T25</th>
        ...
      </tr>
    </thead>
    <tr>
      <td class="first titulo">ITR/DFP</td>
      <td class="icone"><a href="...mzfilemanager..."><img alt="ITR 1T25"></a></td>
      ...
    </tr>
    <tr>
      <td class="first titulo">Release de Resultados</td>
      <td class="icone"><a href="...mzfilemanager..."><img alt="Release 1T25"></a></td>
      ...
    </tr>
  </table>

Uso:
    python scrapers/planoeplano_ri.py
    python scrapers/planoeplano_ri.py 2025 2024 2023
    python scrapers/planoeplano_ri.py --tipo itr_dfp
    python scrapers/planoeplano_ri.py --tipo demonstracoes
"""

import os
import sys
import re
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.base_scraper import BaseScraper
from config.settings import PLANOEPLANO, SELENIUM


# Filtros de inclusao/exclusao por tipo de documento
FILTROS_POR_TIPO = {
    "release": {
        "incluir": [
            "release de resultado", "release resultado",
            "press release", "earnings release", "release",
        ],
        "prefixo_arquivo": "Release",
        "config_dir": "downloads_releases",
    },
    "itr_dfp": {
        "incluir": [
            "itr", "dfp",
        ],
        "prefixo_arquivo": "ITR",
        "config_dir": "downloads_itr_dfp",
    },
    "demonstracoes": {
        "incluir": [
            "demonstra", "dfs consolidadas", "financial statement",
        ],
        "prefixo_arquivo": "DF",
        "config_dir": "downloads_demonstracoes",
    },
}


class PlanoEPlanoRI(BaseScraper):
    """Scraper para documentos trimestrais da Plano&Plano."""

    def __init__(self, tipo_documento="release"):
        self.tipo_documento = tipo_documento
        filtros = FILTROS_POR_TIPO[tipo_documento]
        self.termos_incluir = filtros["incluir"]
        self.prefixo_arquivo = filtros["prefixo_arquivo"]

        super().__init__(
            nome_empresa=PLANOEPLANO["nome"],
            diretorio_downloads=PLANOEPLANO[filtros["config_dir"]],
        )
        self.url_central = PLANOEPLANO["ri_url"]

    # ------------------------------------------------------------------
    # Seletor de ano
    # ------------------------------------------------------------------

    def _obter_anos_disponiveis(self):
        """
        Le o <select id="fano"> e retorna lista de anos disponiveis (int),
        do mais recente para o mais antigo.
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select

        try:
            select_elem = self.driver.find_element(By.ID, "fano")
            anos = []
            for option in Select(select_elem).options:
                val = option.get_attribute("value").strip()
                if val.isdigit() and len(val) == 4:
                    anos.append(int(val))
                    self.logger.info(f"  Ano disponivel: {val}")
            self.logger.info(f"Select #fano: {len(anos)} anos encontrados.")
            return anos
        except Exception as e:
            self.logger.warning(f"Select #fano nao encontrado: {e}")
            return []

    def _selecionar_ano(self, ano):
        """
        Seleciona o ano no <select id="fano"> e aguarda a tabela recarregar.
        Usa Select a cada chamada, evitando stale element exception.
        """
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

    # ------------------------------------------------------------------
    # Extracao da tabela
    # ------------------------------------------------------------------

    def _linha_eh_documento_alvo(self, tipo_doc):
        """Verifica se a linha da tabela corresponde ao tipo de documento configurado."""
        tipo_lower = tipo_doc.lower()
        return any(termo in tipo_lower for termo in self.termos_incluir)

    def _extrair_documentos_da_tabela(self, ano):
        """
        Le a estrutura de table.tb-central e retorna lista de dicts com
        os documentos encontrados para o ano atual da tabela.

        Cada dict contem: tipo, trimestre, ano, alt, url.
        """
        from selenium.webdriver.common.by import By

        documentos = []

        tabelas = self.driver.find_elements(By.CSS_SELECTOR, "table.tb-central")
        if not tabelas:
            self.logger.warning("Nenhuma table.tb-central encontrada na pagina.")
            return documentos

        self.logger.info(f"Tabelas tb-central na pagina: {len(tabelas)}")

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

                if not self._linha_eh_documento_alvo(tipo_doc):
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

                        img_alt = ""
                        try:
                            img_alt = (
                                link.find_element(By.TAG_NAME, "img")
                                .get_attribute("alt") or ""
                            )
                        except Exception:
                            pass

                        trimestre = trimestres[i] if i < len(trimestres) else f"T{i+1}"
                        documentos.append({
                            "tipo": tipo_doc,
                            "trimestre": trimestre,
                            "ano": ano,
                            "alt": img_alt,
                            "url": href,
                        })
                        self.logger.info(
                            f"    ACEITO: {trimestre} | alt='{img_alt}'"
                        )
                    except Exception:
                        continue

        return documentos

    # ------------------------------------------------------------------
    # Nome do arquivo
    # ------------------------------------------------------------------

    def _gerar_nome_arquivo(self, release):
        """
        Gera nome de arquivo a partir do trimestre da tabela.
        Padrao: PlanoePlano_{Prefixo}_{ano}_{num}T.pdf
        Ex: trimestre="1T25" -> "PlanoePlano_ITR_2025_1T.pdf"
        """
        trimestre = release["trimestre"]
        ano = release["ano"]

        match = re.match(r"(\d)[Tt](\d{2,4})", trimestre)
        if match:
            num_t = match.group(1)
            ano_t = match.group(2)
            if len(ano_t) == 2:
                ano_t = f"20{ano_t}"
            return f"PlanoePlano_{self.prefixo_arquivo}_{ano_t}_{num_t}T.pdf"

        url_hash = abs(hash(release["url"])) % 100000
        return f"PlanoePlano_{self.prefixo_arquivo}_{ano}_{url_hash}.pdf"

    # ------------------------------------------------------------------
    # Execucao principal
    # ------------------------------------------------------------------

    def executar(self, anos=None):
        """
        Executa o scraper. Se nenhum ano for informado, processa todos os
        anos disponiveis no select da pagina.
        """
        tipo_label = self.tipo_documento.upper().replace("_", "/")
        self.logger.info("=" * 60)
        self.logger.info(f"INICIANDO SCRAPER DE {tipo_label} - PLANO&PLANO (v3)")
        self.logger.info(f"URL: {self.url_central}")
        self.logger.info("=" * 60)

        contadores = {"novos": 0, "existentes": 0, "erros": 0}

        try:
            self.iniciar_navegador()
            self.logger.info(f"Acessando {self.url_central}...")
            self.driver.get(self.url_central)
            self.aguardar(5)

            self.logger.info(f"Titulo da pagina: {self.driver.title}")

            anos_disponiveis = self._obter_anos_disponiveis()

            if not anos_disponiveis:
                # Fallback: extrai da pagina sem filtrar por ano
                self.logger.warning(
                    "Select de ano nao encontrado. "
                    "Extraindo documentos da pagina atual..."
                )
                documentos = self._extrair_documentos_da_tabela(ano=0)
                for doc in documentos:
                    nome = self._gerar_nome_arquivo(doc)
                    if self.arquivo_ja_existe(nome):
                        contadores["existentes"] += 1
                    elif self.baixar_pdf(doc["url"], nome):
                        contadores["novos"] += 1
                    else:
                        contadores["erros"] += 1
                    self.aguardar()

            else:
                if anos:
                    anos_disponiveis = [a for a in anos_disponiveis if a in anos]
                    self.logger.info(f"Filtrado para: {anos_disponiveis}")

                for ano in anos_disponiveis:
                    self.logger.info(f"\n--- Processando ano {ano} ---")

                    if not self._selecionar_ano(ano):
                        self.logger.warning(f"Falha ao selecionar ano {ano}, pulando.")
                        continue

                    documentos = self._extrair_documentos_da_tabela(ano)
                    self.logger.info(f"Documentos encontrados: {len(documentos)}")

                    for doc in documentos:
                        nome = self._gerar_nome_arquivo(doc)

                        if self.arquivo_ja_existe(nome):
                            self.logger.info(f"Ja existe: {nome}")
                            contadores["existentes"] += 1
                            continue

                        if self.baixar_pdf(doc["url"], nome):
                            contadores["novos"] += 1
                        else:
                            contadores["erros"] += 1

                        self.aguardar()

        except Exception as e:
            self.logger.error(f"Erro durante execucao: {e}", exc_info=True)
            contadores["erros"] += 1

        finally:
            self.fechar_navegador()

        self.gerar_relatorio(
            contadores["novos"], contadores["existentes"], contadores["erros"]
        )
        return contadores


if __name__ == "__main__":
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

    anos = [int(a) for a in args] if args else None

    scraper = PlanoEPlanoRI(tipo_documento=tipo_documento)
    resultado = scraper.executar(anos=anos)
    print(
        f"\nResumo ({tipo_documento}): {resultado['novos']} novos, "
        f"{resultado['existentes']} ja existentes, "
        f"{resultado['erros']} erros."
    )

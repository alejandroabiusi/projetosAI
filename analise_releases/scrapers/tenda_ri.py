"""
Scraper de Documentos de RI - Tenda (TEND3)
==========================================
O site de RI da Tenda (ri.tenda.com) nao usa plataforma MZ Group.
E um site Next.js com URLs estaticas por ano e links diretos para PDFs.
Isso permite usar requests + BeautifulSoup (sem Selenium), o que e
mais rapido e consome menos recursos.

Estrutura do site:
  - Pagina principal: /informacoes-financeiras/central-de-resultados
  - Paginas por ano: /informacoes-financeiras/central-de-resultados/2025
  - PDFs: /docs/Tenda-YYYY-MM-DD-hash.pdf (links relativos)
  - Tabela com linhas: Release de Resultados, ITR/DFP, Apresentacao, etc.

Uso:
    python scrapers/tenda_ri.py              # Releases de todos os anos
    python scrapers/tenda_ri.py 2025 2024    # Apenas anos especificos
    python scrapers/tenda_ri.py --tipo itr_dfp
    python scrapers/tenda_ri.py --tipo demonstracoes
"""

import os
import sys
import re
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.base_scraper import BaseScraper
from config.settings import TENDA, REQUESTS


# Filtros por tipo de documento
FILTROS_POR_TIPO = {
    "release": {
        "incluir": [
            "release de resultado", "release resultado",
            "press release", "earnings release",
        ],
        "excluir": [
            "itr", "dfp", "apresenta", "presentation", "audio", "webcast",
            "teleconferencia", "teleconferência", "prévia", "previa",
            "planilha", "spreadsheet", "demonstra", "formulario",
        ],
        "prefixo_arquivo": "Release",
        "config_dir": "downloads_releases",
    },
    "itr_dfp": {
        "incluir": [
            "itr", "dfp",
        ],
        "excluir": [
            "release", "press release", "apresenta", "presentation",
            "audio", "webcast", "teleconferencia", "teleconferência",
            "prévia", "previa", "planilha", "spreadsheet", "formulario",
            "demonstra",
        ],
        "prefixo_arquivo": "ITR",
        "config_dir": "downloads_itr_dfp",
    },
    "demonstracoes": {
        "incluir": [
            "demonstra", "dfs consolidadas", "financial statement",
        ],
        "excluir": [
            "release", "press release", "apresenta", "presentation",
            "audio", "webcast", "teleconferencia", "teleconferência",
            "prévia", "previa", "planilha", "spreadsheet", "formulario",
            "itr", "dfp",
        ],
        "prefixo_arquivo": "DF",
        "config_dir": "downloads_demonstracoes",
    },
}


class TendaRI(BaseScraper):
    """Scraper de documentos de RI da Tenda via requests + BeautifulSoup."""

    def __init__(self, tipo_documento="release"):
        self.tipo_documento = tipo_documento
        filtros = FILTROS_POR_TIPO[tipo_documento]
        self.termos_incluir = filtros["incluir"]
        self.termos_excluir = filtros["excluir"]
        self.prefixo_arquivo = filtros["prefixo_arquivo"]

        super().__init__(
            nome_empresa=TENDA["nome"],
            diretorio_downloads=TENDA[filtros["config_dir"]],
        )
        self.base_url = TENDA["ri_base_url"]
        self.url_central = TENDA["ri_url"]
        self.anos_configurados = TENDA["ri_anos"]
        self.session = requests.Session()
        self.session.headers.update(REQUESTS["headers"])

    def _fazer_request(self, url):
        """Faz GET e retorna o HTML parseado, ou None em caso de erro."""
        try:
            resp = self.session.get(url, timeout=REQUESTS["timeout"])
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            self.logger.error(f"Erro ao acessar {url}: {e}")
            return None

    def _descobrir_anos(self):
        """
        Acessa a pagina principal e descobre quais anos estao disponiveis
        na sidebar do site. Retorna lista de inteiros em ordem decrescente.
        """
        self.logger.info(f"Acessando pagina principal: {self.url_central}")
        soup = self._fazer_request(self.url_central)
        if not soup:
            self.logger.warning(
                "Nao conseguiu acessar pagina principal. Usando anos configurados."
            )
            return sorted(self.anos_configurados, reverse=True)

        anos = []
        # Busca links que contenham /central-de-resultados/YYYY
        for link in soup.find_all("a", href=True):
            match = re.search(r"/central-de-resultados/(\d{4})", link["href"])
            if match:
                ano = int(match.group(1))
                if ano not in anos:
                    anos.append(ano)

        if anos:
            anos.sort(reverse=True)
            self.logger.info(f"Anos encontrados no site: {anos}")
        else:
            anos = sorted(self.anos_configurados, reverse=True)
            self.logger.info(f"Usando anos configurados: {anos}")

        return anos

    def _eh_documento_alvo(self, texto):
        """Verifica se o texto da celda/linha indica o tipo de documento configurado."""
        texto_lower = texto.lower().strip()

        # Primeiro checa se e algo que queremos excluir
        for termo in self.termos_excluir:
            if termo in texto_lower:
                return False

        # Depois checa se e o tipo desejado
        for termo in self.termos_incluir:
            if termo in texto_lower:
                return True

        return False

    def _extrair_documentos_ano(self, ano):
        """
        Acessa a pagina de um ano especifico e extrai os links dos PDFs
        do tipo de documento configurado. Retorna lista de (trimestre, url_absoluta).
        """
        url_ano = f"{self.url_central}/{ano}"
        self.logger.info(f"Acessando: {url_ano}")

        soup = self._fazer_request(url_ano)
        if not soup:
            return []

        documentos = []

        # Estrategia 1: Tabela com linhas (padrao observado no site)
        tabelas = soup.find_all("table")
        for tabela in tabelas:
            linhas = tabela.find_all("tr")
            for linha in linhas:
                celulas = linha.find_all(["td", "th"])
                if not celulas:
                    continue

                # Primeira celula e o rotulo (ex: "Release de Resultados", "ITR/DFP")
                rotulo = celulas[0].get_text(strip=True)
                if not self._eh_documento_alvo(rotulo):
                    continue

                self.logger.info(f"  Linha encontrada: '{rotulo}'")

                # Demais celulas contem links dos PDFs por trimestre
                trimestre_idx = 0
                for celula in celulas[1:]:
                    trimestre_idx += 1
                    link = celula.find("a", href=True)
                    if link and link["href"].endswith(".pdf"):
                        href = link["href"]
                        # Converte URL relativa para absoluta
                        if href.startswith("/"):
                            url_pdf = f"{self.base_url}{href}"
                        elif href.startswith("http"):
                            url_pdf = href
                        else:
                            url_pdf = f"{self.base_url}/{href}"

                        tri_label = f"{trimestre_idx}T{ano}"
                        documentos.append((tri_label, url_pdf))
                        self.logger.info(f"    {tri_label}: {url_pdf}")

        # Estrategia 2: Se nao achou na tabela, busca links diretos
        if not documentos:
            self.logger.info("  Tabela nao encontrada, buscando links diretos...")
            for link in soup.find_all("a", href=True):
                texto = link.get_text(strip=True)
                href = link["href"]
                if not href.endswith(".pdf"):
                    continue
                if self._eh_documento_alvo(texto):
                    if href.startswith("/"):
                        url_pdf = f"{self.base_url}{href}"
                    elif href.startswith("http"):
                        url_pdf = href
                    else:
                        url_pdf = f"{self.base_url}/{href}"
                    documentos.append((texto, url_pdf))
                    self.logger.info(f"    Link direto: {texto} -> {url_pdf}")

        self.logger.info(f"  Total de documentos para {ano}: {len(documentos)}")
        return documentos

    def _gerar_nome_arquivo(self, tri_label, url, ano):
        """Gera nome padronizado: Tenda_{Prefixo}_XTAnno.pdf"""
        # Tenta extrair trimestre do label (ex: "1T2025")
        padrao = re.search(r"(\d)[Tt](\d{4})", tri_label)
        if padrao:
            return f"Tenda_{self.prefixo_arquivo}_{padrao.group(1)}T{padrao.group(2)}.pdf"

        # Fallback: usa hash da URL
        url_hash = abs(hash(url)) % 100000
        return f"Tenda_{self.prefixo_arquivo}_{ano}_{url_hash}.pdf"

    def executar(self, anos=None):
        """
        Executa o scraper. Parametro 'anos' permite filtrar anos especificos.
        Retorna dict com contadores (compativel com run_releases.py).
        """
        tipo_label = self.tipo_documento.upper().replace("_", "/")
        self.logger.info("=" * 60)
        self.logger.info(f"SCRAPER DE {tipo_label} - Tenda (TEND3)")
        self.logger.info(f"URL base: {self.url_central}")
        self.logger.info("=" * 60)

        contadores = {"novos": 0, "existentes": 0, "erros": 0}

        try:
            anos_disponiveis = self._descobrir_anos()

            if anos:
                anos_disponiveis = [a for a in anos_disponiveis if a in anos]
                self.logger.info(f"Filtrado para anos: {anos_disponiveis}")

            for ano in anos_disponiveis:
                self.logger.info(f"\n--- Tenda - Ano {ano} ---")
                documentos = self._extrair_documentos_ano(ano)

                for tri_label, url_pdf in documentos:
                    nome = self._gerar_nome_arquivo(tri_label, url_pdf, ano)

                    if self.arquivo_ja_existe(nome):
                        self.logger.info(f"Ja existe: {nome}")
                        contadores["existentes"] += 1
                        continue

                    if self.baixar_pdf(url_pdf, nome):
                        contadores["novos"] += 1
                    else:
                        contadores["erros"] += 1

                    self.aguardar(1)  # Pausa curta (site estatico, menos risco de bloqueio)

        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}", exc_info=True)
            contadores["erros"] += 1

        self.gerar_relatorio(contadores["novos"], contadores["existentes"], contadores["erros"])
        return contadores


def main():
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

    scraper = TendaRI(tipo_documento=tipo_documento)
    resultado = scraper.executar(anos=anos)
    print(
        f"\nResumo Tenda ({tipo_documento}): "
        f"{resultado['novos']} novos, "
        f"{resultado['existentes']} existentes, "
        f"{resultado['erros']} erros."
    )


if __name__ == "__main__":
    main()

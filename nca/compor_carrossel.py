"""
Protótipo de composição de carrossel para o projeto @naoculpeosastros.

Pipeline:
1. Carrega SVG base do signo (templates/{signo}.svg)
2. Renderiza SVG para PNG via resvg-py (renderizador escolhido por compatibilidade
   com mascaras e filtros do Canva, problema que cairosvg apresentou)
3. Compõe os textos dos 3 cards via Pillow sobre o PNG renderizado
4. Salva PNGs numerados em out/

Status: validado em 25 de abril de 2026 como prova de conceito funcional.
Pendências marcadas com TODO no código.

Dependências: pip install resvg-py pillow
"""

from io import BytesIO
from pathlib import Path

import resvg_py
from PIL import Image, ImageDraw, ImageFont

# ---------------------------------------------------------------------------
# Constantes do template (extraídas dos SVGs base do Canva)
# ---------------------------------------------------------------------------
LARGURA = 1080
ALTURA = 1350
COR_TEXTO = (236, 230, 216, 255)  # #ECE6D8, mesma cor do handle e ícone

# DM Serif Display fixada em 25/04/2026 após calibragem visual em 8 famílias.
# Empacotada em fonts/ para portabilidade (n8n cloud, CI, qualquer host sem
# instalar a fonte no sistema). Path relativo ao CWD, então o script precisa
# ser executado a partir da raiz nca/.
FONTE_PATH = 'fonts/DMSerifDisplay-Regular.ttf'

# Calibrado em 25/04/2026 via grade comparativa de 56, 64, 72, 80 px.
TAMANHO_FONTE = 64

# Margem horizontal interna ao card (em pixels)
MARGEM_X = 90

# Espaçamento entre linhas como fração do tamanho da fonte
ESPACAMENTO_LINHA = 0.3

# Mapa signo -> arquivo SVG base
TEMPLATES = {
    'aries': 'templates/Aries.svg',
    'touro': 'templates/Touro.svg',
    'gemeos': 'templates/Gemeos.svg',
    'cancer': 'templates/Cancer.svg',
    'leao': 'templates/Leao.svg',
    'virgem': 'templates/Virgem.svg',
    'libra': 'templates/Libra.svg',
    'escorpiao': 'templates/Escorpiao.svg',
    'sagitario': 'templates/Sagitario.svg',
    'capricornio': 'templates/Capricornio.svg',
    'aquario': 'templates/Aquario.svg',
    'peixes': 'templates/Peixes.svg',
    'geral': 'templates/Geral.svg',  # carta coringa para conteúdo não-signo
}


def renderizar_base(signo: str) -> Image.Image:
    """Carrega SVG do signo e renderiza para PNG na resolução final."""
    svg_path = TEMPLATES[signo]
    svg_str = Path(svg_path).read_text()
    png_bytes = resvg_py.svg_to_bytes(
        svg_string=svg_str,
        width=LARGURA,
        height=ALTURA,
    )
    return Image.open(BytesIO(bytes(png_bytes))).convert('RGBA')


def quebrar_linhas(texto: str, fonte: ImageFont.FreeTypeFont,
                   largura_maxima: int) -> list[str]:
    """Quebra o texto em linhas que cabem dentro da largura máxima."""
    draw = ImageDraw.Draw(Image.new('RGBA', (1, 1)))
    palavras = texto.split()
    linhas: list[str] = []
    linha_atual: list[str] = []
    for palavra in palavras:
        teste = ' '.join(linha_atual + [palavra])
        bbox = draw.textbbox((0, 0), teste, font=fonte)
        if bbox[2] - bbox[0] <= largura_maxima:
            linha_atual.append(palavra)
        else:
            if linha_atual:
                linhas.append(' '.join(linha_atual))
            linha_atual = [palavra]
    if linha_atual:
        linhas.append(' '.join(linha_atual))
    return linhas


def compor_card(base: Image.Image, texto: str,
                tamanho_fonte: int = TAMANHO_FONTE,
                font_path: str = FONTE_PATH,
                font_weight: int | None = None) -> Image.Image:
    """Compõe um card: SVG base + texto centralizado vertical e horizontalmente.

    font_weight só é usado para variable fonts com eixo wght (ex: Manrope,
    Space Grotesk, Playfair Display). Para fontes estáticas, ignore.
    """
    img = base.copy()
    draw = ImageDraw.Draw(img)
    fonte = ImageFont.truetype(font_path, tamanho_fonte)
    if font_weight is not None:
        fonte.set_variation_by_axes([font_weight])

    largura_util = LARGURA - 2 * MARGEM_X
    linhas = quebrar_linhas(texto, fonte, largura_util)

    espacamento = int(tamanho_fonte * ESPACAMENTO_LINHA)
    altura_linha = tamanho_fonte + espacamento
    altura_bloco = len(linhas) * altura_linha - espacamento

    # Centralizar verticalmente no canvas inteiro
    y_inicio = (ALTURA - altura_bloco) // 2

    for i, linha in enumerate(linhas):
        bbox = draw.textbbox((0, 0), linha, font=fonte)
        largura_linha = bbox[2] - bbox[0]
        x = (LARGURA - largura_linha) // 2
        y = y_inicio + i * altura_linha
        draw.text((x, y), linha, font=fonte, fill=COR_TEXTO)

    return img


def gerar_carrossel(signo: str, cards: list[str],
                    out_dir: str = 'out') -> list[str]:
    """Gera os 3 PNGs de um carrossel completo. Retorna a lista de caminhos."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    base = renderizar_base(signo)
    paths = []
    for i, texto in enumerate(cards, start=1):
        img = compor_card(base, texto)
        out_path = f'{out_dir}/{signo}_card{i}.png'
        img.save(out_path)
        paths.append(out_path)
    return paths


# ---------------------------------------------------------------------------
# Execução de teste
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    # Tirada de teste fictícia para validar o pipeline ponta a ponta
    cards_teste = [
        "Gêmeos diz que tem dois lados",
        "Os dois evitam responsabilidade",
        "Mas culpa o ascendente",
    ]
    paths = gerar_carrossel('gemeos', cards_teste)
    print(f"Gerados {len(paths)} cards:")
    for p in paths:
        print(f"  {p}")

"""
Valida o contraste do texto creme #ECE6D8 sobre as cinco paletas de fundo
do projeto, usando a fonte e tamanho fixados (DM Serif Display, 64 px).

Renderiza um card por elemento (mais o template Geral) com a mesma tirada
genérica, para que o único fator de variação seja a cor de fundo.

Saída: out/grade_contraste.png (visão lado a lado)
       out/contraste/<elemento>_<signo>.png (cards em tamanho real)
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from compor_carrossel import (
    ALTURA,
    FONTE_PATH,
    LARGURA,
    compor_card,
    renderizar_base,
)

TEXTO = 'Não é o astro, é a desculpa. Cada um inventa a própria.'

# (elemento, signo representativo)
CARDS = [
    ('Fogo (terracota)',         'aries'),
    ('Ar (azul-acinzentado)',    'aquario'),
    ('Água (verde-azulado)',     'escorpiao'),
    ('Terra (verde-oliva)',      'capricornio'),
    ('Geral (carvão)',           'geral'),
]

ESCALA = 0.5
THUMB_W = int(LARGURA * ESCALA)
THUMB_H = int(ALTURA * ESCALA)

ALTURA_ETIQUETA = 80
MARGEM_GRADE = 20
COR_FUNDO_GRADE = (30, 30, 30, 255)
COR_ETIQUETA = (236, 230, 216, 255)
TAMANHO_FONTE_ETIQUETA = 28


def main():
    Path('out/contraste').mkdir(parents=True, exist_ok=True)

    n = len(CARDS)
    largura_grade = MARGEM_GRADE + n * (THUMB_W + MARGEM_GRADE)
    altura_grade = ALTURA_ETIQUETA + THUMB_H + MARGEM_GRADE * 2

    grade = Image.new('RGBA', (largura_grade, altura_grade), COR_FUNDO_GRADE)
    draw = ImageDraw.Draw(grade)
    fonte_etiqueta = ImageFont.truetype(FONTE_PATH, TAMANHO_FONTE_ETIQUETA)

    for i, (rotulo, signo) in enumerate(CARDS):
        base = renderizar_base(signo)
        card = compor_card(base, TEXTO)

        full_path = f'out/contraste/{signo}.png'
        card.save(full_path)

        thumb = card.resize((THUMB_W, THUMB_H), Image.LANCZOS)
        x = MARGEM_GRADE + i * (THUMB_W + MARGEM_GRADE)
        y_thumb = ALTURA_ETIQUETA + MARGEM_GRADE

        bbox = draw.textbbox((0, 0), rotulo, font=fonte_etiqueta)
        largura_rotulo = bbox[2] - bbox[0]
        x_texto = x + (THUMB_W - largura_rotulo) // 2
        y_texto = MARGEM_GRADE + (ALTURA_ETIQUETA - TAMANHO_FONTE_ETIQUETA) // 2
        draw.text((x_texto, y_texto), rotulo, font=fonte_etiqueta,
                  fill=COR_ETIQUETA)

        grade.paste(thumb, (x, y_thumb))
        print(f'  {rotulo} | {signo} gerado ({full_path})')

    out_path = 'out/grade_contraste.png'
    grade.convert('RGB').save(out_path, 'PNG', optimize=True)
    print(f'\nGrade salva em {out_path}')
    print(f'Dimensões: {largura_grade}x{altura_grade}')


if __name__ == '__main__':
    main()

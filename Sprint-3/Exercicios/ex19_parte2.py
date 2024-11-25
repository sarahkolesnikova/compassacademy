import random

random_list = random.sample(range(500), 50)
ordenada = sorted(random_list)

mediana = (ordenada[24] + ordenada[25])/2

media = sum(random_list)/50

valor_minimo = min(random_list)

valor_maximo = max(random_list)

print(f'Media: {media}, Mediana: {mediana}, Mínimo: {valor_minimo}, \
Máximo: {valor_maximo}')
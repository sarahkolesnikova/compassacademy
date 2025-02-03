import random
import time
import os 
import names

random.seed(40)

nomes_unicos = 3000

nomes_aleatorios = 10000000

aux = []

for i in range(nomes_unicos):
    aux.append(names.get_full_name())

print ("Gerando {} nomes aleatórios...".format(nomes_aleatorios))

dados = []

for i in range(nomes_aleatorios):
    dados.append(random.choice(aux))

print ("Nomes gerados. Iniciando a gravação dos dados...")

with open ('nomes.txt', 'w') as arquivo:
    for nome in dados:
        arquivo.write(nome + '\n')

print ("Dados gravados com sucesso!")
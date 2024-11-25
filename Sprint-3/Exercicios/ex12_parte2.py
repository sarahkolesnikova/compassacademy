import math


def pot(num):
    return round(math.pow(num, 2))


def my_map(lista, funcao):
    resposta = []
    for i in lista:
        resposta.append(funcao(i))
    return resposta


entrada = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(my_map(entrada, pot))
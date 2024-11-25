numeros = "1,3,4,6,10,76"


def soma(numeros):
    inteiros = []
    for i in numeros.split(","):
        inteiros.append(int(i))
    return sum(inteiros)


print(soma(numeros))
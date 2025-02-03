import random

Lista = []

for item in range(250):
    numeros = random.randint(0, 1000)
    Lista.append(numeros)

Lista.reverse()

print(Lista)
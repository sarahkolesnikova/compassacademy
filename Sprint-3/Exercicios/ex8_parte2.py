a = ['maça', 'arara', 'audio', 'radio', 'radar', 'moto'] 
palindromo = [x for x in a if x == x[::-1]]
for x in a:
    if x == x[::-1]:
        print("A palavra:", x, "é um palíndromo")
    else:
        print("A palavra:", x, "não é um palíndromo")
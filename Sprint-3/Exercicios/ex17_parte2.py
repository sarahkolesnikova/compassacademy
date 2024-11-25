lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

def criar_listas(lista):
    n = len(lista) // 3
    primeira = lista[:n]
    segunda = lista[n:n*2]
    terceira = lista[n*2:]
    print(primeira, segunda, terceira)


criar_listas(lista)
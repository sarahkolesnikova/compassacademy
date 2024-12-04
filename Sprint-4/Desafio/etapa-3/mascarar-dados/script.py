import hashlib # Biblioteca para gerar hash
from functools import partial # Biblioteca para criar funções parciais

# Função para gerar hash SHA-1 de uma string
def gerar_hash_sha1(string):
    sha1 = hashlib.sha1() # Instanciando o objeto sha1
    sha1.update(string.encode('utf-8')) # Atualizando o objeto sha1 com a string
    return sha1.hexdigest() # Retornando o hash SHA-1 da string

# Função para processar a entrada do usuário
def processar_entrada(entrada):
    if entrada.lower() == 'sair': # Verificando se a entrada é 'sair'
        return None # Retornando None
    return f"Hash SHA-1: {gerar_hash_sha1(entrada)}" # Retornando o hash SHA-1 da entrada

# Função principal
def main(): 
    entradas = iter(partial(input, "Digite uma frase (ou digite 'sair' para encerrar): "), 'sair') 
    # Iterador para receber entradas do usuário
    resultados = map(processar_entrada, entradas) # Mapeando as entradas com a função processar_entrada
    for resultado in filter(None, resultados): # Filtrando os resultados diferentes de None
        print(resultado) # Imprimindo o resultado
if __name__ == "__main__": # Verificando se o script é o principal
    main() # Executando a função principal

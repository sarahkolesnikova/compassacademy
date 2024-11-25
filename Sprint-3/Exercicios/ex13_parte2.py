texto = open('arquivo_texto.txt')
conteudo = texto.read()
texto.close()

print(conteudo, end="")
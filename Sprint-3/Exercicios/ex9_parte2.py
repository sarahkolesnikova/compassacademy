# Você deve Utilizar a função enumerate().

primeirosNomes = ['Joao', 'Douglas', 'Lucas', 'José']
sobreNomes = ['Soares', 'Souza', 'Silveira', 'Pedreira']
idades = [19, 28, 25, 31]

nomeComepleto = (primeirosNomes[0]+ " "+ sobreNomes[0], 
                primeirosNomes[1]+ " "+ sobreNomes[1],
                primeirosNomes[2]+ " "+ sobreNomes[2],
                primeirosNomes[3]+ " "+ sobreNomes[3])
nome_idade = (nomeComepleto[0] +" está com " + str (idades[0]) + " anos",
            nomeComepleto[1] +" está com " + str(idades[1]) + " anos",
            nomeComepleto[2] +" está com " + str(idades[2]) + " anos",   
            nomeComepleto[3] +" está com " + str(idades[3]) + " anos" )
for count, dado in list(enumerate(nome_idade)):
    print (str(count) + " -", dado)
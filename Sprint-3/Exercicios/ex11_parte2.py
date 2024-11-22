import json
with open('person.json') as arquivo:
    dados = json.load(arquivo)
print (dados)
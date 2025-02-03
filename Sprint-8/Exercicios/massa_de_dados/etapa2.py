import csv

animais = ['tarântula', 'pangolin', 'narval', 'cavalo-marinho', 'camaleão', 
           'salamandra', 'ornitorrinco', 'morcego-vampiro', 'lêmure', 'panda-vermelho', 
           'hiena', 'iguana', 'cobra-real', 'raposa-do-ártico', 'toupeira-estrelada',
            'dragão-de-komodo', 'elefante', 'girafa','ovelha', 'porco']

animais.sort()

[print(animal) for animal in animais]

with open('animais.csv', 'w', newline='') as csvfile:
    escrever = csv.writer(csvfile)
    for animal in animais:
         escrever.writerow([animal])
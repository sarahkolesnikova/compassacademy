class Aviao():
    def __init__(self, modelo, velocidade_maxima, capacidade) -> None:
        self.modelo = modelo
        self.velocidade_maxima = velocidade_maxima
        self.cor = 'azul'
        self.capacidadade = capacidade


avioes = [Aviao('BOIENG456', 1500, 400), Aviao('Embraer Praetor 600', 863, 14),
          Aviao('Antonov An-2', 258, 12)]

for n in avioes:
    print(f'O avião de modelo {n.modelo} possui uma velocidade máxima de\
 {n.velocidade_maxima}, capacidade para {n.capacidadade} passageiros\
 e é da cor {n.cor}.')
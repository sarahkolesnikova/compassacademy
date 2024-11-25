class Calculo:
    def __init__(self, x, y) -> None:
        self.x = x
        self.y = y

    def soma(self):
        return f'Somando: {self.x}+{self.y} = {self.x + self.y}'

    def subtracao(self):
        return f'Subtraindo: {self.x}-{self.y} = {self.x - self.y}'


exemplo = Calculo(4, 5)
print(exemplo.soma())
print(exemplo.subtracao())
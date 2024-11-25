class Passaro():
    def __init__(self, ave='', som='') -> None:
        self.ave = ave
        self.som = som

    def voar(self):
        return f'{self.ave}\nVoando...'

    def emitir_som(self):
        return f'{self.ave} emitindo som...\n{self.som}'


class Pato(Passaro):
    def __init__(self) -> None:
        super().__init__('Pato', 'Quack Quack')


class Pardal(Passaro):
    def __init__(self) -> None:
        super().__init__('Pardal', 'Piu Piu')


pato1 = Pato()
pardal1 = Pardal()

print(pato1.voar())
print(pato1.emitir_som())
print(pardal1.voar())
print(pardal1.emitir_som())

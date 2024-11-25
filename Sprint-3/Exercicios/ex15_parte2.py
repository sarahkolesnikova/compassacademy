
class Lampada:
    def __init__(self, esta: bool = False) -> None:
        self.ligada = esta

    def liga(self):
        self.ligada = True

    def desliga(self):
        self.ligada = False

    def esta_ligada(self):
        return self.ligada


lampada_1 = Lampada()

lampada_1.liga()
print(f'A lâmpada está ligada? {lampada_1.esta_ligada()}')

lampada_1.desliga()
print(f'A lâmpada ainda está ligada? {lampada_1.esta_ligada()}')
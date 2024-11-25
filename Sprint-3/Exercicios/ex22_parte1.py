class Pessoa:
    def __init__(self, id) -> None:
        self.id = id
        self.__nome = None

    def _get_nome(self):
        return self.__nome

    def _set_nome(self, nome):
        self.__nome = nome

    nome = property(
        fget=_get_nome,
        fset=_set_nome
    )


pessoa = Pessoa(0)
pessoa.nome = 'Fulano De Tal'
print(pessoa.nome)

pessoa.__nome = 'João'
print(pessoa.nome)
print(pessoa.__nome)
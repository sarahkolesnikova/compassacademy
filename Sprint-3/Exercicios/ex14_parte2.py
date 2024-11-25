
def funcao(*args, **kwargs):
    for n in args:
        print(n)
    for m in kwargs.values():
        print(m)


funcao(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)

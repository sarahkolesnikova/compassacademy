# A função calcula_saldo recebe uma lista de tuplas, correspondendo a um conjunto de 
# lançamentos bancários. Cada lançamento é composto pelo seu valor (sempre positivo) 
# e pelo seu tipo (C - crédito ou D - débito). Abaixo apresentando uma possível entrada para a função.
# lancamentos = [
#    (200,'D'),
#    (300,'C'),
#    (100,'C')
# ]
# A partir dos lançamentos, a função deve calcular o valor final, somando créditos e subtraindo débitos. Na lista anterior, por exemplo, teríamos como resultado final 200.
# Além de utilizar lambdas, você deverá aplicar, obrigatoriamente, as seguintes funções na resolução:
# reduce (módulo functools) , map

def calcula_saldo(lancamentos) -> float:
    from functools import reduce
    return reduce(lambda x, y: x + y, map(lambda x: x[0] if x[1] == 'C' else -x[0], lancamentos))
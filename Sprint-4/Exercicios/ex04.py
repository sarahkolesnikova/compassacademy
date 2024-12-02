# A função calcular_valor_maximo deve receber dois parâmetros, chamados de operadores e operandos. 
# Em operadores, espera-se uma lista de caracteres que representam as operações matemáticas 
# suportadas (+, -, /, *, %), 
# as quais devem ser aplicadas à lista de operadores nas respectivas posições. 
# Após aplicar cada operação ao respectivo par de operandos, a função deverá retornar o maior 
# valor dentre eles.
# Veja o exemplo:
# Entrada
# operadores = ['+','-','*','/','+']
# operandos  = [(3,6), (-7,4.9), (8,-8), (10,2), (8,4)]
# Aplicar as operações aos pares de operandos [ 3+6 , -7-4.9, 8*-8 , 10/2 , 8+4 ] 
# Obter o maior dos valores
# 12
# Na resolução da atividade você deverá aplicar as seguintes funções: max , zip , map

def calcular_valor_maximo(operadores, operandos) -> float:
    def aplicar_operacao(op, nums):
        if op == '+':
            return nums[0] + nums[1]
        elif op == '-':
            return nums[0] - nums[1]
        elif op == '*':
            return nums[0] * nums[1]
        elif op == '/':
            return nums[0] / nums[1]
        elif op == '%':
            return nums[0] % nums[1]
        else:
            raise ValueError(f"Operador desconhecido: {op}")

    resultados = map(lambda x: aplicar_operacao(x[0], x[1]), zip(operadores, operandos))
    return max(resultados)
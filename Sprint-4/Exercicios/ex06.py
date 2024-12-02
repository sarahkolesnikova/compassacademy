def maiores_que_media(conteudo:dict)->list:
    # Calcula a média dos preços dos produtos
    media = sum(conteudo.values()) / len(conteudo)
    
    # Filtra os produtos cujo valor unitário é superior à média
    produtos_acima_da_media = [(produto, preco) for produto, preco in conteudo.items() if preco > media]
    
    # Ordena os produtos pelo preço em ordem crescente
    produtos_acima_da_media.sort(key=lambda x: x[1])
    
    return produtos_acima_da_media

# Exemplo de uso
conteudo = {
    "arroz": 4.99,
    "feijão": 3.49,
    "macarrão": 2.99,
    "leite": 3.29,
    "pão": 1.99
}

resultado = filtrar_produtos_acima_da_media(conteudo)
print(resultado)
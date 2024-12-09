# ETAPA 1

Nesta etapa, foi criada uma imagem docker a partir do código [carguru](/Sprint-4/Desafio/etapa-1/app/carguru.py) e depois executado um container. 

Criei o arquivo Dockerfile para definir as instruções de criação da imagem docker. Especifiquei a imagem base oficial a ser utilizada com  ````FROM python:3```` e assim obtive um ambiente python pronto para uso. Defini o diretório de trabalho do container com ```` WORKDIR /app ```` e com o comando ````COPY . . ```` garanti que todos os arquivos necessários para a aplicação funcionar estivessem disponíveis dentro do container. Com o comando ```` CMD [ "python", "app/carguru.py" ] ```` especifiquei o que seria executado dentro do container. 

Depois, para criar a imagem docker, coloquei o código em uma subpasta chamada **app** e executei o comando ```` docker build -t app .```` no terminal.

O conteúdo do arquivo Dockerfile do código carguru.py pode ser conferido [aqui](/Sprint-4/Desafio/etapa-1/Dockerfile).

Podemos ver a execução da criação da imagem docker na evidência a seguir:

![imagem_carguru](/Sprint-4/Evidencias/etapa1_imagem_carguru.png)

Logo após criar a imagem docker, executei o comando ```` docker run -it app ```` no terminal para criar o container que executa o código carguru na pasta app. 


Podemos ver a execução da criação do container na evidência a seguir:

![container_carguru](/Sprint-4/Evidencias/etapa1_container_carguru.png)

Então prossegui para a próxima etapa.

# ETAPA 2

Na etapa 2, era necessário responder à seguinte pergunta: **É possível reutilizar um container?** 

De forma suscinta a resposta é: sim, com o comando ```docker container start``` para acessar o container.

Para uma resposta mais detalhada, como pede a questão, acesse [aqui](/Sprint-4/Desafio/etapa-2/reutilizar_containers.md)

Então prossegui para a próxima etapa.

# ETAPA 3

Nesta etapa, aprendi a executar um container que permite receber inputs durante a sua execução. Para isso:

## 1.  Criei um script Python 

Este script implementa um algoritmo que processa as entradas do usuário, criptografa e imprimi os resultados até que o usuário digite "sair". 

Para criar esse algoritmo, usei três funções: **gerar_hash_sha1**, **processar_entrada** e **main**. 

A função **gerar_hash_sha1** gera um hash SHA-1 de uma string. O hash SHA-1 é um padrão de criptografia que produz um valor de dispersão de 160 bits e é tratado como um numero hexadecimal de 40 dígitos. Vale ressaltar que o SHA-1 está sendo descontinuado, pois foram encontradas falhas na segurança de sua criptografia.

A função **processar_entrada** recebe uma entrada do usuário e verifica se a entrada é a palavra **sair**. Se a entrada for **sair**, a função retorna **None**. Caso não seja, retorna a string inserida pelo usuário, formatada pela função **gerar_hash_sha1**.

A função **main** é a função principal do script. Ela usa a função ````partial```` do módulo ````functools```` do python, para criar uma função de entrada personalizada e instrui o usuário na ação a ser realizada (digitar uma frase ou a palavra **sair**), e a função ```` iter ```` para criar um iterador que continua solicitando entradas até reconhecer a palavra **sair**. Para aplicar a função **processar_entrada** à função **main**, foi usado o recurso ````map```` .  Um loop ````for```` foi usado com o recurso ````filter```` para remover valores **None** e assim poder imprimir os resultados válidos. Ao final, foi colocada uma verificação para garantir que a função **main** seja executada pelo script criado. 

O código do script pode ser consultado [aqui](/Sprint-4/Desafio/etapa-3/mascarar-dados/script.py).

Então prossegui para criar a imagem docker  a partir do script.

## 2. Criar imagem docker

Nesta subetapa, foi criada uma imagem docker a partir do código do script python "mascarar-dados". 

Para criar a imagem docker, coloquei o código *script* numa subpasta chamada **mascarar-dados** e segui os passos da etapa 1 para criar um arquivo Dockerfile para ele. Em seguida, executei o comando ```` docker build -t mascarar-dados . ```` no terminal.

O conteúdo do arquivo Dockerfile do código script.py [aqui](/Sprint-4/Desafio/etapa-3/Dockerfile).

Podemos ver a execução da criação da imagem docker na evidência a seguir:

![mascarar-dados](/Sprint-4/Evidencias/etapa3_imagem.png)

E a confirmação de que a imagem foi criada:

![confirmação-imagem](/Sprint-4/Evidencias/etapa3_conf_imagem.png)

Então prossegui para iniciar o container.

## 3. Iniciar um container

Logo após criar a imagem docker, executei o comando ```` docker run -it mascarar-dados ```` no terminal para criar o container que executa o código *script* na pasta mascarar-dados. 

Podemos ver a execução da criação do container na evidência a seguir:

![mascarar-dados-container](/Sprint-4/Evidencias/etapa3_container.png)

E a confirmação que o container foi criado:

![confirmação-container](/Sprint-4/Evidencias/etapa3_conf_container.png)

Com isso, encerrei o desafio da sprint 4. 
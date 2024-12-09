# ETAPA 1

Nesta etapa foi criada uma imagem docker a partir do código [carguru](/Sprint-4/Desafio/etapa-1/app/carguru.py) e depois executado um container. 

Para criar a imagem docker usei coloquei o código numa subpasta chamada **app** e executei o comando ```` docker build -t app ````

Podemos ver a execução da criação da imagem docker na evidência a seguir:

![imagem_carguru](/Sprint-4/Evidencias/etapa1_imagem_carguru.png)

Logo após criar a imagem docker, executei o comando ```` docker run -it app ```` para criar o container que executa o código carguru na pasta app. 

Podemos ver a execução da criação do container na evidência a seguir:

![container_carguru](/Sprint-4/Evidencias/etapa1_container_carguru.png)

Então prossegui para a próxima etapa.

# ETAPA 2

Na etapa 2 era necessário responder a seguinte pergunta: **É possível reutilizar um container?** 

De forma suscinta a resposta é: sim, com o comando ```docker container start``` para acessar o container.

Para uma resposta mais detalhada, como pede a questão pede, acesse [aqui](/Sprint-4/Desafio/etapa-2/reutilizar_containers.md)

# ETAPA 3

## 1.  Criar script Python 

## 2. Criar imagem docker

## 3. Iniciar um container

## 4. Conclusão
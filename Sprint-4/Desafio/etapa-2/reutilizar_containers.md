# É possível reutilizar containers? 
## Em caso positivo, apresente o comando necessário para reiniciar um dos containers parados em seu ambiente Docker. Não sendo possível reutilizar, justifique sua resposta.

RESPOSTA:

Sim, é possível reutilizar containers. Para reiniciar um container, pode-se usar o comando:

````docker container start -ai carguru ````

(esse comando acessa o container)

```` touch carguru2.txt ```` 

(esse comando cria um novo arquivo no mesmo container)

Para reutilizar containers é importante renomeá-los para algo relacionado ao que se está desenvolvendo. 
Assim fica mais fácil de identificá-lo no ambiente em que ele se encontra. 
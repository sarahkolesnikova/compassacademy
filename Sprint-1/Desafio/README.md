
### ETAPA 1 - Fundamentos de Linux 
Iniciei o desafio da **Sprint 1** pela compreensão da situação problema proposta.  Minha squad nos primeiros dias se dedicou a que todos da equipe conseguissem  iniciar o desafio e, como síntese da discussão em grupo, elaborei um pseudocódigo em linhagem natural conforme imagem abaixo. 

![Evidencia 1](../Evidencias/pseudocodigo.png)

Em seguida, fiz a preparação do ambiente para realização do desafio, conforme imagem abaixo.  

![Evidencia 2](../Evidencias/preparacao.png)

Concluída a preparação, debrucei-me na escrita do **Script 1** "processamento_de_vendas.sh" que geraria os arquivos para o relatório de vendas.  A princípio decidi usar os comando ensinados no curso para gerar os diretórios.  Após a execução, acrescentei uma estrutura de decisão ao código, devido ao não processamento do agendamento no segundo dia no horário das 15:27.  Dessa forma, foi necessário reagendar o script para outro horário no mesmo dia.  Logo após o processamento, reajustei o horário do agendamento no terminal. 

O código do **Script 1** pode ser conferido em [Script 1](../Evidencias/processamento_de_vendas.sh)
Para agendamento no terminal, foi usado o comando *crontab -e* conforme imagem a abaixo.

![Evidencia 3](../Evidencias/crontabe.png)

Após os quatro dias de agendamento, elaborei o **Script 2** disponivel em [Script 2](Desafio/etapa-1) e finalizei a primeira etapa do desafio.  Os arquivos gerados (relatórios, csv's, códigos e imagens) podem ser acessador em [Evidencias](../Evidencias/).  A imagem a seguir mostra via terminal o que foi gerado como "entregaveis".

![Evidencia 4](../Evidencias/resultados_desafio.png)

OBS: Apesar de agendar o **Script 1** para as 15:27 como pedido, apenas no dia 23/10/2024  o arquivo foi gerado nesse horário e não consegui identificar motivo do *delay* nos dias 21 e 24/10 já que o crontab aparecia ativo (ao consultar via terminal). 
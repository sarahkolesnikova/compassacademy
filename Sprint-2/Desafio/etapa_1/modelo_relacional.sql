/* Ao fazer o esquema relacional, deve-se considerar que o sistema
é capaz de armazenar informações sobre os seguintes elementos construtores:
- Locação
- Cliente
- Vendedor
- Carro
- Combustível
Então, o esquema relacional deve conter as seguintes tabelas:
- Locação
- Cliente
- Vendedor
- Carro
- Combustível
Assim, a base de dados foi renomeada para dados e para iniciar
a normalização foram criadas as tabelas usando os seguintes comandos:*/

-- locacao definição

 CREATE TABLE locacao 
(idLocacao INTEGER PRIMARY KEY AUTOINCREMENT,
 dataLocacao DATE NOT NULL,
 horaLocacao TIME NOT NULL,
 qtdDiaria INT NOT NULL,
 vlrDiaria DECIMAL NOT NULL,
 dataEntrega DATE NOT NULL,
 horaEntrega TIME NOT NULL,
 idVendedor INT NOT NULL,
 idCliente INT NOT NULL,
 idCarro INT NOT NULL,
 FOREIGN KEY (idVendedor) REFERENCES vendedor (idVendedor),
 FOREIGN KEY (idCliente) REFERENCES cliente (idCliente), 
 FOREIGN KEY (idCarro) REFERENCES carro (idCarro)
 );

-- cliente definição
 
CREATE TABLE cliente ( 
			idCliente INTEGER PRIMARY KEY AUTOINCREMENT,
			nomeCliente VARCHAR NOT NULL,
			cidadeCliente VARCHAR NOT NULL,
			estadoCliente VARCHAR NOT NULL,
			paisCliente VARCHAR NOT NULL
);

-- vendedor definição

CREATE TABLE vendedor (
		idVendedor  INTEGER PRIMARY KEY AUTOINCREMENT,
		nomeVendedor VARCHAR NOT NULL,
		sexoVendedor SMALLINT CHECK (sexoVendedor IN (0, 1)),
		estadoVendedor VARCHAR NOT NULL
);

-- carro definição

CREATE TABLE carro (
idCarro INTEGER PRIMARY KEY AUTOINCREMENT,
 km INT NOT NULL,
 classi VARCHAR NOT NULL,
 marca VARCHAR NOT NULL,
 modelo VARCHAR NOT NULL,
 ano INT NOT NULL,
 idCombustivel INT NOT NULL,
 FOREIGN KEY (idCombustivel)
 REFERENCES 
 combustivel (idCombustivel)
 );

 -- combustivel definição

CREATE TABLE combustivel (
		idCombustivel INTEGER PRIMARY KEY AUTOINCREMENT,
		tipoCombustivel VARCHAR NOT NULL
);

/*Após a criação das tabelas, foi necessário inserir o conjunto de instâncias iniciais de cada uma delas, 
para isso foram utilizados os seguintes comandos:*/

-- cliente inserção

INSERT INTO cliente (
			idCliente, nomeCliente,
			cidadeCliente, estadoCliente, paisCliente
)
SELECT DISTINCT idCliente, nomeCliente,
			cidadeCliente, estadoCliente, paisCliente 
FROM dados 

-- vendedor inserção

INSERT INTO vendedor (
		idVendedor, nomeVendedor,
		sexoVendedor, estadoVendedor
)
SELECT DISTINCT idVendedor, nomeVendedor,
		sexoVendedor, estadoVendedor
FROM dados 

/* obs. o campo sexoVendedor foi definido como SMALLINT ao invés do padrão definido pela ISO/IEC 5218,
então não é possível converter para um formato "STRING" com "M" ou "F" para ser compreensivel 
ao usuário sem saber a especificidade do projeto */

-- combustivel inserção

INSERT INTO combustivel (idCombustivel, tipoCombustivel)
SELECT DISTINCT idCombustivel, tipoCombustivel
FROM dados 

-- carro inserção

INSERT INTO carro (idCarro, km, classi, marca, modelo, ano, idCombustivel)
SELECT DISTINCT dados.idCarro,
			MAX (dados.kmCarro),
			dados.classiCarro,
			dados.marcaCarro,
			dados.modeloCarro,
			dados.anoCarro,
			combustivel.idCombustivel	
FROM dados JOIN combustivel ON dados.idcombustivel = combustivel.idCombustivel 
GROUP BY idCarro;

/* obs. no campo kmCarro foi utilizado a função MAX para garantir que o valor inserido seja 
o último km registrado como informação relevante. Porém, caso o cliente queira comparar os valores
de km de cada carro na locação e na entrega é possível ainda criar uma outra tabela apenas para o atributo 
multivalorado kmCarro relacionada a tabela carro.*/

-- locacao inserção
 INSERT INTO locacao (
 					idLocacao, dataLocacao, 
 					horaLocacao, qtdDiaria, 
 					vlrDiaria, dataEntrega,
 					horaEntrega, idVendedor,
 					idCliente, idCarro
 					)
 SELECT DISTINCT dados.idDados, dados.dataLocacao, 
 				dados.horaLocacao, dados.qtdDiaria, 
 				dados.vlrDiaria, dados.dataEntrega,
 				dados.horaEntrega, vendedor.idVendedor,
 				cliente.idCliente, carro.idCarro
 FROM dados 
 JOIN vendedor ON dados.idVendedor = vendedor.idVendedor 
 JOIN cliente ON dados.idCliente = cliente.idCliente 
 JOIN carro ON dados.idCarro = carro.idCarro 
 
/* correção do formato da data nas colunas "dataLocacao" e "dataEntrega", 
pois o DBeaver não reconhece o formato DATATIME, apenas DATE como SUBSTRING.*/

 UPDATE locacao 
SET dataLocacao = SUBSTRING (dataLocacao, 1, 4) || '-' || SUBSTRING(dataLocacao, 5, 2) || '-' ||SUBSTRING(dataLocacao, 7, 2)

UPDATE locacao
SET dataEntrega = SUBSTRING (dataEntrega, 1, 4) || '-' || SUBSTRING(dataEntrega, 5, 2) || '-' ||SUBSTRING(dataEntrega, 7, 2)

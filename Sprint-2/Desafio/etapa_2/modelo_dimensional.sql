/* Com base no modelo relacional foi possivel criar o modelo dimensional
   com as tabelas de dimensão e fato, conforme o script abaixo:*/

-- tabela fato locacao por View 

CREATE VIEW ft_locacao AS 
 SELECT idLocacao, 
        idVendedor, 
        idCliente, 
        idCarro, 
        dataLocacao,
        qtdDiaria, 
        vlrDiaria 
FROM locacao;

-- tabela dimensão data por View:

CREATE VIEW dim_data AS
SELECT
        dataLocacao,
        horaLocacao,
        dataEntrega,
        horaEntrega
FROM locacao;

 -- tabela dimensão vendedor por View:

 CREATE VIEW dim_vendedor AS
SELECT
        idVendedor as id,
        nomeVendedor as nome,
        sexoVendedor as sexo,
        estadoVendedor as estado 
FROM vendedor;

-- tabela dimensão cliente por View: 

CREATE VIEW dim_cliente AS
SELECT
        idCliente as id,
        nomeCliente as nome,
        cidadeCliente as cidade,
        estadoCliente as estado,
        paisCliente as pais
FROM cliente

-- tabela dimensão carro por View:

CREATE VIEW dim_carro AS
SELECT
        carro.idCarro as id,
        carro.km,
        carro.classi,
        carro.marca,
        carro.modelo,
        carro.ano,
        carro.idCombustivel,
        combustivel.tipoCombustivel
FROM carro JOIN combustivel ON carro.idCombustivel = combustivel.idCombustivel;
WITH t3 AS (
	SELECT autor,
		COUNT (titulo) AS qtd
	FROM livro 
	GROUP BY autor
	)
SELECT 
	t2.codautor,
	t2.nome,
	(SELECT MAX (qtd)) AS quantidade_publicacoes
FROM livro AS t1
LEFT JOIN autor AS t2
	ON t1.autor = t2.codautor 
LEFT JOIN t3 
	ON t1.autor = t3.autor 
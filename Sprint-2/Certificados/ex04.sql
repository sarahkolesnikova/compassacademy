SELECT 
	t1.nome,
	t1.codautor,
	t1.nascimento,
	COUNT (T2.autor) AS quantidade
FROM autor AS t1 LEFT JOIN livro AS t2
ON t1.codautor = t2.autor 
GROUP BY t1.codautor 
ORDER BY t1.nome 
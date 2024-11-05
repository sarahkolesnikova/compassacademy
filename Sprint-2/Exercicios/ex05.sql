SELECT 
	t2.nome
FROM livro AS t1
LEFT JOIN autor AS t2 
	ON t2.codautor = t1.autor 
LEFT JOIN editora AS t3 
	ON t3.codeditora = t1.editora 
LEFT JOIN endereco  AS t4 
	ON t4.codendereco = t3.endereco 
WHERE t4.estado NOT IN ('RIO GRANDE DO SUL', 'PARANÁ')
GROUP BY t1.autor
ORDER BY t1.autor  
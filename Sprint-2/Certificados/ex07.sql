
SELECT 
    t1.nome
FROM autor AS t1
 LEFT JOIN livro AS t2
    ON t1.codautor = t2.autor
GROUP BY t1.codautor 
HAVING COUNT (t2.titulo) = 0
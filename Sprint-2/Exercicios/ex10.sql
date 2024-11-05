WITH maior AS (
	SELECT 
		cdpro,
		COUNT (cdpro) AS contagem 
	FROM tbvendas
	WHERE '2014-02-03' < dtven < '2018-02-02'
)
SELECT 
	t1.cdpro,
	t1.nmpro
FROM tbvendas AS t1
LEFT JOIN maior
	ON t1.cdpro = maior.cdpro
GROUP BY t1.nmpro
HAVING contagem = (SELECT MAX (contagem) FROM maior) 
	AND status NOT IN ('Em aberto', 'Cancelado')

WITH maior AS (
SELECT cdvdd,
	COUNT (tbvendas.cdpro) AS contagem
FROM tbvendas
)
SELECT t2.cdvdd, 
	t2.nmvdd
FROM tbvendas AS t1
LEFT JOIN tbvendedor AS t2
	ON t1.cdvdd = t2.cdvdd 
LEFT JOIN maior 
	ON t2.cdvdd = maior.cdvdd
GROUP BY t1.cdvdd
HAVING contagem = (SELECT MAX (contagem) FROM tbvendas)
	AND t1.status NOT IN ('Em aberto', 'Cancelado')
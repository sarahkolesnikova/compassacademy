SELECT 
		dep.cddep,
		dep.nmdep,
		dep.dtnasc,
	    SUM (tbvendas.qtd*tbvendas.vrunt) AS valor_total_vendas
FROM tbvendas 
 JOIN tbvendedor
	ON 	tbvendas.cdvdd = tbvendedor.cdvdd 
 JOIN tbdependente AS dep
		ON	tbvendedor.cdvdd = dep.cdvdd 
WHERE tbvendas.status ='Concluído'
GROUP BY tbvendas.cdvdd 
ORDER BY valor_total_vendas ASC 
LIMIT 1
SELECT 
	cdpro,
	nmcanalvendas,
	nmpro,
  SUM (qtd) AS quantidade_vendas
FROM tbvendas 
WHERE status = 'Concluído'
group by nmcanalvendas, cdpro
order by quantidade_vendas
LIMIT 10
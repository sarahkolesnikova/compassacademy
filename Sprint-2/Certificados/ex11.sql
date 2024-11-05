WITH soma AS (
	SELECT tbvendas.cdcli,
		SUM (tbvendas.qtd*tbvendas.vrunt) AS gasto
	FROM tbvendas
	GROUP BY tbvendas.cdcli
	HAVING tbvendas.status NOT IN ('Em aberto', 'Cancelado')
	AND tbvendas.nmcanalvendas = 'Matriz' 
)
SELECT
		tbvendas.cdcli,
		tbvendas.nmcli,
		soma.gasto
FROM tbvendas
LEFT JOIN soma 
	ON soma.cdcli = tbvendas.cdcli 
WHERE gasto = (SELECT MAX (gasto) FROM soma)
GROUP BY tbvendas.cdcli
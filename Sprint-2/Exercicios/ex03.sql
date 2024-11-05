SELECT 
	COUNT (ed.nome) as quantidade,
	ed.nome,
	en.estado,
	en.cidade
FROM livro LEFT JOIN editora ed on editora = ed.codeditora 
	LEFT JOIN endereco en on ed.endereco = en.codendereco
GROUP BY ed.nome
ORDER BY quantidade DESC 

LIMIT 5
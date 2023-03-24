-- Filter Feeders operations
SELECT a.*  , 
CASE  WHEN "ValorLiquido" = 0 AND "TipoOperacao" in (1,110) 
	THEN a."Quantidade" * d.cota
	WHEN "ValorLiquido"= 0  AND "TipoOperacao" in (4,5) 
	THEN a."Quantidade" * -d.cota
	WHEN "ValorLiquido" !=0 AND "TipoOperacao" in (1,110)
	THEN "ValorLiquido"
	WHEN "ValorLiquido" != 0  AND "TipoOperacao" in (4,5)
	THEN -"ValorLiquido"
	END as UpdatedValorLiquido 
FROM cotista_op a
LEFT JOIN funds c
ON a."IdCotista" = c.cotista_id
JOIN funds_values  as d
ON d.'IdCarteira' = a."IdCarteira"
AND d.date = '{{ds}}'
WHERE c.cotista_id is NULL

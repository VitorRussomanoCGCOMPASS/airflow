WITH subquery as 
(SELECT * from 
( SELECT a.* FROM cotista_op a
LEFT JOIN funds c
ON a."IdCotista" = c.cotista_id
WHERE c.cotista_id is NULL 
AND "TipoOperacao" in (4, 5)
AND "DataConversao" <= '{{ds}}'
AND "ValorLiquido" = 0 ) tbl1
join
(SELECT * from funds_values
JOIN funds 
ON funds.britech_id = funds_values.funds_id
WHERE date <= '{{ds}}')  tbl2
ON tbl1."CnpjCarteira" = tbl2.cnpj)
UPDATE cotista_op
SET ("ValorLiquido","ValorBruto") = ROW(subquery.cota * subquery."Quantidade",subquery.cota * subquery."Quantidade")
FROM subquery
WHERE cotista_op."IdOperacao" = subquery."IdOperacao"
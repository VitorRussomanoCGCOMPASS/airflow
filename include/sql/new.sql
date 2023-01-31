-- Select operations from Cotista_op where the CotistaId does not match any of the cotista_id in the funds table.
SELECT  *
FROM    cotista_op l
LEFT JOIN funds r
ON      r.cotista_id = l."IdCotista"

-- Where ValorBruto is null, multiply the quantidade with the most recent valor cota given a match of cnpjcarteira e cnpj do fundo


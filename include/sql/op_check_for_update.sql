SELECT 
	CASE WHEN EXISTS (
SELECT a.* FROM cotista_op a
LEFT JOIN funds c
ON a."IdCotista" = c.cotista_id
WHERE c.cotista_id is NULL 
AND "TipoOperacao" in (4, 5)
AND "DataConversao" = '{{ds}}'
AND "ValorLiquido" = 0 
		)
		THEN 1
		ELSE 0
	END

-- Need to find if any operations that are not from feeders need update.
-- Meaning that op DataCotizacao is today and ValorLiquido is 0 and TipoOperacao is 4
-- To check if its not from feeders. Operations IdCotista must not match any funds cotista_id.


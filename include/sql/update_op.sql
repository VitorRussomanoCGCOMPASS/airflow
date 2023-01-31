-- Select DataOperacao and IdCotista from Cotista_op where the CotistaId does not match any of the cotista_id in the funds table. 
-- Meaning that it is filtered by Feeders operations. Also, only look for TipoOperacoes 4 and 110.

SELECT  DISTINCT ON ("DataOperacao")
to_char("DataOperacao",'YYYY-MM-DD')
FROM    cotista_op l
LEFT JOIN funds r
ON      r.cotista_id = l."IdCotista"
WHERE "TipoOperacao"  in (4,110)
AND CAST("DataConversao" as DATE) = '{{ds}}'

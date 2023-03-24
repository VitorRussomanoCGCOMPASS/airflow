SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type from funds a 
 JOIN funds_values c 
 ON a.britech_id = c.'IdCarteira' 
 WHERE britech_id in ({{params.ids}})
 AND date = inception_date 
 OR date = '{{macros.anbima_plugin.forward(ds,-1)}}'
AND britech_id in ({{params.ids}})
)  
, lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
FROM Worktable)
SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
COALESCE(("CotaFechamento" - inception_cota)/inception_cota )*100 AS "RentabilidadeInicio"
FROM lagged) as tb
WHERE "RentabilidadeInicio" !=0
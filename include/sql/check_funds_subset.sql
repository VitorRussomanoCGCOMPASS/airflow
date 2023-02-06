
WITH WorkTable(id) as (select britech_id from funds WHERE britech_id in ({{params.ids}}))
SELECT ( SELECT COUNT(*)
          FROM funds_values
         WHERE funds_id IN ( SELECT id FROM WorkTable) 
	   AND date ='{{macros.anbima_plugin.forward(ds,-1)}}') =
	   		(SELECT COUNT(*) FROM WorkTable)

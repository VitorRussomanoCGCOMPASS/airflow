-- Check if all funds have value in d-1
SELECT ( SELECT COUNT(*)
          FROM funds_values
         WHERE funds_id IN ( SELECT britech_id FROM funds) 
	   AND date ='{{macros.anbima_plugin.forward(ds,-1)}}') =
	   		(SELECT COUNT(*) FROM funds)

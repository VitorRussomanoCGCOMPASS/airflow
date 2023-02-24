-- Check if all funds have value in d-1
SELECT ( SELECT COUNT(*)
          FROM funds_values
         WHERE funds_id IN ( SELECT britech_id FROM funds WHERE status='ativo') 
	   AND date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}') =
	   		(SELECT COUNT(*) FROM funds WHERE status='ativo')
select indexes.id from indexes WHERE  indexes.id NOT IN  ( SELECT indexes.id FROM indexes_values WHERE CAST(date AS DATE) =  '{{ds}}')

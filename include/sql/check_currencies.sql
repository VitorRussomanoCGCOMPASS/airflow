select currency.id from currency WHERE  currency.id NOT IN  ( SELECT currency.id FROM currency_values WHERE CAST(date AS DATE) =  '{{ds}}')
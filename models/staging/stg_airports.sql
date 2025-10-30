SELECT *
FROM {{source("flights_data","regions")}}
JOIN {{source("flights_data","airports")}} USING (country)
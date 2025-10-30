SELECT 
	origin,
	a1.city AS o_city,
	a1.country AS o_country,
	a1.name AS o_name,
	dest,
	a2.city AS d_city,
	a2.country AS d_country,
	a2.name AS d_name,
	COUNT(flight_number) AS flights,
	COUNT(DISTINCT tail_number)	AS unique_planes,
	COUNT(DISTINCT airline)	AS unique_airlines,
	AVG(arr_time - dep_time) AS avg_duration,
	AVG(arr_delay_interval) AS avg_arrival_delay,
	SUM(cancelled) AS cancelled_flights,
	SUM(diverted) AS diverted_flights
FROM {{ ref('prep_flights') }} f
JOIN  {{ ref('prep_airports') }} a1 ON a1.faa = f.dest
JOIN  {{ ref('prep_airports') }} a2 ON a2.faa = f.origin
GROUP BY origin, a1.city, a1.country, a1.name, dest, a2.city, a2.country, a2.name
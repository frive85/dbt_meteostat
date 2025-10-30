WITH uac AS (
SELECT 
	faa,
	COUNT(DISTINCT origin) AS arrivals
FROM {{ ref('prep_airports') }}
JOIN {{ ref('prep_flights') }} ON faa = dest
GROUP BY faa
), af_calc AS(
SELECT
	faa,
	COUNT(*) AS actual_flights
	FROM {{ ref('prep_airports') }}
	JOIN {{ ref('prep_flights') }} ON faa = dest
	WHERE cancelled = 0 AND diverted = 0
	GROUP BY faa
), plane_calc AS(
SELECT
	faa,
	COUNT(DISTINCT tail_number) AS airplanes
	FROM {{ ref('prep_airports') }}
	JOIN {{ ref('prep_flights') }} ON faa = dest
	GROUP BY faa
), airline_calc AS(
SELECT
	faa,
	COUNT(DISTINCT airline) AS airlines
	FROM {{ ref('prep_airports') }}
	JOIN {{ ref('prep_flights') }} ON faa = dest
	GROUP BY faa
)
SELECT
	faa,
    name,
    city,
    country,
    uac.arrivals,
    COUNT(DISTINCT dest) AS departures,
    COUNT(*) AS flights_planned,
    SUM(cancelled) AS cancelled_flights,
    SUM(diverted)AS diverted_flights,
    actual_flights,
    airplanes,
    airlines
FROM {{ ref('prep_airports') }}
JOIN {{ ref('prep_flights') }} ON faa = origin
JOIN uac USING (faa)
JOIN af_calc USING (faa)
JOIN plane_calc USING (faa)
JOIN airline_calc USING (faa)
GROUP BY faa, name, city, country, uac.arrivals, actual_flights, airplanes,airlines
ORDER BY flights_planned DESC
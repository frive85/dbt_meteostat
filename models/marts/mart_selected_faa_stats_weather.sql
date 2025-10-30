WITH uac AS (
SELECT 
	date,
	COUNT(DISTINCT origin) AS arrivals
FROM {{ ref('prep_flights') }}
JOIN {{ ref('prep_weather_daily') }} ON date = flight_date
GROUP BY date
), af_calc AS(
SELECT
	date,
	COUNT(*) AS actual_flights
FROM {{ ref('prep_flights') }}
JOIN {{ ref('prep_weather_daily') }} ON date = flight_date
WHERE cancelled = 0 AND diverted = 0
GROUP BY date
), plane_calc AS(
SELECT
	date,
	COUNT(DISTINCT tail_number) AS airplanes
FROM {{ ref('prep_flights') }}
JOIN {{ ref('prep_weather_daily') }} ON date = flight_date 
GROUP BY date
), airline_calc AS(
SELECT
	date,
	COUNT(DISTINCT airline) AS airlines
FROM {{ ref('prep_flights') }}
JOIN {{ ref('prep_weather_daily') }} ON date = flight_date 
GROUP BY date
)
SELECT
	date,
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
    airlines,
    MIN(min_temp_c) min_temperature,
    MAX(max_temp_c) max_temperature,
    SUM(precipitation_mm) AS precipitation,
    SUM(max_snow_mm) AS snow_fall,
    AVG(avg_wind_speed) AS avg_wind_speed,
    AVG(avg_peakgust) AS peakgust
FROM {{ ref('prep_flights') }}
JOIN {{ ref('prep_airports') }} ON faa = origin
JOIN {{ ref('prep_weather_daily') }} ON date = flight_date 
JOIN uac USING (date)
JOIN af_calc USING (date)
JOIN plane_calc USING (date)
JOIN airline_calc USING (date)
GROUP BY faa, date, name, city, country, uac.arrivals, actual_flights, airplanes,airlines
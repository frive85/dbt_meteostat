WITH mod_layer_1 AS (
SELECT 
    *,
    DATE_PART('day', date) AS date_day,
    DATE_PART('month', date) AS date_month,
    DATE_PART('year', date) AS date_year,
    TO_CHAR(date, 'FMmonth') AS month_name,
    TO_CHAR(date, 'FMday') AS weekday,
    TO_CHAR(date, 'IW') AS cw
FROM {{ ref('stg_weather_daily') }}
), moda_layer_2 AS (
SELECT
    *,
    CASE
        WHEN month_name IN ('march','april','may') THEN 'spring'
        WHEN month_name IN ('june','july','august') THEN 'summer'
        WHEN month_name IN ('september','october','november') THEN 'spring'
        WHEN month_name IN ('december','january','february') THEN 'winter'
    END AS season
FROM mod_layer_1
)
SELECT 
    airport_code,
    station_id,
    date,
    avg_temp_c,
    min_temp_c,
    max_temp_c,
    precipitation_mm,
    sun_minutes,
    max_snow_mm,
    avg_wind_speed,
    avg_peakgust,
    date_day,
    date_month,
    date_year,
    cw,
    month_name,
    weekday,
    season
FROM moda_layer_2
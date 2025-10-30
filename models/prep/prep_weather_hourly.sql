WITH mod_layer_1 AS (
SELECT
    *,
    avg_temp_c AS temp_c,
    max_snow_mm AS snow_mm,
    avg_wind_direction AS wind_direction,
    dew_point_c AS dewpoint_c, 
    avg_wind_speed AS wind_speed_kmh,
    avg_pressure_hpa AS pressure_hpa,
    DATE_PART('hour', time) AS hour,
    TO_CHAR(date, 'FMmonth') AS month_name,
    TO_CHAR(date, 'FMday') AS weekday,
    DATE_PART('day', date) AS date_day,
    DATE_PART('month', date) AS date_month,
    DATE_PART('year', date) AS date_year,
    TO_CHAR(date, 'IW') AS cw
FROM {{ ref('stg_weather_hourly') }}
), mod_layer_2 AS (
SELECT
    *,
    CASE
        WHEN hour BETWEEN 00 and 06 THEN 'Night'
        WHEN hour BETWEEN 07 and 18 THEN 'Day'
        WHEN hour BETWEEN 19 and 24 THEN 'Evening'
    END AS day_part
FROM mod_layer_1
)
SELECT 
    precipitation_mm,
    sun_minutes,
    temp_c,
    snow_mm,
    wind_direction,
    dewpoint_c,
    wind_speed_kmh,
    pressure_hpa,
    timestamp,
    date,
    time,
    hour,
    month_name,
    weekday,
    date_day,
    date_month,
    date_year,
    cw,
    day_part
FROM mod_layer_2
SELECT 
    faa,
    name,
    city,
    country,
    region,
    lat,
    lon,
    alt,
    tz,
    dst
FROM {{ ref('stg_airports') }} -- note we do not use the source() any more but ref 
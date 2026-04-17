{{ config(materialized = 'ephemeral') }}

-- Roll hourly → daily. We keep both mean and extrema because dashboards need
-- different things: means for trend lines, max temp for heat-wave detection,
-- precipitation sum for drought/flood analysis.

select
    city_id,
    city_name,
    date(timestamp_micros(cast(floor(observation_ts / 1000) as int64))) as observation_date,

    -- Temperature
    avg(temperature_c)                     as avg_temperature_c,
    min(temperature_c)                     as min_temperature_c,
    max(temperature_c)                     as max_temperature_c,
    avg(apparent_temperature_c)            as avg_apparent_temperature_c,
    max(apparent_temperature_c)            as max_apparent_temperature_c,

    -- Humidity + precipitation
    avg(relative_humidity_pct)             as avg_humidity_pct,
    sum(precipitation_mm)                  as total_precipitation_mm,
    sum(rain_mm)                           as total_rain_mm,

    -- Pressure + cloud
    avg(pressure_msl_hpa)                  as avg_pressure_hpa,
    avg(cloud_cover_pct)                   as avg_cloud_cover_pct,

    -- Wind
    avg(wind_speed_kmh)                    as avg_wind_speed_kmh,
    max(wind_gusts_kmh)                    as max_wind_gusts_kmh,

    -- Solar
    sum(shortwave_radiation_w_m2)          as total_shortwave_radiation_w_m2,

    -- Bookkeeping
    count(*)                               as hour_count,
    max(ingested_at)                       as last_ingested_at

from {{ ref('int_weather_deduped') }}
group by city_id, city_name, observation_date

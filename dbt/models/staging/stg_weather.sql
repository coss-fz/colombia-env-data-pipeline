{{ config(materialized = 'view') }}

-- Staging layer: one row per (city, hour), lightly cleaned.
-- We union historical + daily sources here so downstream models see a single stream.
-- Deduplication happens in intermediate; the staging layer stays a faithful reflection of raw.

with historical as (
    select
        city_id,
        city_name,
        latitude,
        longitude,
        elevation_m,
        observation_ts,
        cast(temperature_2m        as float64) as temperature_c,
        cast(relative_humidity_2m  as float64) as relative_humidity_pct,
        cast(dew_point_2m          as float64) as dew_point_c,
        cast(apparent_temperature  as float64) as apparent_temperature_c,
        cast(precipitation         as float64) as precipitation_mm,
        cast(rain                  as float64) as rain_mm,
        cast(pressure_msl          as float64) as pressure_msl_hpa,
        cast(surface_pressure      as float64) as surface_pressure_hpa,
        cast(cloud_cover           as float64) as cloud_cover_pct,
        cast(wind_speed_10m        as float64) as wind_speed_kmh,
        cast(wind_direction_10m    as float64) as wind_direction_deg,
        cast(wind_gusts_10m        as float64) as wind_gusts_kmh,
        cast(shortwave_radiation   as float64) as shortwave_radiation_w_m2,
        'historical'                           as source_kind,
        ingested_at
    from {{ source('raw', 'ext_weather_hourly') }}
),

daily as (
    select
        city_id,
        city_name,
        latitude,
        longitude,
        elevation_m,
        observation_ts,
        cast(temperature_2m        as float64) as temperature_c,
        cast(relative_humidity_2m  as float64) as relative_humidity_pct,
        cast(dew_point_2m          as float64) as dew_point_c,
        cast(apparent_temperature  as float64) as apparent_temperature_c,
        cast(precipitation         as float64) as precipitation_mm,
        cast(rain                  as float64) as rain_mm,
        cast(pressure_msl          as float64) as pressure_msl_hpa,
        cast(surface_pressure      as float64) as surface_pressure_hpa,
        cast(cloud_cover           as float64) as cloud_cover_pct,
        cast(wind_speed_10m        as float64) as wind_speed_kmh,
        cast(wind_direction_10m    as float64) as wind_direction_deg,
        cast(wind_gusts_10m        as float64) as wind_gusts_kmh,
        cast(shortwave_radiation   as float64) as shortwave_radiation_w_m2,
        'daily'                                as source_kind,
        ingested_at
    from {{ source('raw', 'ext_weather_daily') }}
)

select * from historical
union all
select * from daily

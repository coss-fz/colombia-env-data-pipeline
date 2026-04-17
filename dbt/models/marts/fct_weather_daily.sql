{{ config(
    materialized  = 'table',
    partition_by  = {'field': 'observation_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by    = ['city_id']
) }}

-- Core fact table: one row per (city, date). Partitioned by month and clustered
-- by city_id — the two most common filters in any dashboard query. Looker Studio
-- bills per byte scanned so getting these right pays back immediately.

with daily as (
    select * from {{ ref('int_weather_daily') }}
),

cities as (
    select * from {{ ref('dim_cities') }}
)

select
    -- Surrogate key for BI tool joins
    {{ dbt_utils.generate_surrogate_key(['daily.city_id', 'daily.observation_date']) }} as weather_sk,

    daily.city_id,
    daily.city_name,
    cities.department,
    cities.climate_zone,
    cities.elevation_m,
    cities.population,

    daily.observation_date,
    extract(year    from daily.observation_date) as observation_year,
    extract(month   from daily.observation_date) as observation_month,
    extract(quarter from daily.observation_date) as observation_quarter,
    extract(dayofweek from daily.observation_date) as observation_day_of_week,

    daily.avg_temperature_c,
    daily.min_temperature_c,
    daily.max_temperature_c,
    daily.max_temperature_c - daily.min_temperature_c as temperature_range_c,
    daily.avg_apparent_temperature_c,
    daily.max_apparent_temperature_c,

    daily.avg_humidity_pct,
    daily.total_precipitation_mm,
    daily.total_rain_mm,
    daily.avg_pressure_hpa,
    daily.avg_cloud_cover_pct,
    daily.avg_wind_speed_kmh,
    daily.max_wind_gusts_kmh,
    daily.total_shortwave_radiation_w_m2,

    -- Event flags: turn common analyst questions into one-line WHERE clauses
    daily.total_precipitation_mm > 20                      as is_heavy_rain_day,
    daily.max_temperature_c      >= 32                     as is_hot_day,
    daily.min_temperature_c      <= 5                      as is_cold_day,
    daily.max_wind_gusts_kmh     >= 40                     as is_windy_day,

    daily.hour_count,
    daily.hour_count = 24                                   as is_complete_day,
    daily.last_ingested_at

from daily
left join cities using (city_id)

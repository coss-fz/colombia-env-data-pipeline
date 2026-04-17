{{ config(materialized = 'view') }}

-- Staging layer for air quality. One row per (city, sensor, pollutant, day).
-- We keep sensor_id and location_id here so the marts can drill down if needed.

with src as (
    select
        city_id,
        city_name,
        location_id,
        location_name,
        sensor_id,
        parameter_name,
        parameter_units,
        cast(observation_date as date) as observation_date,
        cast(value              as float64) as value,
        cast(min_value          as float64) as min_value,
        cast(max_value          as float64) as max_value,
        cast(avg_value          as float64) as avg_value,
        cast(measurement_count  as int64)   as measurement_count,
        ingested_at
    from {{ source('raw', 'ext_air_quality') }}
)

select
    *,
    -- Unit normalisation: we enforce µg/m³ for particulates and ppb for gases.
    -- Anything weird gets a NULL normalised value and the dbt tests catch it.
    case
        when parameter_name in ('pm25', 'pm10') and parameter_units = 'µg/m³' then value
        when parameter_name in ('o3', 'no2', 'so2') and parameter_units = 'ppb' then value
        when parameter_name = 'co' and parameter_units = 'ppm' then value * 1000  -- ppm → ppb
        else null
    end as normalised_value
from src
where value is not null
  and value >= 0  -- negative pollutant readings are sensor errors; drop them

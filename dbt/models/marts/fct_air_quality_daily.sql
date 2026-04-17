{{ config(
    materialized  = 'table',
    partition_by  = {'field': 'observation_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by    = ['city_id', 'parameter_name']
) }}

-- One row per (city, pollutant, date). Clustering by (city_id, parameter_name)
-- because the most common query shape is "show me PM2.5 in Medellín over time",
-- which benefits enormously from both being cluster columns.

with daily as (
    select * from {{ ref('int_air_quality_daily') }}
),

cities as (
    select * from {{ ref('dim_cities') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'daily.city_id', 'daily.observation_date', 'daily.parameter_name'
    ]) }} as air_quality_sk,

    daily.city_id,
    daily.city_name,
    cities.department,
    cities.climate_zone,
    cities.population,

    daily.observation_date,
    extract(year    from daily.observation_date) as observation_year,
    extract(month   from daily.observation_date) as observation_month,
    extract(quarter from daily.observation_date) as observation_quarter,

    daily.parameter_name,
    daily.parameter_units,
    daily.median_value,
    daily.mean_value,
    daily.min_value,
    daily.max_value,
    daily.sensor_count,
    daily.total_measurements,

    -- WHO 2021 Air Quality Guidelines — daily targets for health comparison.
    -- Using `median_value` rather than `mean_value` for these flags because
    -- median is less sensitive to sensor spikes.
    case
        when daily.parameter_name = 'pm25' and daily.median_value > 15  then true
        when daily.parameter_name = 'pm10' and daily.median_value > 45  then true
        when daily.parameter_name = 'no2'  and daily.median_value > 25  then true
        when daily.parameter_name = 'so2'  and daily.median_value > 40  then true
        when daily.parameter_name = 'o3'   and daily.median_value > 100 then true
        when daily.parameter_name = 'co'   and daily.median_value > 4000 then true  -- in ppb, ~4 ppm
        else false
    end as exceeds_who_guideline

from daily
left join cities using (city_id)

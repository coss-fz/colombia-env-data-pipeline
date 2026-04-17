{{ config(materialized = 'ephemeral') }}

-- Collapse multiple sensors per city to a single daily value per pollutant.
-- Using the median (percentile_cont 0.5) rather than mean is deliberate — a
-- single broken sensor reporting astronomical PM2.5 values would wreck an
-- average but leaves a median unmoved.

with per_city_pollutant as (
    select
        city_id,
        city_name,
        observation_date,
        parameter_name,
        parameter_units,
        percentile_cont(normalised_value, 0.5) over (
            partition by city_id, observation_date, parameter_name
        ) as median_value,
        avg(normalised_value) over (
            partition by city_id, observation_date, parameter_name
        ) as mean_value,
        min(normalised_value) over (
            partition by city_id, observation_date, parameter_name
        ) as min_value,
        max(normalised_value) over (
            partition by city_id, observation_date, parameter_name
        ) as max_value,
        count(distinct sensor_id) over (
            partition by city_id, observation_date, parameter_name
        ) as sensor_count,
        sum(measurement_count) over (
            partition by city_id, observation_date, parameter_name
        ) as total_measurements
    from {{ ref('stg_air_quality') }}
    where normalised_value is not null
)

select distinct
    city_id,
    city_name,
    observation_date,
    parameter_name,
    parameter_units,
    median_value,
    mean_value,
    min_value,
    max_value,
    sensor_count,
    total_measurements
from per_city_pollutant

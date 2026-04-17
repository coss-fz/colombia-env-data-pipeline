{{ config(
    materialized  = 'table',
    partition_by  = {'field': 'observation_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by    = ['city_id']
) }}

-- The payoff table: weather + air quality in one place per city per day.
-- This is what the Looker Studio dashboard primarily reads from, so it's
-- shaped for the questions the dashboard answers, not for normalisation purity.
--
-- A composite "environmental stress score" rolls up the most health-relevant
-- signals into a single 0-100 index. The weights are a defensible rough-cut,
-- not a clinical standard — documented in the README.

with weather as (
    select * from {{ ref('fct_weather_daily') }}
),

air_quality_wide as (
    -- Pivot from long (one row per pollutant) to wide (one row per city-day)
    select
        city_id,
        observation_date,
        max(case when parameter_name = 'pm25' then median_value end) as pm25,
        max(case when parameter_name = 'pm10' then median_value end) as pm10,
        max(case when parameter_name = 'o3'   then median_value end) as o3,
        max(case when parameter_name = 'no2'  then median_value end) as no2,
        max(case when parameter_name = 'so2'  then median_value end) as so2,
        max(case when parameter_name = 'co'   then median_value end) as co,
        count(distinct parameter_name)                               as pollutants_measured
    from {{ ref('fct_air_quality_daily') }}
    group by city_id, observation_date
)

select
    {{ dbt_utils.generate_surrogate_key(['weather.city_id', 'weather.observation_date']) }} as stress_sk,

    weather.city_id,
    weather.city_name,
    weather.department,
    weather.climate_zone,
    weather.observation_date,
    weather.observation_year,
    weather.observation_month,

    -- Weather stressors
    weather.avg_temperature_c,
    weather.max_temperature_c,
    weather.max_apparent_temperature_c,
    weather.avg_humidity_pct,
    weather.total_precipitation_mm,

    -- Air quality
    aq.pm25,
    aq.pm10,
    aq.o3,
    aq.no2,
    aq.so2,
    aq.co,
    aq.pollutants_measured,

    -- Environmental stress score (0–100, higher = worse for human health):
    --   heat stress      : 0-30 points  (apparent temp > 32°C hurts)
    --   pm2.5 load       : 0-35 points  (WHO-derived, PM2.5 is the single most health-impactful pollutant)
    --   ozone load       : 0-20 points
    --   no2 load         : 0-15 points
    -- We clamp each component then sum. Nulls in any pollutant contribute 0, so cities
    -- with sparse monitoring look artificially healthy — the dashboard shows the
    -- `pollutants_measured` count alongside so this is visible.
    least(greatest(
        coalesce(
            least(greatest((weather.max_apparent_temperature_c - 28) * 3, 0), 30), 0
        ) +
        coalesce(least(aq.pm25 * 1.2, 35), 0) +
        coalesce(least(aq.o3   * 0.2, 20), 0) +
        coalesce(least(aq.no2  * 0.5, 15), 0)
    , 0), 100) as environmental_stress_score,

    weather.last_ingested_at

from weather
left join air_quality_wide aq using (city_id, observation_date)

{{ config(
    materialized = 'table',
    cluster_by   = ['city_id']
) }}

-- Dimension table derived from the cities seed. Kept as a simple pass-through
-- so analysts can join against this instead of the seed directly (gives us a
-- stable contract even if we ever move to a more sophisticated source).

select
    city_id,
    name                             as city_name,
    department,
    latitude,
    longitude,
    elevation_m,
    population,
    timezone,

    -- Derived climate zone. Elevation drives climate in Colombia more than
    -- latitude does, so bucketing by elevation is genuinely useful downstream.
    case
        when elevation_m <   500 then 'tierra_caliente'    -- hot lowlands, >24°C mean
        when elevation_m <  1000 then 'tierra_templada'    -- temperate, 18-24°C
        when elevation_m <  2000 then 'tierra_fria'        -- cool, 12-18°C
        when elevation_m <  3000 then 'tierra_muy_fria'    -- cold, 6-12°C
        else                          'paramo'              -- high paramo, <6°C
    end as climate_zone,

    -- Population bucket for per-capita normalisation
    case
        when population < 600000   then 'medium'
        when population < 2000000 then 'large'
        else                              'metropolitan'
    end as population_tier

from {{ ref('cities') }}

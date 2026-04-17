{{ config(materialized = 'ephemeral') }}

-- Deduplicate (city, observation_ts). The historical backfill and the daily
-- incremental can overlap at the seams, and an accidental double-run of the
-- backfill would duplicate entire months. Using the most recent `ingested_at`
-- as the tiebreaker means "latest wins" — important if Open-Meteo retroactively
-- corrects a reading.

with ranked as (
    select
        *,
        row_number() over (
            partition by city_id, observation_ts
            order by ingested_at desc
        ) as rn
    from {{ ref('stg_weather') }}
)

select * except (rn)
from ranked
where rn = 1

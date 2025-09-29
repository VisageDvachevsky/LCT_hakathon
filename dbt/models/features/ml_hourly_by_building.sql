{{ config(materialized='view', schema='features') }}

with base as (
    select
        hour as ts,
        building_code,
        sum(supply) as supply,
        sum(return) as return,
        sum(consumption) as consumption,
        avg(supply - return) as loss
    from core.hourly_balance
    group by hour, building_code
)
select
    b.ts,
    b.building_code,
    b.supply,
    b.return,
    b.consumption,
    b.loss,
    ta.day_night,
    ta.month,
    ta.season,
    ta.is_weekend
from base b
left join features.time_attributes ta
  on ta.ts = b.ts

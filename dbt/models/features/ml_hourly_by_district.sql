{{ config(materialized='view', schema='features') }}

with by_building as (
    select
        hb.hour as ts,
        b.district_id,
        b.building_id as building_id,
        hb.consumption as consumption,
        hb.supply as supply,
        hb.return as return,
        (hb.supply - hb.return) as loss
    from core.hourly_balance hb
    join core.buildings b
      on b.external_code = hb.building_code
)

select
    ts,
    district_id,
    count(distinct building_id) as buildings_reporting,
    sum(consumption) as sum_consumption,
    sum(supply) as sum_supply,
    sum(return) as sum_return,
    avg(loss) as avg_loss
from by_building
group by ts, district_id
order by ts, district_id

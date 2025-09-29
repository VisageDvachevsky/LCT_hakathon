from etl.utils.db import get_conn
from etl.utils.logger import get_logger

log = get_logger(__name__)


def flow_publish_views(settings):
    log.info("publish_views start")

    sql_measurements_flat = """
    create or replace view core.measurements_flat as
    select
        m.measurement_id,
        b.external_code as building_code,
        i.external_code as itp_code,
        mt.external_code as meter_code,
        coalesce(mt.metric, mt.unit, '') as metric,
        m.ts as timestamp,
        m.value,
        mt.unit as unit,
        m.inserted_at
    from core.measurements m
    left join core.meters mt on m.meter_id = mt.meter_id
    left join core.itp i on mt.itp_id = i.itp_id
    left join core.buildings b on i.building_id = b.building_id;
    """

    sql_daily = """
    create materialized view core.daily_balance as
    select
        building_code,
        date_trunc('day', timestamp) as day,
        sum(case when metric = 'SUPPLY' then value else 0 end) as supply,
        sum(case when metric = 'RETURN' then value else 0 end) as return,
        sum(case when metric = 'CONSUMPTION' then value else 0 end) as consumption,
        sum(case when metric = 'SUPPLY' then value else 0 end) -
        sum(case when metric = 'RETURN' then value else 0 end) as loss
    from core.measurements_flat
    group by building_code, date_trunc('day', timestamp);
    """

    sql_hourly = """
    create materialized view core.hourly_balance as
    select
        building_code,
        date_trunc('hour', timestamp) as hour,
        sum(case when metric = 'SUPPLY' then value else 0 end) as supply,
        sum(case when metric = 'RETURN' then value else 0 end) as return,
        sum(case when metric = 'CONSUMPTION' then value else 0 end) as consumption,
        sum(case when metric = 'SUPPLY' then value else 0 end) -
        sum(case when metric = 'RETURN' then value else 0 end) as loss
    from core.measurements_flat
    group by building_code, date_trunc('hour', timestamp);
    """

    with get_conn(settings) as conn, conn.cursor() as cur:
        try:
            cur.execute("create schema if not exists core;")
            # measurements_flat (view)
            cur.execute("drop view if exists core.measurements_flat cascade;")
            cur.execute(sql_measurements_flat)

            # daily
            cur.execute("drop materialized view if exists core.daily_balance cascade;")
            cur.execute(sql_daily)

            # hourly
            cur.execute("drop materialized view if exists core.hourly_balance cascade;")
            cur.execute(sql_hourly)

            conn.commit()
            log.info("Published views/materialized views in schema core")
        except Exception as e:
            conn.rollback()
            log.error("Failed to publish views", extra={"error": str(e)})
            raise

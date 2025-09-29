# etl/utils/schema.py
from etl.utils.db import exec_sql, get_conn

DDL_STATEMENTS = [
    """
    create schema if not exists core;
    """,
    """
    create table if not exists core.buildings (
        building_id uuid primary key default gen_random_uuid(),
        external_code text not null unique
    );
    """,
    """
    create table if not exists core.buildings (
        building_id uuid primary key default gen_random_uuid(),
        external_code text not null unique,
        district_id text not null
    );
    """,
    """
    create table if not exists core.itp (
        itp_id uuid primary key default gen_random_uuid(),
        building_id uuid not null references core.buildings(building_id) on delete cascade,
        external_code text not null unique
    );
    """,
    """
    create table if not exists core.meters (
        meter_id uuid primary key default gen_random_uuid(),
        itp_id uuid not null references core.itp(itp_id) on delete cascade,
        external_code text not null,
        metric text not null,
        unit text not null,
        unique(itp_id, external_code, metric)
    );
    """,
    """
    create table if not exists core.measurements (
        measurement_id uuid primary key default gen_random_uuid(),
        meter_id uuid not null references core.meters(meter_id) on delete cascade,
        ts timestamptz not null,
        value double precision not null,
        inserted_at timestamptz default now(),
        unique(meter_id, ts)
    );
    """,
    "create index if not exists idx_measurements_ts on core.measurements(ts);",
    "create index if not exists idx_measurements_meter on core.measurements(meter_id);"
]

def ensure_schema(settings):
    with get_conn(settings) as conn:
        for stmt in DDL_STATEMENTS:
            exec_sql(conn, stmt)

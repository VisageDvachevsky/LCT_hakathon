-- Создаём схемы
create schema if not exists core;
create schema if not exists stage;
create schema if not exists features;
create schema if not exists quality;

-- core: справочник зданий
drop table if exists core.buildings cascade;
create table core.buildings (
    building_id uuid primary key default gen_random_uuid(),
    external_code text unique not null,
    district_id text
);

-- core: ИТП
drop table if exists core.itp cascade;
create table core.itp (
    itp_id uuid primary key default gen_random_uuid(),
    building_id uuid not null references core.buildings(building_id),
    external_code text unique not null
);

-- core: счётчики
drop table if exists core.meters cascade;
create table core.meters (
    meter_id uuid primary key default gen_random_uuid(),
    itp_id uuid not null references core.itp(itp_id),
    external_code text unique not null,
    metric text not null,
    unit text not null
);

-- core: измерения
drop table if exists core.measurements cascade;
create table core.measurements (
    measurement_id uuid primary key default gen_random_uuid(),
    meter_id uuid not null references core.meters(meter_id),
    ts timestamptz not null,
    value double precision not null,
    inserted_at timestamptz default now(),
    unique (meter_id, ts)
);

-- stage: парсинг файлов
drop table if exists stage.stage_parsed_measurements cascade;
create table stage.stage_parsed_measurements (
    load_id uuid not null,
    source_file text not null,
    row_num int not null,
    ts timestamptz not null,
    building_code text not null,
    itp_code text not null,
    meter_code text not null,
    metric text not null,
    value double precision not null,
    unit text not null,
    primary key (load_id, row_num)
);


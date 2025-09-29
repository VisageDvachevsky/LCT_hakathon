import uuid
from etl.utils.logger import get_logger
from etl.utils.db import get_conn

log = get_logger(__name__)


def get_or_create_building(cur, building_code: str) -> str:
    cur.execute("""
        insert into core.buildings (external_code)
        values (%s)
        on conflict (external_code) do nothing
        returning building_id
    """, (building_code,))
    row = cur.fetchone()
    if row:
        return row["building_id"]

    cur.execute("select building_id from core.buildings where external_code=%s", (building_code,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Не удалось найти/создать building: {building_code}")
    return row["building_id"]


def get_or_create_itp(cur, building_id: str, itp_code: str) -> str:
    cur.execute("""
        insert into core.itp (building_id, external_code)
        values (%s, %s)
        on conflict (external_code) do nothing
        returning itp_id
    """, (building_id, itp_code))
    row = cur.fetchone()
    if row:
        return row["itp_id"]

    cur.execute("select itp_id from core.itp where external_code=%s", (itp_code,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Не удалось найти/создать itp: {itp_code}")
    return row["itp_id"]


def get_or_create_meter(cur, itp_id: str, meter_code: str, metric: str, unit: str) -> str:
    cur.execute("""
        insert into core.meters (itp_id, external_code, metric, unit)
        values (%s, %s, %s, %s)
        on conflict (external_code) do nothing
        returning meter_id
    """, (itp_id, meter_code, metric, unit))
    row = cur.fetchone()
    if row:
        return row["meter_id"]

    cur.execute("select meter_id from core.meters where external_code=%s", (meter_code,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Не удалось найти/создать meter: {meter_code}")
    return row["meter_id"]


def flow_load_to_core(settings, load_id: str):
    """
    Загружаем данные из stage.stage_parsed_measurements → core.measurements.
    Автоматически создаём справочники (buildings, itp, meters).
    """

    with get_conn(settings) as conn, conn.cursor() as cur:
        log.info("Загружаем данные для load_id=%s", load_id)

        # читаем данные из stage
        cur.execute("""
            select row_num, ts, building_code, itp_code, meter_code, metric, value, unit
            from stage.stage_parsed_measurements
            where load_id = %s
            order by row_num
        """, (load_id,))
        rows = cur.fetchall()

        if not rows:
            log.warning("Нет данных в stage для load_id=%s", load_id)
            return

        inserted = 0
        for row in rows:
            row_num = row["row_num"]
            ts = row["ts"]
            building_code = row["building_code"] or "UNKNOWN_BUILDING"
            itp_code = row["itp_code"] or f"{building_code}_ITP"
            meter_code = row["meter_code"] or f"{itp_code}_METER"
            metric = row["metric"] or "consumption"
            value = row["value"]
            unit = row["unit"] or "m3"


            try:
                building_id = get_or_create_building(cur, building_code)
                itp_id = get_or_create_itp(cur, building_id, itp_code)
                meter_id = get_or_create_meter(cur, itp_id, meter_code, metric, unit)

                measurement_id = str(uuid.uuid4())
                cur.execute("""
                    insert into core.measurements (measurement_id, meter_id, ts, value, inserted_at)
                    values (%s, %s, %s, %s, now())
                    on conflict (meter_id, ts) do nothing
                """, (measurement_id, meter_id, ts, value))
                inserted += cur.rowcount
            except Exception as e:
                log.error(
                    "Ошибка при вставке",
                    extra={
                        "load_id": load_id,
                        "row_num": row_num,
                        "building": building_code,
                        "itp": itp_code,
                        "meter": meter_code,
                        "metric": metric,
                        "error": str(e),
                    }
                )

        log.info("Загружено %s строк в core.measurements для load_id=%s", inserted, load_id)
        conn.commit()

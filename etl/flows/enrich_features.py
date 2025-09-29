"""
etl.flows.enrich_features
-------------------------
Добавляет вычисляемые признаки (например, timezone, hour, day_of_week)
в таблицу stage.stage_parsed_measurements или в отдельную таблицу features.

Здесь простая функция: для каждого parsed row записывает дополнительные поля:
  - ts_hour (timestamp truncated to hour)
  - dow (day of week)
  - is_weekend (boolean)

(Мы не меняем существующую структуру parsed_measurements, а создаём/обновляем
вспомогательную таблицу stage.stage_parsed_measurements_enriched для упрощения)
"""

from etl.utils.db import get_conn
from etl.utils.logger import get_logger

log = get_logger(__name__)


def _ensure_enriched_table(cur):
    cur.execute("create schema if not exists stage;")
    cur.execute(
        """
        create table if not exists stage.stage_parsed_measurements_enriched (
            load_id uuid not null,
            row_num int not null,
            ts_hour timestamptz,
            dow int,
            is_weekend boolean,
            inserted_at timestamptz default now()
        );
        """
    )


def flow_enrich_features(settings, load_id: str):
    log.info("enrich_features start", extra={"load_id": load_id})
    with get_conn(settings) as conn, conn.cursor() as cur:
        try:
            _ensure_enriched_table(cur)

            # удаляем старые обогащения для идемпотентности
            cur.execute("delete from stage.stage_parsed_measurements_enriched where load_id = %s", (load_id,))

            # получаем parsed rows
            cur.execute(
                """
                select load_id, row_num, ts
                from stage.stage_parsed_measurements
                where load_id = %s
                order by row_num
                """,
                (load_id,),
            )
            rows = cur.fetchall()
            inserted = 0
            for r in rows:
                row_num = r["row_num"]
                ts = r["ts"]
                if ts is None:
                    ts_hour = None
                    dow = None
                    is_weekend = None
                else:
                    # ts — это Python datetime (pydatetime)
                    ts_hour = ts.replace(minute=0, second=0, microsecond=0)
                    dow = ts.weekday()  # 0=Mon .. 6=Sun
                    is_weekend = True if dow >= 5 else False

                cur.execute(
                    """
                    insert into stage.stage_parsed_measurements_enriched
                        (load_id, row_num, ts_hour, dow, is_weekend)
                    values (%s, %s, %s, %s, %s)
                    """,
                    (load_id, row_num, ts_hour, dow, is_weekend),
                )
                inserted += 1

            conn.commit()
            log.info("enrich_features: записаны атрибуты времени для load_id=%s", load_id, extra={"inserted": inserted})
        except Exception as e:
            conn.rollback()
            log.error("enrich_features failed", extra={"load_id": load_id, "error": str(e)})
            raise

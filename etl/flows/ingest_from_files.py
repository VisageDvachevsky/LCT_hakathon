"""
etl.flows.ingest_from_files
---------------------------
Найти файлы в data/raw, зарегистрировать их в stage.stage_raw_files
и вернуть список load_id'ов (UUID) для дальнейшей обработки.

Создаёт (если нужно) schema `stage` и таблицу `stage.stage_raw_files`:
  - load_id uuid primary key
  - file_path text
  - file_name text
  - detected_from timestamp with time zone
  - detected_to timestamp with time zone
  - rows int
  - inserted_at timestamptz default now()

Возвращает (через лог) список load_id'ов и возращает список из функций.
"""

import os
import uuid
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.db import get_conn

log = get_logger(__name__)

RAW_DIR = os.path.join(os.getcwd(), "data", "raw")


def _ensure_stage_tables(cur):
    # Schemas + table для регистрации raw файлов
    cur.execute("create schema if not exists stage;")
    cur.execute(
        """
        create table if not exists stage.stage_raw_files (
            load_id uuid primary key,
            file_path text not null,
            file_name text not null,
            detected_from timestamptz,
            detected_to timestamptz,
            rows int,
            inserted_at timestamptz default now()
        );
        """
    )


def _scan_file_for_range(path: str) -> Tuple[Optional[datetime], Optional[datetime], int]:
    """
    Простейший эвристический определитель диапазона дат в файле.
    Попытка прочитать весь первый лист через pandas и найти min/max
    в колонках похожих на дату (ts, timestamp, date, время и т.п.)
    Возвращает (min_ts, max_ts, rows)
    """
    try:
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    except Exception:
        # попытаемся без engine
        df = pd.read_excel(path, sheet_name=0)

    rows = len(df.index)

    # Найдём возможные колонки даты/времени
    date_cols = [c for c in df.columns if any(k in c.lower() for k in ("ts", "time", "date", "дата", "время"))]
    if not date_cols:
        return None, None, rows

    # Попробуем привести к datetime и взять min/max
    for c in date_cols:
        try:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                mn = s.min()
                mx = s.max()
                return mn.to_pydatetime(), mx.to_pydatetime(), rows
        except Exception:
            continue

    return None, None, rows


def flow_ingest_from_files(settings) -> List[str]:
    """
    Находит файлы в data/raw, регистрирует их в stage.stage_raw_files.
    Возвращает список load_id'ов (uuid strings).
    """
    log.info("ingest start: scanning raw dir %s", RAW_DIR)
    files = []
    if not os.path.isdir(RAW_DIR):
        log.warning("Raw dir %s not found", RAW_DIR)
        return []

    for fname in os.listdir(RAW_DIR):
        if fname.startswith("~$"):  # временные файлы Excel
            continue
        if not (fname.lower().endswith(".xlsx") or fname.lower().endswith(".xls") or fname.lower().endswith(".csv")):
            continue
        files.append(os.path.join(RAW_DIR, fname))

    if not files:
        log.info("No files found in %s", RAW_DIR)
        return []

    load_ids = []
    with get_conn(settings) as conn, conn.cursor() as cur:
        try:
            _ensure_stage_tables(cur)

            for path in sorted(files):
                fname = os.path.basename(path)
                load_id = str(uuid.uuid4())
                # try to detect date range / rows
                try:
                    dfrom, dto, rows = _scan_file_for_range(path)
                except Exception as e:
                    log.warning("Can't scan file for range %s: %s", path, e)
                    dfrom, dto, rows = None, None, None

                cur.execute(
                    """
                    insert into stage.stage_raw_files (load_id, file_path, file_name, detected_from, detected_to, rows)
                    values (%s, %s, %s, %s, %s, %s)
                    """,
                    (load_id, path, fname, dfrom, dto, rows),
                )
                load_ids.append(load_id)
                log.info("ingest ok: %s rows=%s load_id=%s", path, rows, load_id)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("ingest failed", extra={"error": str(e)})
            raise

    log.info("Ingest produced load_ids", extra={"count": len(load_ids), "ids": load_ids})
    return load_ids

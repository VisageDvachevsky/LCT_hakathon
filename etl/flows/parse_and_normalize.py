import os
import math
import re
import unicodedata
from decimal import Decimal
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.db import get_conn

log = get_logger(__name__)


def _safe_num(x):
    if x is None:
        return None
    try:
        if isinstance(x, (float, int)):
            if isinstance(x, float) and math.isnan(x):
                return None
            return float(x)
        if isinstance(x, Decimal):
            return float(x)
        s = str(x).strip()
        if s == "":
            return None
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


def _col_matches(cols, keywords):
    for k in keywords:
        for c in cols:
            if k in c.lower():
                return c
    return None


# -----------------------
# Нормализация кода/метрик
# -----------------------
def normalize_metric(metric: str) -> str:
    """Преобразует разные рус/англ названия в единый словарь метрик."""
    if metric is None:
        return None
    m = str(metric).strip().lower()
    if m == "":
        return None
    # точечные проверки / вхождения
    if "подач" in m or "supply" in m:
        return "SUPPLY"
    if "обрат" in m or "return" in m:
        return "RETURN"
    if "расход" in m or "consumption" in m or "за период" in m or "потреблен" in m:
        return "CONSUMPTION"
    if "t1" in m or "т1" in m:
        return "T1"
    if "t2" in m or "т2" in m:
        return "T2"
    if "pump" in m or "насос" in m or "runtime" in m:
        return "PUMP_RUNTIME_HOURS"
    # если ничего не подошло — вернуть верхний регистр очищённой строки
    return re.sub(r"\s+", "_", m).upper()


def normalize_entity_code(code: str) -> str:
    """Нормализует коды сущностей (building, itp) в UPPER_CASE, безопасно для кириллицы/латиницы."""
    if code is None:
        return None
    s = str(code).strip()
    if s == "":
        return None
    s = unicodedata.normalize("NFKC", s)
    # заменяем последовательности не-алфавитно-цифровых символов пробелом -> подчеркивание
    s = re.sub(r"\W+", "_", s, flags=re.UNICODE)
    return s.upper()


def normalize_meter_code(code: str) -> str:
    """Нормализует код счётчика (делаем lower-case, очищаем спецсимволы)."""
    if code is None:
        return None
    s = str(code).strip()
    if s == "":
        return None
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\W+", "_", s, flags=re.UNICODE)
    return s.lower()


# -----------------------
# Разбор файла (основная логика)
# -----------------------
def _parse_file(path, load_id):
    source_file = os.path.basename(path)
    # читаем первый лист
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    cols = list(df.columns)

    ts_col = _col_matches(cols, ("ts", "timestamp", "time", "дата", "время", "date"))
    building_col = _col_matches(cols, ("building", "дом", "здание"))
    itp_col = _col_matches(cols, ("itp", "итп"))
    meter_col = _col_matches(cols, ("meter", "счетчик", "счетчик", "meter_code"))
    metric_col = _col_matches(cols, ("metric", "метрика", "тип", "параметр"))
    value_col = _col_matches(cols, ("value", "значение", "потребление", "потребление за период", "показания"))
    unit_col = _col_matches(cols, ("unit", "ед", "u", "единица"))

    rows_out = []
    for idx, row in df.iterrows():
        ts = None
        if ts_col:
            ts = pd.to_datetime(row[ts_col], errors="coerce")
            if pd.notna(ts):
                ts = ts.to_pydatetime()
            else:
                ts = None

        # raw values
        raw_building = row[building_col] if building_col and not pd.isna(row[building_col]) else None
        raw_itp = row[itp_col] if itp_col and not pd.isna(row[itp_col]) else None
        raw_meter = row[meter_col] if meter_col and not pd.isna(row[meter_col]) else None
        raw_metric = row[metric_col] if metric_col and not pd.isna(row[metric_col]) else None
        raw_unit = row[unit_col] if unit_col and not pd.isna(row[unit_col]) else None

        # фоллбеки и нормализация:
        # building (в верхнем регистре, чтобы совпало с ранее используемыми BUILDING_XVS/BUILDING_GVS)
        building_code = normalize_entity_code(raw_building)
        if not building_code:
            if "хвс" in source_file.lower():
                building_code = "BUILDING_XVS"
            elif "гвс" in source_file.lower():
                building_code = "BUILDING_GVS"
            else:
                building_code = "UNKNOWN"

        itp_code = normalize_entity_code(raw_itp)
        if not itp_code:
            itp_code = f"{building_code}_ITP"

        meter_code = normalize_meter_code(raw_meter)
        if not meter_code:
            meter_code = source_file.replace(".xlsx", "").replace(".xls", "")

        metric = normalize_metric(raw_metric)
        if not metric:
            # если явно не извлечено — считаем как потребление
            metric = "CONSUMPTION"

        unit = str(raw_unit).strip() if raw_unit and not pd.isna(raw_unit) else None

        rows_out.append(
            {
                "load_id": load_id,
                "row_num": int(idx) + 1,
                "ts": ts,
                "building_code": building_code,
                "itp_code": itp_code,
                "meter_code": meter_code,
                "metric": metric,
                "value": _safe_num(row[value_col]) if value_col else None,
                "unit": unit,
            }
        )
    return rows_out


def flow_parse_and_normalize(settings, load_id: str):
    log.info("parse start", extra={"load_id": load_id})
    with get_conn(settings) as conn, conn.cursor() as cur:
        try:
            # достаем путь файла
            cur.execute("select file_path from stage.stage_raw_files where load_id = %s", (load_id,))
            row = cur.fetchone()
            if not row:
                log.warning("No raw file registered for load_id", extra={"load_id": load_id})
                return

            path = row["file_path"]
            source_file = os.path.basename(path)

            parsed_rows = _parse_file(path, load_id)

            cur.execute("delete from stage.stage_parsed_measurements where load_id = %s", (load_id,))
            inserted = 0
            for r in parsed_rows:
                cur.execute(
                    """
                    insert into stage.stage_parsed_measurements
                        (load_id, source_file, row_num, ts, building_code, itp_code, meter_code, metric, value, unit)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        r["load_id"],
                        source_file,
                        r["row_num"],
                        r["ts"],
                        r["building_code"],
                        r["itp_code"],
                        r["meter_code"],
                        r["metric"],
                        r["value"],
                        r["unit"],
                    ),
                )
                inserted += 1

            conn.commit()
            log.info("parse completed", extra={"load_id": load_id, "ok": inserted})
        except Exception as e:
            conn.rollback()
            log.error("parse failed", extra={"load_id": load_id, "error": str(e)})
            raise

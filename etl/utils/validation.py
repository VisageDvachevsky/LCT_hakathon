# etl/utils/validation.py
from typing import Optional
from datetime import datetime, timezone

def is_reasonable_value(metric: str, value: float) -> bool:
    """
    Проверяет, является ли value разумным для данной метрики (канонические имена с подчёркиваниями).
    """
    if value is None:
        return False
    try:
        # Температуры: допускаем расширенный диапазон, но фильтруем явные абсурды
        if metric in ("T1", "T2"):
            return -50.0 <= value <= 200.0
        # Поток/расход/потребление/наработка: неотрицательные
        if metric in ("flow_supply", "flow_return", "consumption_period", "consumption_cumulative", "pump_runtime_hours"):
            return value >= 0
        # По умолчанию — считаем значение допустимым
        return True
    except Exception:
        return False


def parse_timestamp(ts_str: str, default_tz: str) -> Optional[str]:
    """
    Парсит ISO-like timestamp в isoformat с TZ. Возвращает None при ошибке.
    """
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc).isoformat()
        return dt.isoformat()
    except Exception:
        try:
            from dateutil import parser
            dt = parser.parse(ts_str)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat()
            return dt.isoformat()
        except Exception:
            return None

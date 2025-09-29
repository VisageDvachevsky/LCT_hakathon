# etl/utils/units.py
from typing import Optional, Tuple

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _norm_token(s: str) -> str:
    x = (s or "").strip().lower()
    for ch in (" ", "_", "-", "."):
        x = x.replace(ch, "")
    x = x.replace("ё", "е")
    return x

def _norm_unit(u: Optional[str]) -> str:
    raw = (u or "").strip()
    x = raw.lower().replace(" ", "").replace("\t", "")
    m3_aliases = {"м3", "м³", "м^3", "m3", "m^3", "m³", "кубм", "куб.м", "кубометр", "кубометры", "куб.м.", "кубметр"}
    m3h_aliases = {"м3/ч", "м³/ч", "м^3/ч", "м3ч", "м3час", "м3/час", "m3/h", "m^3/h", "m³/h", "m3h", "кубм/ч", "куб.м/ч"}
    lps_aliases = {"л/с", "л/c", "л-с", "лсек", "л/сек", "l/s", "lps"}
    degc_aliases = {"c", "degc", "°c", "°с", "цел", "цельсий", "цельсия", "градусц", "градусыц"}
    hour_aliases = {"ч", "час", "часы", "h", "hr", "hrs", "hour", "hours"}
    if x in m3_aliases:
        return "м3"
    if x in m3h_aliases:
        return "м3ч"
    if x in lps_aliases:
        return "л/с"
    if x in degc_aliases:
        return "C"
    if x in hour_aliases:
        return "час"
    if x in {"гкал", "гигакал", "гигакалория", "gcal"}:
        return "Гкал"
    if x in {"квтч", "квт*ч", "квт·ч", "квт-ч", "kwh", "квтчас", "квтчч", "квтчасы"}:
        return "кВт·ч"
    return raw

def normalize_metric_unit(metric: Optional[str], unit: Optional[str]) -> Tuple[str, str]:
    """
    Приводит метрику и юнит к каноническим значениям, согласованным с dbt-моделями.
    Возвращает (canonical_metric, canonical_unit), где canonical_metric использует стиль с подчёркиваниями,
    например: 'flow_supply', 'flow_return', 'consumption_period', 'consumption_cumulative', 'pump_runtime_hours', 'T1', 'T2'.
    """
    raw_metric = _clean(metric)
    raw_unit = _clean(unit)
    m_norm = _norm_token(raw_metric)

    # Маппинг входных вариантов в канонические имена (с подчёркиваниями)
    metric_alias = {
        # температуры
        "t1": "T1",
        "t1подачи": "T1", "температураподачи": "T1", "tподачи": "T1",
        "t2": "T2", "t2обратки": "T2", "температураобратки": "T2",
        # расход/потоки
        "flowsupply": "flow_supply", "flow_supply": "flow_supply", "расходподачи": "flow_supply", "g1": "flow_supply",
        "flowreturn": "flow_return", "flow_return": "flow_return", "расходобратки": "flow_return", "g2": "flow_return",
        # потребление
        "consumptionperiod": "consumption_period", "consumption_period": "consumption_period",
        "объемзапериод": "consumption_period", "объёмзапериод": "consumption_period",
        "consumptioncumulative": "consumption_cumulative", "consumption_cumulative": "consumption_cumulative",
        "накопленныйрасход": "consumption_cumulative", "показания": "consumption_cumulative",
        # насосы / наработка
        "pumpruntimehours": "pump_runtime_hours", "pump_runtime_hours": "pump_runtime_hours",
        "наработканасоса": "pump_runtime_hours", "часыработынасоса": "pump_runtime_hours",
    }

    canonical_metric = metric_alias.get(m_norm, raw_metric or "")
    # целевая единица по канонике
    metric_map = {
        "consumption_period": "м3",
        "consumption_cumulative": "м3",
        "flow_supply": "м3ч",
        "flow_return": "м3ч",
        "T1": "C",
        "T2": "C",
        "pump_runtime_hours": "час",
    }

    target = metric_map.get(canonical_metric)
    u_norm = _norm_unit(raw_unit)
    if not target:
        # возвращаем очищенные значения (unit нормализованный, либо пустая строка)
        return canonical_metric, (u_norm or "")
    # небольшие правила согласования
    if u_norm.lower() in {"л/с", "l/s"} and target == "м3ч":
        return canonical_metric, "м3ч"
    if u_norm.lower() in {"c", "degc", "°c", "°с"} and target == "C":
        return canonical_metric, "C"
    if not u_norm:
        return canonical_metric, target
    return canonical_metric, target

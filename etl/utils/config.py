import os
from dataclasses import dataclass
from typing import List


@dataclass
class Settings:
    database_url: str
    stage_schema: str
    quality_schema: str
    features_schema: str
    core_schema: str
    raw_dir: str
    file_patterns: List[str]
    default_tz: str
    day_start: int
    day_end: int
    delta_t_min: float
    delta_t_max: float
    refresh_objects: List[str]
    log_level: str
    ingest_year: int
    ingest_month: int

    @staticmethod
    def from_env() -> "Settings":
        """
        Собирает конфигурацию из переменных окружения.
        Обязательные: DATABASE_URL
        Опциональные: остальные с разумными дефолтами.
        """
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is required")

        def list_from_env(key: str, default: str) -> List[str]:
            return [p.strip() for p in os.getenv(key, default).split(",") if p.strip()]

        return Settings(
            database_url=database_url,
            stage_schema=os.getenv("STAGE_SCHEMA", "stage"),
            quality_schema=os.getenv("QUALITY_SCHEMA", "quality"),
            features_schema=os.getenv("FEATURES_SCHEMA", "features"),
            core_schema=os.getenv("CORE_SCHEMA", "core"),
            raw_dir=os.getenv("RAW_DIR", "/app/data/raw"),
            file_patterns=list_from_env("FILE_PATTERNS", "*.csv,*.xlsx"),
            default_tz=os.getenv("DEFAULT_TZ", "Europe/Moscow"),
            day_start=int(os.getenv("DAY_START", "7")),
            day_end=int(os.getenv("DAY_END", "22")),
            delta_t_min=float(os.getenv("DELTA_T_MIN", "17")),
            delta_t_max=float(os.getenv("DELTA_T_MAX", "23")),
            refresh_objects=list_from_env("REFRESH_OBJECTS", ""),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            ingest_year=int(os.getenv("INGEST_YEAR", os.getenv("YEAR", "2025"))),
            ingest_month=int(os.getenv("INGEST_MONTH", os.getenv("MONTH", "4"))),
        )

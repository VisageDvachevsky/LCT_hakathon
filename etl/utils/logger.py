import logging
import os
import sys
import orjson
from datetime import datetime
from typing import Any

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            try:
                payload["exc"] = self.formatException(record.exc_info)
            except Exception:
                payload["exc"] = str(record.exc_info)
        # добавим extra поля, если есть
        try:
            # отфильтруем встроенные атрибуты
            extras = {k: v for k, v in record.__dict__.items() if k not in (
                "name","msg","args","levelname","levelno","pathname","filename","module","exc_info","exc_text",
                "stack_info","lineno","funcName","created","msecs","relativeCreated","thread","threadName","processName","process"
            )}
            if extras:
                payload["extra"] = extras
        except Exception:
            pass
        try:
            return orjson.dumps(payload).decode("utf-8")
        except Exception:
            # fallback to default string formatting
            return str(payload)

def get_logger(name: str) -> logging.Logger:
    """
    Возвращает логгер с JSON-форматтером, поток — stdout.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    # избегаем дублирования логов при повторном создании
    logger.propagate = False
    return logger

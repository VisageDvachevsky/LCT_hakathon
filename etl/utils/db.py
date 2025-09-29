import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from etl.utils.logger import get_logger

logger = get_logger(__name__)


def get_conn(settings_or_url):
    """
    Создаёт соединение с БД.
    Можно передать либо объект settings (с атрибутом database_url),
    либо сразу строку подключения.
    """
    if hasattr(settings_or_url, "database_url"):
        database_url = settings_or_url.database_url
    else:
        database_url = str(settings_or_url)

    try:
        conn = psycopg.connect(
            database_url,
            autocommit=False,
            row_factory=dict_row  # <--- строки будут dict, а не tuple
        )
        return conn
    except Exception as e:
        logger.error("Failed to connect to database", exc_info=e)
        raise


def exec_sql(conn, sql: str, params=None):
    """
    Выполняет SQL с параметрами, логирует ошибки.
    """
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params or ())
        except Exception as e:
            logger.error("Failed to execute SQL: %s", sql, exc_info=e)
            raise


def init_db(settings):
    with get_conn(settings) as conn:
        exec_sql(conn, open("etl/sql/init_core.sql", "r", encoding="utf-8").read())
        conn.commit()


def fetchall(conn, sql: str, params=None):
    """
    Выполняет SQL и возвращает все строки (dict).
    """
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params or ())
            return cur.fetchall()
        except Exception as e:
            logger.error("Failed to fetch SQL: %s", sql, exc_info=e)
            raise


@contextmanager
def get_cursor(settings_or_url):
    """
    Контекстный менеджер: открывает соединение и курсор, закрывает автоматически.
    """
    conn = get_conn(settings_or_url)
    try:
        with conn.cursor() as cur:
            yield conn, cur
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

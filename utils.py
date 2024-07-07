import sqlite3
from settings import logger
from contextlib import contextmanager
from datetime import datetime

@contextmanager
def sqlite_conn_context(db_path):
    try:
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_conn.row_factory = sqlite3.Row 
        yield sqlite_conn
    except Exception as err:
        logger.error(f'SQLite connection context manager caught an error: {err}')
    finally:
        sqlite_conn.close()


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query


class TimeUtils:
    @staticmethod
    def ns_delta_to_str_format(ns: int) -> str:
        delta_time = datetime.now() - datetime.fromtimestamp(ns / 1e9)
        days = delta_time.days
        seconds = delta_time.seconds
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if seconds > 0:
            parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
        return ', '.join(parts) if parts else '0 seconds'


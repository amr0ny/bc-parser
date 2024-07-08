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
    def ns_delta_to_hours(ns: int) -> int:
        delta_time = datetime.now() - datetime.fromtimestamp(ns / 1e9)
        total_seconds = delta_time.total_seconds()
        hours = total_seconds // 3600
        return round(hours) 

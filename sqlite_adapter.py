import sqlite3
from utils import read_sql_file
from typing import Dict
from settings import logger
from models import Transaction
class SQLiteAdapter:

    def __init__(self, sqlite_conn: sqlite3.Connection, schemas: Dict[str, str], db_recordset_size: int):
        self.db_recordset_size = db_recordset_size;
        self.__sqlite_conn = sqlite_conn
        self.__cursor = self.__sqlite_conn.cursor()
        self.__schemas = schemas
        self.init_db()
    
    def init_db(self):
        try:
            logger.info('[SQLiteAdapter] Initializing database')
            sql_query = read_sql_file(self.__schemas['create_schema'])
            self.__cursor.execute(sql_query)
            self.__sqlite_conn.commit()
        except Exception as e:
            self.__sqlite_conn.rollback()
            logger.error(f'[SQLiteAdapter] An error occured while initializing database: {e}')
    
    def truncate(self):
        try:
            self.__cursor.execute("DELETE FROM transactions")
            self.__sqlite_conn.commit()
            logger.info("[SQLiteAdapter] Table 'transactions' truncated successfully")
        except sqlite3.Error as e:
            self.__sqlite_conn.rollback()
            logger.error(f"[SQLiteAdapter] Error truncating table: {e}")

    def upsert_one(self, name, hash=None, quantity=None, near_amount=None, hot_amount=None, age=None, claim_period=None):
        try:
            # Check if the record exists
            self.__cursor.execute("SELECT id FROM transactions WHERE name = ?", (name,))
            record = self.__cursor.fetchone()
            
            if record:
                # Update existing record
                update_fields = []
                params = []
                if hash is not None:
                    update_fields.append("hash = ?")
                    params.append(hash)
                if quantity is not None:
                    update_fields.append("quantity = ?")
                    params.append(quantity)
                if near_amount is not None:
                    update_fields.append("near_amount = ?")
                    params.append(near_amount)
                if hot_amount is not None:
                    update_fields.append("hot_amount = ?")
                    params.append(hot_amount)
                if age is not None:
                    update_fields.append("age = ?")
                    params.append(age)
                if claim_period is not None:
                    update_fields.append("claim_period = ?")
                    params.append(age)
                if not update_fields:
                    logger.warning('[SQLiteAdapter] No fields available for update')
                    return
                params.append(name)
                sql_query = f"UPDATE transactions SET {', '.join(update_fields)} WHERE name = ?"
                self.__cursor.execute(sql_query, params)
                logger.info(f'[SQLiteAdapter] Row with name {name} updated')
            else:
                # Insert new record
                sql_query = "INSERT INTO transactions (name, hash, quantity, near_amount, hot_amount, age, claim_period) VALUES (?, ?, ?, ?, ?, ?, ?)"
                self.__cursor.execute(sql_query, (name, hash, quantity, near_amount, hot_amount, age, claim_period))
                logger.info(f'[SQLiteAdapter] Row with name {name} inserted')
                
            self.__sqlite_conn.commit()
        except Exception as e:
            self.__sqlite_conn.rollback()
            logger.error(f'[SQLiteAdapter] An error occurred while upserting {name}: {e}')


    def read_all(self):
        try:
            self.__cursor = self.__sqlite_conn.cursor()
            sql_query = read_sql_file(self.__schemas['read_all'])
            self.__cursor.execute(sql_query)
            while len(res := self.__cursor.fetchmany(self.db_recordset_size)) > 0:
                logger.debug(f'[SQLiteAdapter] Data read: {res}')
                yield [dict(row) for row in res]
        except Exception as e:
            logger.error(f'[SQLiteAdapter] An error occured while reading data: {e}')

import asyncio
from service_adapter import ServiceUpdater, ServiceReader
from sqlite_adapter import SQLiteAdapter
import sqlite3
from typing import Dict
from target_parser import TargetParser
from models import Transaction
from settings import logger

class ServiceWorker:
    def __init__(self, 
                 parser: TargetParser,
                 service_updater: ServiceUpdater, 
                 service_reader: ServiceReader,
                 sqlite_adapter: SQLiteAdapter,
                 timeout: int = 3600):
        self.__updater = service_updater
        self.__reader = service_reader
        self.__sqlite_adapter = sqlite_adapter
        self.__parser = parser
        self.__contract_name = 'game.hot.tg'
        self.timeout = timeout

    async def run(self):
        logger.info('[Runtime] Starting infinite loop')
        while True:
            try:
                self.__sqlite_adapter.truncate()
                for item in self.__reader.read_columns_generator():
                    await self.__item_iter_func(item)
                    await asyncio.sleep(3)
                
                self.__updater.clear()
                for rows in self.__sqlite_adapter.read_all():
                    self.__updater.append_rows(rows)
                self.__updater.update_last_updated()
            except Exception as e:
                logger.error(f'[RUNTIME] An error occured: {e}')
            finally:
                await asyncio.sleep(self.timeout)

    
    async def __item_iter_func(self, row: list[str]): 
        item = row[0]
        additional_params = {'claim_period': row[1]}
        logger.info(f'[Runtime iteration] Proccessing account: {item}')
        query_params = {'a': item, 'contract_name': self.__contract_name}
        data = await self.__parser.parse(query_params)
        transaction = self.__parser.serialize({**data, **additional_params})
        self.__sqlite_adapter.upsert_one(transaction.name,
                                         hash=transaction.hash,
                                         quantity=transaction.quantity,
                                         age=transaction.age,
                                         near_amount=transaction.near_amount,
                                         hot_amount=transaction.hot_amount,
                                         claim_period=transaction.claim_period)



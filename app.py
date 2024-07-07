import asyncio
import time
from settings import settings, logger
from service_adapter import ServiceUpdater, ServiceReader
from sqlite_adapter import SQLiteAdapter
from service_worker import ServiceWorker
from target_parser import TargetParser 
from utils import sqlite_conn_context

if __name__ == "__main__":
    with sqlite_conn_context(settings.sqlite_path) as sqlite_conn:
        logger.info('[Initialization] Started')
        service_reader = ServiceReader(settings.service_account_file,
                                       settings.scopes,
                                       settings.read_sheet_name,
                                       settings.read_worksheet_name,
                                       headers=('accounts',))
        service_updater = ServiceUpdater(settings.service_account_file,
                                         settings.scopes,
                                         settings.write_sheet_name,
                                         settings.write_worksheet_name,
                                         headers=('name', 'hash', 'quantity', 'age'))
        sqlite_adapter = SQLiteAdapter(sqlite_conn, settings.db_schemas, settings.db_recordset_size)
        target_parser = TargetParser(settings.base_url, parsing_depth=settings.parsing_depth)
        service_worker = ServiceWorker(target_parser, service_updater, service_reader, sqlite_adapter, settings.timeout)
        logger.info('[Initialization] Done')
        asyncio.run(service_worker.run())
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging
import sqlite3
from typing import Dict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='',  validate_default=False)
    scopes: list[str] = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    service_account_file: str
    log_path: str
    log_level: str
    sqlite_path: str
    base_url: str = 'https://api3.nearblocks.io/v1/fts/game.hot.tg/txns'
    read_worksheet_name: str
    write_worksheet_name: str
    read_sheet_name: str
    write_sheet_name: str
    db_recordset_size: int
    timeout: int
    db_schemas: Dict[str, str] = {
        'create_schema': './schemas/create_schema.sql',
        'insert_one': './schemas/insert_one_schema.sql',
        'read_all': './schemas/read_all.sql'
        }
settings = Settings(_env_file = '.env', _env_file_encoding = 'utf-8', _case_sensitive = False) # type: ignore

logging.basicConfig(
    filename=settings.log_path,
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

conn = sqlite3.connect(settings.sqlite_path)
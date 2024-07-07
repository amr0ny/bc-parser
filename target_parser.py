from typing import Any
from settings import logger
from datetime import datetime
from models import Transaction
from utils import TimeUtils
import aiohttp


class TargetParser:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    async def fetch_data(self, query_params: dict[str, Any]) -> dict[str, Any]:
        logger.info('[TargetParser] Starting fetching...')
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=query_params) as response:
                logger.info(f'[TargetParser] Response status: {response.status}')
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f'[TargetParser] Data received (first 300 symbols): {str(data)[:300]}')
                    return data
                else:
                    logger.info(f"[TargetParser]: Response status: {response.status}")
                    raise Exception('Response status is not OK')
                    return {}

        
    async def parse(self, query_params: dict[str, Any]) -> Transaction:
        logger.info(f'[TargetParser] Starting parsing. URL: {self.base_url}\tQuery parameters: {query_params}')
        try:
            for page in range(1, 3):
                query_params['page'] = page
                data = await self.fetch_data(query_params)
                txns = data['txns']
                for txn in txns:
                    if txn['cause'] == 'MINT' and txn['involved_account_id'] == None:
                        logger.debug(f'[TargetParser] First entrance found: {txn}')
                        return self.serialize(txn)
            return Transaction(hash=None, name=query_params['a'], quantity=None, age=None)
        except Exception as e:
            logger.error(f'[TargetParser] An error occurred while parsing: {e}')
            return Transaction(hash=None, name=query_params['a'], quantity=None, age=None)

    def serialize(self, txn: dict[str, Any]) -> Transaction:
        logger.info('[TargetParser] Starting serialization')
        transaction = Transaction(hash=txn['transaction_hash'],
                                  name=txn['affected_account_id'],
                                  quantity=int(txn['delta_amount'])/10e6,
                                  age=TimeUtils.ns_delta_to_str_format(int(txn['block_timestamp'])))
        logger.debug(f'[TargetParser] Serialization successful: {transaction.model_dump()}')
        return transaction
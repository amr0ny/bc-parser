from typing import Any
from settings import logger
from datetime import datetime
from models import Transaction
from utils import TimeUtils
import aiohttp


class TargetParser:
    def __init__(self, base_urls: dict[str, str], principal_account: str, parsing_depth: int = 3):
        self.base_urls = base_urls
        self.principal_account = principal_account
        self.__parsing_depth = parsing_depth
    
    async def fetch_txns_data(self, query_params: dict[str, Any]) -> dict[str, Any]:
        base_url = self.base_urls['txns']
        logger.info(f'[TargetParser] Starting fetching. URL: {base_url}\tQuery parameters: {query_params}')
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=query_params) as response:
                logger.info(f'[TargetParser] Response status: {response.status}')
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f'[TargetParser] Data received (first 300 symbols): {str(data)[:300]}')
                    return data
                else:
                    logger.info(f"[TargetParser]: Response status: {response.status}")
                    raise Exception('Response status is not OK')
                    return {}

        
    async def fetch_txn_data(self, txn_hash: str, params: dict[str, str]) -> bool:
        base_url = self.base_urls['txn']
        url = f"{base_url}/{txn_hash}"
        logger.info(f'[TargetParser] Starting fetching. URL: {url}')
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    logger.info(f'[TargetParser] Response status: {response.status}')
                    if response.status != 200:
                        logger.info(f"[TargetParser] Unexpected response status: {response.status}")
                        raise Exception('Response status is not OK')
                    
                    data = await response.json()
                    logger.debug(f'[TargetParser] Data received (first 300 symbols): {str(data)[:300]}')
                    
                    fts = self._extract_fts(data)
                    if fts:
                        logger.debug(f'[TargetParser] Length of ft objects: {len(fts)}')
                        if fts[0].get('affected_account_id') == params['affected_account_id']:
                            return True
                    
            except aiohttp.ClientError as e:
                logger.error(f"[TargetParser] ClientError while fetching transaction data: {e}")
                raise e
            except Exception as e:
                logger.error(f"[TargetParser] Error while fetching transaction data: {e}")
                raise e

        return False

    def _extract_fts(self, data: dict[str, Any]) -> list:
        """Extracts 'fts' from the transaction data."""
        txns = data.get('txns', [])
        if not txns:
            return []
        
        receipts = txns[0].get('receipts', [])
        if not receipts:
            return []
        return receipts[0].get('fts', [])    

    async def parse(self, query_params: dict[str, Any]) -> dict[str, Any]:
        try:
            for page in range(1, self.__parsing_depth):
                query_params['page'] = page
                data = await self.fetch_txns_data(query_params)
                txns = data['txns']
                for txn in txns:
                    if txn['cause'] == 'MINT' and txn['involved_account_id'] == None:
                        logger.debug(f'[TargetParser] First entrance found: {txn}')
                        if await self.fetch_txn_data(txn['transaction_hash'], txn):
                            return txn
            return { 'affected_account_id': query_params['a'] }
        except Exception as e:
            logger.error(f'[TargetParser] An error occurred while parsing: {e}')
            return { 'affected_account_id': query_params['a']}

    def serialize(self, txn: dict[str, Any]) -> Transaction:
        logger.info('[TargetParser] Starting serialization')
        transaction = Transaction(hash=txn.get('transaction_hash'),
                                  name=txn['affected_account_id'],
                                  quantity=int(txn['delta_amount'])/10e6 if txn.get('delta_amount') is not None else None,
                                  age=TimeUtils.ns_delta_to_hours(int(txn['block_timestamp'])) if txn.get('block_timestamp') is not None else None,
                                  claim_period=int(txn['claim_period']) if txn.get('claim_period') is not None and txn.get('claim_period') != '' else None)
        logger.debug(f'[TargetParser] Serialization successful: {transaction.model_dump()}')
        return transaction
from typing import Any, Optional
from settings import logger
from models import Transaction
from utils import TimeUtils
import aiohttp
import asyncio

class TargetParser:
    def __init__(self, base_urls: dict[str, str], parsing_depth: int = 3):
        self.base_urls = base_urls
        self.__parsing_depth = parsing_depth

    async def _fetch_data(self, url: str, params: Optional[dict] = None) -> dict:
        """Общий метод для выполнения GET-запросов."""
        logger.info(f'[TargetParser] Starting fetching. URL: {url}')
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    logger.info(f'[TargetParser] Response status: {response.status}')
                    if response.status != 200:
                        raise Exception(f'Unexpected response status: {response.status}')
                    data = await response.json()
                    logger.debug(f'[TargetParser] Data received (first 300 symbols): {str(data)[:300]}')
                    return data
            except aiohttp.ClientError as e:
                logger.error(f"[TargetParser] ClientError while fetching data: {e}")
                raise
            except Exception as e:
                logger.error(f"[TargetParser] Error while fetching data: {e}")
                raise

    async def fetch_txns_data(self, query_params: dict[str, Any]) -> dict[str, Any]:
        return await self._fetch_data(self.base_urls['txns'], query_params)

    async def fetch_txn_data(self, txn_hash: str, params: dict[str, str]) -> bool:
        url = f"{self.base_urls['txn']}/{txn_hash}"
        data = await self._fetch_data(url)
        fts = self._extract_fts(data)
        return bool(fts and fts[0].get('affected_account_id') == params['affected_account_id'])

    async def fetch_account_data(self, query_params: dict[str, Any]) -> dict[str, Any]:
        account = query_params['account']
        contract_name = query_params['contract_name']
        urls = [
            f"{self.base_urls['account']}/{account}",
            f"{self.base_urls['account']}/{account}/inventory"
        ]
        
        results = await asyncio.gather(*[self._fetch_data(url) for url in urls])
        account_data, inventory_data = results

        near_amount = account_data['account'][0]['amount']
        hot_amount = self._extract_hot_amount(inventory_data, contract_name)

        return {'near_amount': near_amount, 'hot_amount': hot_amount}

    def _extract_fts(self, data: dict[str, Any]) -> list:
        """Извлекает 'fts' из данных транзакции."""
        txns = data.get('txns', [])
        if not txns:
            return []
        receipts = txns[0].get('receipts', [])
        if not receipts:
            return []
        return receipts[0].get('fts', [])

    def _extract_hot_amount(self, data: dict, contract_name: str) -> Any:
        fts = data['inventory']['fts']
        for ft in fts:
            if ft['contract'] == contract_name:
                return ft['amount']
        return None

    async def parse(self, query_params: dict[str, Any]) -> dict[str, Any]:
        try:
            for page in range(1, self.__parsing_depth + 1):
                query_params['page'] = page
                data = await self.fetch_txns_data(query_params)
                for txn in data['txns']:
                    if txn['cause'] == 'MINT' and txn['involved_account_id'] is None:
                        logger.debug(f'[TargetParser] First entrance found: {txn}')
                        if await self.fetch_txn_data(txn['transaction_hash'], txn):
                            account_data = await self.fetch_account_data({
                                'account': query_params['a'],
                                'contract_name': query_params.get('contract_name')
                            })
                            txn.update(account_data)
                            return txn
            return {'affected_account_id': query_params['a']}
        except Exception as e:
            logger.error(f'[TargetParser] An error occurred while parsing: {e}')
            return {'affected_account_id': query_params['a']}

    def serialize(self, txn: dict[str, Any]) -> Transaction:
        logger.info('[TargetParser] Starting serialization')
        transaction = Transaction(
            hash=txn.get('transaction_hash'),
            name=txn['affected_account_id'],
            quantity=int(txn['delta_amount'])/10e5 if txn.get('delta_amount') is not None else None,
            age=TimeUtils.ns_delta_to_hours(int(txn['block_timestamp'])) if txn.get('block_timestamp') is not None else None,
            near_amount=int(txn['near_amount'])/10e24 if txn.get('near_amount') is not None else None,
            hot_amount=int(txn['hot_amount'])/10e5 if txn.get('hot_amount') is not None else None,
            claim_period=int(txn['claim_period']) if txn.get('claim_period') and txn['claim_period'] != '' else None,

        )
        logger.debug(f'[TargetParser] Serialization successful: {transaction.model_dump()}')
        return transaction
import asyncio
import os
import time
import httpx

from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols
from util.time_util import get_current_time
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("gate_io_chain", os.path.join(project_root, 'log', 'app.log'))
rotator = ProxyRotator()

max_concurrent_requests = 200
retry_limit = 3

all_symbols, _ = get_symbols()


def gate_io_chain():
    data = asyncio.run(gate_io_symbols())
    start_time = time.time()
    found_records = filter_symbols(data)
    result = asyncio.run(gate_io_depth(found_records))
    insert_to_db(result)
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- gate_io executed in {elapsed_time} seconds. ----- symbols : {len(found_records)} success : {len(result)}")


async def gate_io_symbols():
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    async with httpx.AsyncClient(verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json() if response.status_code == 200 else None


def filter_symbols(data):
    found_records = set()
    inst_ids_set = set(item['currency_pair'] for item in data)

    for symbol in all_symbols:
        symbol = str(symbol).replace('-', '_')
        if symbol in inst_ids_set:
            found_records.add(str(symbol).split('_')[0])

    logger.info(f"gate_io - symbols       : {len(data)}")
    logger.info(f"gate_io - symbols found : {len(found_records)}")
    return found_records


async def gate_io_depth(found_records):
    url = "https://api.gateio.ws/api/v4/wallet/currency_chains?currency="
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    tasks = [fetch(symbol, url, semaphore) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url, semaphore):
    proxy = rotator.get_next_proxy()

    for retry in range(retry_limit):
        async with semaphore:
            try:
                async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=20) as client:
                    response = await client.get(url + symbol)
                    if response.status_code == 200:
                        return symbol, response.json()
                    else:
                        logger.info(f"Request failed {symbol} status code {response.status_code} - gate_io")
                        await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching {symbol}. {repr(e)} - gate_io")
                await asyncio.sleep(0.1)

    return symbol, None


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time, ct_addr
        ) VALUES ('gateio', %s, %s, %s, %s, '{current_time}', %s);
    """

    records_to_insert = [
        (
            symbol,
            item['name_en'],
            invert_boolean(item.get('is_deposit_disabled')),
            invert_boolean(item.get('is_withdraw_disabled')),
            item['contract_address']
        )
        for symbol, values in result.items()
        for item in values
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


def invert_boolean(value):
    if value == 0:
        return True
    else:
        return False


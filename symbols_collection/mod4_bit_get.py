import os
import requests

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

symbols, reference = get_symbols()
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bit_get", os.path.join(project_root, 'log', 'app.log'))


def bit_get():
    url = "https://api.bitget.com/api/spot/v1/market/tickers"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['data']
        insert_to_db(data, reference)
    else:
        logger.error(f"Request failed with status code {response.status_code}")


def insert_to_db(data, ref):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data, ref)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO symbols (symbol_name, remark) VALUES (%s, 'bit_get')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    logger.info(f"{len(filtered_symbols)} record(s) inserted bit_get")


def transform_and_filter_symbols(data, ref):
    transformed_symbols = []
    unmatched_symbols = []

    for item in data:
        symbol = item['symbol']
        found = False
        for match in ref:
            if symbol.endswith(match):
                transformed_symbols.append(str(symbol[:-len(match)]) + '-' + match)
                found = True
                break
        if not found:
            unmatched_symbols.append(symbol)
            logger.info(f"big_get_unmatched_symbols : {symbol}")

    logger.info(f"transformed_symbols :   {len(transformed_symbols)}")
    logger.info(f"unmatched_symbols :   {len(unmatched_symbols)}")
    return transformed_symbols

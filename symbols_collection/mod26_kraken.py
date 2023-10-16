import os
import requests

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from database.db_service import get_symbols

symbols, reference = get_symbols()
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("kraken", os.path.join(project_root, 'log', 'app.log'))


def kraken():
    url = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data = data['result']
        insert_to_db(data)
    else:
        logger.error(f"Request failed with status code {response.status_code}")


def insert_to_db(data):
    connection = get_connection()
    cursor = connection.cursor()

    filtered_symbols = transform_and_filter_symbols(data)

    inst_ids_tuples = [(inst_id,) for inst_id in filtered_symbols]
    sql = "INSERT INTO symbols (symbol_name, remark) VALUES (%s, 'kraken')"

    cursor.executemany(sql, inst_ids_tuples)

    connection.commit()
    release_connection(connection)
    logger.info(f"{len(filtered_symbols)} record(s) inserted kraken")


def transform_and_filter_symbols(data):
    transformed_symbols = [str(data[item]['wsname']).replace('/', '-') for item in data]
    logger.info(f"transformed_symbols :   {len(transformed_symbols)}")
    return transformed_symbols

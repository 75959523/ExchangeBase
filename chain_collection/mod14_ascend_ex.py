import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def ascend_ex_chain():
    url = "https://ascendex.com/api/pro/v2/assets"
    response = requests.get(url)
    if response.status_code == 200:
        insert_to_db(response.json()['data'])


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('ascendex', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['assetCode'],
            chain_record['chainName'],
            chain_record['allowDeposit'],
            chain_record['allowWithdraw']
        ) for item in result for chain_record in item['blockChain']
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

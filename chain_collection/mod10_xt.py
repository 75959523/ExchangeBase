import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def xt_chain():
    url = "https://sapi.xt.com/v4/public/wallet/support/currency"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        result = data['result']
        insert_to_db(result)


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('xt', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['currency'].upper(),
            chain_record['chain'],
            chain_record['depositEnabled'],
            chain_record['withdrawEnabled']
        ) for item in result for chain_record in item['supportChains']
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def bit_get_chain():
    url = "https://api.bitget.com/api/spot/v1/public/currencies"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        result = data['data']
        insert_to_db(result)


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('bitget', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['coinName'],
            chain_record['chain'],
            chain_record['rechargeable'],
            chain_record['withdrawable']
        ) for item in result for chain_record in item['chains']
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def huobi_chain():
    url = "https://api.huobi.pro/v1/settings/common/chains"
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
            exchange_name, ccy, chain, canDep, canWd, mainNet, create_time, ct_addr
        ) VALUES ('huobi', %s, %s, %s, %s, %s, '{current_time}', %s);
    """

    records_to_insert = [
        (
            str(record['currency']).upper(),
            record['dn'],
            record['de'],
            record['we'],
            record['default'],
            record['ca']
        ) for record in result
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

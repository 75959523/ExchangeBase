import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def probit_chain():
    url = "https://api.probit.com/api/exchange/v1/currency"
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
        ) VALUES ('probit', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['id'],
            item['platform'],
            'false' if item['deposit_suspended'] else 'true',
            'false' if item['withdrawal_suspended'] else 'true'
        ) for item in result
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

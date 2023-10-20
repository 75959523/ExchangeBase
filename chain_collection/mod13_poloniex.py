import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def poloniex_chain():
    url = "https://api.poloniex.com/currencies"
    response = requests.get(url)
    if response.status_code == 200:
        insert_to_db(response.json())


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('poloniex', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            list(item.keys())[0],
            item_data['blockchain'],
            item_data['walletDepositState'],
            item_data['walletWithdrawalState']
        ) for item in result for item_data in item.values()
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

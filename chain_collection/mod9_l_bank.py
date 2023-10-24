import random
import string
import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time


def l_bank_chain():
    url = "https://api.lbank.info/v2/withdrawConfigs.do"
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
        ) VALUES ('lbank', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['assetCode'].upper(),
            item['chain'],
            'true',
            'true'
        ) for item in result if 'chain' in item
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

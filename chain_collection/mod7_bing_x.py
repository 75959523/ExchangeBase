import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

API_KEY = "DUIYg1waneSavZ6PFRqSbfGJVubIyuTjZFDBnJ2tLLf1T6WdW3M8gpCPLmgopWFyPLZ7u4TzCPvXZSvNEOA"
SECRET_KEY = "Cf9mEo3uhzRwfQz6BFcYOe8B8uJVTPJWJbdr6KL3ycexpcLJ5nkpUxpErJ54y9gIwWfxO7VO1Fh6C6GJ7rg"
url = "https://open-api.bingx.com/openApi/wallets/v1/capital/config/getall"


def bing_x_chain():
    timestamp = int(time.time() * 1000)

    params = {
        'timestamp': timestamp
    }

    query_string = '&'.join(["{}={}".format(d, params[d]) for d in params])
    signature = hmac.new(bytes(SECRET_KEY, 'latin-1'), msg=bytes(query_string, 'latin-1'),
                         digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {
        "X-BX-APIKEY": API_KEY
    }

    response = requests.get(url, headers=headers, params=params)
    insert_to_db(response.json()['data'])


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, mainNet, create_time, ct_addr
        ) VALUES ('bingx', %s, %s, %s, %s, %s, '{current_time}', %s);
    """

    records_to_insert = []
    for record in result:
        coin = record['coin']

        for network in record['networkList']:
            chain = network['network']
            # canDep = network['depositEnable']
            canWd = network['withdrawEnable']
            # mainNet = network['isDefault']
            # ct_addr = network.get('contractAddress', '')

            records_to_insert.append((coin, chain, 'true', canWd, '', ''))

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


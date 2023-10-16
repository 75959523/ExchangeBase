import hashlib
import hmac
import time
import requests

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

API_KEY = "0ba49da247eb8d"
SECRET_KEY = "720dfd1774d524b974e8c0a46083ff60385ac386b0"
URL = "https://openapi.digifinex.com/v3/deposit/address"


def digi_finex_chain():
    url = "https://openapi.digifinex.com/v3/currencies"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        result = data['data']
        insert_to_db(result)


def digi_finex2():
    timestamp = str(int(time.time()))

    params = {
        'currency': 'ETH',
    }

    query_string = '&'.join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
    signature = hmac.new(bytes(SECRET_KEY, 'utf-8'), msg=bytes(query_string, 'utf-8'),
                         digestmod=hashlib.sha256).hexdigest()

    headers = {
        "ACCESS-KEY": API_KEY,
        'ACCESS-TIMESTAMP': timestamp,
        'ACCESS-SIGN': signature
    }

    final_url = "{}?{}".format(URL, query_string)

    response = requests.get(final_url, headers=headers)
    print(response.json())


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('digifinex', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            record['currency'],
            record['chain'],
            invert_boolean(record['deposit_status']),
            invert_boolean(record['withdraw_status'])
        ) for record in result
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


def invert_boolean(value):
    if value == 1:
        return True
    else:
        return False


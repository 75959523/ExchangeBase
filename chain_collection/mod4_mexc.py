import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

MEXC_API_KEY = "mx0vglur2PUHix4RLn"
MEXC_SECRET_KEY = "17dc002d49694714b67a3c5279bd0b85"
url = "https://api.mexc.com/api/v3/capital/config/getall"


def mexc_chain():
    timestamp = int(time.time() * 1000)

    params = {
        'timestamp': timestamp
    }

    query_string = '&'.join(["{}={}".format(d, params[d]) for d in params])

    signature = hmac.new(bytes(MEXC_SECRET_KEY, 'latin-1'), msg=bytes(query_string, 'latin-1'),
                         digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {
        "X-MEXC-APIKEY": MEXC_API_KEY
    }

    response = requests.get(url, headers=headers, params=params)
    insert_to_db(response.json())


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, mainNet, create_time, ct_addr
        ) VALUES ('mexc', %s, %s, %s, %s, %s, '{current_time}', %s);
    """

    records_to_insert = []
    for record in result:
        coin = record['coin']

        for network in record['networkList']:
            chain = network['network']
            canDep = network['depositEnable']
            canWd = network['withdrawEnable']
            ct_addr = network['contract']

            records_to_insert.append((coin, chain, canDep, canWd, '', ct_addr))

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

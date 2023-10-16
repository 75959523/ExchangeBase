import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

API_KEY = "u0bTBJqA9iZ22BKTEN84kUiYFIWLPyl4bDPXJb55sqCxT1uy2a0zYdDMvofF8Do1"
SECRET_KEY = "pdMISLc1RVp70wNSWvqGMeZtOgOx8ZriBab7AEO8ZtfkjKADYvsGnBxPLC2MYLPR"
URL = "https://api.binance.com/sapi/v1/capital/config/getall"


def binance_chain():
    timestamp = int(time.time() * 1000)

    params = {
        'timestamp': timestamp
    }

    query_string = '&'.join(["{}={}".format(d, params[d]) for d in params])
    signature = hmac.new(bytes(SECRET_KEY, 'latin-1'), msg=bytes(query_string, 'latin-1'),
                         digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    response = requests.get(URL, headers=headers, params=params)
    insert_to_db(response.json())


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, mainNet, create_time, ct_addr
        ) VALUES ('binance', %s, %s, %s, %s, %s, '{current_time}', %s);
    """

    records_to_insert = []
    for record in result:
        coin = record['coin']

        for network in record['networkList']:
            chain = network['name']
            canDep = network['depositEnable']
            canWd = network['withdrawEnable']
            mainNet = network['isDefault']
            ct_addr = network.get('contractAddress', '')

            records_to_insert.append((coin, chain, canDep, canWd, mainNet, ct_addr))

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)

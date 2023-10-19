import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

API_KEY = "S2gKlLNViFZXulybeB"
SECRET_KEY = "0EURTvGBXbgAgNdTOSpEyvDzKgcHhbmgTFFM"
url = "https://api.bybit.com/v5/asset/coin/query-info"


def bybit_chain():
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"

    queryString = ""

    sign_plain_text = timestamp + API_KEY + recv_window + queryString

    signature = hmac.new(bytes(SECRET_KEY, 'latin-1'), msg=bytes(sign_plain_text, 'latin-1'),
                         digestmod=hashlib.sha256).hexdigest()

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature
    }

    response = requests.get(url, headers=headers, params=queryString)
    insert_to_db(response.json()['result']['rows'])


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, create_time
        ) VALUES ('bybit', %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            item['name'],
            chains['chain'],
            'true' if chains['chainDeposit'] == 0 else 'false',
            'true' if chains['chainWithdraw'] == 0 else 'false'
        ) for item in result for chains in item['chains']
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


import random
import string
import requests
import hmac
import hashlib
import time

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

API_KEY = "81dc637e-f8bc-48ed-a6f7-01f934944df9"
SECRET_KEY = "EE9BC22AAA4B49E37D1DB4C83A1A52A7"
URL = "https://api.lbkex.com/v2/supplement/user_info.do"


def l_bank_chain():
    timestamp = str(int(time.time() * 1000))

    params = {
        'api_key': API_KEY,
        'echostr': generate_echostr(),
        'signature_method': 'HmacSHA256',
        'timestamp': timestamp
    }

    query_string = '&'.join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
    md5_str = hashlib.md5(query_string.encode('utf-8')).hexdigest().upper()
    signature = hmac.new(SECRET_KEY.encode('utf-8'), md5_str.encode('utf-8'), hashlib.sha256).hexdigest()
    params['sign'] = signature

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(URL, data=params, headers=headers)
    data = response.json()['data']
    print(data)


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


def generate_echostr(length=30):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


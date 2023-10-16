import time
import okx.Funding as Funding

from database.db_pool import get_connection, release_connection
from util.time_util import get_current_time

apikey = "a93ccadf-8e11-4479-90aa-0dd1282c019f"
secretkey = "C55D0E55593EADC74FAE6EC4755F6F87"
passphrase = "xiaoguai520S@"
flag = "0"

fundingAPI = Funding.FundingAPI(apikey, secretkey, passphrase, False, flag)


def okx_chain():
    result = fundingAPI.get_currencies()['data']
    insert_to_db(result)
    ct_addr(result)


def ct_addr(result):
    delay = 1 / 5.0

    unique_ccy_values = set(item.get('ccy') for item in result if item.get('ccy'))

    all_records = []
    for ccy_value in unique_ccy_values:
        response = fundingAPI.get_deposit_address(ccy_value)
        all_records.extend(response['data'])
        time.sleep(delay)

    update_db(all_records)


def insert_to_db(result):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO chain (
            exchange_name, ccy, chain, canDep, canWd, mainNet, create_time
        ) VALUES ('okx', %s, %s, %s, %s, %s, '{current_time}');
    """

    records_to_insert = [
        (
            record['ccy'],
            record['chain'],
            record['canDep'],
            record['canWd'],
            record['mainNet']
        ) for record in result
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


def update_db(all_records):
    connection = get_connection()
    cursor = connection.cursor()

    update_query = """
        UPDATE chain 
        SET ct_addr = %s, addr = %s 
        WHERE ccy = %s AND chain = %s AND exchange_name = 'okx';
    """

    records_to_update = [
        (
            record['ctAddr'],
            record['addr'],
            record['ccy'],
            record['chain']
        ) for record in all_records
    ]

    cursor.executemany(update_query, records_to_update)
    connection.commit()
    cursor.close()
    release_connection(connection)


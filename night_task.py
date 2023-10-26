import os
import sched
import time
import requests

from datetime import datetime, timedelta
from pytz import timezone

from chain_collection.mod10_xt import xt_chain
from chain_collection.mod11_bit_get import bit_get_chain
from chain_collection.mod12_bybit import bybit_chain
from chain_collection.mod13_poloniex import poloniex_chain
from chain_collection.mod14_ascend_ex import ascend_ex_chain
from chain_collection.mod15_probit import probit_chain
from chain_collection.mod9_l_bank import l_bank_chain
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from symbols_collection.mod10_bybit import bybit
from symbols_collection.mod11_xt import xt
from symbols_collection.mod12_hitbtc import hitbtc
from symbols_collection.mod14_bigone import bigone
from symbols_collection.mod15_jubi import jubi
from symbols_collection.mod16_la_token import la_token
from symbols_collection.mod17_coinex import coinex
from symbols_collection.mod18_gate_io import gate_io
from symbols_collection.mod19_coin_w import coin_w
from symbols_collection.mod1_okx import okx
from symbols_collection.mod20_bi_ka import bi_ka
from symbols_collection.mod21_hot_coin import hot_coin
from symbols_collection.mod22_digi_finex import digi_finex
from symbols_collection.mod23_l_bank import l_bank
from symbols_collection.mod24_bing_x import bing_x
from symbols_collection.mod25_probit import probit
from symbols_collection.mod28_poloniex import poloniex
from symbols_collection.mod2_binance import binance
from symbols_collection.mod3_huobi import huobi
from symbols_collection.mod4_bit_get import bit_get
from symbols_collection.mod6_mexc import mexc
from symbols_collection.mod7_bit_venus import bit_venus
from symbols_collection.mod8_deep_coin import deep_coin
from symbols_collection.mod9_ascend_ex import ascend_ex
from chain_collection.mod1_okx import okx_chain
from chain_collection.mod2_binance import binance_chain
from chain_collection.mod3_huobi import huobi_chain
from chain_collection.mod4_mexc import mexc_chain
from chain_collection.mod5_gate_io import gate_io_chain
from chain_collection.mod6_digi_finex import digi_finex_chain
from chain_collection.mod7_bing_x import bing_x_chain

project_root = os.path.dirname(os.path.abspath(__file__))
logger = setup_logger("midnight_task", os.path.join(project_root, 'log', 'app.log'))


def schedule_night_task():
    s = sched.scheduler(time.time, time.sleep)
    shanghai_tz = timezone('Asia/Shanghai')

    def run_task(sc):
        current_time = datetime.now(shanghai_tz).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"midnight task executed {current_time}")
        update_exclusion()
        update_user()
        update_symbols()
        update_chain()
        # update_usd_to_others_rate()
        next_run_time = datetime.now(shanghai_tz) + timedelta(days=1)
        next_run_time = next_run_time.replace(hour=0, minute=0, second=0, microsecond=0)
        s.enterabs(time.mktime(next_run_time.timetuple()), 1, run_task, (sc,))

    now = datetime.now(shanghai_tz)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    time_to_midnight = (midnight - now).total_seconds()

    s.enter(time_to_midnight, 1, run_task, (s,))
    s.run()


def update_exclusion():
    try:
        start_time = time.time()
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT exclusion_id, status, expire_date FROM exclusion_list WHERE status = 1 AND expire_date != ''")
        rows = cursor.fetchall()

        for row in rows:
            exclusion_id, status, expire_date = row

            expire_date = int(expire_date) - 1

            if expire_date == 0:
                cursor.execute("UPDATE exclusion_list SET status = 0 WHERE exclusion_id = %s", (exclusion_id,))

            cursor.execute("UPDATE exclusion_list SET expire_date = %s WHERE exclusion_id = %s",
                           (expire_date, exclusion_id))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- update_exclusion executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def update_user():
    try:
        start_time = time.time()
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("SELECT user_id, status, expire_date FROM users WHERE status = 1 AND expire_date != ''")
        rows = cursor.fetchall()

        for row in rows:
            user_id, status, expire_date = row

            expire_date = int(expire_date) - 1

            if expire_date == 0:
                cursor.execute("UPDATE users SET status = 0 WHERE user_id = %s", (user_id,))

            cursor.execute("UPDATE users SET expire_date = %s WHERE user_id = %s", (expire_date, user_id))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- update_user executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def update_symbols():
    try:
        start_time = time.time()
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE symbols")
        connection.commit()

        okx()
        binance()
        bit_get()
        mexc()
        gate_io()
        huobi()
        xt()
        bybit()
        bigone()
        poloniex()
        ascend_ex()
        digi_finex()
        deep_coin()
        hot_coin()
        la_token()
        coinex()
        jubi()
        hitbtc()
        bi_ka()
        bit_venus()
        bing_x()
        l_bank()
        probit()
        coin_w()
        # bitfinex()
        # kraken()
        # ku_coin()
        # bit_mart()

        cursor.execute("DELETE FROM symbols where symbol_name NOT LIKE '%USDT'")

        patterns_to_delete = [
            '%2L%', '%3L%', '%4L%', '%5L%', '%6L%',
            '%2S%', '%3S%', '%4S%', '%5S%', '%6S%',
            '%EUTF0%', '%BTCF0%', '%USTF0%'
        ]

        for pattern in patterns_to_delete:
            cursor.execute("DELETE FROM symbols WHERE symbol_name LIKE %s", (pattern,))

        cursor.execute("DELETE FROM symbols WHERE symbol_name ILIKE %s", ('%TEST%',))

        cursor.execute(
            "UPDATE symbols SET symbol_name = replace(symbol_name, '_-PERP', '-PERP') WHERE remark = 'hitbtc'")

        connection.commit()

        # update_stable_coin()

        cursor.execute("UPDATE symbols SET remark = ''")

        cursor.execute("""
            DELETE FROM symbols
            WHERE symbol_id IN (
                SELECT symbol_id
                FROM (
                    SELECT ROW_NUMBER() OVER(PARTITION BY symbol_name) rn, t.symbol_id
                    FROM symbols t
                ) T WHERE rn > 1
            )
        """)

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(f"symbols table updated in {elapsed_time} seconds.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


def update_chain():
    start_time = time.time()
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE chain")
    connection.commit()

    okx_chain()
    binance_chain()
    huobi_chain()
    mexc_chain()
    gate_io_chain()
    digi_finex_chain()
    bing_x_chain()
    l_bank_chain()
    xt_chain()
    bit_get_chain()
    bybit_chain()
    poloniex_chain()
    ascend_ex_chain()
    probit_chain()

    with open('update_chains.sql', 'r') as file:
        sql_commands = file.readlines()

    for command in sql_commands:
        if command.strip():
            cursor.execute(command)

    connection.commit()
    cursor.close()
    release_connection(connection)
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"chain table updated in {elapsed_time} seconds.")


def update_stable_coin():
    start_time = time.time()
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE stablecoin")
    connection.commit()

    with open('update_stable_coin.sql', 'r') as file:
        sql_commands = file.readlines()

    for command in sql_commands:
        if command.strip():
            cursor.execute(command)

    connection.commit()
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"stable_coin table updated in {elapsed_time} seconds.")


def update_usd_to_others_rate():
    try:
        start_time = time.time()
        url = "https://api.exchangeratesapi.io/v1/latest?access_key=3776cdaf4046ecefa3f8178dad10ea78&base=USD&symbols=GBP,JPY,EUR,TRY,BRL,AED,AUD,CAD,CHF,SGD,CNY,RUB,NGN"
        response = requests.get(url)
        rates = response.json()['rates']

        connection = get_connection()
        cursor = connection.cursor()

        for currency, rate in rates.items():
            cursor.execute("""
                UPDATE stablecoin
                SET price = %s
                WHERE symbol_name = %s;
            """, (rate, currency))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- update_usd_to_others_rate executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


update_symbols()
update_chain()
# update_usd_to_others_rate()

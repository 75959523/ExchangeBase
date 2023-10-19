import os
import sched
import time
import requests

from datetime import datetime, timedelta
from pytz import timezone

from chain_collection.mod10_xt import xt_chain
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from symbols_collection.mod10_bybit import bybit
from symbols_collection.mod11_xt import xt
from symbols_collection.mod12_hitbtc import hitbtc
from symbols_collection.mod13_bit_mart import bit_mart
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
from symbols_collection.mod26_kraken import kraken
from symbols_collection.mod27_ku_coin import ku_coin
from symbols_collection.mod28_poloniex import poloniex
from symbols_collection.mod2_binance import binance
from symbols_collection.mod3_huobi import huobi
from symbols_collection.mod4_bit_get import bit_get
from symbols_collection.mod5_bitfinex import bitfinex
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
from chain_collection.mod8_ku_coin import ku_coin_chain

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
        huobi()
        bit_get()
        bitfinex()
        mexc()
        bit_venus()
        deep_coin()
        ascend_ex()
        bybit()
        xt()
        hitbtc()
        bit_mart()
        bigone()
        jubi()
        la_token()
        coinex()
        gate_io()
        coin_w()
        bi_ka()
        hot_coin()
        digi_finex()
        l_bank()
        bing_x()
        probit()
        kraken()
        ku_coin()
        poloniex()

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

        update_stable_coin()

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


def update_stable_coin():
    start_time = time.time()
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE stablecoin")
    connection.commit()

    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'okx' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'okx'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'binance' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'binance'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'huobi' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'huobi'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bitget' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bit_get'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bitfinex' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bitfinex'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'mexc' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'mexc'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bitvenus' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bit_venus'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'deepcoin' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'deep_coin'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'ascendex' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'ascend_ex'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bybit' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bybit'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'xt' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'xt'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'hitbtc' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'hitbtc'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bitmart' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bit_mart'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bigone' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bigone'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'jubi' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'jubi'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'latoken' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'la_token'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'coinex' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'coinex'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'gateio' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'gate_io'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'coinw' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'coin_w'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bika' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bi_ka'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'hotcoin' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'hot_coin'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'digifinex' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'digi_finex'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'lbank' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'l_bank'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'bingx' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'bing_x'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'probit' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'probit'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'kraken' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'kraken'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'kucoin' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'kucoin'")
    cursor.execute(
        "INSERT INTO stablecoin(exchange_name, symbol_name) SELECT DISTINCT 'poloniex' AS exchange_name, SPLIT_PART(symbol_name, '-', 2) AS symbol_name FROM SYMBOLS WHERE remark = 'poloniex'")

    cursor.execute("UPDATE stablecoin SET price = 0")

    cursor.execute(
        "UPDATE stablecoin SET price = 1 WHERE symbol_name IN ('USDT','USD','TUSD','BUSD','LUSD','USDC','USDP','DAI','PAX','XTUSD')")

    cursor.execute(
        "UPDATE stablecoin SET status = 1")

    connection.commit()
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"stable_coin table updated in {elapsed_time} seconds.")


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
    ku_coin_chain()
    xt_chain()

    # okx
    cursor.execute("UPDATE chain SET chain = split_part(chain, '-', 2) WHERE exchange_name = 'okx'")
    # binanceTron (TRC20)
    cursor.execute(
        "UPDATE chain SET chain = 'BEP2' WHERE chain = 'BNB Beacon Chain (BEP2)' AND exchange_name = 'binance'")
    cursor.execute(
        "UPDATE chain SET chain = 'BEP20' WHERE chain = 'BNB Smart Chain (BEP20)' AND exchange_name = 'binance'")
    cursor.execute("UPDATE chain SET chain = 'ERC20' WHERE chain = 'Ethereum (ERC20)' AND exchange_name = 'binance'")
    cursor.execute(
        "UPDATE chain SET chain = 'TRC20' WHERE chain = 'Tron (TRC20)' AND exchange_name = 'binance'")
    # mexc
    cursor.execute("UPDATE chain SET chain = 'BEP20' WHERE chain = 'BEP20(BSC)' AND exchange_name = 'mexc'")
    # gateio
    cursor.execute("UPDATE chain SET chain = 'ERC20' WHERE chain = 'ETH/ERC20' AND exchange_name = 'gateio'")
    cursor.execute("UPDATE chain SET chain = 'BEP20' WHERE chain = 'BSC/BEP20' AND exchange_name = 'gateio'")
    # digifinex
    cursor.execute("DELETE FROM chain WHERE chain = '' AND exchange_name = 'digifinex'")
    # xt
    cursor.execute(
        "UPDATE chain SET chain = 'BEP' WHERE chain = 'BNB Smart Chain' AND exchange_name = 'xt'")
    cursor.execute(
        "UPDATE chain SET chain = 'ERC20' WHERE chain = 'Ethereum' AND exchange_name = 'xt'")

    connection.commit()
    cursor.close()
    release_connection(connection)
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"chain table updated in {elapsed_time} seconds.")


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

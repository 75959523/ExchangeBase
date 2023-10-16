import os
import sched
import time
import requests

from datetime import datetime, timedelta
from pytz import timezone
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection

project_root = os.path.dirname(os.path.abspath(__file__))
logger = setup_logger("ten_minute_task", os.path.join(project_root, 'log', 'app.log'))


def schedule_minute_task():
    s = sched.scheduler(time.time, time.sleep)
    shanghai_tz = timezone('Asia/Shanghai')

    def run_task(sc):
        current_time = datetime.now(shanghai_tz).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"ten minute task executed {current_time}")
        update_usd_to_cny_rate()
        next_run_time = datetime.now(shanghai_tz) + timedelta(minutes=10)
        s.enterabs(time.mktime(next_run_time.timetuple()), 1, run_task, (sc,))

    now = datetime.now(shanghai_tz)
    minutes_to_next_interval = 10 - (now.minute % 10)
    next_interval = now + timedelta(minutes=minutes_to_next_interval)
    time_to_next_interval = (next_interval - now).total_seconds()

    s.enter(time_to_next_interval, 1, run_task, (s,))
    s.run()


def update_usd_to_cny_rate():
    try:
        start_time = time.time()
        url = "https://open.er-api.com/v6/latest/USD"
        response = requests.get(url)
        data = response.json()
        cny_rate = data['rates']['CNY']

        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("""
            UPDATE usd_to_cny_rate 
            SET rate = %s
            WHERE name = 'CNY';
        """, (cny_rate,))

        connection.commit()
        cursor.close()
        release_connection(connection)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(
            f"-------------------------------------------------- update_usd_to_cny_rate executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


update_usd_to_cny_rate()

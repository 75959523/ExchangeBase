from datetime import datetime
from pytz import timezone


def get_current_time():
    current_time = datetime.now().astimezone(timezone('Asia/Shanghai'))
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time

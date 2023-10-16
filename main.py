from multiprocessing import Process
from night_task import schedule_night_task
from minute_task import schedule_minute_task

if __name__ == '__main__':
    p1 = Process(target=schedule_night_task)
    p1.start()
    p2 = Process(target=schedule_minute_task)
    p2.start()

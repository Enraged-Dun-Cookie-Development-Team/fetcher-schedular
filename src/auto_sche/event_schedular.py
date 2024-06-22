import time
from datetime import datetime, timedelta
import tornado.ioloop
from tornado.ioloop import PeriodicCallback


# 蹲饼的事件
def event():
    print(f"Dun Event triggered at {datetime.now()}")


# 先按每秒1个点写demo.
predictions = [False] * 86400 
predictions[10] = True
predictions[20] = True

# 记录开始执行的时间
start_time = datetime.now()

def check_and_trigger():
    current_time = datetime.now()
    elapsed_time = (current_time - start_time).seconds
    if elapsed_time < len(predictions) and predictions[elapsed_time]:
        event()


def model_predict():
    global predictions, start_time
    print(f"Model prediction triggered at {datetime.now()}")

    # 先按每秒1个点写demo.
    predictions = [False] * 86400
    predictions[10] = True
    predictions[20] = True
    # 重新记录程序开始执行的时间
    start_time = datetime.now()
    # 预测完成，预约下一个4点执行下一次.
    setup_daily_task()

# 计算当前时间到下一个凌晨4点的时间间隔
def time_until_next_4am():
    now = datetime.now()
    next_4am = datetime(now.year, now.month, now.day, 4, 0, 0)
    if now >= next_4am:
        next_4am += timedelta(days=1)
    return (next_4am - now).total_seconds()

# 设置每天凌晨4点执行的任务
def setup_daily_task():
    io_loop = tornado.ioloop.IOLoop.current()
    # 计算到下一个4点的时间间隔
    delay = time_until_next_4am()
    print(f"Next model prediction scheduled in {delay} seconds")
    # 设定延时任务
    io_loop.call_later(delay, model_predict)


# 启动IOLoop并设置初始任务
if __name__ == "__main__":
    # 初始化IOLoop
    io_loop = tornado.ioloop.IOLoop.current()
    # 设置初始任务
    setup_daily_task()
    # 创建并开始一个PeriodicCallback，每秒检查一次
    callback = PeriodicCallback(check_and_trigger, 1000)  # 每秒调用一次
    callback.start()
    # 启动IOLoop
    io_loop.start()
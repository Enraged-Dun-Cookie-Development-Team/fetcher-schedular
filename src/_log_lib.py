import logging
import logging.config
import psutil

logger = None

def set_logger(log_conf_file=''):

    """初始化log，如果不指定conf_file的话，默认输出到stderr"""
    if len(log_conf_file):
        try:
            logging.config.fileConfig(log_conf_file)
        except:
            set_default_logger()
    else:
        set_default_logger()


def set_default_logger():
    """set_default_logger"""

    global logger

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()  # 输出到stderr
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(module)s' \
                                  '* %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_memory_usage():
    # 获取当前进程的内存使用情况，返回字符串。
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_in_mb = memory_info.rss / 1024 / 1024  # 使用rss，即Resident Set Size，返回以字节为单位; 转化成mb
    return f"{memory_in_mb:.2f} MB"

set_logger(log_conf_file='../conf.conf.log')

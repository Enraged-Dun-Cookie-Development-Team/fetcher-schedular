import logging
import logging.config

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


set_logger(log_conf_file='../conf.conf.log')

import os
import json
import sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


class ConfigParser(object):

    def __init__(self):
        try:
            conf = self.load_json_config()
            # print(conf)
        except:
            # traceback.print_exc()
            conf = self.load_environ_config()

        self.CONFIG = conf

    def load_json_config(self):
        """
        读取配置文件作为config.
        :return:
        """
        with open('../conf/aconf.json', 'r') as f:
            conf = json.load(f)

        return conf

    def load_environ_config(self):
        """
        读取环境变量作为config.
        :return:
        """
        conf = os.environ
        # CEOBE开头的环境变量是调度器使用的.
        conf = {c: conf[c] for c in conf if c.startswith('CEOBE')}

        parsed_conf = dict()
        # 解析所有环境变量
        for k, v in conf.items():
            k_parsed = k.split('__')
            # 单层
            if len(k_parsed) == 2:
                parsed_conf[k_parsed[1]] = v
            # 两层
            else:
                k_1 = k_parsed[1]
                k_2 = k_parsed[2]

                if k_1 in parsed_conf:
                    parsed_conf[k_1][k_2] = v
                else:
                    parsed_conf[k_1] = dict()
                    parsed_conf[k_1][k_2] = v

        return parsed_conf


conf_parser = ConfigParser()
CONFIG = conf_parser.CONFIG
print(CONFIG)
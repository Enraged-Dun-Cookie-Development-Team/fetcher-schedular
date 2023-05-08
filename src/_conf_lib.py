import os
import json
import sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# 将json的所有key转换为大写
def upper_json(json_info):
    if isinstance(json_info,dict):
        for key in list(json_info.keys()):
            if key.isupper():
                upper_json(json_info[key])
            else:
                key_upper = key.upper()
                json_info[key_upper] = json_info[key]
                del json_info[key]
                upper_json(json_info[key_upper])

    elif isinstance(json_info,list):
        for item in json_info:
            upper_json(item)

class ConfigParser(object):

    def __init__(self):
        try:
            conf = self.load_json_config()
            print(conf)
        except:
            traceback.print_exc()
            conf = self.load_environ_config()

        self.CONFIG = conf

    def load_json_config(self):
        """
        读取配置文件作为config.
        :return:
        """
        with open('./conf/conf.json', 'r') as f:
            conf = json.load(f)
            upper_json(conf)

        return conf

    def load_environ_config(self):
        """
        读取环境变量作为config.
        :return:
        """
        conf = os.environ
        # CEOBE开头的环境变量是调度器使用的.
        tmp_conf = {c: conf[c] for c in conf if c.startswith('CEOBE')}

        # 固定为1个下划线，为保证兼容，调整为2个下划线
        conf = dict()
        for k in tmp_conf:
            conf[k.replace('CEOBE_', 'CEOBE__').replace('___', '__')] = tmp_conf[k]

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

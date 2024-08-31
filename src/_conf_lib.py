import os
import json
import sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src._log_lib import logger

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
                # print(key)
                upper_json(json_info[key_upper])

    elif isinstance(json_info,list):
        for item in json_info:
            upper_json(item)


class ConfigParser(object):

    def __init__(self):
        try:
            conf = self.load_json_config()
            # print(conf)
        except:
            # traceback.print_exc()
            conf = self.load_environ_config()
            logger.info('config初始化：使用环境变量')
        # 自动蹲饼的config构造：
        # 1. 读取 datasource -> encoded feature的映射表
        # 2. 读取其他配置
        print(__file__)
        auto_sche_conf = self.load_json_config(config_name='./conf/auto_sche.conf')

        # datasource_to_idx_mapping = self.load_json_config('./conf/datasource_to_idx_mapping.json')
        # idx_to_datasource_mapping = {datasource_to_idx_mapping[c]['datasource_idx']: c for c in datasource_to_idx_mapping}
        # auto_sche_conf['datasource_to_idx_mapping'] = datasource_to_idx_mapping
        # auto_sche_conf['idx_to_datasource_mapping'] = idx_to_datasource_mapping
        
        # 3. 配置合并

        self.CONFIG = conf
        self.AUTO_SCHE_CONFIG = auto_sche_conf

    def load_json_config(self, config_name='./conf/conf.json'):
        """
        读取配置文件作为config.
        :return:
        """
        with open(config_name, 'r') as f:
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
AUTO_SCHE_CONFIG = conf_parser.AUTO_SCHE_CONFIG

# print(CONFIG)

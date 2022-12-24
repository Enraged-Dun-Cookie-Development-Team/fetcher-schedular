import os

import configparser
import io

from internal.conf.nacos_client import NacosRead

a = ''
n = ''
u = ''
p = ''


class ReadConfig:
    """读取配置文件类"""

    def __init__(self, address, namespace, user, password):
        global a, n, u, p
        a = address
        n = namespace
        u = user
        p = password
        rootDir = os.getcwd()
        configPath = os.path.join(rootDir, "configs/*.ini")
        # 读取nacos配置
        nacos = NacosRead(a, n, u, p)
        readConfig = nacos.GetConfig()
        self.cf = configparser.ConfigParser()
        if readConfig:
            self.cf.readfp(io.StringIO(readConfig))
        else:
            self.cf.read(configPath)

    def getDb(self, param):
        """获取mysql配置信息"""
        value = self.cf.get("Mysql", param)
        return value

    def getRedis(self, param):
        """获取redis配置信息"""
        value = self.cf.get("Redis", param)
        return value

    def getEs(self, param):
        """获取es配置信息"""
        value = self.cf.get("Elasticsearch", param)
        return value

    def getItems(self, name):
        """获取当前名称下的配置"""
        items = self.cf.items(name)
        return items

    def getAll(self):
        """获取所有配置信息"""
        secs = self.cf.sections()
        return secs

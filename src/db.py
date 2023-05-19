import pymysql.cursors
import redis
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from src._conf_lib import CONFIG

class HandleMysql:
    def __init__(self, conf):
        self.cur = None
        self.conn = None
        # print(conf)
        self.data = conf['DB']

    def connMyql(self):
        """连接数据库"""
        host = self.data.get("HOST", '127.0.0.1')
        user = self.data.get("USER", 'root')
        password = self.data.get("PASSWORD", '123')

        db = self.data.get("DB_NAME", 'ceobe_canteen')
        port = self.data.get("PORT", 3306)
        charset = self.data.get("CHARSET", 'utf8mb4')
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, port=int(port), charset=charset)
        self.cur = self.conn.cursor()

    def executeSql(self, sql):
        """执行sql"""
        self.connMyql()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def closeMysql(self):
        """关闭连接"""
        self.cur.close()
        self.conn.close()


class HandleRedis:
    def __init__(self, conf):
        conf = conf['REDIS']
        self.host = conf.get('HOST', 'localhost')
        self.port = int(conf.get('PORT', 6379))
        self.db = int(conf.get('DB', 0))
        self.password = conf.get('PASSWORD', 0)

        self.conn = redis.StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

    def get(self, name):
        return self.conn.get(name)

    def set(self, name, value):
        return self.conn.set(name, value)


sql_client = HandleMysql(CONFIG)


def fetch_col_names(table_name_list) -> dict:
    output_dict = dict()
    for table_name in table_name_list:
        df_tmp = pd.DataFrame(sql_client.executeSql('show columns from {};'.format(table_name)))
        output_dict[table_name] = df_tmp[0].tolist()

    return output_dict

table_name_list = ['fetcher_datasource_config',
                   'fetcher_config',
                   'fetcher_global_config',
                  'fetcher_platform_config']

table_col_names = fetch_col_names(table_name_list)


def select_fetcher_datasource_config(platform='') -> pd.DataFrame:
    if platform:
        df = pd.DataFrame(
            sql_client.executeSql('select * from fetcher_datasource_config where platform="{}";'.format(platform)))
    else:
        df = pd.DataFrame(sql_client.executeSql('select * from fetcher_datasource_config;'.format(platform)))

    df.columns = table_col_names['fetcher_datasource_config']
    return df


def select_fetcher_config(platform='') -> pd.DataFrame:
    if platform:
        df = pd.DataFrame(sql_client.executeSql('select * from fetcher_config where platform="{}";'.format(platform)))
    else:
        df = pd.DataFrame(sql_client.executeSql('select * from fetcher_config;'.format(platform)))

    df.columns = table_col_names['fetcher_config']
    return df


def select_fetcher_global_config() -> pd.DataFrame:
    df = pd.DataFrame(sql_client.executeSql('select * from fetcher_global_config;'))
    df.columns = table_col_names['fetcher_global_config']

    return df


def select_fetcher_platform_config(platform='') -> pd.DataFrame:
    if platform:
        df = pd.DataFrame(
            sql_client.executeSql('select * from fetcher_platform_config where platform="{}";'.format(platform)))
    else:
        df = pd.DataFrame(sql_client.executeSql('select * from fetcher_platform_config;'.format(platform)))

    df.columns = table_col_names['fetcher_platform_config']
    return df


if __name__ == '__main__':

    # r = HandleRedis(dict())
    # r.set('a', 'b')
    # print(r.get('a')) # ✓
    print(sql_client.executeSql('select * from fetcher_datasource_config'))

    fetcher_datasource_config_df = select_fetcher_datasource_config(platform='bilibili')
    fetcher_config_df = select_fetcher_config()
    fetcher_global_config_df = select_fetcher_global_config()
    fetcher_platform_config_df = select_fetcher_platform_config()

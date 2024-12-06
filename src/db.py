import pymysql.cursors
import redis
import pandas as pd
from pandas import DataFrame  # 取数解压时使用.
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from src._conf_lib import CONFIG

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, inspect, create_engine, event
from sqlalchemy import Table
from sqlalchemy.exc import DisconnectionError
# 专门处理优化这里.
import pickle
import zlib


def checkout_listener(dbapi_con, con_record, con_proxy):
    try:
        try:
            dbapi_con.ping(False)
        except TypeError:
            dbapi_con.ping()
    except dbapi_con.OperationalError as exc:
        if exc.args[0] in (2006, 2013, 2014, 2045, 2055):
            raise DisconnectionError()
        else:
            raise


class HandleMysql:
    def __init__(self, conf):
        self.cur = None
        self.conn = None
        # print(conf)
        self.data = conf.get('DB',dict())
        self.connMyql()

    def connMyql(self):
        """连接数据库"""
        host = self.data.get("HOST", '127.0.0.1')
        user = self.data.get("USER", 'root')
        password = self.data.get("PASSWORD", '123')

        db = self.data.get("DB_NAME", 'ceobe_canteen')
        port = self.data.get("PORT", 3306)
        charset = self.data.get("CHARSET", 'utf8mb4')

        self.engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset={}'.format(
                    user, password, host, port, db, charset), pool_size=100, pool_recycle=600, pool_pre_ping=True)

        self.metadata = MetaData(bind=self.engine)

        
    def sessMyql(self):
        Session = sessionmaker(self.engine)
        db_session = Session()
        
        return db_session

    def queryMyql(self, table_name=''):
        '''
        封装查询步骤, 每次重新启动session; 如果失败最多重试3次.
        '''
        max_retry = 3
        success_mark = False
        while max_retry > 0 and not success_mark:
            try:
                db_session = self.sessMyql()
                table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
                db_query = db_session.query(table)
                db_session.close()
                success_mark = True
            except:
                max_retry -= 1

        return db_query, table


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

    def set_with_ttl(self, name, value, seconds):
        """
        存数时设置秒为单位的ttl.
        :param name: key
        :param value: value
        :param seconds: ttl，单位为秒
        :return:
        """
        # 参数: 键名、过期时间（秒）、键值
        self.conn.setex(name, seconds, value)

        # 检查ttl是否确实存入
        if self.conn.exists(name):
            ttl = self.conn.ttl(name)
            return "The TTL of {}' is {} seconds.".format(name, ttl)
        else:
            return "The key 'key_with_ttl' does not exist."

    def update_ttl(self, name, seconds):

        # 如果需要更新已有键的ttl
        return self.conn.expire(name, seconds)  # TTL = seconds

    def set_ttl_in_future_timestamp(self, name, timestamp_in_future):

        # timestamp_in_future = int(time.time()) + 60  # 当前时间加60秒

        # 如果需要设置ttl为某个时间点
        return self.conn.expireat(name, timestamp_in_future)

    @staticmethod
    def compress_data(data):
        """
        dataframe在传入redis前要先压缩成bytes.
        :param data:
        :return:
        """
        bytes_ = zlib.compress(pickle.dumps(data), 5)
        return bytes_

    @staticmethod
    def extract_data(self, bytes_):
        """
        redis取出数据时要还原成对应的数据结构.
        :return:
        """
        pass


sql_client = HandleMysql(CONFIG)
event.listen(sql_client.engine, 'checkout', checkout_listener)


def fetch_col_names(table_name_list) -> dict:
    
    output_dict = dict()
    # 反射获取数据表meta信息.
    for table_name in table_name_list:
        _, table = sql_client.queryMyql(table_name)
        output_dict[table_name] = list(table.columns.keys())

    return output_dict

table_name_list = ['fetcher_datasource_config',
                   'fetcher_config',
                   'fetcher_global_config',
                  'fetcher_platform_config']

table_col_names = fetch_col_names(table_name_list)


def select_fetcher_config(platform='') -> pd.DataFrame:
    table_name = 'fetcher_config'
    db_query, table = sql_client.queryMyql(table_name)
    
    if platform:
        content = db_query.filter(table.columns.platform == platform).all()
    else:
        content = db_query.all()
        
    df = pd.DataFrame(content, columns=table_col_names[table_name]) 
    return df


def select_fetcher_global_config() -> pd.DataFrame:
    
    table_name = 'fetcher_global_config'
    db_query, table = sql_client.queryMyql(table_name)
    content = db_query.all() 
    df = pd.DataFrame(content, columns=table_col_names[table_name]) 
    
    return df


def select_fetcher_platform_config(platform='') -> pd.DataFrame:
    
    table_name = 'fetcher_platform_config'
    db_query, table = sql_client.queryMyql(table_name)
    
    if platform:
        content = db_query.filter(table.columns.platform == platform).all()
    else:
        content = db_query.all()
        
    df = pd.DataFrame(content, columns=table_col_names[table_name]) 
    return df

def select_fetcher_datasource_config(platform='') -> pd.DataFrame:
    
    table_name = 'fetcher_datasource_config'
    db_query, table = sql_client.queryMyql(table_name)
    
    if platform:
        content = db_query.filter(table.columns.platform == platform).all()
    else:
        content = db_query.all()
        
    df = pd.DataFrame(content, columns=table_col_names[table_name])  
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

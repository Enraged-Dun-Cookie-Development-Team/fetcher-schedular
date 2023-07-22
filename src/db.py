import pymysql.cursors
import redis
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from src._conf_lib import CONFIG

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, inspect, create_engine
from sqlalchemy import Table


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
                    user, password, host, port, db, charset))
        
        self.metadata = MetaData(bind=self.engine)

        
    def sessMyql(self):
        Session = sessionmaker(self.engine)
        db_session = Session()
        
        return db_session
    
    def queryMyql(self, table_name=''):
        '''
        封装查询步骤
        '''
        db_session = self.sessMyql()
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
        db_query = db_session.query(table)
        db_session.close()
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


sql_client = HandleMysql(CONFIG)


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

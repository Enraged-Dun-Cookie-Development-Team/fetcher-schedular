import pymysql.cursors
import redis

class HandleMysql:
    def __init__(self, conf):
        self.cur = None
        self.conn = None
        self.data = conf

    def connMyql(self):
        """连接数据库"""
        host = self.data["Host"]
        user = self.data["User"]
        password = self.data["Password"]
        db = self.data["DbName"]
        port = self.data["Port"]
        charset = self.data["Charset"]
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, port=int(port), charset=charset)
        self.cur = self.conn.cursor()

    def executeSql(self, sql):
        """执行sql"""
        self.connMyql()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def search(self, sql):
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
        self.port = int(conf.get('port', 6379))
        self.db = int(conf.get('db', 0))
        self.conn = redis.StrictRedis(host='localhost', port=self.port, db=self.db)

    def get(self, name):
        return self.conn.get(name)

    def set(self, name, value):
        return self.conn.set(name, value)


if __name__ == '__main__':

    r = HandleRedis(dict())
    r.set('a', 'b')
    print(r.get('a')) # ✓

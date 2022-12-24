import pymysql.cursors


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

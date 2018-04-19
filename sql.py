import psycopg2

from sqlstate import SQL_CURRENT_ALARM

class AlarmSql(object):
    def __init__(self, dbname, host, user, root):
        self.dbname = dbname
        self.host = host
        self.dbuser = user
        self.root = root
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     host=self.host,
                                     user=self.dbuser)
        self.cur = self.conn.cursor()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print "close"

    def current_alarm(self):
        sql_str = SQL_CURRENT_ALARM.format(self.root)
        self.cur.execute(sql_str)
        data = self.cur.fetchall()
        return data

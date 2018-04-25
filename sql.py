import re

import psycopg2

from sqlstate import *

class AlarmSql(object):
    def __init__(self, dbname, logdbname, host, user, root):
        self.dbname = dbname
        self.host = host
        self.dbuser = user
        self.root = root
        self.logdbname = logdbname

        self.conn_alm = None
        self.cur_alm = None
        self.conn_log = None
        self.cur_log = None

        self.pvlist = {}
        self.grouplist = {}

    def connect(self):
        self.conn_alm = psycopg2.connect(dbname=self.dbname,
                                     host=self.host,
                                     user=self.dbuser)
        self.cur_alm = self.conn_alm.cursor()

        self.conn_log = psycopg2.connect(dbname=self.logdbname,
                                     host=self.host,
                                     user=self.dbuser)
        self.cur_log = self.conn_log.cursor()

    def close(self):
        if self.cur_alm:
            self.cur_alm.close()
        if self.conn_alm:
            self.conn_alm.close()

        if self.cur_log:
            self.cur_log.close()
        if self.conn_log:
            self.conn_log.close()

        print "close"

    def current_alarm(self):
        sql_str = SQL_CURRENT_ALARM.format(self.root)
        self.cur_alm.execute(sql_str)
        data = self.cur_alm.fetchall()
        return data

    def history_alarm_all(self, starttime, endtime):
        # id, datum, record_name, severity, eventtime, status
        self.cur_log.execute(SQL_HISTORY_ALL, (starttime, endtime))
        sql_res = self.cur_log.fetchall()

        data = []
        for row in sql_res:
            if row[2] in self.pvlist:
                t = (self.pvlist[row[2]]["group"], self.pvlist[row[2]]["msg"])
            else:
                t = ("Unkown", "Unkown")
            data.append(row + t)

        return data

    def history_alarm_group(self, group, starttime, endtime):
        pattern = re.compile(group)

        pvlist = []
        for g, pvs in self.grouplist.items():
            if pattern.match(g):
                pvlist.extend(pvs)

        # id, datum, record_name, severity, eventtime, status
        self.cur_log.execute(SQL_HISTORY_GROUP, (starttime, endtime, pvlist))
        data = self.cur_log.fetchall()

        data = [r + (self.pvlist[r[2]]["group"], self.pvlist[r[2]]["msg"])
                for r in data]

        return data

    def update_pvlist(self):
        # name, message, group, sgroup, ssgroup
        sql_str = SQL_PV_LIST.format(self.root)
        self.cur_alm.execute(sql_str)
        data = self.cur_alm.fetchall()

        for row in data:
            if not row[2]:
                continue
            group = row[2]
            group += " / " + row[3] if row[3] else ""
            group += " / " + row[4] if row[4] else ""

            if not group in self.grouplist:
                self.grouplist[group] = []

            self.grouplist[group].append(row[0])
            self.pvlist[row[0]] = {"msg": row[1], "group": group}

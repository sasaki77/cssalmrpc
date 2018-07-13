import time
import re
import argparse
from datetime import datetime

from collections import OrderedDict

import pvaccess as pva

from sql import AlarmSql


class AlarmRPC(object):
    def __init__(self, dbname, logdbname, host, dbuser, root):
        self._rdb = AlarmSql(dbname, logdbname, host, dbuser, root)
        self._rdb.connect()
        self._rdb.update_pvlist()

    def close(self):
        self._rdb.close()

    def get_current(self, arg):
        entity = arg.getString("entity") if arg.hasField("entity") else ".*"

        pattern = re.compile(entity)

        # "time", "group", "subgroup", "subsubgroup"
        # "severity", "status", "message", "record"
        sql_res = self._rdb.current_alarm()

        filtered_res = []
        for row in sql_res:
            group = row[1] + self._sgstr(row[2]) + self._sgstr(row[3])
            if pattern.match(group):
                filtered_res.append(row)

        data = zip(*filtered_res)

        vals = OrderedDict([("column0", [pva.STRING]),
                            ("column1", [pva.STRING]),
                            ("column2", [pva.LONG]),
                            ("column3", [pva.STRING]),
                            ("column4", [pva.STRING]),
                            ("column5", [pva.STRING]),
                            ("column6", [pva.STRING])])
        table = pva.PvObject(OrderedDict({"labels": [pva.STRING],
                                          "value": vals}
                                         ),
                             'epics:nt/NTTable:1.0')
        labels = ["time", "group",  "severity_id", "severity",
                  "status", "message", "record"]
        table.setScalarArray("labels", labels)

        if not data:
            return table

        time = [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in data[0]]
        group = [g + self._sgstr(sg) + self._sgstr(ssg)
                 for g, sg, ssg in zip(data[1], data[2], data[3])]

        table.setStructure("value", OrderedDict({"column0": time,
                                                 "column1": group,
                                                 "column2": list(data[8]),
                                                 "column3": list(data[4]),
                                                 "column4": list(data[5]),
                                                 "column5": list(data[6]),
                                                 "column6": list(data[7])}))

        return table

    def get_history(self, arg):
        group = arg.getString("entity") if arg.hasField("entity") else ".*"
        msg = arg.getString("message") if arg.hasField("message") else ""

        try:
            starttime = arg.getString("starttime")
            endtime = arg.getString("endtime")
        except (pva.FieldNotFound, pva.InvalidRequest):
            print "Error: Invalid argumets"
            return pva.PvBoolean(False)

        # id, datum, record_name, severity, eventtime, status, group, message
        try:
            start = self._iso_to_dt(starttime)
            end = self._iso_to_dt(endtime)
        except ValueError:
            print "Error: Invalid argumets"
            return pva.PvBoolean(False)

        if group == "all":
            sql_res = self._rdb.history_alarm_all(msg, start, end)
        else:
            sql_res = self._rdb.history_alarm_group(group, msg, start, end)

        alarms = []
        recovers = []
        for row in sql_res:
            alarm = str(row[7]) if row[3] != "OK" else ""
            recover = str(row[7]) if row[3] == "OK" else ""

            alarms.append(alarm)
            recovers.append(recover)

        data = zip(*sql_res)

        vals = OrderedDict([("column0", [pva.STRING]),
                            ("column1", [pva.STRING]),
                            ("column2", [pva.STRING]),
                            ("column3", [pva.STRING]),
                            ("column4", [pva.STRING]),
                            ("column5", [pva.STRING]),
                            ("column6", [pva.STRING])
                            ])
        table = pva.PvObject(OrderedDict({"labels": [pva.STRING],
                                         "value": vals}
                                         ),
                             'epics:nt/NTTable:1.0')
        labels = ["time", "group", "severity", "status",
                  "alarm", "recover", "record"]
        table.setScalarArray("labels", labels)

        if not data:
            return table

        table.setStructure("value", OrderedDict({"column0": list(data[4]),
                                                 "column1": list(data[6]),
                                                 "column2": list(data[3]),
                                                 "column3": list(data[5]),
                                                 "column4": alarms,
                                                 "column5": recovers,
                                                 "column6": list(data[2])
                                                 })
                           )

        return table

    def _sgstr(self, sg_str):
        return " / " + sg_str if sg_str else ""

    def _iso_to_dt(self, iso_str):
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S")
            return dt
        except ValueError:
            raise


def parsearg():
    parser = argparse.ArgumentParser(description="CSS Alarm API pvAccess RPC.")
    parser.add_argument("-r", "--root", dest="root", required=True,
                        help="Alarm RDB Root Name")
    parser.add_argument("-p", "--prefix", dest="prefix", required=True,
                        help="PV Name Prefix")
    parser.add_argument("-H", "--host", dest="host", default="localhost",
                        help="Alarm RDB Host")
    parser.add_argument("-d", "--db", dest="db", default="alarm",
                        help="Alarm RDB DB Name")
    parser.add_argument("-l", "--logdb", dest="logdb", default="log",
                        help="Log DB Name")
    parser.add_argument("-u", "--user", dest="user", default="report",
                        help="Alarm RDB User Name")

    return parser.parse_args()


def main():
    arg = parsearg()
    alarm_rpc = AlarmRPC(arg.db, arg.logdb, arg.host, arg.user, arg.root)

    srv = pva.RpcServer()
    srv.registerService(arg.prefix + "current", alarm_rpc.get_current)
    srv.registerService(arg.prefix + "history", alarm_rpc.get_history)
    srv.startListener()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print "exit"
    finally:
        alarm_rpc.close()


if __name__ == "__main__":
    main()

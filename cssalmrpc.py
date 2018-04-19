import time
import re
import argparse

from collections import OrderedDict

import pvaccess as pva

from sql import AlarmSql


class AlarmRPC(object):
    def __init__(self, dbname, host, dbuser, root):
        self._rdb = AlarmSql(dbname, host, dbuser, root)
        self._rdb.connect()

    def close(self):
        self._rdb.close()
        
    def get(self, arg):
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
                            ("column2", [pva.STRING]),
                            ("column3", [pva.STRING]),
                            ("column4", [pva.STRING]),
                            ("column5", [pva.STRING]),
                            ("column6", [pva.LONG])])
        table = pva.PvObject(OrderedDict({"labels": [pva.STRING], "value": vals}),
                         'epics:nt/NTTable:1.0')
        labels = ["time", "group", "severity",
                  "status", "message", "record", "severity_id"]
        table.setScalarArray("labels", labels)

        if not data:
            return table

        time = [dt.strftime("%Y-%m-%d %H:%M:%S.%f") for dt in data[0]]
        group = [g + self._sgstr(sg) + self._sgstr(ssg)
                 for g, sg, ssg in zip(data[1], data[2], data[3])]

        table.setStructure("value", OrderedDict({"column0": time,
                                                 "column1": group,
                                                 "column2": list(data[4]),
                                                 "column3": list(data[5]),
                                                 "column4": list(data[6]),
                                                 "column5": list(data[7]),
                                                 "column6": list(data[8])}))

        return table

    def _sgstr(self, sg_str):
        return " / " + sg_str if sg_str else ""


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
    parser.add_argument("-u", "--user", dest="user", default="report",
                        help="Alarm RDB User Name")

    return parser.parse_args()


def main():
    arg = parsearg()
    alarm_rpc = AlarmRPC(arg.db, arg.host, arg.user, arg.root)

    srv = pva.RpcServer()
    srv.registerService(arg.prefix + "get", alarm_rpc.get)
    srv.startListener()

    try:
        while True:
            time.sleep(1)
    except:
        print "exit"
    finally:
        alarm_rpc.close()


if __name__ == "__main__":
    main()

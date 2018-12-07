import time
import re
import argparse
from datetime import datetime

from collections import OrderedDict

import pandas as pd
import psycopg2
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
        msg = arg.getString("message") if arg.hasField("message") else ""

        try:
            df = self._get_current_alarm(entity, msg)
        except ValueError:
            msg = "RDB Error: entity = {}, msg = {}".format(entity, msg)
            ret = self._make_error_res(msg)
            return ret
        except re.error as e:
            msg = "regex error ({}) entity = {}, msg = {}"
            msg = msg.format(e, entity, msg)
            ret = self._make_error_res(msg)
            return ret

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

        time = df["alarm_time"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        value = OrderedDict({"column0": time.astype(str).tolist(),
                             "column1": df["groups"].tolist(),
                             "column2": df["severity_id"].tolist(),
                             "column3": df["severity"].tolist(),
                             "column4": df["status"].tolist(),
                             "column5": df["descr"].tolist(),
                             "column6": df["pv_name"].tolist()})

        table.setStructure("value", value)

        return table

    def get_current_ann(self, arg):
        entity = arg.getString("entity") if arg.hasField("entity") else ".*"
        msg = arg.getString("message") if arg.hasField("message") else ""

        try:
            df = self._get_current_alarm(entity, msg)
        except ValueError:
            msg = "RDB Error: entity = {}, msg = {}".format(entity, msg)
            ret = self._make_error_res(msg)
            return ret
        except re.error as e:
            msg = "regex error ({}) entity = {}, msg = {}"
            msg = msg.format(e, entity, msg)
            ret = self._make_error_res(msg)
            return ret

        vals = OrderedDict([("column0", [pva.ULONG]),
                            ("column1", [pva.STRING]),
                            ("column2", [pva.STRING]),
                            ("column3", [pva.STRING])])
        table = pva.PvObject(OrderedDict({"labels": [pva.STRING],
                                          "value": vals}
                                         ),
                             'epics:nt/NTTable:1.0')
        table.setScalarArray("labels", ["time", "title", "tags", "text"])

        time = df["alarm_time"].dt.strftime("%s%f").str[:-3]

        value = OrderedDict({"column0": time.astype(int).tolist(),
                             "column1": df["descr"].tolist(),
                             "column2": df["groups"].tolist(),
                             "column3": df["pv_name"].tolist()})
        table.setStructure("value", value)

        return table

    def get_history(self, arg):
        group = arg.getString("entity") if arg.hasField("entity") else "all"
        msg = arg.getString("message") if arg.hasField("message") else ""

        try:
            start, end = self._get_time_from_arg(arg)
        except (pva.FieldNotFound, pva.InvalidRequest, ValueError):
            print "Error: Invalid argumets"
            msg = "Arguments Error: starttime or endtime are invalid"
            msg += ". args = " + str(arg)
            ret = self._make_error_res(msg)
            return ret

        try:
            if group == "all":
                df = self._rdb.history_alarm_all(msg, start, end)
            else:
                df = self._rdb.history_alarm_group(group, msg, start, end)
        except psycopg2.Error:
            temp = ("RDB Error: entity = {}, msg = {},"
                    "starttime = {}, endtime={}")
            msg = temp.format(entity, msg, starttime, endtime)
            ret = self._make_error_res(msg)
            return ret

        alarms = df["message"].copy()
        recovers = df["message"].copy()

        alarms[df["severity"] == "OK"] = ""
        recovers[df["severity"] != "OK"] = ""

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

        value = OrderedDict({"column0": df["eventtime"].tolist(),
                             "column1": df["group"].tolist(),
                             "column2": df["severity"].tolist(),
                             "column3": df["status"].tolist(),
                             "column4": alarms.tolist(),
                             "column5": recovers.tolist(),
                             "column6": df["record_name"].tolist()
                             })
        table.setStructure("value", value)

        return table

    def _sgstr(self, sg_str):
        return " / " + sg_str if sg_str else ""

    def _iso_to_dt(self, iso_str):
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S")
            return dt
        except ValueError:
            raise

    def _make_error_res(self, message):
        labels = OrderedDict({"value": pva.BOOLEAN, "descriptor": pva.STRING})
        ret = pva.PvObject(labels, "epics:nt/NTScalar:1.0")
        ret["value"] = False
        ret["descriptor"] = message
        return ret

    def _get_current_alarm(self, group, msg):
        # "alarm_time", "group", "sub_group", "sub_sub_group"
        # "severity", "status", "descr", "pv_name", "severity_id"
        try:
            if msg:
                df = self._rdb.current_alarm_msg(msg)
            else:
                df = self._rdb.current_alarm_all()
        except ValueError:
            raise

        df["groups"] = (df["group"] + df["sub_group"].apply(self._sgstr)
                        + df["sub_sub_group"].apply(self._sgstr))

        try:
            filtered_df = df[df["groups"].str.match(group)]
        except re.error as e:
            raise

        return filtered_df

    def _get_time_from_arg(self, arg):
        try:
            starttime = arg.getString("starttime")
            endtime = arg.getString("endtime")
        except (pva.FieldNotFound, pva.InvalidRequest):
            raise

        # id, datum, record_name, severity, eventtime, status, group, message
        try:
            start = self._iso_to_dt(starttime)
            end = self._iso_to_dt(endtime)
        except ValueError:
            raise

        return start, end


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
    srv.registerService(arg.prefix + "current:ann", alarm_rpc.get_current_ann)
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

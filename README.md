# cssalmrpc

This program is pvAccess RPC server to publish CSS alarm status and log RDB.

## Features
- Supports only PostgreSQL backend
- Supports 2nd depth system from area

## Installing

Before use this program you need to install [PvaPy](https://github.com/epics-base/pvaPy).

After install PvaPy, clone this package and install other requirements.

```bash
$ pip install -r requirements.txt
```

## Usage

```bash
usage: cssalmrpc.py [-h] -r ROOT -p PREFIX [-H HOST] [-d DB] [-l LOGDB]
                    [-u USER]

CSS Alarm API pvAccess RPC.

optional arguments:
  -h, --help            show this help message and exit
  -r ROOT, --root ROOT  Alarm RDB Root Name
  -p PREFIX, --prefix PREFIX
                        PV Name Prefix
  -H HOST, --host HOST  Alarm RDB Host
  -d DB, --db DB        Alarm RDB DB Name
  -l LOGDB, --logdb LOGDB
                        Log DB Name
  -u USER, --user USER  Alarm RDB User Name
```

## API

- $(prefix):current
- $(prefix):history

### Example 1: current status

Example request
```
structure 
    string mode current
    string entity MPS
```


Example response
```
epics:nt/NTTable:1.0
    string[] labels [time,group,severity_id,severity,status,message,record]
    structure value
        string[] column0 [2018-05-17 09:00:02.476161]
        string[] column1 [MPS]
        long[] column2 [6]
        string[] column3 [MAJOR]
        string[] column4 [STATE_ALARM]
        string[] column5 [MPS issued]
        string[] column6 [RECORD:PREFIX:MPS]
```

### Example 2: alarm history

Example request
```
structure 
    string starttime 2018-05-17T11:00:00
    string endtime 2018-05-17T11:30:00
    string mode history
    string entity VAC
```


Example response
```
epics:nt/NTTable:1.0
    string[] labels [time,group,severity,status,alarm,recover,record]
    structure value
        string[] column0 [2018-05-17 11:27:25.084,2018-05-17 11:27:25.084]
        string[] column1 [VAC,VAC]
        string[] column2 [MAJOR,MAJOR]
        string[] column3 [HIHI_ALARM,HIHI_ALARM]
        string[] column4 [Vacuum Pressure is High,Vacuum Pressure is High]
        string[] column5 []
        string[] column6 [RECORD:PREFIX:VAC,RECORD:PREFIX:VAC]

```

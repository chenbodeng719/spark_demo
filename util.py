import time,datetime

def get_dtstr_by_ts(ts=None):
    if not ts:
        ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def get_ts8dtstr(dtstr):
    return int(time.mktime(datetime.datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S").timetuple()))

def get_time_part_by_ts(ts=None):
    if not ts:
        ts = int(time.time())
    dts = get_dtstr_by_ts(ts)
    dts_list = dts.split()
    dts_date_list = dts_list[0].split("-")
    dts_time_list = dts_list[1].split(":")
    return {
        "year":int(dts_date_list[0]),
        "month":int(dts_date_list[1]),
        "day":int(dts_date_list[2]),
        "hour":int(dts_time_list[0]),
        "minute":int(dts_time_list[1]),
        "second":int(dts_time_list[2]),
    }

def make_date_key(tlast):
    year = tlast["year"]
    if tlast["year"] < 10 :
        year = "0%s" % (tlast["year"],)
    month = tlast["month"]
    if tlast["month"] < 10 :
        month = "0%s" % (tlast["month"],)
    day = tlast["day"]
    if tlast["day"] < 10 :
        day = "0%s" % (tlast["day"],)
    tkey = "year=%s/month=%s/day=%s" % (year,month,day)
    return tkey


def path_exists(sc,path):
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


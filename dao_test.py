import time

import taos
from sqlalchemy import or_, and_
from models import CurveEntity, PointEntity
from exts import db
from datetime import datetime
from obspy import read
from util import convert_utc_to_datetime, get_all_file_in_path
import os
import datetime
from typing import List
from models import PointEntity
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from util import split_time_ranges
import threading
from taos import SmlProtocol, SmlPrecision

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
bucket = "earthquake_bucket"
url = "http://stephanie:8086"
sep = "/"
earthquake_bucket = "earthquake_bucket"
import os
import threading


class QueryInfluxDbThread(threading.Thread):
    def __init__(self, query_args):
        threading.Thread.__init__(self)
        self.query_args = query_args

    def run(self):
        return get_curve_points(arg_dict=self.query_args)


def check_params(arg_dict, need_fields):
    """
    check fields is in args
    :param arg_dict:
    :param need_fields:
    :return:
    """
    for field in need_fields:
        if field not in arg_dict:
            raise ValueError(f"{field} not in conf dict : {arg_dict}, please check!!!")


def get_curve_points(arg_dict):
    """
     use args make flux to query

         query_args = {
        "measurement": "XJ_AKS_00_BHE",
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "filter": {
            "channel": "BHE",
            "location": "00"
        },
        "window": {
            "window_len": "30s",
            "fn": "max"
        }

        ===================================================
            from(bucket: "earthquake_bucket")
        |> range(start: 1671267260)
        |> filter(fn: (r) => r["_measurement"] == "XJ_AKS_00_BHE")
        |> filter(fn: (r) => r["_field"] == "raw_data")
        |> filter(fn: (r) => r["channel"] == "BHE")
        |> filter(fn: (r) => r["location"] == "00")
        |> aggregateWindow(every: 30s, fn: max, createEmpty: false)
        |> yield(name: "XJ_AKS_00_BHE")
    }
    """
    try:
        print(arg_dict)
        check_params(arg_dict, ["measurement", "time_range", "field"])
        check_params(arg_dict["time_range"], ["start_ts"])
    except ValueError as e:
        print("查询influx参数有问题", e)
        return

    query = f"""
    from(bucket: "{earthquake_bucket}")
    """

    # TODO time range condition
    time_range = arg_dict["time_range"]
    if time_range.get("end_ts") is not None:
        time_range_str = f"""
        |> range(start: {time_range["start_ts"]}, stop: {time_range["end_ts"]})
    """
    else:
        time_range_str = f"""
        |> range(start: {time_range["start_ts"]})
        """
    query = query + time_range_str

    # todo measurement
    measurement_list = []
    for measurement in arg_dict["measurement"]:
        measurement_str = f""" r["_measurement"] == "{measurement}" """
        measurement_list.append(measurement_str)
    measurement_condition = " or ".join(measurement_list)
    print(measurement_condition)
    query = query + f"""
        |> filter(fn: (r) => {measurement_condition})
    """
    # todo fields
    query = query + f"""
        |> filter(fn: (r) => r["_field"] == "{arg_dict["field"]}")
    """
    # todo tag
    for tag_name, tag_value in arg_dict["filter"].items():
        query = query + f"""
        |> filter(fn: (r) => r["{tag_name}"] == "{tag_value}")
    """

    # todo window
    if arg_dict.__contains__("window") and arg_dict["window"].__contains__("window_len"):
        window = arg_dict["window"]
        query = query + f"""
            |> aggregateWindow(every: {window["window_len"]}, fn: {window.get("fn", "mean")}, createEmpty: false)
        """

    # todo drop columns
    query = query + """
        |> drop(columns: ["channel","location","network","station","_start","_stop"])
        """
    # todo yield
    # query = query + f"""
    #     |> yield(name: "{arg_dict["measurement"]}")
    # """

    print(query)

    measurement_res = {}
    for measurement in arg_dict["measurement"]:
        measurement_res[measurement] = []
    with InfluxDBClient(url=url, token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                measurement_res[record.values["_measurement"]].append(record)

    for measurement in list(measurement_res.keys()):
        if len(measurement_res[measurement]) == 0:
            del measurement_res[measurement]
    return measurement_res


def test_multi_influx_query():
    begin = time.time()
    measurements = [["XJ.AHQ.00.BHE", "XJ.AHQ.00.BHN", "XJ.AHQ.00.BHZ"],
                    ["XJ.ALS.00.BHE", "XJ.ALS.00.BHN", "XJ.ALS.00.BHZ"],
                    ["XJ.ATS.00.BHE", "XJ.ATS.00.BHN", "XJ.ATS.00.BHZ"]
                    ]
    query_args_template = {
        "measurement": [
        ],
        "filter": {
        },
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "window": {
        }
    }
    query_args_list = []
    for measurement in measurements:
        query_args_template["measurement"] = measurement
        print(query_args_template)
        query_args_list.append(query_args_template.copy())

    query_threads = []
    for query_arg in query_args_list:
        query_threads.append(QueryInfluxDbThread(query_args=query_arg))

    query_results = []
    for query_thread in query_threads:
        query_results.append(query_thread.start())

    for query_thread in query_threads:
        query_thread.join()

    end = time.time()
    print(end - begin)


def test_influx_query():
    query_args = {
        "measurement": [
            "XJ.AHQ.00.BHE", "XJ.AHQ.00.BHN", "XJ.AHQ.00.BHZ", "XJ.ALS.00.BHE", "XJ.ALS.00.BHN", "XJ.ALS.00.BHZ"
        ],
        "filter": {
        },
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "window": {
        }
    }
    begin = time.time()
    res = get_curve_points(query_args)
    end = time.time()
    print(end - begin)


class WriteTDengineThread(threading.Thread):
    def __init__(self, one_curve_datas, file_path):
        threading.Thread.__init__(self)
        self.file_path = file_path
        self.raw_data = one_curve_datas
        self.curve_id = one_curve_datas.id
        self.curve_stats = one_curve_datas.stats
        self.data_list = one_curve_datas.data

    def run(self):
        start_write_ts = int(time.time())
        print(f"start write curve_id : {self.curve_id} into tdengine")
        self.curve_stats.start_time = convert_utc_to_datetime(self.curve_stats.starttime)
        self.curve_stats.end_time = convert_utc_to_datetime(self.curve_stats.endtime)
        curve_points = []

        ts_list = split_time_ranges(self.curve_stats.start_time, self.curve_stats.end_time, len(self.data_list))
        for index in range(len(self.data_list)):
            curve_point = PointEntity(network=self.curve_stats.network,
                                      station=self.curve_stats.station,
                                      location=self.curve_stats.location,
                                      channel=self.curve_stats.channel,
                                      curve_id=self.curve_id,
                                      file_name=self.file_path.split(sep)[-1],
                                      join_time=ts_list[index].to_pydatetime() + datetime.timedelta(days=67),
                                      point_data=int(self.data_list[index])
                                      )
            curve_points.append(curve_point)
        dump_point_data_to_tdengine(curve_points)

        end_write_ts = int(time.time())
        print(f"end write curve_id : {self.curve_id} into tdengine cost time : {end_write_ts - start_write_ts}")


def dump_point_data_to_tdengine(curve_points):
    lines = list(map(lambda curve_point: curve_point.covert_to_influx_row(), curve_points))
    conn = get_tdengine_conn()
    affected_rows = conn.schemaless_insert(
        lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
    print(affected_rows)  #
    conn.close()


def get_tdengine_conn():
    conn: taos.TaosConnection = taos.connect(host="stephanie", user="root", password="taosdata", database="test",
                                             port=6030)
    return conn


def create_database(conn):
    # the default precision is ms (microsecond), but we use us(microsecond) here.
    conn.execute("USE test")


def dump_one_curve_to_tdengine(file_path):
    start_time = time.time()
    raw_datas = read(file_path)
    write_tdengine_threads = []
    for raw_data in raw_datas:
        write_tdengine_threads.append(WriteTDengineThread(one_curve_datas=raw_data, file_path=file_path))
    for thread in write_tdengine_threads:
        thread.start()
    for thread in write_tdengine_threads:
        thread.join()
    end_time = time.time()
    print(f"dump {file_path} cost :", end_time - start_time)


def fetch_all_demo(conn: taos.TaosConnection, sql):
    start_time = time.time()
    create_database(conn)
    result: taos.TaosResult = conn.query(sql)
    rows = result.fetch_all_into_dict()
    print("row count:", result.row_count)
    end_time = time.time()
    print("query all cost :", end_time - start_time)
    # print("===============all data===================")
    # print(rows)
    conn.close()


def get_curve_points_by_tdengine(arg_dict):
    """
     use args make flux to query
         arg_dict = {
        "measurement": ["XJ.AHQ.00.BHE","XJ.ALS.00.BHZ"],
        "field": ["raw_data"],
        "time_range": {
            "start_ts": 1671261260000000,
            "end_ts": 1673176507000000,
        },
        "filter": {
            "channel": "BHE",
            "location": "00"
        },
        "window": {
            "window_len": "30s",
            "fn": "max"
        }
    """
    try:
        print(arg_dict)
        check_params(arg_dict, ["measurement", "time_range", "field"])
        check_params(arg_dict["time_range"], ["start_ts"])
    except ValueError as e:
        print("查询tdengine参数有问题", e)
        return

    res = {}
    for curve_id in arg_dict["measurement"]:
        fields = arg_dict["field"]
        field_str = (", ").join(fields)
        sql = f"""select {field_str} , _ts from earthquake where curve_id = "{curve_id}" """
        for filter_name, filter_value in arg_dict["filter"].items():
            sql = sql + f""" and {filter_name} = "{filter_value}" """
        sql = sql + f""" and _ts >= {arg_dict["time_range"]["start_ts"]} and _ts <= {arg_dict["time_range"]["end_ts"]} """
        print(sql)
        fetch_all_demo(get_tdengine_conn(), sql)

    return


def get_curves(curve_ids=None):
    """ 根据curve_ids 查找curve
    :param curve_ids: list[curve_id]
    :return: {
        "curve_id":{
            “curve_info” ：{

            }
        }
    }
    """
    if curve_ids is None or len(curve_ids) == 0:
        curve_infos = CurveEntity.query.order_by(CurveEntity.file_name).all()
    else:
        curve_infos = CurveEntity.query.filter(CurveEntity.curve_id.in_(curve_ids)).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


def format_curve_infos_res(curve_infos):
    res = {}
    for curve_info in curve_infos:
        res[curve_info["curve_id"]] = {}
        res[curve_info["curve_id"]]["curve_info"] = curve_info
    return res


if __name__ == '__main__':
    # dump_one_curve_to_tdengine("XJ.AHQ.00.20221016085459.mseed")
    # dump_one_curve_to_tdengine("XJ.ALS.00.20221016085608.mseed")
    # dump_one_curve_to_tdengine("XJ.ATS.00.20221016085504.mseed")
    # fetch_all_demo(get_tdengine_conn())


    arg_dict = {
        "measurement": ['XJ.ATS.00.BHE', 'XJ.ATS.00.BHN', 'XJ.ATS.00.BHZ'],
        "field": ["raw_data"],
        "time_range": {
            "start_ts": 1675329068,
            "end_ts": 1675325626000,
        },
        "filter": {
            # "channel": "BHE",
            # "location": "00"
        },
        "window": {
            "window_len": "30s",
            "fn": "avg"
        }
    }
    get_curve_points_by_tdengine(arg_dict)

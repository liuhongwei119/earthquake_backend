import time

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

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
url = "http://stephanie:8086"
sep = "/"
earthquake_bucket = "earthquake_bucket_linux"


# TODO ================================dump data================================

class WriteInfluxDbThread(threading.Thread):
    def __init__(self, one_curve_datas, file_path):
        threading.Thread.__init__(self)
        self.file_path = file_path
        self.raw_data = one_curve_datas
        self.curve_id = one_curve_datas.id
        self.curve_stats = one_curve_datas.stats
        self.data_list = one_curve_datas.data

    def run(self):
        start_write_ts = int(time.time())
        print(f"start write curve_id : {self.curve_id} into influxDB")
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
        dump_point_data(curve_points)

        end_write_ts = int(time.time())
        print(f"end write curve_id : {self.curve_id} into influxDB cost time : {end_write_ts - start_write_ts}")


def dump_one_curve(file_path):
    raw_datas = read(file_path)
    # TODO dump curve info to mysql
    for raw_data in raw_datas:
        curve_data = raw_data.data
        curve_id = raw_data.id
        curve_stats = raw_data.stats
        curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
        curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
        earth_curve = CurveEntity(network=curve_stats.network,
                                  station=curve_stats.station,
                                  location=curve_stats.location,
                                  channel=curve_stats.channel,
                                  start_time=curve_stats.start_time,
                                  end_time=curve_stats.end_time,
                                  sampling_rate=curve_stats.sampling_rate,
                                  delta=curve_stats.delta,
                                  npts=curve_stats.npts,
                                  calib=curve_stats.calib,
                                  _format=curve_stats._format,
                                  curve_id=curve_id,
                                  file_name=file_path.split(sep)[-1]
                                  )
        db.session.add(earth_curve)
    db.session.commit()

    # TODO dump curve points to influxDB
    write_influxDb_threads = []
    for raw_data in raw_datas:
        write_influxDb_threads.append(WriteInfluxDbThread(one_curve_datas=raw_data, file_path=file_path))
    for thread in write_influxDb_threads:
        thread.start()


def dump_point_data(curve_points: List[PointEntity]):
    with InfluxDBClient(url=url, token=token, org=org) as client:
        write_api = client.write_api(write_options=ASYNCHRONOUS)
        for curve_point in curve_points:
            point = Point(curve_point.curve_id) \
                .tag("network", curve_point.network) \
                .tag("station", curve_point.station) \
                .tag("location", curve_point.location) \
                .tag("channel", curve_point.channel) \
                .field("raw_data", curve_point.point_data) \
                .time(curve_point.join_time, WritePrecision.NS)
            write_api.write(bucket=earthquake_bucket, org=org, record=point)
        write_api.flush()
        client.close()


# TODO ======================curve part=========================

def format_curve_infos_res(curve_infos):
    res = {}
    for curve_info in curve_infos:
        res[curve_info["curve_id"]] = {}
        res[curve_info["curve_id"]]["curve_info"] = curve_info
    return res


def get_all_curves():
    """
    :return: {
        "curve_id":{
            “curve_info” ：{

            }
        }
    }
    """
    curve_infos = CurveEntity.query.order_by(CurveEntity.file_name).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


def get_curves(curve_ids):
    """ 根据curve_ids 查找curve
    :param curve_ids: list[curve_id]
    :return: {
        "curve_id":{
            “curve_info” ：{

            }
        }
    }
    """
    curve_infos = CurveEntity.query.filter(CurveEntity.curve_id.in_(curve_ids)).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


def get_curves_with_or_condition(arg_dict):
    """
    :param arg_dict: {
		"channel": "BHE",
		"location": "00",
		"network": "XJ",
		"station": "AKS"
    }
    :return: {
        "curve_id":{
            “curve_info” ：{

            }
        }
    }
    """

    check_params(arg_dict, ["channel", "location", "network", "station"])
    curve_infos = CurveEntity.query.filter(or_(CurveEntity.channel == arg_dict["channel"],
                                               CurveEntity.location == arg_dict["location"],
                                               CurveEntity.network == arg_dict["network"],
                                               CurveEntity.station == arg_dict["station"])).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


def get_curves_with_and_condition(arg_dict):
    """
    :param arg_dict: {
		"channel": "BHE",
		"location": "00",
		"network": "XJ",
		"station": "AKS"
    }
    :return: {
        "curve_id":{
            “curve_info” ：{

            }
        }
    }
    """

    check_params(arg_dict, ["channel", "location", "network", "station"])
    curve_infos = CurveEntity.query.filter(and_(CurveEntity.channel == arg_dict["channel"],
                                                CurveEntity.location == arg_dict["location"],
                                                CurveEntity.network == arg_dict["network"],
                                                CurveEntity.station == arg_dict["station"])).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


# TODO ======================points part=========================
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
    # todo fiels
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
            |> aggregateWindow(every: {window["window_len"]}, fn: {window["fn"]}, createEmpty: false)
        """

    # todo yield
    query = query + f"""
        |> yield(name: "{arg_dict["measurement"]}")
    """

    print(query)

    measurement_res = {}
    for measurement in arg_dict["measurement"]:
        measurement_res[measurement] = []
    with InfluxDBClient(url=url, token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                measurement_res[record.values["_measurement"]].append(record)

    return measurement_res


def query_test():
    """
    from(bucket: "earthquake_bucket")
    |> range(start: -70d, stop:-65d)
    |> filter(fn: (r) => r["_measurement"] == "XJ_RUQ_00_BHZ")
    |> filter(fn: (r) => r["_field"] == "raw_data")
    |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    :return:
    """
    query = '''
        from(bucket: "earthquake_bucket")
    |> range(start: -90d)
    |> filter(fn: (r) => r["_measurement"] == "AH_SCH_00_BHE")
    |> filter(fn: (r) => r["_field"] == "raw_data")
    |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    '''
    with InfluxDBClient(url="http://stephanie:8086", token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                print(record)


def test_influx_write():
    file_path = "mseed_data/SF202210160854A-B758-08/XJ.AHQ.00.20221016085459.mseed"
    files = get_all_file_in_path(path=file_path, all_files=[])
    curve_points = []
    for file in files:
        print(file)
        raw_datas = read(file)
        for raw_data in raw_datas:
            curve_id = raw_data.id
            curve_stats = raw_data.stats
            curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
            curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
            data_list = raw_data.data
            ts_list = split_time_ranges(curve_stats.start_time, curve_stats.end_time, len(data_list))
            for index in range(len(data_list)):
                curve_point = PointEntity(network=curve_stats.network,
                                          station=curve_stats.station,
                                          location=curve_stats.location,
                                          channel=curve_stats.channel,
                                          file_name=file_path.split(os.sep)[-1],
                                          curve_id="_".join(curve_id.split(".")),
                                          join_time=ts_list[index].to_pydatetime() + datetime.timedelta(days=60),
                                          point_data=int(data_list[index])
                                          )

                curve_points.append(curve_point)
    dump_point_data(curve_points)

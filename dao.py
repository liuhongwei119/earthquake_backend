import time

import numpy as np
import pandas as pd
from flask import make_response
from sqlalchemy import or_, and_

from curvilinear_transformation.curve_change import get_frequency_by_my_fft
from curvilinear_transformation.feature_extraction_util import TimeDomainFeatureExtraction, \
    FrequencyDomainFeatureExtraction
from curvilinear_transformation.pretreatment_util import downsample, divide_sensitivity, mean_normalization, \
    standardization, min_normalization, none_normalization
from curvilinear_transformation.transformation_util import frequency_domain_transformation, \
    time_frequency_transformation_to_png
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
import taos
from taos import SmlProtocol, SmlPrecision
import p_waves.pickWave
import gzip
import json

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
url = "http://stephanie:8086"
sep = "/"
earthquake_bucket = "earthquake_bucket"

# TODO sqlalchemy 动态查询字段
condition_fields = {
    "network": CurveEntity.network,
    "channel": CurveEntity.channel,
    "location": CurveEntity.location,
    "station": CurveEntity.station
}

normalization_dict = {
    "zero_center": mean_normalization,
    "zs_score": standardization,
    "rescale_zero_one": min_normalization,
    "none": none_normalization
}


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
        dump_point_data_to_influx(curve_points)

        end_write_ts = int(time.time())
        print(f"end write curve_id : {self.curve_id} into influxDB cost time : {end_write_ts - start_write_ts}")


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
                                      join_time=ts_list[index].to_pydatetime(),
                                      point_data=int(self.data_list[index])
                                      )
            curve_points.append(curve_point)
        dump_point_data_to_tdengine(curve_points)


def dump_point_data_to_tdengine(curve_points):
    lines = list(map(lambda curve_point: curve_point.covert_to_influx_row(), curve_points))
    conn = get_tdengine_conn()
    affected_rows = conn.schemaless_insert(
        lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
    print(affected_rows)  #
    conn.close()


def dump_one_curve(file_path):
    raw_datas = read(file_path)
    # 设置p波开始时间
    boolean, problem, p_wave_starttime, s_wave_starttime = p_waves.pickWave.pickWave(file_path)
    print(p_wave_starttime)
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
                                  file_name=file_path.split(sep)[-1],
                                  p_start_time=convert_utc_to_datetime(p_wave_starttime)
                                  )
        db.session.add(earth_curve)
    db.session.commit()

    # TODO dump curve points to influxDB
    # write_influxDb_threads = []
    # for raw_data in raw_datas:
    #     write_influxDb_threads.append(WriteInfluxDbThread(one_curve_datas=raw_data, file_path=file_path))
    # for thread in write_influxDb_threads:
    #     thread.start()

    # TODO dump curve points to tdengine
    write_tdengine_threads = []
    for raw_data in raw_datas:
        write_tdengine_threads.append(WriteTDengineThread(one_curve_datas=raw_data, file_path=file_path))
    for thread in write_tdengine_threads:
        thread.start()


def dump_point_data_to_influx(curve_points: List[PointEntity]):
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


# TODO ======================curve part=========================

def format_curve_infos_res(curve_infos):
    res = {}
    for curve_info in curve_infos:
        res[curve_info["curve_id"]] = {}
        res[curve_info["curve_id"]]["curve_info"] = curve_info
    return res


def get_curves(curve_ids=None):
    """
    根据curve_ids 查找curve,如curve_ids查询所有
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
    query_list = []
    for field_name, field_value in arg_dict.items():
        query_list.append(condition_fields[field_name] == field_value)

    curve_infos = CurveEntity.query.filter(or_(*query_list)).all()
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
    query_list = []
    for field_name, field_value in arg_dict.items():
        query_list.append(condition_fields[field_name] == field_value)
    curve_infos = CurveEntity.query.filter(and_(*query_list)).all()
    curve_infos = list(map(lambda x: x.convert_to_dict(), curve_infos))
    return format_curve_infos_res(curve_infos)


def get_file_name_by_curve_id(curve_id):
    """
    curve_id
    return file_name
    """
    file_name = CurveEntity.query.with_entities(CurveEntity.file_name).filter(CurveEntity.curve_id == curve_id).first()
    return file_name[0]


def get_curve_ids_by_file_name(file_name):
    """
    file_name
    return curve_ids
    """
    curve_ids = CurveEntity.query.with_entities(CurveEntity.curve_id).filter(CurveEntity.file_name == file_name).all()
    return list(map(lambda x: x[0], curve_ids))


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


def get_tdengine_conn():
    conn: taos.TaosConnection = taos.connect(host="stephanie", user="root", password="taosdata", database="test",
                                             port=6030)
    return conn


def create_database(conn):
    # the default precision is ms (microsecond), but we use us(microsecond) here.
    conn.execute("USE test")


def fetch_all_point(conn: taos.TaosConnection, sql):
    print(sql)
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
    raw_data_list = []
    point_fre_list = []
    point_amp_list = []
    ts_list = []
    # print(rows)
    # print(type(rows[0]["_ts"]))
    for row in rows:
        if row.__contains__("raw_data"):
            raw_data_list.append(row["raw_data"])
        # 转时间戳
        ts_list.append(int(row["_ts"].timestamp()))
    return {
        "ts_list": ts_list,
        "raw_data_list": raw_data_list
    }


def get_curve_points_by_tdengine(arg_dict):
    """
     use args make flux to query
         arg_dict = {
        "measurement": ["XJ.AHQ.00.BHE","XJ.ALS.00.BHZ"],
        "fields": ["raw_data"],
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
        },
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
        fields = arg_dict["fields"]
        field_str = (", ").join(fields)
        sql = f"""select {field_str} , _ts from earthquake where curve_id = "{curve_id}" """
        for filter_name, filter_value in arg_dict["filter"].items():
            sql = sql + f""" and {filter_name} = "{filter_value}" """
        sql = sql + f""" and _ts >= {arg_dict["time_range"]["start_ts"]} and _ts <= {arg_dict["time_range"]["end_ts"]} """
        res[curve_id] = fetch_all_point(get_tdengine_conn(), sql)

    return res


def get_curve_points_by_influx(arg_dict):
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
    dump_point_data_to_influx(curve_points)


def gzip_compress_response(very_long_content):
    content = gzip.compress(json.dumps(very_long_content).encode('utf8'), 5)
    response = make_response(content)
    response.headers['Content-length'] = len(content)
    response.headers['Content-Encoding'] = 'gzip'
    return response


def build_pretreatment_args(pretreatment_args):
    """
    设置 pretreatment_args 默认值
    :param pretreatment_args:
    :return:
    """
    if not pretreatment_args.__contains__("downsample"):
        pretreatment_args["downsample"] = "1"
    if not pretreatment_args.__contains__("divide_sensitivity"):
        pretreatment_args["divide_sensitivity"] = "1"
    if not pretreatment_args.__contains__("normalization"):
        pretreatment_args["normalization"] = "none"
    return pretreatment_args


def pretreatment_points(curve_points_dicts, pretreatment_args):
    """
    根据预处理参数进行预处理
    :param curve_points_dicts: 曲线和原始点信息的结合
    :param pretreatment_args:
            ”pretreatment_args“：{
            "downsample": "10", 指定降采样比例，默认不采样
            ”divide_sensitivity“： "1" ，指定仪器灵敏度，默认1
            "normalization"："zero_center" | "zs_score" | "rescale_zero_one"| "none"  默认”none“
    :return:
        }
    """

    for curve_id, curve_points_dict in curve_points_dicts.items():
        points_info = curve_points_dict["points_info"]
        y_array = np.array(points_info["raw_datas"])
        t_array = np.array(points_info["ts"])
        t_array, y_array = downsample(t_array=t_array, y_array=y_array, n=pretreatment_args["downsample"])
        t_array, y_array = divide_sensitivity(t_array=t_array, y_array=y_array,
                                              n=pretreatment_args["divide_sensitivity"])
        t_array, y_array = normalization_dict[pretreatment_args["normalization"]](t_array, y_array)
        points_info["raw_datas"] = y_array.tolist()
        points_info["ts"] = t_array.tolist()


#
class MakeTimeZoneThread(threading.Thread):
    def __init__(self, curve_points_dicts):
        threading.Thread.__init__(self)
        self.curve_points_dicts = curve_points_dicts

    def run(self):
        time_frequency_transformation_to_png(curve_points_dicts=self.curve_points_dicts)


def transformation_points(curve_points_dicts):
    """
    将预处理后的数据进行转化成频率图和时域图
    :param curve_points_dicts: 曲线和原始点信息的结合 -> 以及预处理后的数据
    :return:
    """

    for curve_id, curve_points_dict in curve_points_dicts.items():
        points_info = curve_points_dict["points_info"]
        y_array = points_info["raw_datas"]
        t_array = points_info["ts"]
        point_amp_list, point_fre_list = frequency_domain_transformation(t_array=t_array, y_array=y_array)
        # point_time_cwtmatr, point_time_freqs = time_domain_transformation(t_array=t_array, y_array=y_array)
        points_info["point_amp_list"] = point_amp_list.tolist()
        points_info["point_fre_list"] = point_fre_list.tolist()
        # 遇到转换问题了，complex无法序列化，看是不是要把图片传给前端
        # points_info["point_time_cwtmatr"] = point_time_cwtmatr.tolist()
        # points_info["point_time_freqs"] = point_time_freqs.tolist()
    time_zone_thread = MakeTimeZoneThread(curve_points_dicts)
    time_zone_thread.start()
    png_addr = os.getcwd() + "\\time_domain_pngs\\" + "_".join(curve_points_dicts.keys()) + ".jpg"
    return png_addr


def get_pd_raw_datas(curve_points_dicts):
    """
    :param curve_points_dicts: 数据
    :return: 将所有曲线的的raw_datas提取出来，转化成pd格式，供后面批计算，多一些属性需要多曲线信息一起计算，因此要结合起来
    """
    datas = []
    for curve_id, curve_points_dict in curve_points_dicts.items():
        points_info = curve_points_dict["points_info"]
        y_array = np.array(points_info["raw_datas"])
        t_array = np.array(points_info["ts"])
        datas.append(y_array)
    pd_data = pd.DataFrame(datas)
    pd_data.dropna(axis=1, how='any', inplace=True)
    return pd_data


def time_and_frequency_feature_extraction(curve_points_dicts):
    pd_data = get_pd_raw_datas(curve_points_dicts)
    time_domain_feature_extract_result = TimeDomainFeatureExtraction.get_time_domain_feature(data=pd_data)
    frequency_domain_feature_extract_result = FrequencyDomainFeatureExtraction.get_frequency_domain_feature(
        data=pd_data, sampling_frequency=100)

    index = 0
    for curve_id, curve_points_dict in curve_points_dicts.items():
        points_info = curve_points_dict["points_info"]
        # 获取基本的提取信息
        points_info["time_domain_feature_extract_result"] = time_domain_feature_extract_result[index]
        points_info["frequency_domain_feature_extract_result"] = frequency_domain_feature_extract_result[index]
        # 补充时域提取信息
        time_extract_tool = TimeDomainFeatureExtraction(raw_datas=points_info["raw_datas"], ts_list=points_info["ts"])
        # 自相关系数
        for k in [1, 2]:
            points_info["time_domain_feature_extract_result"][f"{k}阶自相关系数"] = \
                time_extract_tool.get_auto_correlation_coefficient(k)
        points_info["time_domain_feature_extract_result"][f"波形复杂度"] = time_extract_tool.get_waveform_complexity()
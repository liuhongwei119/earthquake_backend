from flask import Blueprint, jsonify
from util import convert_utc_to_datetime, get_all_file_in_path
from dao import dump_one_curve
from flask import request
from exts import db
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
import time

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
bucket = "earthquake_bucket"
url = "http://stephanie:8086"
sep = "/"
earthquake_bucket = "earthquake_bucket"
import os


def curve_upload(path):
    dump_one_curve(path)


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


def query_points(arg_dict):
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
        check_params(arg_dict, ["measurement", "time_range", "field"])
        check_params(arg_dict["time_range"], ["start_ts"])
        if arg_dict.get("window") is not None:
            check_params(arg_dict["window"], ["window_len", "fn"])
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


if __name__ == '__main__':
    query_args = {
        "measurement": [
            "XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"
        ],
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "filter": {

        },
        "window": {
            "window_len": "30s",
            "fn": "max"
        }
    }
    res = query_points(query_args)
    print(res)

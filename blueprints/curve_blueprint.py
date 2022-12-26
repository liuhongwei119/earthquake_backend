from flask import Blueprint, jsonify
from util import convert_utc_to_datetime, get_all_file_in_path
from dao import dump_one_curve, get_curve_points, get_curves, get_curves_with_or_condition, \
    get_curves_with_and_condition, check_params
from flask import request
from exts import db
import time
import json

import os

bp = Blueprint("offline_mysql_curve", __name__, url_prefix="/offline_mysql_curve")
file_prefix = "offline_earthquake_files"


@bp.route("/upload_curve", methods=['GET', 'POST'])
def curve_upload():
    upload_file = request.files.get("file")
    offline_earthquake_files_path = os.path.join(os.getcwd(), file_prefix)
    if not os.path.exists(offline_earthquake_files_path):
        os.mkdir(offline_earthquake_files_path)
    if upload_file is not None:
        file_name = upload_file.filename
        save_path = os.path.join(offline_earthquake_files_path, file_name)
        print(save_path)
        upload_file.save(save_path)
        # TODO dump_one_curve
        dump_one_curve(save_path)
        rst = jsonify({"status": 200, "msg": f"update  {file_name} success"})
        return rst
    else:
        return jsonify({"status": 400, "msg": f"file is null"})


# TODO ======================only curve info=========================

def delete_none_value_in_dict(transmit_dict):
    result_dict = {}
    for key, value in transmit_dict.items():
        print(f"key {key} value {value}")
        if len(value) != 0:
            result_dict[key] = value

    return result_dict


@bp.route("/get_curves", methods=['GET', 'POST'])
def search_curves():
    curve_ids_str = request.form.get("curve_ids", "[]")
    curve_ids = json.loads(curve_ids_str)
    return jsonify({"res": get_curves(curve_ids)})


@bp.route("/get_curves_with_condition", methods=['GET', 'POST'])
def search_curves_with_condition():
    """
    :param
    args: {
        "conditions" :{
            "channel": "BHE",
            "location": "00",
            "network": "XJ",
            "station": "AKS"
        },
        "conjunction" : "or" / "and"
    }
    :return:  curves
    """
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    print(args)
    if (not args.__contains__("conditions") or len(args["conditions"]) == 0) \
            or (not args.__contains__("conjunction") or args["conjunction"] not in ["or", "and"]):
        return jsonify({"res": get_curves()})
    else:
        args["conditions"] = delete_none_value_in_dict(args["conditions"])
        if args["conjunction"] == "or":
            return jsonify({"res": get_curves_with_or_condition(args["conditions"])})
        elif args["conjunction"] == "and":
            return jsonify({"res": get_curves_with_and_condition(args["conditions"])})


# TODO ======================curve info and points=========================
def build_influx_query_arg(curve_ids, start_ts, end_ts, filters, window):
    query_args = {
        "measurement": curve_ids,
        "field": "raw_data",
        "filter": filters,
        "time_range": {
            "start_ts": start_ts,
            "end_ts": end_ts
        },
        "window": window
    }
    return query_args


def query_influx(query_args, curve_ids, curve_infos):
    # TODO query influxDB
    curve_points_dict = get_curve_points(query_args)
    # TODO round curve_ids
    for curve_id in curve_ids:
        raw_datas = []
        ts_list = []
        curve_points = curve_points_dict[curve_id]
        for curve_point in curve_points:
            ts = int(time.mktime(curve_point.values["_time"].timetuple()))
            value = curve_point.values["_value"]
            ts_list.append(ts)
            raw_datas.append(value)
        curve_infos[curve_id]["points_info"] = {}
        curve_infos[curve_id]["points_info"]["raw_datas"] = raw_datas
        curve_infos[curve_id]["points_info"]["ts"] = ts_list
    return curve_infos


@bp.route("/get_curves_and_points", methods=['GET', 'POST'])
def search_curves_and_points():
    """
    args =
    {
        "curve_ids": ["XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"],
        "end_ts": 1671793259,
        "filters": {
            "channel": "BHE"
        },
        "window": {"window_len": "5s", "fn": "mean"}
    }
    :return:
    """
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    print(args)

    # TODO 1. parse args and get curve
    # if curve_id is None , then search all curve
    curve_infos = get_curves(args.get("curve_ids", []))
    curve_ids = curve_infos.keys()

    # TODO 2. get min start_ts from
    stat_ts_list = []
    for curve_id in curve_ids:
        stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
    start_ts = min(stat_ts_list)

    # TODO 3. gen args
    end_ts = args["end_ts"] if args.__contains__("end_ts") else int(time.time())
    filters = args["filters"] if args.__contains__("filters") else {}
    window = args["window"] if args.__contains__("window") else {}
    filters = delete_none_value_in_dict(filters)
    window = delete_none_value_in_dict(window)
    query_args = build_influx_query_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters=filters,
                                        window=window)
    res = query_influx(query_args, curve_ids, curve_infos)

    return jsonify({"res": res, "status": 200})


@bp.route("/upload_test")
def test_curve_upload():
    dump_one_curve("XJ.AHQ.00.20221016085459.mseed")
    rst = jsonify({"status": 200})
    return rst


if __name__ == '__main__':
    print("1")

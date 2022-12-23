from flask import Blueprint, jsonify
from util import convert_utc_to_datetime, get_all_file_in_path
from dao import dump_one_curve, get_all_curves, get_curve_points, get_curves, get_curves_with_or_condition, \
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
@bp.route("/get_all_curves")
def search_all_curves():
    return jsonify({"res": get_all_curves()})


@bp.route("/get_curves", methods=['POST'])
def search_curves():
    curve_ids_str = request.form.get("curve_ids")
    curve_ids = json.loads(curve_ids_str)
    return jsonify({"res": get_curves(curve_ids)})


@bp.route("/get_curves_with_or_condition", methods=['POST'])
def search_curves_with_or_condition():
    args_str = request.form.get("args")
    args = json.loads(args_str)
    check_params(args, ["channel", "location", "network", "station"])
    return jsonify({"res": get_curves_with_or_condition(args)})


@bp.route("/get_curves_with_and_condition", methods=['POST'])
def search_curves_with_and_condition():
    condition_str = request.form.get("condition")
    condition = json.loads(condition_str)
    check_params(condition, ["channel", "location", "network", "station"])
    return jsonify({"res": get_curves_with_and_condition(condition)})


# TODO ======================curve info and points=========================
def build_influx_query_arg(curve_ids, start_ts, end_ts, filters, windows):

    query_args = {
        "measurement": curve_ids,
        "field": "raw_data",
        "filter": filters,
        "time_range": {
            "start_ts": start_ts,
            "end_ts": end_ts
        },
        "window": windows
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


@bp.route("/get_curves_with_points", methods=['POST'])
def search_curves_with_points():
    """
    args =
    {
        "curve_ids": ["XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"],
        "end_ts": 1671793259,
        "filters": {
            "channel": "BHE"
        },
        "windows": {"window_len": "5s", "fn": "mean"}
    }
    :return:
    """
    args_str = request.form.get("args")
    args = json.loads(args_str)

    # TODO 1. parse args and get curve
    # if curve_id is None , then search all curve
    if args.__contains__("curve_ids") and len(args["curve_ids"]) > 0:
        curve_infos = get_curves(args["curve_ids"])
    else:
        curve_infos = get_all_curves()
    curve_ids = curve_infos.keys()

    # TODO 2. get min start_ts from
    stat_ts_list = []
    for curve_id in curve_ids:
        stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
    start_ts = min(stat_ts_list)

    # TODO 3. gen args
    end_ts = args["end_ts"] if args.__contains__("end_ts") else int(time.time())
    filters = args["filters"] if args.__contains__("filters") else {}
    windows = args["windows"] if args.__contains__("windows") else {}
    query_args = build_influx_query_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters=filters,
                                        windows=windows)
    res = query_influx(query_args, curve_ids, curve_infos)

    return jsonify({"res": res, "status": 200})


@bp.route("/upload_test")
def test_curve_upload():
    dump_one_curve("mseed_data/SF202210160854A-B758-08/XJ.AKS.00.20221016085511.mseed")
    rst = jsonify({"status": 200})
    return rst



if __name__ == '__main__':
    print("1")

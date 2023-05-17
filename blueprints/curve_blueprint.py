import math

from flask import Blueprint, jsonify, make_response

from util import convert_utc_to_datetime, get_all_file_in_path
from dao import dump_one_curve, get_curve_points_by_influx, get_curves, get_curves_with_or_condition, \
    get_curves_with_and_condition, check_params, get_curve_points_by_tdengine, get_file_name_by_curve_id, \
    get_curve_ids_by_file_name, gzip_compress_response, pretreatment_points, build_pretreatment_args, \
    transformation_points, time_and_frequency_feature_extraction, chang_curve_p_s_start_time, do_filtering_data \
    , get_page_curves_by_conditions, get_page_curves_by_ids
from flask import request
from exts import db
import time
import json
from flask import current_app
import gzip
import datetime
from flask import send_file
import sys
import os

bp = Blueprint("offline_mysql_curve", __name__, url_prefix="/offline_mysql_curve")
file_prefix = "offline_earthquake_files"
# 时频图存放路径文件夹
tf_pngs_dir_path = os.path.realpath("time_frequency_pngs")


@bp.route("/upload_curve", methods=['GET', 'POST'])
def curve_upload():
    upload_file = request.files.get("file")
    offline_earthquake_files_path = os.path.join(os.getcwd(), file_prefix)
    current_app.logger.info(offline_earthquake_files_path)
    if not os.path.exists(offline_earthquake_files_path):
        os.mkdir(offline_earthquake_files_path)
    if upload_file is not None:
        file_name = upload_file.filename
        save_path = os.path.join(offline_earthquake_files_path, file_name)
        current_app.logger.info(save_path)
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
        if len(value) != 0:
            result_dict[key] = value

    return result_dict


@bp.route("/get_curves", methods=['GET', 'POST'])
def search_curves():
    curve_ids_str = request.form.get("curve_ids", "[]")
    curve_ids = json.loads(curve_ids_str)
    res = {"res": get_curves(curve_ids)}
    return gzip_compress_response(res)


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
    current_app.logger.info(args)
    query_start_time = time.time()
    res = {}
    if (not args.__contains__("conditions") or len(args["conditions"]) == 0) \
            or (not args.__contains__("conjunction") or args["conjunction"] not in ["or", "and"]):
        curve_infos = get_curves()
        query_end_time = time.time()
        res = {"res": curve_infos, "cost_time": query_end_time - query_start_time}
    else:
        args["conditions"] = delete_none_value_in_dict(args["conditions"])
        if args["conjunction"] == "or":
            res = jsonify({"res": get_curves_with_or_condition(args["conditions"])})
        elif args["conjunction"] == "and":
            res = jsonify({"res": get_curves_with_and_condition(args["conditions"])})
    return gzip_compress_response(res)


@bp.route("/get_curve_page", methods=['POST'])
def get_curve_page():
    """
    """

    response_object = {'status': 'success'}
    if request.method == 'POST':
        args_str = request.form.get("args", "{}")
        post_data = json.loads(args_str)
        print('调用query方传过来的参数是', post_data)
        pagesize = int(post_data.get('pagesize'))
        page = int(post_data.get('page'))
        conditions_dict = delete_none_value_in_dict(post_data.get('conditions_dict'))
        print('pagesize', pagesize)
        print('conditions_dict', conditions_dict)
        curve_total, curves = get_page_curves_by_conditions(pagesize, page, conditions_dict)
        response_object['message'] = '分页曲线查询成功!'
        response_object['data'] = curves  # 当前页面数据
        response_object['pagesize'] = pagesize  # 页面size
        response_object['page'] = page  # 当前页码
        response_object['curve_total'] = curve_total  # 结果数量总和
        response_object['page_total'] = math.ceil(curve_total / pagesize)  # 页数量总和
        return response_object


# TODO ======================curve info and points=========================


@bp.route("/get_point_page", methods=['POST'])
def get_point_page():
    """
    """

    response_object = {'status': 'success'}
    if request.method == 'POST':
        args_str = request.form.get("args", "{}")
        post_data = json.loads(args_str)
        print('调用query方传过来的参数是', post_data)
        pagesize = int(post_data.get('pagesize'))
        page = int(post_data.get('page'))
        conditions_dict = delete_none_value_in_dict(post_data.get('conditions_dict'))
        curve_ids = post_data.get('curve_ids')
        if curve_ids is None or len(curve_ids) == 0:
            curve_total, curves = get_page_curves_by_conditions(pagesize, page, conditions_dict)
        else:
            curve_total, curves = get_page_curves_by_ids(pagesize, page, curve_ids, conditions_dict)
        print(curves)
        if len(curves) != 0:
            start_ts = post_data.get('start_ts')
            start_ts = int(start_ts) \
                if start_ts is not None \
                else min(list(map(lambda x: x.get('start_ts'), curves)))
            end_ts = post_data.get('end_ts')
            end_ts = int(end_ts) \
                if end_ts is not None \
                else max(list(map(lambda x: x.get('end_ts'), curves)))

            for curve in curves:
                curve_id = curve['curve_id']
                print(curve_id)
                curve_points = get_curve_points_by_tdengine(
                    {
                        "measurement": [curve_id],
                        "time_range": {
                            "start_ts": str(start_ts) + "000000",
                            "end_ts": str(end_ts) + "000000"
                        },
                        "filter": {}
                    }
                )
                curve["points"] = curve_points[curve_id].get("raw_data_list")
                curve["ts_list"] = curve_points[curve_id].get("ts_list")
        response_object['message'] = '分页序列查询成功!'
        response_object['res'] = packaging_get_point_page(curves)  # 当前页面数据
        response_object['pagesize'] = pagesize  # 页面size
        response_object['page'] = page  # 当前页码
        response_object['curve_total'] = curve_total  # 结果数量总和
        response_object['page_total'] = math.ceil(curve_total / pagesize)  # 页数量总和

        return gzip_compress_response(response_object)
        # return response_object

def packaging_get_point_page(curves):
    res_dict = {}
    for curve in curves:
        curve_id = curve["curve_id"]
        points_list = curve["points"]
        ts_list = curve["ts_list"]
        res_dict[curve_id] = {}
        del curve["points"]
        del curve["ts_list"]
        res_dict[curve_id]["curve_info"] = curve
        res_dict[curve_id]["points_info"] = {}
        res_dict[curve_id]["points_info"]["raw_datas"] = points_list
        res_dict[curve_id]["points_info"]["ts"] = ts_list
    return  res_dict

def build_get_points_arg(curve_ids, start_ts, end_ts, filters, window, fields):
    """
    此方法用作构造 通过tdengine的查询raw_data（原始数据）
    :param curve_ids:
    :param start_ts:
    :param end_ts:
    :param filters:
    :param window:
    :param fields:
    :return:
    """
    query_args = {
        "measurement": curve_ids,
        "field": ["raw_data"],
        "filter": filters,
        "time_range": {
            "start_ts": start_ts,  # us为单位
            "end_ts": end_ts
        },
        "window": window,
        "fields": fields
    }
    return query_args


def encapsulation_curve_points_res(curve_points_dict, curve_ids, curve_infos):
    """
    封装获取raw_datas的查询结果
    :param curve_points_dict: 点查询结果
    :param curve_ids: 曲线id
    :param curve_infos: 曲线查询结果
    :return:  曲线和点结合返回结果
    """

    for curve_id in curve_ids:
        curve_points = curve_points_dict[curve_id]
        curve_infos[curve_id]["points_info"] = {}
        curve_infos[curve_id]["points_info"]["raw_datas"] = curve_points["raw_data_list"]
        curve_infos[curve_id]["points_info"]["ts"] = curve_points["ts_list"]

    # 若曲线信息点信息为空，不返回曲线信息:
    for curve_id in list(curve_ids):
        raw_datas = curve_infos[curve_id]["points_info"]["raw_datas"]
        if len(raw_datas) == 0:
            del curve_infos[curve_id]


@bp.route("/get_curves_and_points", methods=['GET', 'POST'])
def search_curves_and_points():
    """
    参数：
    args =
    {
        "curve_ids": ["XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"],
        "start_ts": 1675329068
        "end_ts": 1671793259,
        "filters": {
            "channel": "BHE"
        },
        "window": {"window_len": "5s", "fn": "mean"},
        "fields":["raw_data"]
    }
    查询步骤：
    1.解析参数
    2.通过mysql查询获取curve曲线信息（曲线信息存储在mysql，曲线点信息存储在tdengine）
    3.设置相关参数及其默认值
    4.根据参数构造查询格式
    5.查询tdengine获取原始数据信息
    6.将曲线数据和原始数据结合
    :return:
    """
    # step one
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)

    # step two
    curve_infos = get_curves(args.get("curve_ids", []))
    curve_ids = curve_infos.keys()

    # step three
    start_ts = 0
    if args.__contains__("start_ts") and args['start_ts'] != "":
        start_ts = args['start_ts']
    else:
        stat_ts_list = []
        for curve_id in curve_ids:
            stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
        start_ts = min(stat_ts_list)
    end_ts = args["end_ts"] if args.__contains__("end_ts") else int(time.time())
    filters = args["filters"] if args.__contains__("filters") else {}
    window = args["window"] if args.__contains__("window") else {}
    fields = args["fields"] if args.__contains__("fields") else ["raw_data"]
    filters = delete_none_value_in_dict(filters)
    window = delete_none_value_in_dict(window)
    # tdengine 查询需要用us
    start_ts = str(start_ts) + "000"
    end_ts = str(end_ts) + "000"

    # step four
    query_args = build_get_points_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters=filters,
                                      window=window, fields=fields)

    # step five
    curve_points_dict = get_curve_points_by_tdengine(arg_dict=query_args)
    # step six
    encapsulation_curve_points_res(curve_points_dict=curve_points_dict, curve_infos=curve_infos,
                                   curve_ids=curve_ids)
    query_end_time = time.time()

    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)


@bp.route("/get_points_and_transform", methods=['GET', 'POST'])
def search_points_and_transform():
    """
    args =
    {
        "curve_ids": ["XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"],
        ”pretreatment_args“：{
            "downsample": 10, 指定降采样比例，默认不采样
            ”divide_sensitivity“： 1 ，指定仪器灵敏度，默认1
            "normalization"："zero_center" | "zs_score" | "rescale_zero_one"| "none"  默认”none“
        }
    }
    查询步骤：
    1.解析参数
    2.通过mysql查询获取curve曲线信息（曲线信息存储在mysql，曲线点信息存储在tdengine）
    3.设置相关参数及其默认值
    4.根据参数构造查询格式
    5.查询tdengine获取原始数据信息
    6.将曲线数据和原始数据结合
       6.1 将原始数据进行滤波变换
    7.预处理曲线数据
    8.添加转化成频域时域数据,并获取时频图存放位置
    10.特征提取
    :return:
    """
    # step one
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    # step two
    curve_infos = get_curves(args.get("curve_ids", []))
    curve_ids = curve_infos.keys()

    # step three
    start_ts = 0
    if args.__contains__("start_ts") and args['start_ts'] != "":
        start_ts = args['start_ts']
    else:
        stat_ts_list = []
        for curve_id in curve_ids:
            stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
        start_ts = min(stat_ts_list)
    end_ts = args["end_ts"] if args.__contains__("end_ts") else int(time.time())
    # tdengine 查询需要用us
    start_ts = str(start_ts) + "000"
    end_ts = str(end_ts) + "000"

    if args.__contains__("start_ts") and args['start_ts'] != "":
        start_ts = args['start_ts']
    else:
        stat_ts_list = []
        for curve_id in curve_ids:
            stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
        start_ts = min(stat_ts_list)

    # step four
    tdengine_query_args = build_get_points_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters={},
                                               window={}, fields=["raw_data"])

    # step five
    curve_points_dict = get_curve_points_by_tdengine(arg_dict=tdengine_query_args)

    # step six
    encapsulation_curve_points_res(curve_points_dict=curve_points_dict, curve_infos=curve_infos,
                                   curve_ids=curve_ids)

    # 6.1 将原始数据进行滤波变换
    do_filtering_data(curve_infos, args)

    # step seven
    pretreatment_args = args["pretreatment_args"] if args.__contains__("pretreatment_args") else {}
    build_pretreatment_args(pretreatment_args)
    pretreatment_points(curve_infos, pretreatment_args)
    # step eight
    t_f_png_name = transformation_points(curve_infos)

    # step ten
    time_and_frequency_feature_extraction(curve_infos)
    query_end_time = time.time()
    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time,
           "t_f_png_name": t_f_png_name}
    return gzip_compress_response(res)


@bp.route("/get_curves_in_same_file", methods=['GET', 'POST'])
def search_curves_in_same_file():
    """
      one file has three curves, get all curve_ids in the same file by a curve_id
    """

    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)
    if not args.__contains__("curve_id"):
        raise ValueError("无curve_id")

    curve_id = args["curve_id"]
    file_name = get_file_name_by_curve_id(curve_id)
    curve_ids = get_curve_ids_by_file_name(file_name)
    query_end_time = time.time()
    res = {
        "cost_time": query_end_time - query_start_time,
        "status": "200",
        "curve_ids": curve_ids
    }
    return gzip_compress_response(res)


@bp.route("/upload_test")
def test_curve_upload():
    dump_one_curve("XJ.ALS.00.20221016085608.mseed")
    dump_one_curve("XJ.AHQ.00.20221016085459.mseed")
    dump_one_curve("XJ.ATS.00.20221016085504.mseed")
    rst = jsonify({"status": 200})
    return rst


@bp.route("/test_png")
def test_png():
    image_data = open(
        r"D:\python_projects\earthquake_backend\time_frequency_pngs\XJ.AHQ.00.BHE_XJ.AHQ.00.BHN_XJ.AHQ.00.BHZ.jpg",
        "rb").read()
    rst = jsonify({"status": 200})
    response = make_response(image_data)
    response.headers['Content-Type'] = 'image/png'  # 返回的内容类型必须修改
    return response


@bp.route("/get_tf_png", methods=['GET', 'POST'])
def get_tf_png():
    """
      get t_f_png from local file with png_addr
    """

    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)
    if not args.__contains__("t_f_png_name"):
        raise ValueError("无t_f_png_name")
    png_addr = os.path.join(tf_pngs_dir_path, args["t_f_png_name"])

    current_app.logger.info(f"get png from {png_addr}")
    image_data = open(png_addr, "rb").read()
    response = make_response(image_data)
    response.headers['Content-Type'] = 'image/png'  # 返回的内容类型必须修改
    return response


@bp.route("/change_p_s_start_time", methods=['GET', 'POST'])
def change_p_s_start_time():
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)
    if not args.__contains__("curve_id"):
        raise ValueError("无curve_id")
    if not args.__contains__("p_start_time"):
        raise ValueError("无p_start_time")
    if not args.__contains__("s_start_time"):
        raise ValueError("无s_start_time")
    file_name = get_file_name_by_curve_id(args["curve_id"])
    curve_ids = get_curve_ids_by_file_name(file_name)
    p_start_date = datetime.datetime.fromtimestamp(int(args["p_start_time"]))
    s_start_date = datetime.datetime.fromtimestamp(int(args["s_start_time"]))
    for curve_id in curve_ids:
        chang_curve_p_s_start_time(curve_id=curve_id, p_start_date=p_start_date, s_start_date=s_start_date)

    query_end_time = time.time()
    res = {
        "cost_time": query_end_time - query_start_time,
        "status": "200",
        "curve_ids": curve_ids
    }
    return res


@bp.route("/test_form_check", methods=["GET", "POST"])
def test_form_check():
    """
      test_form_check
    """
    print(request.form)
    return "1"


@bp.route("/get_filter_data", methods=['GET', 'POST'])
def search_filter_data():
    """
    args =
    {
        "curve_ids": ["XJ.AKS.00.BHE", "XJ.AKS.00.BHN", "XJ.AKS.00.BHZ"],
        "filters": ["bandpass", "highpass","lowpass"]
    }
    查询步骤：
    1.解析参数
    2.通过mysql查询获取curve曲线信息（曲线信息存储在mysql，曲线点信息存储在tdengine）
    3.设置相关参数及其默认值
    4.根据参数构造查询格式
    5.查询tdengine获取原始数据信息
    6.增加滤波数据
    :return:
    """
    # step one
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    # step two
    curve_infos = get_curves(args.get("curve_ids", []))
    curve_ids = curve_infos.keys()

    # step three
    start_ts = 0
    if args.__contains__("start_ts") and args['start_ts'] != "":
        start_ts = args['start_ts']
    else:
        stat_ts_list = []
        for curve_id in curve_ids:
            stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
        start_ts = min(stat_ts_list)
    end_ts = args["end_ts"] if args.__contains__("end_ts") else int(time.time())
    # tdengine 查询需要用us
    start_ts = str(start_ts) + "000000"
    end_ts = str(end_ts) + "000000"

    if args.__contains__("start_ts") and args['start_ts'] != "":
        start_ts = args['start_ts']
    else:
        stat_ts_list = []
        for curve_id in curve_ids:
            stat_ts_list.append(curve_infos[curve_id]["curve_info"]["start_ts"])
        start_ts = min(stat_ts_list)

    # step four
    tdengine_query_args = build_get_points_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters={},
                                               window={}, fields=["raw_data"])

    # step five
    curve_points_dict = get_curve_points_by_tdengine(arg_dict=tdengine_query_args)

    # step six
    encapsulation_curve_points_res(curve_points_dict=curve_points_dict, curve_infos=curve_infos,
                                   curve_ids=curve_ids)

    # step nine 增加滤波数据
    do_filtering_data(curve_infos, args)

    query_end_time = time.time()
    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)

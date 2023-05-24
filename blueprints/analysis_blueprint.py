from flask import Blueprint, jsonify, make_response, request, current_app
import json
import time
import os

from dao import dump_one_curve, get_curve_points_by_influx, get_curves, get_curves_with_or_condition, \
    get_curves_with_and_condition, check_params, get_curve_points_by_tdengine, get_file_name_by_curve_id, \
    get_curve_ids_by_file_name, gzip_compress_response, pretreatment_points, build_pretreatment_args, \
    transformation_points, time_and_frequency_feature_extraction, chang_curve_p_s_start_time, do_filtering_data, \
    get_frequency_domain_curve, get_time_frequency_curve, get_feature_extraction_curve

bp = Blueprint("offline_curve_analysis", __name__, url_prefix="/offline_curve_analysis")
# 时频图存放路径文件夹
tf_pngs_dir_path = os.path.realpath("time_frequency_pngs")


@bp.route('/tf_pngs', methods=['GET'])
def get_tf_png():
    """
      get t_f_png from local file with png_addr
    """

    png_name = request.args.get("png_name")
    png_addr = os.path.join(tf_pngs_dir_path, png_name)

    current_app.logger.info(f"get png from {png_addr}")
    try:
        image_data = open(png_addr, "rb").read()
    except Exception as e:
        current_app.logger.error("open tf_png error")
        image_data = open("t_f_error.png", "rb").read()
        current_app.logger.error(e)
    response = make_response(image_data)
    response.headers['Content-Type'] = 'image/png'  # 返回的内容类型必须修改
    return response


@bp.route("/get_time_domain_info", methods=['GET', 'POST'])
def get_time_domain_info():
    """
    args =
            {
            "curve_ids": [
                "XJ.AHQ.00.BHE"
            ],
            "pretreatment_args": {
                "downsample": 1,
                "divide_sensitivity": 1,
                "normalization": "none"
            },
            "filter": {
                "filter_name": "lowpass",
                "filter_args": [
                    "1",
                    "100",
                    "4",
                    "False"
                ]
            }
            }
    :return:
    """
    # 解析参数
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    curve_infos = get_pretreatment_data(args)
    if curve_infos is None:
        return {"res": "curve_id is empty", "status": 405}

    query_end_time = time.time()
    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)


@bp.route("/get_feature_extraction_info", methods=['GET', 'POST'])
def get_feature_extraction_info():
    """
    args =
            {
            "curve_ids": [
                "XJ.AHQ.00.BHE"
            ],
            "pretreatment_args": {
                "downsample": 1,
                "divide_sensitivity": 1,
                "normalization": "none"
            },
            "filter": {
                "filter_name": "lowpass",
                "filter_args": [
                    "1",
                    "100",
                    "4",
                    "False"
                ]
            }
            }
    :return:
    """
    # 解析参数
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    # 根据参数预处理
    curve_infos = get_pretreatment_data(args)
    if curve_infos is None:
        return {"res": "curve_id is empty", "status": 405}
    # 特征提取
    get_feature_extraction_curve(curve_infos)

    query_end_time = time.time()
    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)


@bp.route("/get_frequency_domain_info", methods=['GET', 'POST'])
def get_frequency_domain_info():
    """
    args =
            {
            "curve_ids": [
                "XJ.AHQ.00.BHE"
            ],
            "pretreatment_args": {
                "downsample": 1,
                "divide_sensitivity": 1,
                "normalization": "none"
            },
            "filter": {
                "filter_name": "lowpass",
                "filter_args": [
                    "1",
                    "100",
                    "4",
                    "False"
                ]
            }
            }
    :return:
    """
    # 解析参数
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    # 根据参数预处理
    curve_infos = get_pretreatment_data(args)
    if curve_infos is None:
        return {"res": "curve_id is empty", "status": 405}
    # 获取时频图
    get_frequency_domain_curve(curve_infos)

    query_end_time = time.time()
    res = {"res": curve_infos, "status": 200, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)


@bp.route("/get_time_frequency_info", methods=['GET', 'POST'])
def get_time_frequency_info():
    """
    args =
            {
            "curve_ids": [
                "XJ.AHQ.00.BHE"
            ],
            "pretreatment_args": {
                "downsample": 1,
                "divide_sensitivity": 1,
                "normalization": "none"
            },
            "filter": {
                "filter_name": "lowpass",
                "filter_args": [
                    "1",
                    "100",
                    "4",
                    "False"
                ]
            }
            }
    :return:
    """
    # 解析参数
    query_start_time = time.time()
    args_str = request.form.get("args", "{}")
    args = json.loads(args_str)
    current_app.logger.info(args)

    # 根据参数预处理
    curve_infos = get_pretreatment_data(args)
    if curve_infos is None:
        return {"res": "curve_id is empty", "status": 405}
    # 获取时频图
    t_f_png_name = get_time_frequency_curve(curve_infos)

    query_end_time = time.time()
    res = {"status": 200, "t_f_png_name": t_f_png_name, "cost_time": query_end_time - query_start_time}
    return gzip_compress_response(res)


def get_pretreatment_data(args):
    # 通过mysql查询获取curve曲线信息（曲线信息存储在mysql，曲线点信息存储在tdengine）
    ids = args.get("curve_ids", [])
    if ids is None or len(ids) == 0:
        return None
    curve_infos = get_curves()
    curve_ids = curve_infos.keys()

    # 设置相关参数及其默认值
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

    # 根据参数构造查询格式
    tdengine_query_args = build_get_points_arg(curve_ids=curve_ids, start_ts=start_ts, end_ts=end_ts, filters={},
                                               window={}, fields=["raw_data"])

    # 查询tdengine获取原始数据信息
    curve_points_dict = get_curve_points_by_tdengine(arg_dict=tdengine_query_args)

    # 将曲线数据和原始数据结合
    encapsulation_curve_points_res(curve_points_dict=curve_points_dict, curve_infos=curve_infos,
                                   curve_ids=curve_ids)

    # 将原始数据进行滤波变换
    do_filtering_data(curve_infos, args)

    # 预处理曲线数据
    pretreatment_args = args["pretreatment_args"] if args.__contains__("pretreatment_args") else {}
    build_pretreatment_args(pretreatment_args)
    pretreatment_points(curve_infos, pretreatment_args)

    return curve_infos


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

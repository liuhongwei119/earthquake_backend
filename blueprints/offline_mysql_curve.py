from flask import Blueprint, jsonify
from models import EarthCurveModel
from obspy import read
from util import convert_utc_to_datetime, get_all_file_in_path
from flask import request
from exts import db
import os

bp = Blueprint("offline_mysql_curve", __name__, url_prefix="/offline_mysql_curve")
file_prefix = "offline_earthquake_files"


@bp.route("/search", methods=['GET'])
def search():
    curve_id = ""
    if request.method == 'GET':
        curve_id = request.args.get("curve_id")
    curve_info = EarthCurveModel.query.filter_by(curve_id=curve_id).first()
    rst = jsonify({"status": 200, "curve_info": curve_info.convert_to_json_res()})
    rst.headers['Access-Control-Allow-Origin'] = '*'
    rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return rst


@bp.route("/test/upload", methods=['GET'])
def upload():
    if request.method == 'GET':
        path = request.args.get("path")
    # else:
    #     path = request.json["path"]
    files = get_all_file_in_path(path=path, all_files=[])
    for file in files:
        dump_one(file)
    return jsonify({"status": 200, "msg": "update success", "files": files})


def dump_one(file_path):
    raw_datas = read(file_path)
    for raw_data in raw_datas:
        curve_data = raw_data.data
        curve_id = raw_data.id
        curve_stats = raw_data.stats
        curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
        curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
        curve_data = ",".join(map(str, curve_data.tolist()))
        curve_id = f"{curve_id}_{curve_stats.start_time}_{curve_stats.end_time}"  # unique_key
        print(curve_id)
        earth_curve = EarthCurveModel(network=curve_stats.network,
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
                                      curve_data=curve_data,
                                      file_name=file_path.split(os.sep)[-1]
                                      )
        db.session.add(earth_curve)
    db.session.commit()


@bp.route("/upload", methods=['GET', 'POST'])
def earthquake_offline_upload():
    upload_file = request.files.get("file")
    offline_earthquake_files_path = os.path.join(os.getcwd(), file_prefix)
    if not os.path.exists(offline_earthquake_files_path):
        os.mkdir(offline_earthquake_files_path)
    if upload_file != None:
        file_name = upload_file.filename
        save_path = os.path.join(offline_earthquake_files_path, file_name)
        print(save_path)
        upload_file.save(save_path)
        dump_one(save_path)
        rst = jsonify({"status": 200, "msg": f"update  {file_name} success"})
        rst.headers['Access-Control-Allow-Origin'] = '*'
        rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
        rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
        return rst


@bp.route("/search", methods=['POST'])
def earthquake_offline_search():
    curve_id = request.args.get("curve_id")
    curve_info = EarthCurveModel.query.filter_by(curve_id=curve_id).first()
    rst = jsonify({"status": 200, "curve_info": curve_info.convert_to_json_res()})
    rst.headers['Access-Control-Allow-Origin'] = '*'
    rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return rst


@bp.route("/searchAll", methods=['GET'])
def earthquake_offline_search_all():
    curve_infos = EarthCurveModel.query.all()
    curve_res = []
    for curve_info in curve_infos:
        curve_res.append(curve_info.convert_to_json_res())
    rst = jsonify({"status": 200, "function": "search all earth curve", "datas": curve_res})
    rst.headers['Access-Control-Allow-Origin'] = '*'
    rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return rst


@bp.route("delete", methods=['GET', 'POST'])
def earthquake_offline_delete():
    upload_file = request.files.get("file")
    offline_earthquake_files_path = os.path.join(os.getcwd(), file_prefix)
    if not os.path.exists(offline_earthquake_files_path):
        os.mkdir(offline_earthquake_files_path)
    if upload_file != None:
        file_name = upload_file.filename
        save_path = os.path.join(offline_earthquake_files_path, file_name)
        print(save_path)
        upload_file.save(save_path)
        dump_one(save_path)
        rst = jsonify({"status": 200, "msg": f"update  {file_name} success"})
        rst.headers['Access-Control-Allow-Origin'] = '*'
        rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
        rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
        return rst


if __name__ == '__main__':
    print("1")
    dump_one( "mseed_data/SF202210160854A-B758-08/XJ.BAC.00.20221016085530.mseed")

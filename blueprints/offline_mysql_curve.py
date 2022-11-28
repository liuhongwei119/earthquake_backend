from flask import Blueprint, jsonify
from models import EarthCurveModel
from obspy import read
from util import convert_utc_to_datetime, get_all_file_in_path
from flask import request
from exts import db

bp = Blueprint("offline_mysql_curve", __name__, url_prefix="/offline_mysql_curve")


@bp.route("/search", methods=['GET'])
def search():
    curve_id = ""
    if request.method == 'GET':
        curve_id = request.args.get("curve_id")
    curve_info = EarthCurveModel.query.filter_by(curve_id=curve_id).first()
    return jsonify({"status": 200, "curve_info": curve_info.convert_to_json_res()})



@bp.route("/upload", methods=['GET'])
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
    # for raw_data in raw_datas:
    raw_data = raw_datas[0]
    curve_data = raw_data.data
    curve_id = raw_data.id
    curve_stats = raw_data.stats
    # print(f"curve_id :{curve_id} , stats: {curve_stats} , curve_data:{curve_data}")
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
                                  curve_data=curve_data
                                  )
    db.session.add(earth_curve)
    db.session.commit()


if __name__ == '__main__':
    print("1")
    # dump("1" , "mseed_data")

from flask import Blueprint, jsonify
from util import convert_utc_to_datetime, get_all_file_in_path
from dao import dump_one_curve
from flask import request
from exts import db

import os

bp = Blueprint("offline_mysql_curve", __name__, url_prefix="/offline_mysql_curve")
file_prefix = "offline_earthquake_files"


@bp.route("/upload", methods=['GET', 'POST'])
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
        dump_one_curve(save_path)
        rst = jsonify({"status": 200, "msg": f"update  {file_name} success"})
        return rst
    else:
        return jsonify({"status": 400, "msg": f"file is null"})


# @bp.route("/search", methods=['POST'])
# def earthquake_offline_search():
#     curve_id = request.form.get("curve_id")
#     curve_info = EarthCurveModel.query.filter_by(curve_id=curve_id).first()
#     rst = jsonify({"status": 200, "function": "search curve", "res": curve_info.convert_to_json_res()})
#     return rst
#
#
# @bp.route("/delete", methods=['POST'])
# def earthquake_offline_delete():
#     curve_id = request.args.get("curve_id")
#     curve_info = EarthCurveModel.query.filter_by(curve_id=curve_id).first()
#     db.session.delete(curve_info)
#     db.session.commit()
#     rst = jsonify({"status": 200, "function": "delete curve", "res": f"delete curve {curve_info.curve_id} success"})
#     return rst
#
#
# @bp.route("/searchAll", methods=['GET'])
# def earthquake_offline_search_all():
#     curve_infos = EarthCurveModel.query.all()
#     curve_res = []
#     for curve_info in curve_infos:
#         curve_res.append(curve_info.convert_to_json_res())
#     rst = jsonify({"status": 200, "function": "search all earth curve", "datas": curve_res})
#     return rst


if __name__ == '__main__':
    print("1")

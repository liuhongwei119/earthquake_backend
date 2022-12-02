from exts import db
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean, DECIMAL, Enum, Date, DateTime, Time, Text
from sqlalchemy.dialects.mysql import LONGTEXT
import json


class EarthCurveModel(db.Model):
    __tablename__ = "earth_curve"
    id = Column(Integer, primary_key=True, autoincrement=True)
    network = Column(String(100), nullable=False)
    station = Column(String(100), nullable=False)
    location = Column(String(100), nullable=False)
    channel = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    sampling_rate = Column(String(100), nullable=False)
    delta = Column(String(100), nullable=False)
    npts = Column(String(100), nullable=False)
    calib = Column(String(100), nullable=False)
    _format = Column(String(100), nullable=False)
    curve_id = Column(String(100), nullable=False)  # "{network}.{station}.{location}.{channel}_{start_time}_{end_time}"
    file_name = Column(String(100), nullable=False, default="default")
    curve_data = Column(LONGTEXT, nullable=False)
    join_time = Column(DateTime, default=datetime.now)  # func

    def convert_to_json_res(self):
        curve_dict = self.__dict__.copy()
        print(curve_dict)
        del curve_dict["_sa_instance_state"]
        curve_dict["curve_data"] = list(map(int, list(curve_dict["curve_data"].split(","))))
        curve_dict["start_time"] = curve_dict["start_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["end_time"] = curve_dict["end_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["join_time"] = curve_dict["join_time"].strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(curve_dict)

    def convert_to_json_res2(self):
        curve_dict = self.__dict__.copy()
        print(curve_dict)
        del curve_dict["_sa_instance_state"]
        curve_dict["curve_data"] = list(map(int, list(curve_dict["curve_data"].split(","))))
        curve_dict["start_time"] = curve_dict["start_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["end_time"] = curve_dict["end_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["join_time"] = curve_dict["join_time"].strftime("%Y-%m-%d %H:%M:%S")
        return curve_dict

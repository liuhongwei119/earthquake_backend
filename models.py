from exts import db
from datetime import datetime
import time
from sqlalchemy import Column, Integer, String, Float, Boolean, DECIMAL, Enum, Date, DateTime, Time, Text


class CurveEntity(db.Model):
    # TODO 存储曲线相关联的信息
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
    curve_id = Column(String(100), nullable=False)  # "{network}.{station}.{location}.{channel}"
    file_name = Column(String(100), nullable=False, default="default")
    join_time = Column(DateTime, default=datetime.now)
    longitude = Column(String(100))  # 震源经度
    latitude = Column(String(100))  # 纬度
    event_type = Column(String(100))  # 事件类型
    magnitude = Column(String(100))  # 震级
    p_start_time = Column(DateTime)  # p波开始时间
    intensity = Column(String(100))  # 烈度

    def convert_to_dict(self):
        curve_dict = self.__dict__.copy()
        del curve_dict["_sa_instance_state"]
        curve_dict["start_ts"] = int(time.mktime(curve_dict["start_time"].timetuple()))
        curve_dict["end_ts"] = int(time.mktime(curve_dict["end_time"].timetuple()))
        curve_dict["join_ts"] = int(time.mktime(curve_dict["join_time"].timetuple()))
        curve_dict["start_time"] = curve_dict["start_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["end_time"] = curve_dict["end_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["join_time"] = curve_dict["join_time"].strftime("%Y-%m-%d %H:%M:%S")

        return curve_dict


class PointEntity:
    # TODO 存储曲线时许点相关信息
    def __init__(self, network, station, location, channel, file_name, curve_id, point_data, join_time):
        self.network = network
        self.station = station
        self.location = location
        self.channel = channel
        self.file_name = file_name
        self.curve_id = curve_id
        self.point_data = point_data
        self.join_time = join_time

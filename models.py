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
    s_start_time = Column(DateTime)  # s波开始时间
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
        curve_dict["p_start_time"] = curve_dict["p_start_time"].strftime("%Y-%m-%d %H:%M:%S")
        curve_dict["s_start_time"] = curve_dict["s_start_time"].strftime("%Y-%m-%d %H:%M:%S")
        return curve_dict


class PointEntity:
    # measurement,tag_set field_set timestamp
    taos_measurement = "earthquake,"
    taos_tag_set = "network={},station={},location={},channel={},file_name={},curve_id={} "
    taos_field_set = "raw_data={} "
    taos_ts = "{}"

    # TODO 存储曲线时许点相关信息
    def __init__(self, network, station, location, channel, file_name, curve_id, point_data, join_time):
        self.network = network,
        self.station = station
        self.location = location
        self.channel = channel
        self.file_name = file_name
        self.curve_id = curve_id
        self.point_data = point_data
        self.join_time = join_time

    # TODO 转化为influxDD行协议，写入TDengine使用
    def covert_to_influx_row(self):
        # 写入时以us单位写入Tdengine
        us = str(self.join_time.timestamp()).split(".")[0] + str(self.join_time.timestamp()).split(".")[1].ljust(6, "0")
        influx_row_str = PointEntity.taos_measurement + \
                         PointEntity.taos_tag_set.format(self.network[0], self.station, self.location,
                                                         self.channel, self.file_name, self.curve_id) + \
                         PointEntity.taos_field_set.format(self.point_data) + \
                         PointEntity.taos_ts.format(us)
        return influx_row_str


class OnlineFlashInfo(db.Model):
    __tablename__ = "online_flash_info"
    id = Column(Integer, primary_key=True, autoincrement=True)
    network = Column(String(100), nullable=False)
    station = Column(String(100), nullable=False)
    location = Column(String(100), nullable=False)
    channel = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
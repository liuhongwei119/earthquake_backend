import taos
from taos import SmlProtocol, SmlPrecision
from typing import List


class RealtimePointEntity:
    taos_measurement = "earthquake,"
    taos_tag_set = "network={},station={},location={},channel={},curve_id={} "
    taos_data_set = "data={} "
    taos_ts = "{}"

    def __init__(self, network, station, location, channel, curve_id, data, ms):
        self.network = network,
        self.station = station
        self.location = location
        self.channel = channel
        self.curve_id = curve_id
        self.data = data
        self.ms = ms  # 毫秒时间戳

    # TODO 转化为influxDD行协议，写入TDengine使用
    def covert_to_schemaless_row(self):
        influx_row_str = RealtimePointEntity.taos_measurement + \
                         RealtimePointEntity.taos_tag_set.format(
                             self.network[0], self.station, self.location, self.channel, self.curve_id) + \
                         RealtimePointEntity.taos_data_set.format(self.point_data) + \
                         RealtimePointEntity.taos_ts.format(self.ms)
        return influx_row_str

    @staticmethod
    def dump_points_to_tdengine(schemaless_line_list: List):
        conn = RealtimePointEntity.get_earthquake_realtime_tdengine_conn()
        affected_rows = conn.schemaless_insert(
            schemaless_line_list, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MILLI_SECONDS)
        print(affected_rows)
        conn.close()
        return affected_rows

    @staticmethod
    def get_earthquake_realtime_tdengine_conn():
        conn: taos.TaosConnection = taos.connect(host="stephanie",
                                                 user="root",
                                                 password="taosdata",
                                                 database="earthquake_realtime",
                                                 port=6030)
        return conn

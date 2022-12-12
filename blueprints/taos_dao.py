from obspy import read
from util import convert_utc_to_datetime, get_all_file_in_path
import os
from datetime import datetime
from typing import List
import pandas as pd
import taos


def split_time_ranges(from_time, to_time, frequency):
    from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
    time_range = list(pd.date_range(from_time, to_time, periods=frequency))
    ts_list = list(map(lambda dt: int(str(dt.timestamp() * 1000).split(".")[0]), time_range))
    return ts_list


def get_connection() -> taos.TaosConnection:
    """
    create connection use firstEp in taos.cfg and use default user and password.
    """
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    return taos.connect()


def create_stable(conn: taos.TaosConnection):
    # conn.execute("CREATE DATABASE flask_earthquake")
    conn.execute("USE flask_earthquake")
    # conn.execute("CREATE STABLE curve_meters (ts TIMESTAMP, point_data INT) "
    #              "TAGS (network BINARY(64), station BINARY(64), location BINARY(64), channel BINARY(64))")


class EarthCurvePointEntity():
    def __init__(self, network, station, location, channel, sampling_rate, delta, npts,
                 calib, format, file_name, curve_id, point_data, join_time):
        self.network = network
        self.station = station
        self.location = location
        self.channel = channel
        self.sampling_rate = sampling_rate
        self.delta = delta
        self.npts = npts
        self.calib = calib
        self.format = format
        self.file_name = file_name

        self.curve_id = curve_id
        self.point_data = point_data
        self.join_time = join_time


def prepare_data(file_path) -> List[EarthCurvePointEntity]:
    files = get_all_file_in_path(path=file_path, all_files=[])
    earth_points = []
    for file in files:
        raw_datas = read(file)
        for raw_data in raw_datas:

            curve_id = raw_data.id
            curve_stats = raw_data.stats
            curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
            curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
            data_list = raw_data.data
            ts_list = split_time_ranges(curve_stats.start_time, curve_stats.end_time, len(data_list))
            for index in range(len(data_list)):
                earth_point = EarthCurvePointEntity(network=curve_stats.network,
                                                    station=curve_stats.station,
                                                    location=curve_stats.location,
                                                    channel=curve_stats.channel,
                                                    sampling_rate=curve_stats.sampling_rate,
                                                    delta=curve_stats.delta,
                                                    npts=curve_stats.npts,
                                                    calib=curve_stats.calib,
                                                    format=curve_stats._format,
                                                    file_name=file_path.split(os.sep)[-1],
                                                    curve_id="_".join(curve_id.split(".")),
                                                    join_time=ts_list[index],
                                                    point_data=data_list[index]
                                                    )
                # print(earth_point.__dict__)
                earth_points.append(earth_point)
    return sorted(earth_points, key=lambda x: x.curve_id)


def bind_row_by_row(earth_points: List[EarthCurvePointEntity], stmt: taos.TaosStmt):
    tb_name = None
    for row in earth_points:
        if tb_name != row.curve_id:
            tb_name = row.curve_id
            tags: taos.TaosBind = taos.new_bind_params(4)  # 2 is count of tags
            tags[0].binary(row.network)  # network
            tags[1].binary(row.station)  # station
            tags[2].binary(row.location)  # location
            tags[3].binary(row.channel)  # channel
            print(tb_name)
            stmt.set_tbname_tags(f"flask_earthquake.{tb_name}", tags)
        values: taos.TaosBind = taos.new_bind_params(2)  # 4 is count of columns
        values[0].timestamp(row.join_time)
        stmt.bind_param(values)


def create_table(earth_points: List[EarthCurvePointEntity], conn: taos.TaosConnection):
    curve_ids = set(map(lambda earth_point: earth_point.curve_id, earth_points))
    for curve_id in curve_ids:
        tags = curve_id.split("_")
        create_str = f'CREATE TABLE IF NOT EXISTS {curve_id}  USING curve_meters TAGS ("{tags[0]}","{tags[1]}","{tags[2]}","{tags[3]}")'
        print(create_str)
        conn.execute(create_str)


def get_sql(earth_points: List[EarthCurvePointEntity]):
    sql = "INSERT INTO "
    tb_name = None
    for ps in earth_points:
        tmp_tb_name = ps.curve_id
        if tb_name != tmp_tb_name:
            tb_name = tmp_tb_name
            sql += f'{tb_name} USING curve_meters TAGS("{ps.network}", "{ps.station}","{ps.location}","{ps.channel}") VALUES '
        sql += f'({ps.join_time}, "{ps.point_data}") '
    return sql


def insert_data(earth_points: List[EarthCurvePointEntity], conn: taos.TaosConnection):
    # stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ? ,?, ? ) VALUES(?, ?)")
    # bind_row_by_row(earth_points, stmt)
    # stmt.execute()
    # stmt.close()
    batch = 100
    for start in range(0, len(earth_points), batch):
        affected_rows = conn.execute(get_sql(earth_points[start:min(start + batch, len(earth_points))]))
        print("affected_rows", affected_rows)


if __name__ == '__main__':
    conn = get_connection()
    create_stable(conn)
    earth_points = prepare_data("mseed_data/SF202210160854A-B758-08/XJ.AHQ.00.20221016085459.mseed")
    create_table(earth_points, conn)
    insert_data(earth_points, conn)

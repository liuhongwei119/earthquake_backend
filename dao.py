from models import CurveEntity, PointEntity
from exts import db
from datetime import datetime
from obspy import read
from util import convert_utc_to_datetime, get_all_file_in_path
import os
import datetime
from typing import List
from models import PointEntity
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from util import split_time_ranges

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
bucket = "earthquake_bucket"
url = "http://stephanie:8086"


def dump_one_curve(file_path):
    raw_datas = read(file_path)
    for raw_data in raw_datas:
        curve_data = raw_data.data
        curve_id = raw_data.id
        curve_stats = raw_data.stats
        curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
        curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
        print(curve_id)

        # TODO dump curve info to mysql
        earth_curve = CurveEntity(network=curve_stats.network,
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
                                  file_name=file_path.split(os.sep)[-1]
                                  )
        db.session.add(earth_curve)

        # TODO dump curve points to influxDB
        # curve_points = []
        # data_list = raw_data.data
        # ts_list = split_time_ranges(curve_stats.start_time, curve_stats.end_time, len(data_list))
        # for index in range(len(data_list)):
        #     curve_point = PointEntity(network=curve_stats.network,
        #                               station=curve_stats.station,
        #                               location=curve_stats.location,
        #                               channel=curve_stats.channel,
        #                               file_name=file_path.split(os.sep)[-1],
        #                               join_time=ts_list[index].to_pydatetime() + datetime.timedelta(days=60),
        #                               point_data=int(data_list[index])
        #                               )
        #     curve_points.append(curve_point)
        # dump_point_data(curve_points)
    db.session.commit()


def dump_point_data(curve_points: List[PointEntity]):
    with InfluxDBClient(url=url, token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for curve_point in curve_points:
            point = Point(curve_point.curve_id) \
                .tag("network", curve_point.network) \
                .tag("station", curve_point.station) \
                .tag("location", curve_point.location) \
                .tag("channel", curve_point.channel) \
                .field("raw_data", curve_point.point_data) \
                .time(curve_point.join_time, WritePrecision.NS)
            write_api.write(bucket=bucket, org=org, record=point)
        write_api.flush()
        client.close()


def query_test():

    """
    from(bucket: "earthquake_bucket")
    |> range(start: -70d, stop:-65d)
    |> filter(fn: (r) => r["_measurement"] == "XJ_RUQ_00_BHZ")
    |> filter(fn: (r) => r["_field"] == "raw_data")
    |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    :return:
    """
    query = '''
        from(bucket: "earthquake_bucket")
    |> range(start: -90d)
    |> filter(fn: (r) => r["_measurement"] == "AH_SCH_00_BHE")
    |> filter(fn: (r) => r["_field"] == "raw_data")
    |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    '''
    with InfluxDBClient(url="http://stephanie:8086", token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                print(record)


def test_influx_write():
    file_path = "mseed_data/SF202210160854A-B758-08/XJ.AHQ.00.20221016085459.mseed"
    files = get_all_file_in_path(path=file_path, all_files=[])
    curve_points = []
    for file in files:
        print(file)
        raw_datas = read(file)
        for raw_data in raw_datas:
            curve_id = raw_data.id
            curve_stats = raw_data.stats
            curve_stats.start_time = convert_utc_to_datetime(curve_stats.starttime)
            curve_stats.end_time = convert_utc_to_datetime(curve_stats.endtime)
            data_list = raw_data.data
            ts_list = split_time_ranges(curve_stats.start_time, curve_stats.end_time, len(data_list))
            for index in range(len(data_list)):
                curve_point = PointEntity(network=curve_stats.network,
                                          station=curve_stats.station,
                                          location=curve_stats.location,
                                          channel=curve_stats.channel,
                                          file_name=file_path.split(os.sep)[-1],
                                          curve_id="_".join(curve_id.split(".")),
                                          join_time=ts_list[index].to_pydatetime() + datetime.timedelta(days=60),
                                          point_data=int(data_list[index])
                                          )
                curve_points.append(curve_point)
    dump_point_data(curve_points)
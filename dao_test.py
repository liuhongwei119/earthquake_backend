from dao import dump_one_curve
from influxdb_client import InfluxDBClient, Point, WritePrecision
import time
import taos

# You can generate an API token from the "API Tokens Tab" in the UI
token = "6CayNW5Hv3QK32-UvVPQCWrGSwpHiXCYTPb_oJtKNaJm7ZaqqW92ZcMpQ1yDmw40q6elq9qncQpw5xpZMWhf6Q=="
org = "东北大学"
bucket = "earthquake_bucket"
url = "http://stephanie:8086"
sep = "/"
earthquake_bucket = "earthquake_bucket"
import os
import threading


class QueryInfluxDbThread(threading.Thread):
    def __init__(self, query_args):
        threading.Thread.__init__(self)
        self.query_args = query_args

    def run(self):
        return get_curve_points(arg_dict=self.query_args)


def curve_upload(path):
    dump_one_curve(path)


def check_params(arg_dict, need_fields):
    """
    check fields is in args
    :param arg_dict:
    :param need_fields:
    :return:
    """
    for field in need_fields:
        if field not in arg_dict:
            raise ValueError(f"{field} not in conf dict : {arg_dict}, please check!!!")


def get_curve_points(arg_dict):
    """
     use args make flux to query

         query_args = {
        "measurement": "XJ_AKS_00_BHE",
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "filter": {
            "channel": "BHE",
            "location": "00"
        },
        "window": {
            "window_len": "30s",
            "fn": "max"
        }

        ===================================================
            from(bucket: "earthquake_bucket")
        |> range(start: 1671267260)
        |> filter(fn: (r) => r["_measurement"] == "XJ_AKS_00_BHE")
        |> filter(fn: (r) => r["_field"] == "raw_data")
        |> filter(fn: (r) => r["channel"] == "BHE")
        |> filter(fn: (r) => r["location"] == "00")
        |> aggregateWindow(every: 30s, fn: max, createEmpty: false)
        |> yield(name: "XJ_AKS_00_BHE")
    }
    """
    try:
        print(arg_dict)
        check_params(arg_dict, ["measurement", "time_range", "field"])
        check_params(arg_dict["time_range"], ["start_ts"])
    except ValueError as e:
        print("查询influx参数有问题", e)
        return

    query = f"""
    from(bucket: "{earthquake_bucket}")
    """

    # TODO time range condition
    time_range = arg_dict["time_range"]
    if time_range.get("end_ts") is not None:
        time_range_str = f"""
        |> range(start: {time_range["start_ts"]}, stop: {time_range["end_ts"]})
    """
    else:
        time_range_str = f"""
        |> range(start: {time_range["start_ts"]})
        """
    query = query + time_range_str

    # todo measurement
    measurement_list = []
    for measurement in arg_dict["measurement"]:
        measurement_str = f""" r["_measurement"] == "{measurement}" """
        measurement_list.append(measurement_str)
    measurement_condition = " or ".join(measurement_list)
    print(measurement_condition)
    query = query + f"""
        |> filter(fn: (r) => {measurement_condition})
    """
    # todo fields
    query = query + f"""
        |> filter(fn: (r) => r["_field"] == "{arg_dict["field"]}")
    """
    # todo tag
    for tag_name, tag_value in arg_dict["filter"].items():
        query = query + f"""
        |> filter(fn: (r) => r["{tag_name}"] == "{tag_value}")
    """

    # todo window
    if arg_dict.__contains__("window") and arg_dict["window"].__contains__("window_len"):
        window = arg_dict["window"]
        query = query + f"""
            |> aggregateWindow(every: {window["window_len"]}, fn: {window.get("fn", "mean")}, createEmpty: false)
        """

    # todo drop columns
    query = query + """
        |> drop(columns: ["channel","location","network","station","_start","_stop"])
        """
    # todo yield
    # query = query + f"""
    #     |> yield(name: "{arg_dict["measurement"]}")
    # """

    print(query)

    measurement_res = {}
    for measurement in arg_dict["measurement"]:
        measurement_res[measurement] = []
    with InfluxDBClient(url=url, token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                measurement_res[record.values["_measurement"]].append(record)

    for measurement in list(measurement_res.keys()):
        if len(measurement_res[measurement]) == 0:
            del measurement_res[measurement]
    return measurement_res


def test_multi_influx_query():
    begin = time.time()
    measurements = [["XJ.AHQ.00.BHE", "XJ.AHQ.00.BHN", "XJ.AHQ.00.BHZ"],
                    ["XJ.ALS.00.BHE", "XJ.ALS.00.BHN", "XJ.ALS.00.BHZ"],
                    ["XJ.ATS.00.BHE", "XJ.ATS.00.BHN", "XJ.ATS.00.BHZ"]
                    ]
    query_args_template = {
        "measurement": [
        ],
        "filter": {
        },
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "window": {
        }
    }
    query_args_list = []
    for measurement in measurements:
        query_args_template["measurement"] = measurement
        print(query_args_template)
        query_args_list.append(query_args_template.copy())

    query_threads = []
    for query_arg in query_args_list:
        query_threads.append(QueryInfluxDbThread(query_args=query_arg))

    query_results = []
    for query_thread in query_threads:
        query_results.append(query_thread.start())

    for query_thread in query_threads:
        query_thread.join()

    end = time.time()
    print(end - begin)


def test_influx_query():
    query_args = {
        "measurement": [
            "XJ.AHQ.00.BHE", "XJ.AHQ.00.BHN", "XJ.AHQ.00.BHZ", "XJ.ALS.00.BHE", "XJ.ALS.00.BHN", "XJ.ALS.00.BHZ"
        ],
        "filter": {
        },
        "field": "raw_data",
        "time_range": {
            "start_ts": 1671267260
        },
        "window": {
        }
    }
    begin = time.time()
    res = get_curve_points(query_args)
    end = time.time()
    print(end - begin)


def test_save_to_tDengine():
    conn: taos.TaosConnection = taos.connect(host="stephanie", user="root", password="taosdata", database="test",
                                             port=6030)

    server_version = conn.server_info
    print("server_version", server_version)
    client_version = conn.client_info
    print("client_version", client_version)  # 3.0.0.0

    conn.close()


if __name__ == '__main__':
    test_save_to_tDengine()

import os
import threading
import time

from factory import create_app
from flask_cors import CORS
from gevent import pywsgi
from flask_socketio import SocketIO, emit
from flask import Blueprint, jsonify, make_response, request, current_app, render_template
from flask_socketio import emit, send
import asyncio
import io
import logging
from kafka import KafkaConsumer
from obspy import read
import config
from algorithms.p_wave_detection import pick_wave_with_single_channel

app = create_app()
socketio = SocketIO(app)
socketio.init_app(app, cors_allowed_origins='*')
realtime_info_room = "/realtime_info"  # 实时数据websocket命名传输Room名称
request_sids = set()
tasks = []

@app.route('/')
def index():
    return render_template('realtime_info.html')


@app.route('/test_send')
def test_send():
    print("test_send")
    for i in range(1, 9):
        socketio.emit("test_send", "test_send", namespace=realtime_info_room)
    return "test_send"


@socketio.on('message', namespace=realtime_info_room)
def handle_message(message):
    print('received message: ' + message['data'])
    socketio.emit("response", {'age': 18}, namespace=realtime_info_room)


@socketio.on('connect', namespace=realtime_info_room)
def connect():
    print('backend connected: {}'.format(request.sid))
    request_sids.add(request.sid)
    print(os.getcwd())  # 获得当前工作目录
    socketio.emit("connect", "start receive realtime data", namespace=realtime_info_room)

    if len(tasks) == 0:
        # handle_realtime_mseed = RealTimeMseedTask()
        # handle_realtime_mseed.start()
        handle_test_realtime_mseed = ImitateTimeMseedTask()
        handle_test_realtime_mseed.start()
        tasks.append(handle_test_realtime_mseed)


@socketio.on('disconnect', namespace=realtime_info_room)
def disconnect():
    print("backend disconnect...")


class ImitateTimeMseedTask(threading.Thread):

    def run(self) -> None:
        # 设置mseed文件列表所在的文件夹路径
        folder_path = "./my_kafka/kafka_mseed"

        # 获取文件夹中所有的mseed文件名列表
        file_list = [f for f in os.listdir(folder_path) if f.endswith('.mseed')]

        # 循环遍历文件列表，读取mseed文件
        try:
            while True:
                for file_name in file_list:
                    file_path = os.path.join(folder_path, file_name)
                    stream = read(file_path)
                    # 处理stream对象
                    start_time = stream[0].stats.starttime
                    stime = stream[0].stats.starttime.datetime
                    channel_data = stream[0].data
                    curve_id = stream[0].id
                    stats = stream[0].stats
                    channel = stats.channel
                    location = stats.location
                    station = stats.station
                    network = stats.network
                    sampling_rate = stats.sampling_rate
                    mseed_id = stream[0].id
                    flag, p_start_time, s_start_time = pick_wave_with_single_channel(
                        single_channel_data=channel_data,
                        sampling_rate=sampling_rate,
                        start_time=start_time)
                    # TODO websocket 通知前端告知台站收到数据
                    msg = f"{curve_id}_{flag}_{p_start_time}_{s_start_time}"
                    socketio.emit("real_time_monitor", msg, namespace=realtime_info_room, broadcast=True)
                    time.sleep(1)
                    if flag:
                        # 出现地震
                        print("地震来了")
                        # TODO 定位算法

                        # TODO 震级检测算法

                    time.sleep(1)

        except Exception as e:
            logging.error(e)


class RealTimeMseedTask(threading.Thread):
    # handle_realtime_mseed
    def run(self) -> None:
        consumer = KafkaConsumer("mseed-zk",
                                 group_id='mykafka_consumer',
                                 auto_offset_reset="latest",
                                 # latest：从当前开始读 earliest：从头读
                                 enable_auto_commit=True,
                                 bootstrap_servers=["10.5.107.10:9092"])
        print("start receive mykafka msg")
        for message in consumer:
            if message:
                try:
                    if message.key is not None:
                        # upload_file(str(message.key, 'utf-8') + ".mseed", message.value)
                        # stream.write("kafka_mseed" + '/' + str(message.key, 'utf-8') + ".mseed", format='MSEED')
                        print("message.key is not None")
                    else:
                        stream = read(io.BytesIO(message.value))
                        start_time = stream[0].stats.starttime
                        stime = stream[0].stats.starttime.datetime
                        channel_data = stream[0].data
                        curve_id = stream[0].id
                        stats = stream[0].stats
                        channel = stats.channel
                        location = stats.location
                        station = stats.station
                        network = stats.network
                        sampling_rate = stats.sampling_rate
                        mseed_id = stream[0].id
                        flag, p_start_time, s_start_time = pick_wave_with_single_channel(
                            single_channel_data=channel_data,
                            sampling_rate=sampling_rate,
                            start_time=start_time)

                        # TODO websocket 通知前端告知台站收到数据
                        msg = f"{curve_id}_{flag}_{p_start_time}_{s_start_time}"
                        socketio.emit("real_time_monitor", msg, namespace=realtime_info_room)
                        time.sleep(5)
                    # 出现地震
                    # TODO 定位算法

                    # TODO 震级检测算法

                    # 写入文件
                    # stime = stime.strftime("%Y%m%d%H%M%S")
                    # key = stream[0].id + "." + stime + ".mseed"
                    # stream.write(dirname + '/' + key, format='MSEED')
                except Exception as e:
                    logging.error(e)


@app.after_request
def cross_region(response):
    """
    添加跨域功能
    :param response: 返回数据
    :return:
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    response.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return response


if __name__ == '__main__':
    CORS(app, supports_credentials=True)
    socketio.run(app, host='0.0.0.0', debug=True, port=5100)

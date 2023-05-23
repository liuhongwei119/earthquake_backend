# -*- coding:utf-8 -*-
import io
import logging
from kafka import KafkaConsumer
from obspy import read
import config
from algorithms.p_wave_detection import pick_wave_with_single_channel

dirname = "kafka_mseed"


# 接收mykafka的events数据
def receive_mykafka_msg():
    consumer = KafkaConsumer(kafka_topic,
                             group_id='mykafka_consumer',
                             auto_offset_reset="latest",
                             # latest：从当前开始读 earliest：从头读
                             enable_auto_commit=True,
                             bootstrap_servers=kafka_servers)
    print("start receive mykafka msg")
    for message in consumer:
        if message:
            try:
                if message.key is not None:
                    # upload_file(str(message.key, 'utf-8') + ".mseed", message.value)
                    stream.write(dirname + '/' + str(message.key, 'utf-8') + ".mseed", format='MSEED')
                else:
                    stream = read(io.BytesIO(message.value))
                    start_time = stream[0].stats.starttime
                    stime = stream[0].stats.starttime.datetime
                    channel_data = stream[0].data
                    curve_id = stream[0].id
                    stats = stream[0].stats
                    print(stats)
                    print(type(stats))
                    channel = stats.channel
                    location = stats.location
                    station = stats.station
                    network = stats.network
                    sampling_rate = stats.sampling_rate
                    mseed_id = stream[0].id
                    flag, p_start_time, s_start_time = pick_wave_with_single_channel(single_channel_data=channel_data,
                                                                                     sampling_rate=sampling_rate,
                                                                                     start_time=start_time)

                    # TODO websocket 通知前端告知台站收到数据
                    handle_realtime_mseed(f"{curve_id}_{flag}_{p_start_time}_{s_start_time}")

                # 出现地震
                # TODO 定位算法

                # TODO 震级检测算法

                # 写入文件
                # stime = stime.strftime("%Y%m%d%H%M%S")
                # key = stream[0].id + "." + stime + ".mseed"
                # stream.write(dirname + '/' + key, format='MSEED')
            except Exception as e:
                logging.error(e)


if __name__ == '__main__':
    kafka_topic = config.kafka_topic
    kafka_servers = config.kafka_servers
    receive_mykafka_msg()



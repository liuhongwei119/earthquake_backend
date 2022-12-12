from datetime import datetime
import os
import pandas as pd


def split_time_ranges(from_time, to_time, frequency):
    from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
    time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
    if to_time not in time_range:
        time_range.append(to_time)
    time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
    time_ranges = []
    for item in time_range:
        f_time = item
        t_time = (datetime.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=frequency))
        if t_time >= to_time:
            t_time = to_time.strftime("%Y-%m-%d %H:%M:%S")
            time_ranges.append([f_time, t_time])
            break
        time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
    return time_ranges


def convert_utc_to_datetime(utc_date):
    # 2022-10-20T19:01:18.090000Z => 2022-10-20 19:01:18
    return datetime.strptime(str(utc_date).split(".")[0], '%Y-%m-%dT%H:%M:%S')


def get_all_file_in_path(path, all_files):
    if os.path.isfile(path):
        all_files.append(path)
    else:
        # 首先遍历当前目录所有文件及文件夹
        file_list = os.listdir(path)
        # 准备循环判断每个元素是否是文件夹还是文件，是文件的话，把名称传入list，是文件夹的话，递归
        for file in file_list:
            # 利用os.path.join()方法取得路径全名，并存入cur_path变量，否则每次只能遍历一层目录
            cur_path = os.path.join(path, file)
            # 判断是否是文件夹
            if os.path.isdir(cur_path):
                get_all_file_in_path(cur_path, all_files)
            else:
                all_files.append(cur_path)
    return all_files


def get_ts(ts: str):
    dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
    return int(dt.timestamp() * 1000)

if __name__ == '__main__':
    print(get_ts("2018-10-03 14:38:05.000"))

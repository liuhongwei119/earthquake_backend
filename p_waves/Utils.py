import os
import sys
from obspy import read


# 参数mseed文件地址
# 返回Boolean，String，map：
# Boolean：文件是否可用，False为文件格式不符合要求/或文件是否有大规模缺失
# String：当Boolean为False时，具体问题
# map：key为BHE/BHN/BHZ，value为通道对用的array

def ValidationFile(filePath):
    map = {}

    # 判断文件后缀
    if not filePath.endswith('.mseed'):
        return False, '文件格式错误', map

    # 判断文件本身是否损坏
    try:
        file = read(filePath)
    except:
        return False, '文件读取失败，请检查地址是否正确', map

    for (i, tr) in enumerate(file):
        channel = tr.stats.channel

        # 判断mseed文件必要源数据是否为空
        if channel.isspace():
            return False, 'mseed文件缺少必要元数据', map

        map[channel] = tr.data

    return True, '', map


boolean, problem, map = ValidationFile('test.mseed')
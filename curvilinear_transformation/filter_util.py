import obspy
from obspy.signal import filter


# 巴特沃斯带通滤波器
# data_path – 要过滤数据的mseed文件路径。
# freqmin — 通带低转角频率。
# freqmax – 通带高角频率。
# df – 以 Hz 为单位的采样率。
# corners – 过滤角点/顺序。
# zerophase – 如果为 True，则向前和向后应用一次过滤器。这导致滤波器阶数增加一倍，但所产生的滤波轨迹中的相移为零。
def bandpass(data, freqmin=1, freqmax=3, df=100, corners=4, zerophase=False):
    return filter.bandpass(data, freqmin, freqmax, df, corners, zerophase)


# 巴特沃斯高通滤波器
# data_path – 要过滤数据的mseed文件路径。
# freq——滤波器转角频率。
# df – 以 Hz 为单位的采样率。
# corners – 过滤角点/顺序。
# zerophase – 如果为 True，则向前和向后应用一次过滤器。这会导致转角数量增加一倍，但所产生的滤波迹线中的相移为零。
def highpass(data, freq=1, df=100, corners=4, zerophase=False):
    return obspy.signal.filter.highpass(data, freq, df, corners, zerophase)


# 巴特沃斯低通滤波器
# data_path – 要过滤数据的mseed文件路径。
# freq——滤波器转角频率。
# df – 以 Hz 为单位的采样率。
# corners – 过滤角点/顺序。
# zerophase – 如果为 True，则向前和向后应用一次过滤器。这会导致转角数量增加一倍，但所产生的滤波迹线中的相移为零。
def lowpass(data, freq=1, df=100, corners=4, zerophase=False):
    return obspy.signal.filter.lowpass(data, freq, df, corners, zerophase)


pass_dict = {
    "bandpass": bandpass,
    "highpass": highpass,
    "lowpass": lowpass
}

if __name__ == '__main__':
    te = bandpass("XJ.AHQ.00.20221016085459.mseed", 1, 3, 100)
    print(te)

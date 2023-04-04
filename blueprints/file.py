import pandas as pd
from obspy import read
import os
import numpy as np

def test1():
    import obspy
    import numpy as np

    # 读取mseed文件
    st = obspy.read("../XJ.AHQ.00.20221016085459.mseed")

    # 获取数据的时间序列和数值序列
    t = st[0].times()
    y = st[0].data
    pd_data = pd.DataFrame([st[0].data, st[1].data, st[2].data])
    for y in pd_data.to_numpy():

        # 计算频率三次矩
        fft_y = np.fft.fft(y)
        freq = np.fft.fftfreq(len(y), d=st[0].stats.delta)
        freq_pos = freq[freq > 0]
        fft_pos = abs(fft_y[freq > 0])
        freq_third_moment = np.sum(freq_pos ** 3 * fft_pos) / np.sum(fft_pos)
        # 打印频率三次矩
        print("Frequency third moment:", freq_third_moment)

def test2():
    # 导入所需的包
    from obspy import read
    from obspy.signal.tf_misfit import cwt
    from obspy.signal.filter import bandpass

    # 读取mseed文件
    st = read('../XJ.AHQ.00.20221016085459.mseed')

    # 选择一个通道
    tr = st[0]

    # 设置变换参数
    freqs = cwt.freqs(1 / tr.stats.delta, 10)

    # 带通滤波
    tr_filt = tr.copy()
    tr_filt.filter('bandpass', freqmin=1, freqmax=10)

    # 计算实倒谱
    cwt_result = cwt(tr_filt.data, freqs)
    power = abs(cwt_result) ** 2
    log_power = np.log(power)
    print(log_power)

def test3():
    from obspy import read
    from obspy.signal.tf_misfit import fisher

    # 读取mseed文件
    st = read('example.mseed')

    # 选择一个通道
    tr = st[0]

    # 计算Fisher变换
    freqs, fisher_data = fisher(tr.data, tr.stats.sampling_rate, df=0.5)

    # 可视化Fisher谱
    import matplotlib.pyplot as plt
    plt.semilogx(freqs, fisher_data)
    plt.xlabel('frequency (Hz)')
    plt.ylabel('Fisher')
    plt.show()


if __name__ == '__main__':
    # print(os.getcwd())
    # a = read("../XJ.AHQ.00.20221016085459.mseed")
    # print(type(a))
    # print(a)
    # print(a[0].stats)
    # print(1)

    test1()

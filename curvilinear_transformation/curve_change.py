import numpy as np
import pywt
from scipy.fftpack import fft
from obspy import read
import os


def get_fft_power_spectrum(y_values, N, f_s, f):
    f_values = np.linspace(0.0, f_s / f, N / f)
    fft_values_ = np.abs(fft(y_values))
    fft_values = 2.0 / N * (fft_values_[0:N / 2])  # 频率真实幅值分布，单边频谱图，再把所有点频率大小表示出来*2

    # power spectrum 直接周期法
    ps_values = fft_values ** 2 / N

    # 自相关傅里叶变换法
    cor_x = np.correlate(y_values, y_values, 'same')  # 自相关
    cor_X = fft(cor_x, N)
    ps_cor = np.abs(cor_X)
    ps_cor_values = 10 * np.log10(ps_cor[0:N / 2] / np.max(ps_cor))
    # f_values设置的范围，fft_values为所有信号点的傅里叶变换值，ps_values是直接周期法功率, ps_cor_values是自相关下的对数功率。
    return f_values, fft_values, ps_values, ps_cor_values


def get_frequency1(x):
    N = len(x)
    f_s = 12000

    f_values, fft_values, ps_values, ps_cor_values = get_fft_power_spectrum(x, N, f_s, 2)
    # 直接取周期法功率
    P = ps_values
    f = fft_values

    S = []
    for i in range(N // 2):
        P1 = P[i]
        f1 = fft_values[i]
        s1 = P1 * f1
        S.append(s1)

    # 求取重心频率
    S1 = np.sum(S) / np.sum(P)

    # 平均频率
    S2 = np.sum(P) / N  # 这个N是P的个数，
    print(S2)


def get_frequency_by_scipy_fft(data_rec):
    # https://www.jianshu.com/p/f3748ca1193b
    from scipy.fftpack import fft

    data_freq = fft(data_rec)

    # mdata = np.abs(data_freq) # magnitude
    #
    # pdata = np.angle(data_freq)  # phase
    print(len(data_freq))
    return data_freq


# 简单定义一个FFT函数
def myfft(x, t):
    fft_x = fft(x)  # fft计算
    amp_x = abs(fft_x) / len(x) * 2  # 纵坐标变换
    #label_x = np.linspace(0, int(len(x) / 2) - 1, int(len(x) / 2))  # 生成频率坐标
    label_x = np.linspace(0, len(x), len(x))  # 生成频率坐标
    print(len(label_x))
    #amp = amp_x[0:int(len(x) / 2)]  # 选取前半段计算结果即可
    amp = amp_x.copy()
    # amp[0] = 0                                              # 可选择是否去除直流量信号
    fs = 1 / (t[2] - t[1])  # 计算采样频率
    fre = label_x / len(x) * fs  # 频率坐标变换
    pha = np.unwrap(np.angle(fft_x))  # 计算相位角并去除2pi跃变
    return amp, fre, pha  # 返回幅度和频

#TODO 频率图绘制
def get_frequency_by_my_fft(time_array, y_array):
    """
    https://huaweicloud.csdn.net/63802f4fdacf622b8df8648f.html?spm=1001.2101.3001.6650.5&utm_medium=distribute.pc_relevant.none-task-blog-2~default~CTRLIST~activity-5-115522729-blog-111082390.pc_relevant_aa2&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2~default~CTRLIST~activity-5-115522729-blog-111082390.pc_relevant_aa2&utm_relevant_index=9
    :param time_array:  时间数组
    :param y_array:  时域值数组
    :return: fre 频率，amp 幅度
    """
    amp, fre, pha = myfft(y_array, time_array)  # 调用函数
    return fre, amp


#TODO 时域图绘制
def get_time_frequency(time_array, y_array):
    amp, fre, pha = myfft(y_array, time_array)  # 调用函数
    return fre, amp

def get_time_frequency_test():
    import matplotlib.pyplot as plt
    import numpy as np
    import pywt

    sampling_rate = 1024
    t = np.arange(0, 1.0, 1.0 / sampling_rate)
    f1 = 100
    f2 = 200
    f3 = 300
    data = np.piecewise(t, [t < 1, t < 0.8, t < 0.3],
                        [lambda t: np.sin(2 * np.pi * f1 * t), lambda t: np.sin(2 * np.pi * f2 * t),
                         lambda t: np.sin(2 * np.pi * f3 * t)])
    wavename = 'cgau8'
    totalscal = 256
    fc = pywt.central_frequency(wavename)
    cparam = 2 * fc * totalscal
    scales = cparam / np.arange(totalscal, 1, -1)
    [cwtmatr, frequencies] = pywt.cwt(data, scales, wavename, 1.0 / sampling_rate)
    plt.figure(figsize=(8, 4))
    plt.subplot(211)
    plt.plot(t, data)
    plt.subplot(212)
    plt.contourf(t, frequencies, abs(cwtmatr))
    plt.subplots_adjust(hspace=0.4)
    plt.show()


if __name__ == '__main__':
    # print(os.getcwd())
    # a = read("../XJ.AHQ.00.20221016085459.mseed")
    # print(type(a))
    # print(a)
    # get_frequency1(a[0].data)
    # t = np.linspace(0, 5 * np.pi, 200)  # 时间坐标
    # x = np.sin(2 * np.pi * t)  # 正弦函数
    # fre, amp =get_frequency_by_my_fft(time_array=t, y_array=x)
    # print(f"len(fre) {len(fre)}")
    # print(f"len(amp) {len(amp)}")
    get_time_frequency_test()


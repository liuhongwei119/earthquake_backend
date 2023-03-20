import numpy as np
import matplotlib.pyplot as plt
from scipy.fftpack import fft
import pywt

"""
曲线变化相关参数
"""

def frequency_domain_transformation(t_array, y_array):
    """
    傅里叶变换只能得到一个信号包含哪些频率成分，
    :param t_array: 时间范围
    :param y_array: 原始值
    :return: 时间戳和频度值
    """
    return t_array, abs(fft(y_array))


def time_domain_transformation(t_array, y_array):
    """
    小波变换 得到时域图
    :param t_array: 时间范围
    :param y_array: 原始值
    :return: 具体返回啥真搞不清除
    """
    cwtmatr, freqs = pywt.cwt(y_array, np.arange(1, 200), 'cgau8', 1 / t_array)
    # 可通过这个绘画出来plt.contourf(t_array, freqs, cwtmatr)
    return cwtmatr, freqs

def frequency_domain_eg():
    """
    测试频域：
    傅里叶变换只能得到一个信号包含哪些频率成分，但无法从频域上得知信号在不同时间的频率信息，因此对频率会随着时间而改变的信号是无能为力的
    :return:
    """
    t = np.linspace(0, 1, 400, endpoint=False)
    cond = [t < 0.25, (t >= 0.25) & (t < 0.5), t >= 0.5]
    f1 = lambda t: np.cos(2 * np.pi * 10 * t)
    f2 = lambda t: np.cos(2 * np.pi * 50 * t)
    f3 = lambda t: np.cos(2 * np.pi * 100 * t)

    y1 = np.piecewise(t, cond, [f1, f2, f3])
    y2 = np.piecewise(t, cond, [f2, f1, f3])

    Y1 = abs(fft(y1))
    Y2 = abs(fft(y2))

    plt.figure(figsize=(12, 9))
    plt.subplot(221)
    plt.plot(t, y1)
    plt.title('signal_1 in time domain')
    plt.xlabel('Time/second')

    plt.subplot(222)
    plt.plot(range(400), Y1)
    plt.title('signal_1 in frequency domain')
    plt.xlabel('Frequency/Hz')

    plt.subplot(223)
    plt.plot(t, y2)
    plt.title('signal_2 in time domain')
    plt.xlabel('Time/second')

    plt.subplot(224)
    plt.plot(range(400), Y2)
    plt.title('signal_2 in frequency domain')
    plt.xlabel('Frequency/Hz')

    plt.tight_layout()
    plt.show()





def time_domain_eg():
    """
    利用小波变换测试时域
    在 Python 中可以使用 pywt.cwt 实现连续小波变换
    :return:
    """
    t = np.linspace(0, 1, 400, endpoint=False)
    cond = [t < 0.25, (t >= 0.25) & (t < 0.5), t >= 0.5]
    f1 = lambda t: np.cos(2 * np.pi * 10 * t)
    f2 = lambda t: np.cos(2 * np.pi * 50 * t)
    f3 = lambda t: np.cos(2 * np.pi * 100 * t)

    y1 = np.piecewise(t, cond, [f1, f2, f3])
    y2 = np.piecewise(t, cond, [f2, f1, f3])

    cwtmatr1, freqs1 = pywt.cwt(y1, np.arange(1, 200), 'cgau8', 1 / 400)
    cwtmatr2, freqs2 = pywt.cwt(y2, np.arange(1, 200), 'cgau8', 1 / 400)

    plt.figure(figsize=(12, 9))
    plt.subplot(221)
    plt.plot(t, y1)
    plt.title('signal_1 in time domain')
    plt.xlabel('Time/second')

    plt.subplot(222)
    plt.contourf(t, freqs1, abs(cwtmatr1))
    print(len(t))
    print(len(freqs1))
    print(len(cwtmatr1))
    plt.title('time-frequency relationship of signal_1')
    plt.xlabel('Time/second')
    plt.ylabel('Frequency/Hz')

    plt.subplot(223)
    plt.plot(t, y2)
    plt.title('signal_2 in time domain')
    plt.xlabel('Time/second')

    plt.subplot(224)
    plt.contourf(t, freqs2, abs(cwtmatr2))
    plt.title('time-frequency relationship of signal_2')
    plt.xlabel('Time/second')
    plt.ylabel('Frequency/Hz')

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    # 参考网站
    # https://www.jianshu.com/p/9bad9466ad21
    # https://blog.csdn.net/Lwwwwwwwl/article/details/122025309
    # frequency_domain_eg()
    time_domain_eg()

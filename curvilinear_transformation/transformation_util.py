"""
曲线变化相关操作， 用于获取时频图和频率图
"""
import numpy as np
import matplotlib.pyplot as plt
from scipy.fftpack import fft
import pywt
import os
import time


def frequency_domain_transformation(t_array, y_array):
    """
    频率图转换
    https://huaweicloud.csdn.net/63802f4fdacf622b8df8648f.html?spm=1001.2101.3001.6650.5&utm_medium=distribute.pc_relevant.none-task-blog-2~default~CTRLIST~activity-5-115522729-blog-111082390.pc_relevant_aa2&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2~default~CTRLIST~activity-5-115522729-blog-111082390.pc_relevant_aa2&utm_relevant_index=9
    :param t_array: 时间范围
    :param y_array: 原始值
    :return: 时间戳和频度值
    """
    from scipy.fft import fft
    fft_x = fft(y_array)  # fft计算
    amp_x = abs(fft_x) / len(y_array) * 2  # 纵坐标变换
    label_x = np.linspace(0, int(len(y_array) / 2) - 1, int(len(y_array) / 2))  # 生成频率坐标
    amp = amp_x[0:int(len(y_array) / 2)]  # 选取前半段计算结果即可
    # amp[0] = 0                                              # 可选择是否去除直流量信号
    fs = (t_array[-1] - t_array[0]) / len(t_array)  # 计算采样频率
    fre = label_x / len(y_array) * fs  # 频率坐标变换
    pha = np.unwrap(np.angle(fft_x))  # 计算相位角并去除2pi跃变
    return amp, fre  # 返回幅度和频率
    # return t_array, abs(fft(y_array))


def time_frequency_transformation_to_png(curve_points_dicts):
    """
    小波变换 得到时频图， 频率随着时间变化的图
    小波变换并将产生的图 通过f.savefig保存下来，返回地址
    :param curve_points_dicts: 时间范围
    :return: 具体返回啥真搞不清除,前端课参考plt.contourf看能否画出来
    """
    png_addr = os.getcwd() + "\\time_domain_pngs\\" + "_".join(curve_points_dicts.keys()) + ".jpg"
    plt.figure(figsize=(12, 4))
    size = len(curve_points_dicts)
    i = 1
    for curve_id, curve_points_dict in curve_points_dicts.items():
        points_info = curve_points_dict["points_info"]
        y_array = points_info["raw_datas"]
        t_array = points_info["ts"]
        cwtmatr, freqs = time_frequency_transformation(t_array=t_array, y_array=y_array)
        plt.subplot(1, size, i)
        i = i + 1
        plt.contourf(t_array, freqs, abs(cwtmatr))
        plt.title(f'time-frequency of {curve_id}')
        plt.xlabel('Time/second')
        plt.ylabel('Frequency/Hz')
    plt.tight_layout()
    f = plt.gcf()  # 获取当前图像
    f.savefig(png_addr)
    return png_addr


def time_frequency_transformation(t_array, y_array):
    """
    小波变换 得到时频
    :param t_array: 时间范围
    :param y_array: 原始值
    :return: 具体返回啥真搞不清除,前端课参考plt.contourf看能否画出来
    """
    cwtmatr, freqs = pywt.cwt(y_array, np.arange(1, 200), 'cgau8', 1 / len(t_array))
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

    print(type(Y1[2]))
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


def time_frequency_eg():
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
    f = plt.gcf()  # 获取当前图像
    f.savefig(r'D:\python_projects\earthquake_backend\test.png')
    f.clear()  # 释放内存
    plt.show()


if __name__ == '__main__':
    # 参考网站
    # https://www.jianshu.com/p/9bad9466ad21
    # https://blog.csdn.net/Lwwwwwwwl/article/details/122025309
    # frequency_domain_eg()
    time_frequency_eg()

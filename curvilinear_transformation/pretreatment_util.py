"""
预处理相关操作
"""

import numpy as np


def downsample(t_array, y_array, n):
    """
    降采样
    :param t_array:
    :param y_array:
    :param n:
    :return:
    """
    n = int(n)
    return t_array[::n], y_array[::n]


def mean_normalization(t_array, y_array):
    """
    均值归一化 (-1,1) (x - np.mean(arr))/(np.max(arr)- np.min(arr))
    :param t_array:
    :param y_array:
    :return:
    """
    y_array = np.array(y_array)
    y_mean = np.mean(y_array)
    y_max = np.max(y_array)
    y_min = np.min(y_array)
    max_min = y_max - y_min
    y_array = (y_array - y_mean) / max_min
    return t_array, y_array


def min_normalization(t_array, y_array):
    """
    （0，1） 离差归一化 (x - np.min(arr))/(np.max(arr)- np.min(arr))
    :param t_array:
    :param y_array:
    :return:
    """
    y_array = np.array(y_array)
    y_max = np.max(y_array)
    y_min = np.min(y_array)
    max_min = y_max - y_min
    y_array = (y_array - y_min) / max_min
    return t_array, y_array


def none_normalization(t_array, y_array):
    """
    返回原始数据
    :param t_array:
    :param y_array:
    :return:
    """
    return t_array, y_array


def standardization(t_array, y_array):
    """
    标准化
    :param t_array:
    :param y_array:
    :return:
    """
    mu = np.mean(y_array, axis=0)
    sigma = np.std(y_array, axis=0)
    return t_array, (y_array - mu) / sigma


def divide_sensitivity(t_array, y_array, n):
    """
    仪器响应去除：将输入数组除上仪器灵敏度n
    :param t_array:
    :param y_array:
    :param n:
    :return:
    """
    n = int(n)
    return t_array, y_array / n


if __name__ == '__main__':
    a = [1, 3, 5, 6, 7]
    print(mean_normalization(a, a))
    print(min_normalization(a, a))
    print(standardization(a, a))
    print(downsample(a, a, 1))

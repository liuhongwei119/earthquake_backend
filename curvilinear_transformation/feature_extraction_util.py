import numpy as np
import pandas as pd
from obspy import read
import os
import EntropyHub as EH
from scipy.signal import find_peaks
from scipy import stats


class TimeDomainFeatureExtraction:
    """
    类作用： 时域特征提取方法
    """

    def __init__(self, raw_datas, ts_list):
        self.raw_datas = np.array(raw_datas)
        self.ts_list = np.array(ts_list)

    def get_waveform_complexity(self, r=0.2, m=2):
        """
        函数作用： 计算波形复杂度
        波形复杂度作用： 帮助我们了解一个时间序列是否可以预测，或者说了解可预测能力有多强
        参考网站： https://blog.csdn.net/anaijiabao/article/details/106679271/
                 https://blog.csdn.net/ZHOU_YONG915/article/details/127380182
        :param r: 相似度的度量阙值
        :param m: 模板向量维数
        :return: 返回结果熵越小，乱七八糟的波动越小，预测能力越强。
        """
        th = r * np.std(self.raw_datas)  # 度量阙值
        return EH.SampEn(self.raw_datas, m, r=th)[0][-1]

    def get_auto_correlation_coefficient(self, k=1):
        """
        函数作用：自相关系数：
        自相关系数作用：描述曲线趋势的平稳程度，
        参考网站；https://blog.csdn.net/qushoushi0594/article/details/80096213
        :param k: k阶自相关
        :return: 自相关系数
        """
        l = len(self.raw_datas)
        timeSeries1 = self.raw_datas[0:l - k]
        timeSeries2 = self.raw_datas[k:]
        timeSeries_mean = self.raw_datas.mean()
        timeSeries_var = np.array([i ** 2 for i in self.raw_datas - timeSeries_mean]).sum()
        auto_corr = 0
        for i in range(l - k):
            temp = (timeSeries1[i] - timeSeries_mean) * (timeSeries2[i] - timeSeries_mean) / timeSeries_var
            auto_corr = auto_corr + temp
        return auto_corr

    def get_peak_info(self):
        """
        函数作用：计算时域图峰值相关信息
        参考网站；https://www.zadmei.com/zpzjcfz.html
        :return: 峰值相关信息
        """
        max_index = np.argmax(self.raw_datas)
        min_index = np.min(self.raw_datas)
        max_value = self.raw_datas[max_index]
        min_value = self.raw_datas[min_index]
        peaks_index, peak_property = find_peaks(self.raw_datas, height=1)
        peak_info = {
            "max_index": max_index,
            "min_index": min_index,
            "max_value": max_value,
            "min_value": min_value,
            "peaks_index": peaks_index,
            "peaks_value": peak_property["peak_heights"]
        }
        return peak_info

    def get_avg_info(self):
        """
        函数作用：计算时域图均值相关信息
        :return: 峰值相关信息
        """
        return np.mean(self.raw_datas)

    @staticmethod
    def get_time_domain_feature(data):
        """
        批式提取 15个 时域特征
        ["最大值","最大绝对值","最小值","均值","峰峰值","绝对平均值","均方根值","方根幅值","标准差","峭度","偏度","裕度指标","波形指标","脉冲指标","峰值指标"]
        @param data: shape 为 (m, n) 的 2D array 数据，其中，m 为样本个数， n 为样本（信号）长度
        @return: shape 为 (m, 15)  的 2D array 数据，其中，m 为样本个数。即 每个样本的16个时域特征
        参考资料： https://blog.csdn.net/qq_28053421/article/details/128467074?spm=1001.2101.3001.6650.3&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7EYuanLiJiHua%7EPosition-3-128467074-blog-103418201.pc_relevant_landingrelevant&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7EYuanLiJiHua%7EPosition-3-128467074-blog-103418201.pc_relevant_landingrelevant&utm_relevant_index=6
        """
        feature_names = ["最大值", "最大绝对值", "最小值", "均值", "峰峰值", "绝对平均值", "均方根值", "方根幅值",
                         "标准差", "峭度", "偏度", "裕度指标", "波形指标", "脉冲指标", "峰值指标"]
        rows, cols = data.shape

        # 有量纲统计量
        max_value = np.amax(data, axis=1)  # 最大值
        peak_value = np.amax(abs(data), axis=1)  # 最大绝对值
        min_value = np.amin(data, axis=1)  # 最小值
        mean = np.mean(data, axis=1)  # 均值
        p_p_value = max_value - min_value  # 峰峰值
        abs_mean = np.mean(abs(data), axis=1)  # 绝对平均值
        rms = np.sqrt(np.sum(data ** 2, axis=1) / cols)  # 均方根值
        square_root_amplitude = (np.sum(np.sqrt(abs(data)), axis=1) / cols) ** 2  # 方根幅值
        # variance = np.var(data, axis=1)  # 方差
        std = np.std(data, axis=1)  # 标准差
        kurtosis = stats.kurtosis(data, axis=1)  # 峭度
        skewness = stats.skew(data, axis=1)  # 偏度
        # mean_amplitude = np.sum(np.abs(data), axis=1) / cols  # 平均幅值 == 绝对平均值

        # 无量纲统计量
        clearance_factor = peak_value / square_root_amplitude  # 裕度指标
        shape_factor = rms / abs_mean  # 波形指标
        impulse_factor = peak_value / abs_mean  # 脉冲指标
        crest_factor = peak_value / rms  # 峰值指标
        # kurtosis_factor = kurtosis / (rms**4)  # 峭度指标

        features = [max_value, peak_value, min_value, mean, p_p_value, abs_mean, rms, square_root_amplitude,
                    std, kurtosis, skewness, clearance_factor, shape_factor, impulse_factor, crest_factor]

        # 封装结果
        features = np.array(features).T
        feature_list = []
        for feature in features:
            feature_dict = dict(zip(feature_names, feature))
            feature_list.append(feature_dict)
        return feature_list


class FrequencyDomainFeatureExtraction:
    def __init__(self, raw_datas, ts_list):
        self.raw_datas = np.array(raw_datas)
        self.ts_list = np.array(ts_list)

    @staticmethod
    def get_frequency_domain_feature(data, sampling_frequency):
        """
        批式提取 4个 频域特征
        ["重心频率","平均频率","均方根频率","频率方差"]
        @param data: shape 为 (m, n) 的 2D array 数据，其中，m 为样本个数， n 为样本（信号）长度
        @param sampling_frequency: 采样频率
        @return: shape 为 (m, 4)  的 2D array 数据，其中，m 为样本个数。即 每个样本的4个频域特征
        参考资料： https://blog.csdn.net/qq_28053421/article/details/128467074?spm=1001.2101.3001.6650.3&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7EYuanLiJiHua%7EPosition-3-128467074-blog-103418201.pc_relevant_landingrelevant&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7EYuanLiJiHua%7EPosition-3-128467074-blog-103418201.pc_relevant_landingrelevant&utm_relevant_index=6
        """
        feature_names = ["重心频率", "平均频率", "均方根频率", "频率方差"]
        data_fft = np.fft.fft(data, axis=1)
        m, N = data_fft.shape  # 样本个数 和 信号长度

        # 傅里叶变换是对称的，只需取前半部分数据，否则由于 频率序列 是 正负对称的，会导致计算 重心频率求和 等时正负抵消
        mag = np.abs(data_fft)[:, : N // 2]  # 信号幅值
        freq = np.fft.fftfreq(N, 1 / sampling_frequency)[: N // 2]
        # mag = np.abs(data_fft)[: , N // 2: ]  # 信号幅值
        # freq = np.fft.fftfreq(N, 1 / sampling_frequency)[N // 2: ]

        ps = mag ** 2 / N  # 功率谱

        fc = np.sum(freq * ps, axis=1) / np.sum(ps, axis=1)  # 重心频率
        mf = np.mean(ps, axis=1)  # 平均频率
        rmsf = np.sqrt(np.sum(ps * np.square(freq), axis=1) / np.sum(ps, axis=1))  # 均方根频率

        freq_tile = np.tile(freq.reshape(1, -1), (m, 1))  # 复制 m 行
        fc_tile = np.tile(fc.reshape(-1, 1), (1, freq_tile.shape[1]))  # 复制 列，与 freq_tile 的列数对应
        vf = np.sum(np.square(freq_tile - fc_tile) * ps, axis=1) / np.sum(ps, axis=1)  # 频率方差

        features = [fc, mf, rmsf, vf]
        features = np.array(features).T
        feature_list = []
        for feature in features:
            feature_dict = dict(zip(feature_names, feature))
            feature_list.append(feature_dict)
        return feature_list

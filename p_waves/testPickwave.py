import pickWave

# 参数说明：
# 第一个参数(必填)：字符串格式，文件地址
# 返回值说明：
# boolean：算法计算状态，True表示完成计算未出现问题，False为计算过程中发现问题
# problem: 如果状态为True，则问题为空，否则problem中存储为计算过程中具体问题，字符串类型
# p_wave_starttime：P波开始时间，绝对时间
# s_wave_starttime：S波开始时间，绝对时间
if __name__ == '__main__':
    boolean, problem, p_wave_starttime, s_wave_starttime = pickWave.pickWave("test.mseed")
    print(boolean)
    print(problem)
    print(p_wave_starttime)
    print(s_wave_starttime)
    print("计算完成")
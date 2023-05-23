
import datetime
import obspy.signal
from obspy.signal.trigger import recursive_sta_lta
from obspy.signal.trigger import plot_trigger
from p_waves.Utils import ValidationFile

def pick_wave_with_single_channel(single_channel_data, sampling_rate, start_time):
    cft = recursive_sta_lta(single_channel_data, int(5 * sampling_rate), int(10 * sampling_rate))
    p_start_time = 0
    s_start_time = 0
    for i in range(len(cft)):
        if cft[i] >= 1.5 and p_start_time == 0:
            p_start_time = i
        if cft[i] <= 0.5 and s_start_time == 0 and p_start_time != 0:
            s_start_time = i
            break
    p_start_time = start_time + datetime.timedelta(seconds=p_start_time / sampling_rate)
    s_start_time = start_time + datetime.timedelta(seconds=s_start_time / sampling_rate)
    print(f"p_start_time : {p_start_time}, s_start_time: {s_start_time}")
    # 一般由于传递介质不同，地震发生时 p波开始时间 一定早于s_start_time
    if p_start_time < s_start_time:
        # 地震发生
        return True, p_start_time, s_start_time
    else:
        # 异常数据，地震未发生
        return False, p_start_time, s_start_time



def pickWave(filePath):
    boolean, problem, map = ValidationFile(filePath)
    if boolean:
        r = obspy.read(filePath)
        r = r.select(component="Z")
        sampling = r.traces[0].stats.sampling_rate
        starttime = r.traces[0].stats.starttime
        cft = recursive_sta_lta(r.traces[0].data, int(5 * sampling), int(10 * sampling))
        p_start_time = 0
        s_start_time = 0
        for i in range(len(cft)):
            if cft[i] >= 1.5 and p_start_time == 0:
                p_start_time = i
            if cft[i] <= 0.5 and s_start_time == 0 and p_start_time != 0:
                s_start_time = i
                break
        p_start_time = starttime + datetime.timedelta(seconds=p_start_time / sampling)
        s_start_time = starttime + datetime.timedelta(seconds=s_start_time / sampling)
        return True, problem, p_start_time, s_start_time,
    else:
        return False, problem, datetime.datetime.now(), datetime.datetime.now()

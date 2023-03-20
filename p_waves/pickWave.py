import datetime
import obspy.signal
from obspy.signal.trigger import recursive_sta_lta
from obspy.signal.trigger import plot_trigger
from Utils import ValidationFile

def pickWave(filePath):
    boolean, problem, map = ValidationFile(filePath)
    if boolean:
        r = obspy.read(filePath)
        r = r.select(component="Z")
        sampling = r.traces[0].stats.sampling_rate
        starttime = r.traces[0].stats.starttime
        cft = recursive_sta_lta(r.traces[0].data, int(5*sampling), int(10*sampling))
        p_start_time = 0
        s_start_time = 0
        for i in range(len(cft)):
            if cft[i] >= 1.5 and p_start_time == 0:
                p_start_time = i
            if cft[i] <= 0.5 and s_start_time == 0 and p_start_time != 0:
                s_start_time = i
                break
        p_start_time = starttime+datetime.timedelta(seconds=p_start_time/sampling)
        s_start_time = starttime+datetime.timedelta(seconds=s_start_time/sampling)
        return True, problem, p_start_time, s_start_time,
    else:
        return False, problem, datetime.datetime.now(), datetime.datetime.now()



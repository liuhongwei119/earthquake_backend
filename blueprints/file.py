from obspy import read
import os

if __name__ == '__main__':
    print(os.getcwd())
    a = read("../mseed_data/SF202210160854A-B758-08/XJ.AHQ.00.20221016085459.mseed")
    print(type(a))
    print(a)
    print(a[0].stats)
    print(1)



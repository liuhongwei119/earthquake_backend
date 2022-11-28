from obspy import read
import os

if __name__ == '__main__':
    print(os.getcwd())
    a = read("mseed_data/SF202210201900A-EE6A-16/BJ.SSL.00.20221020190211.mseed")
    print(type(a))
    print(a)
    print(a[0].stats)
    print(1)



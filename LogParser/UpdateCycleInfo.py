import pandas as pd


def get_opp_side(_side):
    return 'S' if _side == 'B' else 'B'


def GetResponseValue(_split):
    return _split.split(':')[-1]


def GetLogTime(_split):
    return pd.to_datetime(_split[0][:27], format=GetLogTimeFormat())


def GetEpochTime(_split):
    return ConvertEpochToTs(int(GetEnigmaValue(_split)))


def ConvertEpochToTs(_epoch):
    return pd.to_datetime(_epoch, unit='ns')


def GetEnigmaValue(_split):
    return _split.split('=')[-1].strip()


def GetEnigmaKey(_split):
    return _split.split('=')[0].strip()


def GetLogTimeFormat():
    return '%Y%m%d-%H:%M:%S.%f'


def IsOnTimer(_split_line):
    return 'onTimer' in _split_line[0]


def IsStartTrade(_split_line):
    return 'Git Version Info' in _split_line[0]


def IsData(_split_line):
    return len(_split_line) > 1 and _split_line[1] == 'UPC'


class UpdateCycleInfo:
    def __init__(self):
        self.time = None
        self.cycle_number = None
        self.cycle_time = None
        self.hedge_time = 0
        self.start_ts = None
        self.first_pkt_rcv_ts = None
        self.first_pkt_exch_ts = None
        self.last_pkt_rcv_ts = None
        self.last_pkt_exch_ts = None

    def ParseLine(self, _split_line):
        self.time = GetLogTime(_split_line)
        self.cycle_number = float(_split_line[2])
        self.cycle_time = float(_split_line[4])
        self.hedge_time = float(_split_line[11])

        # Extract additional values from the line
        self.start_ts = float(_split_line[5])
        self.first_pkt_rcv_ts = float(_split_line[6])
        self.first_pkt_exch_ts = float(_split_line[8])
        self.last_pkt_rcv_ts = float(_split_line[9])
        self.last_pkt_exch_ts = float(_split_line[10])

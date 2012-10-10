import base64
import redis
import time
"""
All data is key'd by the timestep so there's a separate key for each timestep
Therefore, you can add all sorts to data from different sources and it will be grouped
by the timestep
"""
def tsencode(data):
    """
    data is a string
    """
    if data.find("\x00") > -1 or data.find("\x01") > -1:
        return "E%s" % (base64.b64encode(data))
    else:
        return "R%s" % (data)
    # end else
# end def

def tsdecode(data):
    """
    data is a string
    """
    if data[0:1] == 'E':
        return base64.b64decode(data[1:])
    else:
        return data[1:]
    # end else
# end def

def decode_record(r):
    """
    r is a string
    """
    res = {}
    # print "the record", r
    s = r.split("\x01")
    # print "what", s[0]
    res["time"] = float(s[0])
    res["data"] = tsdecode(s[1])
    if len(s) > 2:
        res["origin_time"] = tsdecode(s[2])
    else:
        res["origin_time"] = None
    # end else
    return res
# end def

class RedisTimeSeries(object):
    def __init__(self, prefix, timestep, redis):
        self.prefix = prefix
        self.timestep = timestep
        self.redis = redis
    # end def

    def normalize_time(self, t):
        t = int(t)
        return t - (t % self.timestep)
    # end def

    def getkey(self, t):
        return "ts:%s:%d" % (self.prefix, self.normalize_time(t))
    # end def

    def add(self, data, origin_time=None):
        data = tsencode(data)
        if origin_time:
            origin_time = tsencode(origin_time) 
        now = time.time()
        value = str(float(now)) + "\x01" + str(data)
        if origin_time:
            value += ("\x01" + str(origin_time))
        value += "\x00"
        self.redis.append(self.getkey(int(now)), value)
    # end def

    def seek(self, the_time):
        best_start = None
        best_time = None
        rangelen = 64
        key = self.getkey(int(the_time))
        length = self.redis.strlen(key)
        if length == 0:
            return 0
        min_idx = 0
        max_idx = length-1
        time_str = str(the_time)
        while True:
            p = min_idx + ( (max_idx - min_idx)/2 )
            # Seek the first complete record starting from position 'p'.
            # We need to search for two consecutive \x00 chars, and enlarnge
            # the range if needed as we don't know how big the record is.
            while True:
                range_end = p + rangelen - 1
                range_end = length if range_end > length else range_end
                r = self.redis.getrange(key, p, range_end) # returns a string
                if p == 0:
                    sep = -1
                else:
                    sep = r.find("\x00")
                # end else
                sep2 = r.find("\x00", sep + 1) if sep > -1 else -1
                if sep2 > -1:
                    record = r[(sep + 1):sep2]
                    record_start = p + sep + 1
                    record_end = p + sep2 - 1
                    dr = decode_record(record)

                    # Take track of the best sample, that is the sample
                    # that is greater than our sample, but with the smallest
                    # increment.
                    if dr["time"] >= the_time and (not best_time or best_time > dr["time"]):
                        best_start = record_start
                        best_time = dr["time"]
                        # puts "NEW BEST: #{best_time}"
                    # end if
                    if max_idx - min_idx == 1:
                        return best_start 
                    break
                # end if
                # Already at the end of the string but still no luck?
                if range_end == length:
                    return length + 1 
                # We need to enlrange the range, it is interesting to note
                # that we take the enlarged value: likely other time series
                # will be the same size on average.
                rangelen *= 2
            # end while
            # puts dr.inspect
            if dr["time"] == the_time:
                return record_start
            if dr["time"] > the_time:
                max_idx = p
            else:
                min_idx = p
            # end else
        # end while
    # end def

    def produce_result(self, res, key, range_begin, range_end):
        r = self.redis.getrange(key, range_begin, range_end)
        if r:
            # print "the range", r
            s = r.split("\x00")
            for rec in s:
                if len(rec) > 0:
                    record = decode_record(rec)
                    res.append(record)
        # end if
    # end def

    def fetch_range(self, begin_time, end_time):
        res = []
        begin_key = self.getkey(begin_time)
        end_key = self.getkey(end_time)
        begin_off = self.seek(begin_time)
        end_off = self.seek(end_time)
        if begin_key == end_key:
            self.produce_result(res, begin_key, begin_off, end_off-1)
        else:
            self.produce_result(res, begin_key, begin_off,-1)
            t = self.normalize_time(begin_time)
            while True:
                t += self.timestep
                key = self.getkey(t)
                if key == end_key:
                    break 
                self.produce_result(res, key, 0, -1)
            # end def
            self.produce_result(res, end_key, 0, end_off-1)
        # end def
        return res
    # end def

    def fetch_timestep(self, the_time): 
        res = []
        key = self.getkey(the_time)
        self.produce_result(res,key,0,-1)
        return res
    # end def
# end class
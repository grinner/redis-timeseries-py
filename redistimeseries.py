import base64
import redis
import time
import json

from functools import partial

"""
All data is key'd by the timestep so there's a separate key for each timestep
Therefore, you can add all sorts to data from different sources and it will be 
grouped by the timestep

Every data point is terminated by a \x00 byte
Every field inside the data point is delimited by \x01 byte.
base64 encoding is used if \x00 or \x01 is inside the data
"""
def tsencode(data):
    """
    data is a string
    remember \x00 and \x01 bytes are delimiters so if data has a delimiter
    it gets stored prefixed by E, otherwise it is prefixed by R
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
    Currently no error checking on decoding
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
    The record has fields of:
        time, data, and 
    """
    res = {}
    # print "the record", r
    s = r.split("\x01")
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
        """ prefix is a 
            timestep is the time step to track
            redis is the redis connection handle
        """
        self.prefix = prefix
        self.timestep = timestep
        self.redis = redis
    # end def

    def normalize_time(self, t):
        """ Times for keys need to be linear multiples of the time step
        """
        t = int(t)
        return t - (t % self.timestep)
    # end def

    def getkey(self, t):
        """
        Keys are of the form:
        key = ts:prefix:(UNIX_TIME - UNIX_TIME % TIMESTEP)
        """
        return "ts:%s:%d" % (self.prefix, self.normalize_time(t))
    # end def

    def add(self, data, origin_time=None):
        """ Add data to the time series
        - time is automatically added to the record
        - data is of course added
        - origin_time is used when a different time, such as a time recorded
        at a remote source, is available and the additional accuracy is desired
        origin time doesn't really even need to be a time since it is handled
        as a string
        
        returns the insertion time 
        (you might use for real time plotting purposes)
        """
        data = tsencode(data)
        if origin_time:
            origin_time = tsencode(origin_time)
        now = time.time()
        value = str(float(now)) + "\x01" + str(data)
        if origin_time:
            value += ("\x01" + str(origin_time))
        value += "\x00"
        self.redis.append(self.getkey(int(now)), value)
        return now
    # end def
    
    def seek(self, the_time):
        """
        Binary search for the best matching time to the_time in the 
        time series
        """
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
            # We need to search for two consecutive \x00 chars, and enlarge
            # the range if needed as we don't know how big the record is.
            while True:
                range_end = p + rangelen - 1
                range_end = length if range_end > length else range_end
                r = self.redis.getrange(key, p, range_end) # returns a string
                # sepN denotes 0th and 1st or start and end separators/delimiters
                if p == 0:
                    sep0 = -1
                else:
                    sep0 = r.find("\x00")
                # end else
                sep1 = r.find("\x00", sep0 + 1) if sep0 > -1 else -1
                if sep1 > -1:
                    record = r[(sep0 + 1):sep1]
                    record_start = p + sep0 + 1
                    record_end = p + sep1 - 1
                    dr = decode_record(record)

                    # Take track of the best sample, that is the sample
                    # that is greater than our sample, but with the smallest
                    # increment.
                    if dr["time"] >= the_time and (not best_time or best_time > dr["time"]):
                        best_start = record_start
                        best_time = dr["time"]
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
            if dr["time"] == the_time:
                return record_start
            if dr["time"] > the_time:
                max_idx = p
            else:
                min_idx = p
            # end else
        # end while
    # end def

    def produce_result(self, result, key, range_begin, range_end):
        """Used once a set of keys bookending a time range of interest are
        found to append to the result list passed to the function
        """
        # get the records
        r = self.redis.getrange(key, range_begin, range_end)
        if r:
            s = r.split("\x00")
            for rec in s:
                if len(rec) > 0:
                    record = decode_record(rec)
                    result.append(record)
        # end if
    # end def
    
    def produce_result_general(self, record_grabber, result, key, range_begin, range_end):
        """Used once a set of keys bookending a time range of interest are
        found to append to the result list passed to the function
        """
        # get the records
        r = self.redis.getrange(key, range_begin, range_end)
        if r:
            s = r.split("\x00")
            for rec in s:
                if len(rec) > 0:
                    record = decode_record(rec)
                    record_grabber(record, result)
        # end if
    # end def

    def fetch_range(self, begin_time, end_time):
        """ Get all data begining and ending at times of interest
        
        return a list of strings of decoded data
        """
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
    
    def key_tester_keepall(test_list, parser, record, results):
        data = parser(record["data"])
        for test_key in test_list:
            if test_key in data:
                # overwrite the simple string with the parser data structure
                record["data"] = data
                results.append(record)
                return
    # end def 
    
    def key_tester_keepsome(test_list, parser, record, results):
        data = parser(record["data"])
        subrecord = {}
        for test_key in test_list:
            if test_key in data:
                # store the key explicitly
                subrecord[test_key] = data[test_key]
        if len(subrecord) > 0:
            record["data"] = subrecord
            results.append(record)
    # end def
    
    def keep_everything(parser, record, results):
        data = parser(record["data"])
        if len(data > 0):
            record["data"] = data
            results.append(record)
            
    def fetch_range_json(self, begin_time, end_time, test_list=[], keepall=True):
        """ Get all data begining and ending at times of interest
        
        return a list of dictionaries of decoded data based on a test list of
        keys.  
            if testlist is empty it json decodes everything
            if keepall is true it selectively keeps data 
        """
        res = []
        begin_key = self.getkey(begin_time)
        end_key = self.getkey(end_time)
        begin_off = self.seek(begin_time)
        end_off = self.seek(end_time)
    
        if len(test_list) > 0:
            # we want every data point in the range
            record_grabber = partial(self.keep_everything, json.loads)
        else:
            key_tester = self.key_tester_keepall if keepall else self.key_tester_keepsome
            record_grabber = partial(key_tester, test_list, json.loads)
        
        producer = partial(produce_result_general, record_grabber)
 
        if begin_key == end_key:
            producer(res, begin_key, begin_off, end_off-1)
        else:
            producer(res, begin_key, begin_off,-1)
            t = self.normalize_time(begin_time)
            while True:
                t += self.timestep
                key = self.getkey(t)
                if key == end_key:
                    break 
                producer(res, key, 0, -1)
            # end def
            producer(res, end_key, 0, end_off-1)
        # end def
        return res
    # end def
    
    def fetch_timestep(self, the_time):
        """ Get all data points in a given timestep 
        """ 
        res = []
        key = self.getkey(the_time)
        self.produce_result(res,key,0,-1)
        return res
    # end def
# end class
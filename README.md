Python port of redis-timeseries https://github.com/antirez/redis-timeseries

Uses redis-py https://github.com/andymccurdy/redis-py

See the example.  

    >>> from redis import Redis
    >>> from redistimeseries import RedisTimeSeries
    >>> import time
    >>> import sys
    >>> r = Redis(host='localhost', port=6379, db=0)
    >>> r.flushdb()
    >>> ts = RedisTimeSeries("test",1, r)
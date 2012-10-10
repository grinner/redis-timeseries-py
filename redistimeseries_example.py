from redis import Redis
from redistimeseries import RedisTimeSeries
import time
import sys

# To show the lib implementation here we use a timestep of just one second.
# Then we sample every 0.1 seconds, producing on average 10 samples per key.
# This way we should how multi-key range queries are working.
r = Redis(host='localhost', port=6379, db=0)
r.flushdb()
ts = RedisTimeSeries("test",1, r)

now = float(time.time())
print "Adding data points: "
for i in range(31):
    sys.stdout.write('%d ' % (i))
    sys.stdout.flush()
    ts.add(str(i))
    time.sleep(0.1)
print ""

# Get the second in the middle of our sampling.
begin_time = now+1
end_time = now+2
print"\nGet range from %f to %f" % (begin_time, end_time)

for record in ts.fetch_range(begin_time,end_time):
    print "Record time %f, data %s" % (record["time"], record["data"])

# Show API to get a single timestep
print "\nGet a single timestep near %f" % (begin_time)
for record in ts.fetch_timestep(begin_time): 
    print "Record time %f, data %s" % (record["time"], record["data"])
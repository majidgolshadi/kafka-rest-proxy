Kafka rest proxy
================
Stand-alone Kafka rest proxy writen in go.
Any instance register himself on zookeeper (register namespace is configurable) and connect to any kafka instances.
you can pass message on `<URI>:<PORT>/topic/<TOPIC_NAME>` uri as message body

Options
-------
```bash
  -addr string
        The address to bind to example 192.168.1.101:8080
  -logtopic string
        kafka topic to produce log in (default "kafka_producer_access_log")
  -proId string
        Zookeeper namespace to producer register itself in (default "/kafka_producer/")
  -retry int
        Retry up to N times to produce the message (default 10)
  -verbose
        Turn on Sarama logging
  -zookeeper string
        The Kafka brokers to connect to, as a comma separated list example: 192.168.1.101:2181,...
```

Benchmark test
--------------
Benchmark was given on a system with 2 core cpu and 2 gig ram and a kafka instance was installed on
```bash
ab -kl  -n 1000000 -c 100 -p post.json  192.168.120.84:8080/topic/archive-messages

Benchmarking 192.168.120.84 (be patient)
Completed 100000 requests
Completed 200000 requests
Completed 300000 requests
Completed 400000 requests
Completed 500000 requests
Completed 600000 requests
Completed 700000 requests
Completed 800000 requests
Completed 900000 requests
Completed 1000000 requests
Finished 1000000 requests


Server Software:        
Server Hostname:        192.168.120.84
Server Port:            8080

Document Path:          /topic/archive-messages
Document Length:        Variable

Concurrency Level:      100
Time taken for tests:   79.293 seconds
Complete requests:      1000000
Failed requests:        0
Keep-Alive requests:    1000000
Total transferred:      202001114 bytes
Total body sent:        4279000000
HTML transferred:       61001114 bytes
Requests per second:    12611.45 [#/sec] (mean)
Time per request:       7.929 [ms] (mean)
Time per request:       0.079 [ms] (mean, across all concurrent requests)
Transfer rate:          2487.82 [Kbytes/sec] received
                        52699.61 kb/s sent
                        55187.43 kb/s total

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       4
Processing:     1    8   4.1      8     214
Waiting:        1    8   4.1      8     214
Total:          1    8   4.1      8     214

Percentage of the requests served within a certain time (ms)
  50%      8
  66%      9
  75%     10
  80%     10
  90%     12
  95%     13
  98%     16
  99%     21
 100%    214 (longest request)
```
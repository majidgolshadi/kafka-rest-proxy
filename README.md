Kafka rest proxy
================
Stand-alone Kafka rest proxy written in go.
Any instance register himself on zookeeper (register namespace is configurable) and connect to all kafka brokers.
you can pass your content on `<URI>:<PORT>/topic/<TOPIC_NAME>` uri as message body to put on kafka topics

Configuration
-------------
In order to configure service it need to make a `config.toml` file and put it near the application binary

```toml
advertised_listener="127.0.0.1:8090" #Optional ip and port that http proxy bind on (default: 0.0.0.0:8080)
debug_port=":6060"                   #Optional debugging port (default: 6060)

[log]
format="json"           #text,json (default: text)
log_level="debug"       #Optiona debug,info,warning,error (default: warning)
log_point="/directory/path/to/store/logs" #Optional (default: stdout)

[kafka-producer]
zookeeper="127.0.0.1:2181/kafka"
zookeeper_timeout=5             #Second
max_retries=10                  #Optional (default: 10)
producer_registration_namespace="/kafka-rest-proxy" #Optional (default: /kafka-rest-proxy)
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

Debugging
---------
Debugging rest APIs

- http://`<SERVER_IP>:<debug_port>`/debug/pprof/goroutine
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/heap
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/threadcreate
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/block
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/mutex
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/profile
- http://`<SERVER_IP>:<debug_port>`/debug/pprof/trace?seconds=5

Call `http://<SERVER_IP>:<debug_port>/debug/pprof/trace?seconds=5` to get 5 second of application trace file and then you can see application trace. With
`go tool trace <DOWNLOADED_FILE_PATH>` command you can see what's happen in application on that period of time

Call `http://<SERVER_IP>:<debug_port>/debug/pprof/profile` to get service profile and then run `go tool pprof <DOWNLOADED_FILE_PATH>` command go see more details about appli   cation processes

To get more information you can see [How to use pprof](https://www.integralist.co.uk/posts/profiling-go/) article
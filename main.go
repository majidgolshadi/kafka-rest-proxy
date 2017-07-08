package main

import (
	"os"
	"flag"
	"strings"
	"log"
	"time"
	"fmt"
	"path"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	addr      = flag.String("addr", "", "The address to bind to example 192.168.1.101:8080")
	proId     = flag.String("proId", "/kafka_producer/", "Zookeeper namespace to producer register itself in")
	retry	  = flag.Int("retry", 10, "Retry up to N times to produce the message")
	zookeeper = flag.String("zookeeper", "", "The Kafka brokers to connect to, as a comma separated list example: 192.168.1.101:2181,...")
	logTopic  = flag.String("logtopic", "kafka_producer_access_log", "kafka topic to produce log in")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
)


func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *zookeeper == "" || *addr == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	zkServer := strings.Split(*zookeeper, "/")[0]
	rootNamespace := strings.Replace(*zookeeper, zkServer, "", 1)

	zkConn, _, err := zk.Connect(strings.Split(zkServer, ","), 10 * time.Second)
	if err != nil {
		println(err.Error())
		return
	}

	defer zkConn.Close()
	RegisterProducer(zkConn, *proId, *addr)
	brokers, err := GetBrokers(zkConn, rootNamespace)

	server := NewRestProxy(brokers)
	server.LogTopic = *logTopic
	server.RetryConnecting = *retry
	log.Printf("Kafka brokers: %s", strings.Join(brokers, ", "))
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	go func() {
		_, _, brokerWatch, _ := zkConn.ChildrenW(fmt.Sprintf("%s/brokers/ids", rootNamespace))
		for true {
			select {
			case event := <-brokerWatch:
				_, _, brokerWatch, _ = zkConn.ChildrenW(fmt.Sprintf("%s/brokers/ids", rootNamespace))
				log.Println("Restart kafka connections")
				println(event.Path)
				br, _ := GetBrokers(zkConn, rootNamespace)
				fmt.Printf("brokers: %v", br)
				server.Restart(br)
			}
		}
	}()

	log.Fatal(server.Run(*addr))
}

func GetBrokers (zkConn *zk.Conn, chRoot string) (brokers []string, err error) {
	var children []string
	root := fmt.Sprintf("%s/brokers/ids", chRoot)
	children, _, err = zkConn.Children(root)
	if err != nil {
		return
	}

	type brokerEntry struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	for _, child := range children {
		value, _, err := zkConn.Get(path.Join(root, child))
		if err != nil {
			return nil, err
		}

		var brokerNode brokerEntry
		if err := json.Unmarshal(value, &brokerNode); err != nil {
			return nil, err
		}

		brokers = append(brokers, fmt.Sprintf("%s:%d", brokerNode.Host, brokerNode.Port))
	}

	return
}

func RegisterProducer(zkConn *zk.Conn, namespace string, advertiseIP string) {
	zkConn.Create(
		namespace,
		[]byte(advertiseIP),
		int32(zk.FlagEphemeral|zk.FlagSequence),
		zk.WorldACL(zk.PermAll))
}
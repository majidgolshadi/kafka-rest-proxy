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
	"github.com/majidgolshadi/ejabberd-to-kafka-producer/producer"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	addr      = flag.String("addr", ":8080", "The address to bind to")
	retry	  = flag.Int("retry", 10, "Retry up to N times to produce the message")
	zookeeper = flag.String("zookeeper", "192.168.120.81:2181,192.168.120.82:2181,192.168.120.83:2181/kafka", "The Kafka brokers to connect to, as a comma separated list")
	logTopic  = flag.String("logtopic", "kafka_producer_access_log", "kafka topic to produce log in")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
)


func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *zookeeper == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	zkServer := strings.Split(*zookeeper, "/")[0]
	rootNamespace := strings.Replace(*zookeeper, zkServer, "", 1)
	if rootNamespace == "" {
		rootNamespace = "/"
	}

	zkConn, ech, err := zk.Connect(strings.Split(zkServer, ","), 10 * time.Second)
	if err != nil {
		println(err.Error())
		return
	}

	defer zkConn.Close()

	brokers, err := GetBrokers(zkConn, rootNamespace)

	server := producer.New(brokers)
	server.LogTopic = *logTopic
	server.RetryConnecting = *retry
	log.Printf("Kafka brokers: %s", strings.Join(brokers, ", "))
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()
	log.Fatal(server.Run(*addr))

	_, _, brokerWatch, _ := zkConn.ChildrenW(fmt.Sprintf("%s/brokers/ids", rootNamespace))

	select {

	case <- ech:
	case <- brokerWatch:
		br, _ := GetBrokers(zkConn, rootNamespace)
		server.Restart(br)
	}
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
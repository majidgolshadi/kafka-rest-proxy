package main

import (
	"flag"
	"github.com/Shopify/sarama"
	"os"
	"strings"
	"log"
)

var (
	addr      = flag.String("addr", ":8080", "The address to bind to")
	brokers   = flag.String("brokers", "192.168.120.84:9092,192.168.120.85:9092,192.168.120.86:9092", "The Kafka brokers to connect to, as a comma separated list")
	retry	  = flag.Int("retry", 10, "Retry up to N times to produce the message")
	zookeeper = flag.String("zookeeper", "192.168.120.84:9092,192.168.120.85:9092,192.168.120.86:9092", "The Kafka brokers to connect to, as a comma separated list")
	logTopic  = flag.String("logtopic", "kafka_producer_access_log", "kafka topic to produce log in")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	server := &RestProxy{
		DataCollector:     newDataCollector(brokerList),
		AccessLogProducer: newAccessLogProducer(brokerList),
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Fatal(server.Run(*addr))
}

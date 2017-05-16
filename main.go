package main

import (
	"github.com/Shopify/sarama"
	"github.com/prometheus/common/log"
	"net/http"
	"time"
	"fmt"
)

type server struct {
	Producer sarama.SyncProducer
}

func (s *server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		s.Producer.SendMessage(&sarama.ProducerMessage{
			Topic: "archive-messages",
			Value: sarama.StringEncoder("ss"),
		})
	})
}

func (s *server) Close() {
	s.Producer.Close()
}

func main() {
	server := &server{}
	var err error
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionGZIP
	brokerList := []string{"192.168.120.84:9092", "192.168.120.85:9092", "192.168.120.86:9092"}

	server.Producer, err = sarama.NewSyncProducer(brokerList , config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// Kafka critical error happening
	//go func() {
	//	for err := range server.Producer.Errors() {
	//		println("Failed to write access log entry:", err)
	//	}
	//}()

	s := &http.Server{
		Addr:           ":8080",
		Handler:        server.Handler(),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	defer server.Close()

	fmt.Printf("Listening for requests on %s...\n", s.Addr)
	s.ListenAndServe()
}


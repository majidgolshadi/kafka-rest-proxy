package main

import (
	"github.com/Shopify/sarama"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"io/ioutil"
)


type RestProxy struct {
	RetryConnecting int
	FlushFrequency time.Duration
	LogTopic string

	brokerList []string
	addr string

	dataCollector     sarama.SyncProducer
}

func NewRestProxy(brokerList []string) *RestProxy {
	s := &RestProxy{}

	s.RetryConnecting = 10
	s.FlushFrequency = 500
	s.brokerList = brokerList

	return s
}

func (s *RestProxy) Close() {
	if err := s.dataCollector.Close(); err != nil {
		log.Println("Failed to shutdown data collector cleanly ", err)
	}
}

func (s *RestProxy) Handler() http.Handler {
	return s.withAccessLog(s.collectQueryStringData())
}

func (s *RestProxy) Run(addr string) error {
	s.addr = addr
	s.dataCollector = newDataCollector(s.brokerList, s.RetryConnecting)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
	}

	log.Printf("Listening for requests on %s...\n", addr)
	return httpServer.ListenAndServe()
}

func (s *RestProxy) Restart(brokers []string) error {
	s.Close()
	return s.Run(s.addr)
}

func (s *RestProxy) collectQueryStringData() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/topic") {
			http.NotFound(w, r)
			return
		}

		uriArray := strings.Split(r.RequestURI, "/")
		body, err := ioutil.ReadAll(r.Body);
		partition, offset, err := s.dataCollector.SendMessage(&sarama.ProducerMessage{
			Topic: uriArray[len(uriArray) - 1],
			Value: sarama.StringEncoder(body),
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to store your data:, %s", err)
		} else {
			fmt.Fprintf(w, "Your data is stored with unique identifier important/%d/%d", partition, offset)
		}
	})
}

func (s *RestProxy) withAccessLog(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)

		log.Printf("%s | %s | %s | %s | %f", r.Method, r.Host, r.RequestURI, r.RemoteAddr,
			float64(time.Since(started)) / float64(time.Second))
	})
}

func newDataCollector(brokerList []string, retry int) sarama.SyncProducer {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = retry

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

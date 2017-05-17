package producer

import (
	"github.com/Shopify/sarama"

	"encoding/json"
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
	accessLogProducer sarama.AsyncProducer
}

func New(brokerList []string) *RestProxy {
	s := &RestProxy{}

	s.RetryConnecting = 10
	s.FlushFrequency = 500
	s.LogTopic = "kafka_producer_access_log"
	s.brokerList = brokerList

	return s
}

func (s *RestProxy) Close() error {
	if err := s.dataCollector.Close(); err != nil {
		log.Println("Failed to shut down data collector cleanly", err)
	}

	if err := s.accessLogProducer.Close(); err != nil {
		log.Println("Failed to shut down access log producer cleanly", err)
	}

	return nil
}

func (s *RestProxy) Handler() http.Handler {
	return s.withAccessLog(s.collectQueryStringData())
}

func (s *RestProxy) Run(addr string) error {

	s.addr = addr
	s.dataCollector = newDataCollector(s.brokerList, s.RetryConnecting)
	s.accessLogProducer = newAccessLogProducer(s.brokerList,s.FlushFrequency)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
	}

	log.Printf("Listening for requests on %s...\n", addr)
	return httpServer.ListenAndServe()
}

func (s *RestProxy) Restart(brokers []string) error {

	err := s.Close(); if err != nil {
		return err
	}

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

		entry := &accessLogEntry{
			Method:       r.Method,
			Host:         r.Host,
			Path:         r.RequestURI,
			IP:           r.RemoteAddr,
			ResponseTime: float64(time.Since(started)) / float64(time.Second),
		}

		s.accessLogProducer.Input() <- &sarama.ProducerMessage{
			Topic: s.LogTopic,
			Value: entry,
		}
	})
}

type accessLogEntry struct {
	Method       string  `json:"method"`
	Host         string  `json:"host"`
	Path         string  `json:"path"`
	IP           string  `json:"ip"`
	ResponseTime float64 `json:"response_time"`

	encoded []byte
	err     error
}

func (ale *accessLogEntry) ensureEncoded() {
	if ale.encoded == nil && ale.err == nil {
		ale.encoded, ale.err = json.Marshal(ale)
	}
}

func (ale *accessLogEntry) Length() int {
	ale.ensureEncoded()
	return len(ale.encoded)
}

func (ale *accessLogEntry) Encode() ([]byte, error) {
	ale.ensureEncoded()
	return ale.encoded, ale.err
}

func newDataCollector(brokerList []string, retry int) sarama.SyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = retry

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func newAccessLogProducer(brokerList []string, flushFrequency time.Duration) sarama.AsyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Flush.Frequency = flushFrequency * time.Millisecond

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// Log to STDOUT if app not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}

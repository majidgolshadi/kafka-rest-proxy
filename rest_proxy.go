package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type restProxy struct {
	opt           *RestProxyOpt
	dataCollector sarama.SyncProducer
}

type RestProxyOpt struct {
	AdvertisedListener string
	Brokers            []string
	RetryConnecting    int
}

func (opt *RestProxyOpt) init() error {
	if len(opt.Brokers) < 1 {
		return errors.New("brokers does not set")
	}

	if opt.RetryConnecting == 0 {
		opt.RetryConnecting = 10
	}

	if opt.AdvertisedListener == "" {
		opt.AdvertisedListener = "127.0.0.0:8080"
	}

	return nil
}

func NewRestProxy(opt *RestProxyOpt) (*restProxy, error) {
	if err := opt.init(); err != nil {
		return nil, err
	}

	return &restProxy{
		opt: opt,
	}, nil
}

func (s *restProxy) Close() {
	if s.dataCollector == nil {
		return
	}

	if err := s.dataCollector.Close(); err != nil {
		log.WithField("error", err.Error()).Error("kafka producer failed to shutdown data collector cleanly")
	}
}

func (s *restProxy) Handler() http.Handler {
	return s.withAccessLog(s.collectQueryStringData())
}

func (s *restProxy) Listen() error {
	s.dataCollector = newDataCollector(s.opt.Brokers, s.opt.RetryConnecting)

	httpServer := &http.Server{
		Addr:    s.opt.AdvertisedListener,
		Handler: s.Handler(),
	}

	log.Info(fmt.Sprintf("http request listening on %s", s.opt.AdvertisedListener))

	return httpServer.ListenAndServe()
}

func (s *restProxy) Restart(brokers []string) error {
	s.Close()
	return s.Listen()
}

func (s *restProxy) collectQueryStringData() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/topic") {
			http.NotFound(w, r)
			return
		}

		uriArray := strings.Split(r.RequestURI, "/")
		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.WithField("error", err.Error()).Error("failed to read body")
		}

		_, _, err = s.dataCollector.SendMessage(&sarama.ProducerMessage{
			Topic: uriArray[len(uriArray)-1],
			Value: sarama.StringEncoder(body),
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.WithField("error", err.Error()).Error("failed to stored")
		}
	})
}

func (s *restProxy) withAccessLog(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)

		log.WithFields(log.Fields{
			"method":        r.Method,
			"host":          r.Host,
			"request":       r.RequestURI,
			"remote_add":    r.RemoteAddr,
			"response_time": float64(time.Since(started)) / float64(time.Second),
		}).Debug("rest api")
	})
}

func newDataCollector(brokerList []string, retry int) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = retry

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("kafka producer failed to start")
	}

	return producer
}

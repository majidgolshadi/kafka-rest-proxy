package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"os"
)

type config struct {
	AdvertisedListener string `toml:"advertised_listener"`
	DebugPort          string `toml:"debug_port"`
	Log                Log
	KafkaProducer      KafkaProducer `toml:"kafka-producer"`
}

type Log struct {
	Format   string `toml:"format"`
	LogLevel string `toml:"log_level"`
	LogPoint string `toml:"log_point"`
}

type KafkaProducer struct {
	Zookeeper                     string `toml:"zookeeper"`
	ZookeeperTimeout              int    `toml:"zookeeper_timeout"`
	ProducerRegistrationNamespace string `toml:"producer_registration_namespace"`
	MaxRetries                    int    `toml:"max_retries"`
}

func (cnf *config) init() {
	if cnf.AdvertisedListener == "" {
		cnf.AdvertisedListener = ":8080"
	}

	if cnf.DebugPort == "" {
		cnf.DebugPort = ":6060"
	}

	if cnf.Log.Format == "" {
		cnf.Log.Format = "json"
	}

	if cnf.Log.LogLevel == "" {
		cnf.Log.LogLevel = "info"
	}

	if cnf.KafkaProducer.MaxRetries < 1 {
		cnf.KafkaProducer.MaxRetries = 10
	}

	if cnf.KafkaProducer.ProducerRegistrationNamespace == "" {
		cnf.KafkaProducer.ProducerRegistrationNamespace = "/kafka-rest-proxy/"
	}
}

func main() {
	var cnf config
	var err error

	if _, err := toml.DecodeFile("config.toml", &cnf); err != nil {
		log.Fatal("read configuration file error ", err.Error())
	}

	initLogService(cnf.Log)

	go func() {
		log.Info(fmt.Sprintf("debugging server listening on port %s", cnf.DebugPort))
		http.ListenAndServe(cnf.DebugPort, nil)
	}()

	zk := &Zookeeper{
		Address:     cnf.KafkaProducer.Zookeeper,
		ConnTimeout: cnf.KafkaProducer.ZookeeperTimeout,
	}

	if err := zk.Connect(); err != nil {
		log.WithField("error", err.Error()).Fatal("connection to zookeeper error")
	}

	defer zk.Close()

	zk.registerProducer(cnf.KafkaProducer.ProducerRegistrationNamespace, cnf.AdvertisedListener)

	brokers, err := zk.getBrokers()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("failed to fetch brokers")
	}

	restProxy, err := NewRestProxy(&RestProxyOpt{
		Brokers:            brokers,
		AdvertisedListener: cnf.AdvertisedListener,
		RetryConnecting:    cnf.KafkaProducer.MaxRetries,
	})

	if err != nil {
		log.WithField("error", err.Error()).Fatal("failed to create http proxy")
	}

	defer restProxy.Close()

	go func() {
		for br := range zk.WatchOnKafkaNodeCluster() {
			restProxy.Restart(br)
		}
	}()

	log.Fatal(restProxy.Listen().Error())
}

func initLogService(logConfig Log) {
	switch logConfig.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.WarnLevel)
	}

	switch logConfig.Format {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	default:
		log.SetFormatter(&log.TextFormatter{})
	}

	if logConfig.LogPoint != "" {
		f, err := os.Create(logConfig.LogPoint)
		if err != nil {
			log.Fatal("create log file error: ", err.Error())
		}

		log.SetOutput(f)
	}
}

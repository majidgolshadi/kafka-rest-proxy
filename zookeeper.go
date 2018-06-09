package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type Zookeeper struct {
	Address     string
	ConnTimeout int
	conn        *zk.Conn
	kafkaRoot   string
}

type brokerEntry struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (z *Zookeeper) Connect() (err error) {
	zkServer := strings.Split(z.Address, "/")[0]
	z.kafkaRoot = strings.Replace(z.Address, zkServer, "", 1)
	z.conn, _, err = zk.Connect(strings.Split(zkServer, ","), time.Duration(z.ConnTimeout)*time.Second)

	log.Info("connect to zookeeper ", z.Address)
	return err
}

func (z *Zookeeper) WatchOnKafkaNodeCluster() (brokers chan []string) {
	brokers = make(chan []string)

	go func() {
	watchAgain:
		_, _, brokerWatch, _ := z.conn.ChildrenW(fmt.Sprintf("%s/brokers/ids", z.kafkaRoot))
		for range brokerWatch {
			br, _ := z.getBrokers()
			brokers <- br
			goto watchAgain
		}
	}()

	return brokers
}

func (z *Zookeeper) getBrokers() (brokers []string, err error) {
	var children []string
	brokersDir := fmt.Sprintf("%s/brokers/ids", z.kafkaRoot)

	children, _, err = z.conn.Children(brokersDir)
	if err != nil {
		return
	}

	for _, child := range children {
		value, _, err := z.conn.Get(path.Join(brokersDir, child))
		if err != nil {
			return nil, err
		}

		var broker brokerEntry
		if err := json.Unmarshal(value, &broker); err != nil {
			return nil, err
		}

		brokers = append(brokers, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}

	return brokers, nil
}

func (z *Zookeeper) registerProducer(namespace string, advertiseIP string) {
	// create directory
	var uri bytes.Buffer
	dirs := strings.Split(namespace, "/")

	// create parent directories
	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		uri.WriteString("/")
		uri.WriteString(dir)

		z.conn.Create(
			uri.String(),
			[]byte("bar"),
			int32(0),
			zk.WorldACL(zk.PermAll))
	}

	z.conn.Create(
		fmt.Sprintf("%s/id", uri.String()),
		[]byte(advertiseIP),
		int32(zk.FlagEphemeral|zk.FlagSequence),
		zk.WorldACL(zk.PermAll))
}

func (z *Zookeeper) Close() {
	log.Warn("zookeeper close")
	z.conn.Close()
}

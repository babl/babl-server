package kafka

import (
	"io/ioutil"
	stdlog "log"
	"os"

	"github.com/Shopify/sarama"
	. "github.com/larskluge/babl-server/utils"
	"gopkg.in/bsm/sarama-cluster.v2"
)

// NewClient Sarama client connection object
func NewClient(brokers []string, clientID string, debug bool) *sarama.Client {
	setSaramaLogger(debug)

	cfg := config(clientID)
	client, err := sarama.NewClient(brokers, cfg)
	Check(err)
	return &client
}

// NewClientGroup Sarama/Cluster client connection object
func NewClientGroup(brokers []string, clientID string, debug bool) *cluster.Client {
	setSaramaLogger(debug)

	cfg := clusterConfig(clientID)
	client, err := cluster.NewClient(brokers, cfg)
	Check(err)
	return client
}

func setSaramaLogger(debug bool) {
	logger := stdlog.New(os.Stderr, "", stdlog.LstdFlags)
	if debug {
		logger.SetOutput(os.Stderr)
	} else {
		logger.SetOutput(ioutil.Discard)
	}
	sarama.Logger = logger
}

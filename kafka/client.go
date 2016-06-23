package kafka

import (
	"io/ioutil"
	stdlog "log"
	"os"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/bsm/sarama-cluster.v2"
)

// NewClient Sarama client connection object
func NewClient(brokers []string, clientID string, debug bool) sarama.Client {
	setSaramaLogger(debug)

	// sarama Config object
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = clientID
	log.WithFields(log.Fields{"client id": config.ClientID}).Debug("Client id set")
	// config.ChannelBufferSize = 1024

	config.Producer.Partitioner = sarama.NewManualPartitioner
	// config.Producer.Partitioner = sarama.NewRandomPartitioner

	client, err := sarama.NewClient(brokers, config)
	check(err)
	return client
}

// NewClientGroup Sarama/Cluster client connection object
func NewClientGroup(brokers []string, clientID string, debug bool) *cluster.Client {
	setSaramaLogger(debug)

	// cluster Config object
	config := cluster.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = clientID
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	log.WithFields(log.Fields{"client id": config.ClientID}).Debug("Client id set")
	// config.ChannelBufferSize = 1024

	config.Producer.Partitioner = sarama.NewManualPartitioner
	// config.Producer.Partitioner = sarama.NewRandomPartitioner

	client, err := cluster.NewClient(brokers, config)
	check(err)
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

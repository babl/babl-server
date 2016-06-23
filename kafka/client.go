package kafka

import (
	"io/ioutil"
	stdlog "log"
	"os"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func NewClient(brokers []string, clientID string, debug bool) sarama.Client {
	logger := stdlog.New(os.Stderr, "", stdlog.LstdFlags)
	if debug {
		logger.SetOutput(os.Stderr)
	} else {
		logger.SetOutput(ioutil.Discard)
	}
	sarama.Logger = logger

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

package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/bsm/sarama-cluster.v2"
)

func config(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	log.WithFields(log.Fields{"client id": cfg.ClientID}).Debug("Client id set")

	cfg.Consumer.Return.Errors = true

	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	// cfg.ChannelBufferSize = 1024

	return cfg
}

func clusterConfig(clientID string) *cluster.Config {
	c := config(clientID)
	cfg := cluster.NewConfig()
	cfg.Config = *c

	cfg.Group.Return.Notifications = true

	return cfg
}

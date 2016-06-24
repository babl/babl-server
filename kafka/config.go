package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/bsm/sarama-cluster.v2"
)

func config(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	log.WithFields(log.Fields{"client id": cfg.ClientID}).Debug("Client id set")

	cfg.Consumer.Return.Errors = true

	cfg.Producer.RequiredAcks = 1 //sarama.WaitForAll
	cfg.Producer.Retry.Backoff = 3 * time.Second
	cfg.Producer.Retry.Max = 0
	cfg.Producer.Flush.MaxMessages = 1
	cfg.Producer.Flush.Frequency = 50 * time.Millisecond
	// cfg.Producer.Partitioner = sarama.NewManualPartitioner
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	cfg.Metadata.Retry.Max = 0
	// cfg.Metadata.Retry.Backoff = 2 * time.Second
	cfg.Metadata.Retry.Backoff = 0
	cfg.Metadata.RefreshFrequency = 0

	// cfg.ChannelBufferSize = 1024
	cfg.ChannelBufferSize = 1

	return cfg
}

func clusterConfig(clientID string) *cluster.Config {
	c := config(clientID)
	cfg := cluster.NewConfig()
	cfg.Config = *c

	cfg.Group.Return.Notifications = true

	return cfg
}

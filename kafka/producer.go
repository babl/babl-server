package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
)

// NewProducer create a new Producer object
func NewProducer(brokers []string, clientID string) sarama.SyncProducer {
	// producer, err := sarama.NewSyncProducerFromClient(client) // for unknown reason, if a producer uses an existing client, the producing messages is 10x slower
	producer, err := sarama.NewSyncProducer(brokers, config(clientID))
	check(err)
	return producer
}

// SendMessage send message to sarama.Producer
func SendMessage(producer sarama.SyncProducer, key, topic string, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	var partition int32
	var offset int64
	var err error

	fn := func() error {
		log.WithFields(log.Fields{"key": key, "topic": topic}).Info("Producer: sending message")
		partition, offset, err = producer.SendMessage(msg)
		return err
	}
	start := time.Now()
	err = backoff.Retry(fn, backoff.NewExponentialBackOff())
	check(err)
	elapsed := float64(time.Since(start).Seconds() * 1000)
	log.WithFields(log.Fields{"topic": topic, "partition": partition, "offset": offset, "duration_ms": elapsed}).Info("Producer: message sent")
}

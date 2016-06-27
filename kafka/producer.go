package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
)

// NewProducer create a new Producer object
func NewProducer(brokers []string, clientID string) *sarama.SyncProducer {
	// producer, err := sarama.NewSyncProducerFromClient(client) // for unknown reason, if a producer uses an existing client, the producing messages is 10x slower
	producer, err := sarama.NewSyncProducer(brokers, config(clientID))
	check(err)
	return &producer
}

// SendMessage send message to sarama.Producer
func SendMessage(producer *sarama.SyncProducer, key, topic string, value []byte) {
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
		_, _, err = (*producer).SendMessage(msg)
		return err
	}
	notify := func(err error, duration time.Duration) {
		log.WithFields(log.Fields{"error": err, "duration": duration, "topic": topic}).Warn("Producer: send message error, retrying..")
	}
	start := time.Now()
	err = backoff.RetryNotify(fn, backoff.NewExponentialBackOff(), notify)
	check(err)
	elapsed := float64(time.Since(start).Seconds() * 1000)
	log.WithFields(log.Fields{"topic": topic, "key": key, "partition": partition, "offset": offset, "duration_ms": elapsed}).Info("Producer: message sent")
}

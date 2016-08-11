package kafka

import (
	"time"

	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
	"gopkg.in/Shopify/sarama.v1.9.0"
)

// NewProducer create a new Producer object
func NewProducer(brokers []string, clientID string) *sarama.SyncProducer {
	// producer, err := sarama.NewSyncProducerFromClient(client) // for unknown reason, if a producer uses an existing client, the producing messages is 10x slower
	producer, err := sarama.NewSyncProducer(brokers, config(clientID))
	Check(err)
	return &producer
}

// SendMessage send message to sarama.Producer
func SendMessage(producer *sarama.SyncProducer, key, topic string, value *[]byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(*value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	rid := SplitLast(key, ".")
	var partition int32
	var offset int64
	var err error

	start := time.Now()
	_, _, err = (*producer).SendMessage(msg)
	Check(err)
	elapsed := float64(time.Since(start).Seconds() * 1000)
	log.WithFields(log.Fields{"topic": topic, "key": key, "partition": partition, "offset": offset, "duration_ms": elapsed, "rid": rid}).Info("Producer: message sent")
}

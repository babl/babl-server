package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
)

func NewProducer(client sarama.Client) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(client)
	check(err)
	return producer
}

// Producer Kafka Sarama Producer
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
	err = backoff.Retry(fn, backoff.NewExponentialBackOff())
	check(err)
	log.WithFields(log.Fields{"topic": topic, "partition": partition, "offset": offset}).Info("Producer: message sent")
}

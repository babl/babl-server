package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
)

// Producer Kafka Sarama Producer
func Producer(client sarama.Client, key, topic string, value []byte) {
	log.Infof("Producer: key = %s\r\n", key)
	log.Infof("Producer: topic = %s\r\n", topic)
	log.Infof("Producer: value = %s\r\n", value)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	check(err)
	defer func() {
		log.Infof("Producer: Close Producer")
		err := producer.Close()
		check(err)
	}()

	var partition int32
	var offset int64
	fn := func() error {
		log.Infof("Producer: sending message")
		partition, offset, err = producer.SendMessage(msg)
		return err
	}
	err = backoff.Retry(fn, backoff.NewExponentialBackOff())
	check(err)
	log.Infof("Producer: SendMessage(): topic=%s\tpartition=%d\toffset=%d", topic, partition, offset)
}

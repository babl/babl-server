package kafka

import (
	"io/ioutil"
	stdlog "log"
	"os"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
)

// Producer Kafka Sarama Producer
func Producer(brokers []string, key, topic string, value []byte, debug bool) {
	logger := stdlog.New(os.Stderr, "", stdlog.LstdFlags)
	if debug {
		logger.SetOutput(os.Stderr)
	} else {
		logger.SetOutput(ioutil.Discard)
	}
	sarama.Logger = logger

	log.Infof("Producer: key = %s\r\n", key)
	log.Infof("Producer: topic = %s\r\n", topic)
	log.Infof("Producer: value = %s\r\n", value)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = "producer-" + getRandomID()
	log.Infof("Producer: ClientID: %s\n", config.ClientID)

	config.Producer.Partitioner = sarama.NewManualPartitioner
	// config.Producer.Partitioner = sarama.NewRandomPartitioner
	// msg := &sarama.ProducerMessage{Topic: topic}

	log.Infof("Producer: Prepare message: topic=%s\tpartition=%d\n", topic, 0)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
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

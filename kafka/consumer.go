package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

type ConsumerData struct {
	Key   string
	Value []byte
}

// Consumer Kafka Sarama Consumer ((reqTopic string, debug bool)
func Consume(client sarama.Client, topic string, ch chan ConsumerData) {
	log.WithFields(log.Fields{"topic": topic, "partition": 0, "offset": "newest"}).Info("Consuming")

	consumer, err := sarama.NewConsumerFromClient(client)
	check(err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	check(err)
	defer pc.Close()

	for msg := range pc.Messages() {
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value}
		log.WithFields(log.Fields{"topic": topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value)}).Info("New Message Received")
		ch <- data
	}
	log.Println("Consumer: Done consuming topic", topic)
}

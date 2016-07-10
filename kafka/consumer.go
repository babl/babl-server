package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

// ConsumerData struct used by Consume() and ConsumeGroup()
type ConsumerData struct {
	Topic     string
	Key       string
	Value     []byte
	Processed chan bool
}

// Consume Kafka Sarama Consumer
func Consume(client *sarama.Client, topic string, ch chan *ConsumerData) {
	log.WithFields(log.Fields{"topic": topic, "partition": 0, "offset": "newest"}).Info("Consuming")

	consumer, err := sarama.NewConsumerFromClient(*client)
	check(err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	check(err)
	defer pc.Close()

	for msg := range pc.Messages() {
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value, Processed: make(chan bool, 1)}
		log.WithFields(log.Fields{"topic": topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value)}).Info("New Message Received")
		ch <- &data
		<-data.Processed
	}
	log.Println("Consumer: Done consuming topic", topic)
}

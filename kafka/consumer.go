package kafka

import (
	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
	"gopkg.in/Shopify/sarama.v1.9.0"
)

// ConsumerData struct used by Consume() and ConsumeGroup()
type ConsumerData struct {
	Topic     string
	Key       string
	Value     []byte
	Processed chan bool
}

type ConsumerOptions struct {
	Offset int64
}

// Consume Kafka Sarama Consumer
func Consume(client *sarama.Client, topic string, ch chan *ConsumerData, options ...ConsumerOptions) {
	log.WithFields(log.Fields{"topic": topic, "partition": 0, "offset": "newest"}).Info("Consuming")

	consumer, err := sarama.NewConsumerFromClient(*client)
	Check(err)
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, getOptionOffset(options))
	Check(err)
	defer pc.Close()

	for msg := range pc.Messages() {
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value, Processed: make(chan bool, 1)}
		log.WithFields(log.Fields{"topic": topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value), "rid": data.Key}).Info("New Message Received")
		ch <- &data
		<-data.Processed
	}
	log.Println("Consumer: Done consuming topic", topic)
}

func getOptionOffset(options []ConsumerOptions) int64 {
	offset := sarama.OffsetNewest
	if len(options) > 0 {
		offset = options[0].Offset
	}
	return offset
}

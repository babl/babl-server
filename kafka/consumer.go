package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
)

// ConsumerData struct used by Consume() and ConsumeGroup()
type ConsumerData struct {
	Topic     string
	Key       string
	Value     []byte
	Processed chan string
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
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value, Processed: make(chan string, 1)}
		log.WithFields(log.Fields{"topic": topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value), "rid": data.Key}).Info("New Message Received")
		ch <- &data
		<-data.Processed
	}
	log.Println("Consumer: Done consuming topic", topic)
}

// ConsumeLastN Reads last n records from a specific topic/partition
func ConsumeLastN(client *sarama.Client, topic string, partition int32, lastn int64, ch chan *ConsumerData) {
	log.WithFields(log.Fields{"topic": topic, "partition": partition, "lastn": lastn}).Info("Consuming Last N")
	if lastn <= 0 {
		return
	}
	consumer, err := sarama.NewConsumerFromClient(*client)
	Check(err)
	defer consumer.Close()

	offsetNewest, err1 := (*client).GetOffset(topic, partition, sarama.OffsetNewest)
	Check(err1)
	offsetOldest, err2 := (*client).GetOffset(topic, partition, sarama.OffsetOldest)
	Check(err2)

	offset := offsetNewest - lastn
	if offset < 0 || offset < offsetOldest {
		offset = offsetOldest
	}

	pc, err2 := consumer.ConsumePartition(topic, partition, offset)
	if err2 != nil && strings.Contains(err2.Error(), "offset is outside the range") {
		data := ConsumerData{Key: string(""), Value: []byte(""), Processed: make(chan string, 1)}
		log.WithFields(log.Fields{"topic": topic, "partition": 0, "offset": offset, "key": "", "value size": 0}).Error("Kafka Topic/Partition offset is outside the range")
		ch <- &data
		<-data.Processed
		close(ch)
		return
	}
	Check(err2)
	defer pc.Close()

	for msg := range pc.Messages() {
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value, Processed: make(chan string, 1)}
		log.WithFields(log.Fields{"topic": topic, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value), "rid": data.Key}).Info("New Message Received")
		ch <- &data
		<-data.Processed
		if msg.Offset == offsetNewest-1 {
			close(ch)
			break
		}
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

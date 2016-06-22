package kafka

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// ConsumerData struct for the kafka received messages
type ConsumerData struct {
	key   string
	value []byte
}

// ConsumerOptions data struct required for Config()
type ConsumerOptions struct {
	Brokers    string
	Partitions string
	Verbose    bool
	BufferSize int
}

var (
	consumerDefaults ConsumerOptions
	consumerMessage  sarama.ConsumerMessage
	consumerLogger   = log.New(os.Stderr, "", log.LstdFlags)
)

func init() {
	consumerDefaults.Brokers = "localhost:9092"
	consumerDefaults.Partitions = "all"
	consumerDefaults.Verbose = false
	consumerDefaults.BufferSize = 1024
}

func getConsumerCustomOptions(op ConsumerOptions) ConsumerOptions {
	result := ConsumerOptions{}
	result = consumerDefaults
	if len(op.Brokers) > 0 {
		result.Brokers = op.Brokers
	}
	if len(op.Partitions) > 0 {
		result.Partitions = op.Partitions
	}
	if op.BufferSize > 0 {
		result.BufferSize = op.BufferSize
	}
	result.Verbose = op.Verbose
	return result
}

// Consumer Kafka Sarama Consumer ((reqTopic string, debug bool)
func Consumer(consTopic string, args ...interface{}) (string, []byte) {
	// get options from args if configured, else get consumerDefaults (check each property)
	options := ConsumerOptions{}
	options = consumerDefaults
	if len(args) > 0 {
		op, ok := args[0].(ConsumerOptions)
		if ok {
			options = getConsumerCustomOptions(op)
		}
	}

	var (
		closing = make(chan struct{})
		wait    = make(chan bool)
	)
	data := ConsumerData{"", []byte{}}

	// set consumerLogger options
	if !options.Verbose {
		consumerLogger.SetOutput(ioutil.Discard)
	} else {
		consumerLogger.SetOutput(os.Stderr)
	}
	sarama.Logger = consumerLogger

	consumerLogger.Printf("Consumer: topic = %s\r\n", consTopic)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = "consumer-" + getRandomID()
	config.ChannelBufferSize = options.BufferSize
	producerLogger.Printf("Consumer: ClientID: %s\n", config.ClientID)

	c, err := sarama.NewConsumer(strings.Split(brokerList, ","), config)
	if err != nil {
		consumerLogger.Printf("Consumer: Failed to start consumer: %s", err)
		panic(err)
	}

	partitionList, err := getPartitions(c, consTopic, options.Partitions)
	if err != nil {
		consumerLogger.Printf("Consumer: Failed to get the list of partitions: %s", err)
		panic(err)
	}

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(consTopic, partition, initialOffset)
		if err != nil {
			consumerLogger.Printf("Consumer: Failed to start consumer for partition %d: %s", partition, err)
			panic(err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				if message.Offset == 0 && len(message.Value) == 0 {
					// When creating topics with Producer will create an empty 1st message
					continue
				}
				consumerLogger.Printf("Consumer: Partition: %d\n", message.Partition)
				consumerLogger.Printf("Consumer: Offset: %d\n", message.Offset)
				consumerLogger.Printf("Consumer: Key: %s\n", string(message.Key))
				consumerLogger.Printf("Consumer: Value: %q\n", string(message.Value))
				consumerLogger.Println()
				data.key = string(message.Key)
				data.value = message.Value
				wait <- true
			}
		}(pc)
	}
	<-wait
	consumerLogger.Println("Consumer: Done consuming topic", consTopic)
	close(closing)

	if err := c.Close(); err != nil {
		consumerLogger.Println("Consumer: Failed to close consumer: ", err)
	}

	return data.key, data.value
}

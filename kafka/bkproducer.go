package bablkafka

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// ProducerOptions data struct required for Config()
type ProducerOptions struct {
	Brokers   string
	Partition int32
	Verbose   bool
}

var (
	producerDefaults ProducerOptions
	producerMessage  sarama.ProducerMessage
	producerLogger   = log.New(os.Stderr, "", log.LstdFlags)
)

func init() {
	producerDefaults.Brokers = "localhost:9092"
	producerDefaults.Partition = 0
	producerDefaults.Verbose = false
}

func getCustomOptions(op ProducerOptions) ProducerOptions {
	result := ProducerOptions{}
	result = producerDefaults
	if len(op.Brokers) > 0 {
		result.Brokers = op.Brokers
	}
	if op.Partition >= 0 {
		result.Partition = op.Partition
	}
	result.Verbose = op.Verbose
	return result
}

// Producer Kafka Sarama Producer
func Producer(prodKey string, prodTopic string, prodPayload []byte, args ...interface{}) {
	// get options from args if configured, else get producerDefaults (check each property)
	options := ProducerOptions{}
	options = producerDefaults
	if len(args) > 0 {
		op, ok := args[0].(ProducerOptions)
		if ok {
			options = getCustomOptions(op)
		}
	}
	producerLogger.Println("Producer: options=", options)

	// set producerLogger options
	if !options.Verbose {
		producerLogger.SetOutput(ioutil.Discard)
	} else {
		producerLogger.SetOutput(os.Stderr)
	}
	sarama.Logger = producerLogger

	producerLogger.Printf("Producer: key = %s\r\n", prodKey)
	producerLogger.Printf("Producer: topic = %s\r\n", prodTopic)
	producerLogger.Printf("Producer: value = %s\r\n", prodPayload)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = "producer-" + getRandomID()
	producerLogger.Printf("Producer: ClientID: %s\n", config.ClientID)

	config.Producer.Partitioner = sarama.NewManualPartitioner
	producerMessage := &sarama.ProducerMessage{Topic: prodTopic, Partition: options.Partition}
	// config.Producer.Partitioner = sarama.NewRandomPartitioner
	// producerMessage := &sarama.ProducerMessage{Topic: prodTopic}

	producerLogger.Printf("Producer: Prepare message: topic=%s\tpartition=%d\n", prodTopic, options.Partition)

	if prodKey != "" {
		producerMessage.Key = sarama.StringEncoder(prodKey)
	}
	producerMessage.Value = sarama.ByteEncoder(prodPayload)

	producer, err := sarama.NewSyncProducer(strings.Split(options.Brokers, ","), config)
	if err != nil {
		printError(69, "Producer: Failed to open Kafka producer: %s", err)
		panic(err)
	}
	defer func() {
		if errX := producer.Close(); err != nil {
			producerLogger.Println("Producer: Failed to close Kafka producer cleanly:", errX)
			panic(errX)
		}
	}()

	partition, offset, err := producer.SendMessage(producerMessage)
	if err != nil {
		printError(69, "Producer: Failed to produce message: %s", err)
		panic(err)
	} else if options.Verbose {
		producerLogger.Printf("Producer: SendMessage(): topic=%s\tpartition=%d\toffset=%d\n", prodTopic, partition, offset)
	}
}

func printError(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Producer: ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
}

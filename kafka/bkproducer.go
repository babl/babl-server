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
	defaults ProducerOptions
	message  sarama.ProducerMessage
	logger   = log.New(os.Stderr, "", log.LstdFlags)
)

func init() {
	defaults.Brokers = "localhost:9092"
	defaults.Partition = 0
	defaults.Verbose = false
}

func getCustomOptions(op ProducerOptions) ProducerOptions {
	result := ProducerOptions{}
	result = defaults
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
	// get options from args if configured, else get defaults (check each property)
	options := ProducerOptions{}
	options = defaults
	if len(args) > 0 {
		op, ok := args[0].(ProducerOptions)
		if ok {
			options = getCustomOptions(op)
		}
	}
	logger.Println("Producer: options=", options)

	// set logger options
	if !options.Verbose {
		logger.SetOutput(ioutil.Discard)
	} else {
		logger.SetOutput(os.Stderr)
	}
	sarama.Logger = logger

	logger.Printf("Producer: key = %s\r\n", prodKey)
	logger.Printf("Producer: topic = %s\r\n", prodTopic)
	logger.Printf("Producer: value = %s\r\n", prodPayload)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.ClientID = "producer-" + getRandomID()
	logger.Printf("Producer: ClientID: %s\n", config.ClientID)

	config.Producer.Partitioner = sarama.NewManualPartitioner
	message := &sarama.ProducerMessage{Topic: prodTopic, Partition: options.Partition}
	// config.Producer.Partitioner = sarama.NewRandomPartitioner
	// message := &sarama.ProducerMessage{Topic: prodTopic}

	logger.Printf("Producer: Prepare message: topic=%s\tpartition=%d\n", prodTopic, options.Partition)

	if prodKey != "" {
		message.Key = sarama.StringEncoder(prodKey)
	}
	message.Value = sarama.ByteEncoder(prodPayload)

	producer, err := sarama.NewSyncProducer(strings.Split(options.Brokers, ","), config)
	if err != nil {
		printErrorAndExit(69, "Producer: Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if errX := producer.Close(); err != nil {
			logger.Println("Producer: Failed to close Kafka producer cleanly:", errX)
		}
	}()

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		printErrorAndExit(69, "Producer: Failed to produce message: %s", err)
	} else if options.Verbose {
		logger.Printf("Producer: SendMessage(): topic=%s\tpartition=%d\toffset=%d\n", prodTopic, partition, offset)
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Producer: ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	//os.Exit(code)
}

package kafka

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

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
	var producerLogger = log.New(os.Stderr, "", log.LstdFlags)

	// get options from args if configured, else get producerDefaults (check each property)
	options := ProducerOptions{}
	options = producerDefaults
	if len(args) > 0 {
		op, ok := args[0].(ProducerOptions)
		if ok {
			options = getCustomOptions(op)
		}
	}

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
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
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
		producerLogger.Printf("Producer: Fail to open Kafka producer: %s", err)
		panic(err)
	}

	msgRetry := int(0)
	for {
		partition, offset, err := producer.SendMessage(producerMessage)
		if err != nil && msgRetry < 10 {
			msgRetry++
			producerLogger.Printf("Producer: Fail to produce message, Retry ... #%d", msgRetry)
			timerWait := time.NewTimer(time.Millisecond * 100)
			<-timerWait.C
			continue
		}
		if err != nil {
			producerLogger.Printf("Producer: Fail to produce message: %s", err)
			panic(err)
		}
		producerLogger.Printf("Producer: SendMessage(): topic=%s\tpartition=%d\toffset=%d\n", prodTopic, partition, offset)
		break
	}

	// close kafka connection
	producerLogger.Println("Producer: Close Producer")
	if errClose := producer.Close(); errClose != nil {
		producerLogger.Println("Producer: Failed to close producer cleanly:", errClose)
		panic(errClose)
	}
}

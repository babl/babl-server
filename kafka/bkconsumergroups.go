package bablkafka

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"gopkg.in/bsm/sarama-cluster.v2"
)

type consumerData struct {
	key   string
	value []byte
}

// Options options data struct required for ConsumerGroupsConfig()
type Options struct {
	Brokers    string
	Offset     int64
	BufferSize int64
	Verbose    bool
	Silent     bool
}

var (
	initialized   bool
	brokerList    string
	topicList     string
	group         string
	message       sarama.ConsumerMessage
	data          consumerData
	initialOffset int64
	bufferSize    int64
	verbose       bool
	silent        bool

	pConsumer *cluster.Consumer
	wait      = make(chan bool)

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func init() {
	initialized = false
	brokerList = "127.0.0.1:9092"
	topicList = "babl.default-module"
	group = ""
	data = consumerData{"", nil}
	initialOffset = sarama.OffsetOldest
	bufferSize = 256
	verbose = true
	silent = false
	//pConsumer = cluster.Consumer{}
}

// ConsumerGroupsConfig Set internal configuration data
func ConsumerGroupsConfig(options Options) {
	if len(options.Brokers) > 0 {
		brokerList = options.Brokers
	}
	if options.Offset != 0 {
		// https://godoc.org/github.com/Shopify/sarama#pkg-constants
		initialOffset = options.Offset
	}
	if options.BufferSize > 0 {
		bufferSize = options.BufferSize
	}
	verbose = options.Verbose
	silent = options.Silent
}

// ConsumerGroups Consume messages, retieves when first message arrives
func ConsumerGroups(reqTopic string) (string, []byte) {
	// Initialize Consumer
	consumerInit(reqTopic)
	<-wait
	return data.key, data.value
}

// ConsumerGroupsMarkOffset Marks message offset after being sucessfully processed
func ConsumerGroupsMarkOffset() {
	pConsumer.MarkOffset(&message, "")
}

// ConsumerGroupsClose function to Close Consumer
func ConsumerGroupsClose() {
	if !initialized {
		logger.Println("Consumer--> Consumer can not be closed, not initialized!")
		return
	}
	initialized = false

	fmt.Printf("Consumer--> Closing consumer: ")
	if err := pConsumer.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

// Initialize and Consumer Sarama/BSM groups
func consumerInit(reqTopic string) {
	// returns if consumer is already initialized
	if initialized {
		//fmt.Printf("%s already initialized!\n", reqTopic)
		go kafkaMessages()
		return
	}
	initialized = true

	randNbr := uint32(random(1, 999999))
	randStr := strconv.FormatUint(uint64(randNbr), 10)

	// sarama/bsm config
	config := cluster.NewConfig()
	config.ClientID = "bkconsumergroups-" + randStr
	logger.Printf("ClientID: %s\n", config.ClientID)

	if verbose {
		sarama.Logger = logger
	} else {
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
	}

	topicList = reqTopic
	group = "groups." + topicList
	// fmt.Printf("Consumer --> topic=%s\r\n", topicList)
	// fmt.Printf("Consumer --> group=%s\r\n", group)

	// Init consumer, consume errors & messages
	consumer, err := cluster.NewConsumer(strings.Split(brokerList, ","), group, strings.Split(topicList, ","), config)
	if err != nil {
		printError(69, "Failed to start consumer: %s", err)
	}
	pConsumer = consumer

	go func() {
		for err := range consumer.Errors() {
			logger.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for note := range consumer.Notifications() {
			logger.Printf("Rebalanced: %+v\n", note)
		}
	}()

	go kafkaMessages()
}

func kafkaMessages() {
	message, ok := <-pConsumer.Messages()
	if ok {
		//fmt.Fprintf(os.Stdout, "bkconsumergroups => %s/%d/%d\t%s\n", message.Topic, message.Partition, message.Offset, message.Value)
		data.key = string(message.Key)
		data.value = message.Value
		//pConsumer.MarkOffset(message, "")
		wait <- true
	}
}

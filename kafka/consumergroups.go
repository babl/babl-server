package kafka

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"gopkg.in/bsm/sarama-cluster.v2"
)

type consumerData struct {
	key   string
	value []byte
}

// ConsumerGroupsOptions options data struct
type ConsumerGroupsOptions struct {
	Brokers    string
	Offset     int64
	BufferSize int64
	Verbose    bool
}

var (
	consumergroupsDefaults ConsumerGroupsOptions
	consumergroupsMessage  sarama.ConsumerMessage
	consumergroupsLogger   = log.New(os.Stderr, "", log.LstdFlags)

	// options
	brokerList    string
	initialOffset int64
	bufferSize    int64
	verbose       bool

	// data
	initialized bool
	topicList   string
	group       string
	data        consumerData

	// Pointers/Channels
	pConsumer *cluster.Consumer
	wait      = make(chan bool)
)

func init() {
	consumergroupsDefaults.Brokers = "127.0.0.1:9092"
	consumergroupsDefaults.Offset = sarama.OffsetOldest
	consumergroupsDefaults.BufferSize = 256
	consumergroupsDefaults.Verbose = false

	initialized = false
	brokerList = consumergroupsDefaults.Brokers
	topicList = "babl.default-module"
	group = ""
	data = consumerData{"", nil}
	initialOffset = consumergroupsDefaults.Offset
	bufferSize = consumergroupsDefaults.BufferSize
	verbose = consumergroupsDefaults.Verbose
	//pConsumer = cluster.Consumer{}
}

// ConsumerGroupsConfig Set internal configuration data
func ConsumerGroupsConfig(options ConsumerGroupsOptions) {
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
	pConsumer.MarkOffset(&consumergroupsMessage, "")
}

// ConsumerGroupsClose function to Close Consumer
func ConsumerGroupsClose() {
	if !initialized {
		consumergroupsLogger.Println("ConsumerGroups: Consumer can not be closed, not initialized!")
		return
	}
	initialized = false

	consumergroupsLogger.Printf("ConsumerGroups: Closing consumer")
	if err := pConsumer.Close(); err != nil {
		consumergroupsLogger.Println("ConsumerGroups: Failed to close consumer: ", err)
		panic(err)
	}
}

// Initialize and Consumer Sarama/BSM groups
func consumerInit(reqTopic string) {
	// returns if consumer is already initialized
	if initialized {
		//consumergroupsLogger.Printf("ConsumerGroups: %s already initialized!\n", reqTopic)
		go kafkaMessages()
		return
	}
	initialized = true

	// set producerLogger options
	if verbose { //!options.Verbose {
		consumergroupsLogger.SetOutput(os.Stderr)
	} else {
		consumergroupsLogger.SetOutput(ioutil.Discard)
	}
	sarama.Logger = consumergroupsLogger

	// sarama/bsm config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = verbose

	config.ClientID = "consumergroups-" + getRandomID()
	consumergroupsLogger.Printf("ConsumerGroups: ClientID: %s\n", config.ClientID)

	topicList = reqTopic
	group = "groups." + topicList
	consumergroupsLogger.Printf("ConsumerGroups: topic=%s\r\n", topicList)
	consumergroupsLogger.Printf("ConsumerGroups: group=%s\r\n", group)

	// Init consumer, consume errors & messages
	consumer, err := cluster.NewConsumer(strings.Split(brokerList, ","), group, strings.Split(topicList, ","), config)
	if err != nil {
		consumergroupsLogger.Printf("ConsumerGroups: Failed to start consumer: %s", err)
		panic(err)
	}
	pConsumer = consumer

	go func() {
		for err := range consumer.Errors() {
			consumergroupsLogger.Printf("ConsumerGroups: Error: %s\n", err.Error())
			panic(err)
		}
	}()

	go func() {
		for note := range consumer.Notifications() {
			consumergroupsLogger.Printf("ConsumerGroups: Rebalanced: %+v\n", note)
		}
	}()

	go kafkaMessages()
}

func kafkaMessages() {
	consumergroupsMessage, ok := <-pConsumer.Messages()
	if ok {
		// consumergroupsLogger.Printf("ConsumerGroups: %s/%d/%d\t%s\n",
		// 	consumergroupsMessage.Topic, consumergroupsMessage.Partition,
		// 	consumergroupsMessage.Offset, consumergroupsMessage.Value)
		data.key = string(consumergroupsMessage.Key)
		data.value = consumergroupsMessage.Value
		//pConsumer.MarkOffset(consumergroupsMessage, "")
		wait <- true
	}
}

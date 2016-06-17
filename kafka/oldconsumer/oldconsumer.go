package oldconsumer

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	key           string
	value         []byte
	brokerList    string
	topic         string
	partitions    string
	messages      sarama.ConsumerMessage
	initialOffset int64
	bufferSize    int
	verbose       bool
	silent        bool

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

//./kafka-console-producer -verbose -brokers 192.168.99.100:9092 -topic test1 -partition 1 -value "Hello Kafka"
func init() {

	key = ""
	value = []byte{}
	brokerList = "localhost:9092"
	topic = "babl.default-module"
	partitions = "all"
	value = []byte{}
	initialOffset = sarama.OffsetNewest
	bufferSize = 256
	verbose = true
	silent = false
}

// Consumer babl-kafka-producer function ...
func Consumer(reqTopic string) (string, []byte) {

	if verbose {
		sarama.Logger = logger
	}

	//brokerList = "192.168.99.100:9092"

	value = []byte{}
	key = ""
	topic = reqTopic
	fmt.Printf("Consumer --> topic=%s\r\n", topic)

	c, err := sarama.NewConsumer(strings.Split(brokerList, ","), nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		//close(closing)
		os.Exit(1)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("Consumer --> Partition:\t%d\n", msg.Partition)
			fmt.Printf("Consumer --> Offset:\t%d\n", msg.Offset)
			fmt.Printf("Consumer --> Key:\t%s\n", string(msg.Key))
			// fmt.Printf("Consumer --> Value:\t%q\n", string(msg.Value))
			fmt.Println()
			key = string(msg.Key)
			value = append(value, msg.Value...)
			// value += msg.Value
			//wg.Done()
			close(closing)
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
	return key, value
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	os.Exit(64)
}

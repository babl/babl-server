package singleconsumer

import (
	"fmt"
	"io/ioutil"
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
}

// Consumer babl-kafka-producer function ...
func Consumer(reqTopic string, debug bool) (string, []byte) {
	// set producerLogger options
	if debug {
		logger.SetOutput(os.Stderr)
	} else {
		logger.SetOutput(ioutil.Discard)
	}
	sarama.Logger = logger

	value = []byte{}
	key = ""
	topic = reqTopic
	logger.Printf("Consumer: topic=%s\r\n", topic)

	c, err := sarama.NewConsumer(strings.Split(brokerList, ","), nil)
	if err != nil {
		printError(69, "Consumer: Failed to start consumer: %s", err)
		panic(err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printError(69, "Consumer: Failed to get the list of partitions: %s", err)
		panic(err)
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
		logger.Println("Consumer: Initiating shutdown of consumer...")
		//close(closing)
		os.Exit(1)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			printError(69, "Consumer: Failed to start consumer for partition %d: %s", partition, err)
			panic(err)
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
			logger.Printf("Consumer: Partition:\t%d\n", msg.Partition)
			logger.Printf("Consumer: Offset:\t%d\n", msg.Offset)
			logger.Printf("Consumer:Key:\t%s\n", string(msg.Key))
			// logger.Printf("Consumer: Value:\t%q\n", string(msg.Value))
			logger.Println()
			key = string(msg.Key)
			value = append(value, msg.Value...)
			// value += msg.Value
			//wg.Done()
			close(closing)
		}
	}()

	wg.Wait()
	logger.Println("Consumer: Done consuming topic", topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Consumer: Failed to close consumer: ", err)
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

func printError(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Consumer: ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
}

package kafka

import (
	"strings"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/bsm/sarama-cluster.v2"
)

// ConsumeGroup Kafka Sarama/Cluster Group Consumer
func ConsumeGroup(client *cluster.Client, topic string, ch chan ConsumerData) {
	group := "group." + topic
	log.WithFields(log.Fields{"topic": topic, "group": group, "offset": "newest"}).Info("Consuming Groups")

	consumer, err := cluster.NewConsumerFromClient(client, group, strings.Split(topic, ","))
	check(err)
	defer consumer.Close()

	go consumerErrors(consumer)
	go consumerNotifications(consumer)

	for msg := range consumer.Messages() {
		data := ConsumerData{Key: string(msg.Key), Value: msg.Value}
		log.WithFields(log.Fields{"topic": topic, "group": group, "partition": msg.Partition, "offset": msg.Offset, "key": data.Key, "value size": len(data.Value)}).Info("New Group Message Received")
		ch <- data
		consumer.MarkOffset(msg, "")
	}
	log.Println("ConsumerGroups: Done consuming topic/groups", topic)
}

func consumerErrors(consumer *cluster.Consumer) {
	for err := range consumer.Errors() {
		log.WithFields(log.Fields{"error": err.Error()}).Info("Group Message Error")
		check(err)
	}
}

func consumerNotifications(consumer *cluster.Consumer) {
	for note := range consumer.Notifications() {
		log.WithFields(log.Fields{"rebalanced": note}).Info("Group Message Rebalanced Notification")
	}
}

package main

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	bn "github.com/larskluge/babl/bablnaming"
	pb "github.com/larskluge/babl/protobuf/messages"
)

func listenToMetadata(client *sarama.Client) {
	topic := bn.ModuleToTopic(ModuleName, true)
	ch := make(chan *kafka.ConsumerData)
	go kafka.Consume(client, topic, ch) // TODO read last 1,000 messages and place in cache upon start
	for msg := range ch {
		var meta pb.Meta
		if err := proto.Unmarshal(msg.Value, &meta); err != nil {
			log.WithError(err).Warn("Unknown meta data received")
		} else {
			if meta.Ping != nil {
				handlePingRequest(meta.Ping)
			}
			if meta.Cancel != nil {
				handleCancelRequest(meta.Cancel)
			}
		}
		msg.Processed <- "success"
	}
	log.Panic("listenToMetadata: Lost connection to Kafka")
}

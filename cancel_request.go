package main

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	bn "github.com/larskluge/babl/bablnaming"
	pb "github.com/larskluge/babl/protobuf/messages"
	"github.com/muesli/cache2go"
)

var (
	CancelledRequestsCache = cache2go.Cache("cancelled-requests")
)

func listenToMetadata(client *sarama.Client) {
	topic := bn.ModuleToTopic(ModuleName, true)
	log.Debug("Consuming from module meta topic")
	ch := make(chan *kafka.ConsumerData)
	go kafka.Consume(client, topic, ch) // TODO read last 1,000 messages and place in cache upon start
	for msg := range ch {
		log.WithFields(log.Fields{"key": msg.Key}).Debug("Response received from module exec")

		var meta pb.Meta
		if err := proto.Unmarshal(msg.Value, &meta); err != nil {
			log.Warn("Unknown meta data received")
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
	panic("listenToModuleResponses: Lost connection to Kafka")
}

func handlePingRequest(req *pb.PingRequest) {
	log.Error("Ping requested; not implemented") // TODO implement ping reponse
}

func handleCancelRequest(req *pb.CancelRequest) {
	log.WithFields(log.Fields{"rid": req.RequestId}).Info("Cancel request received")

	CancelledRequestsCache.Add(req.RequestId, 15*time.Minute, true)
}

func IsRequestCancelled(rid uint64) {
	CancelledRequestsCache.Exists(rid)
}

package main

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	bn "github.com/larskluge/babl/bablnaming"
	pb "github.com/larskluge/babl/protobuf/messages"
)

func scheduleRestart(producer *sarama.SyncProducer) {
	log.WithFields(log.Fields{"module": ModuleName, "hostname": Hostname(), "interval": RestartTimeout.String()}).Info("Scheduled Restart Interval Set")
	dr := pb.RestartRequest{InstanceId: Hostname()}
	req := pb.Meta{
		Restart: &dr,
	}
	msg, err := proto.Marshal(&req)
	Check(err)

	time.AfterFunc(RestartTimeout, func() {
		log.WithFields(log.Fields{"module": ModuleName, "hostname": Hostname()}).Info("Scheduled Restarted Send")
		topic := bn.ModuleToTopic(ModuleName, true)
		kafka.SendMessage(producer, "", topic, &msg)
	})
}

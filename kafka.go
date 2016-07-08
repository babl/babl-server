package main

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"gopkg.in/bsm/sarama-cluster.v2"
)

func registerModule(producer *sarama.SyncProducer, mod string) {
	now := []byte(time.Now().UTC().String())
	kafka.SendMessage(producer, mod, "modules", &now)
}

func startWorker(clientgroup *cluster.Client, producer *sarama.SyncProducer, topics []string) {
	ch := make(chan *kafka.ConsumerData)
	go kafka.ConsumeGroup(clientgroup, topics, ch)

	for {
		log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		async := false
		var msg []byte
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			check(err)
			_, async = in.Env["BABL_ASYNC"]
			out, err := IO(in)
			check(err)
			msg, err = proto.Marshal(out)
			check(err)
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			check(err)
			out, err := Ping(in)
			check(err)
			msg, err = proto.Marshal(out)
			check(err)
		}

		if !async {
			n := strings.LastIndex(data.Key, ".")
			host := data.Key[:n]
			skey := data.Key[n+1:]
			stopic := "supervisor." + host
			kafka.SendMessage(producer, skey, stopic, &msg)
		}
	}
}

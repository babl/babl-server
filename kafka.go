package main

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
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
		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request received in module's topic/group")

		req := &pbm.BinRequest{}
		err := proto.Unmarshal(data.Value, req)
		Check(err)

		l := log.WithFields(log.Fields{"rid": FmtRid(req.Id)})

		async := false
		status := "error"
		res := &pbm.BinReply{Id: req.Id, Module: req.Module}
		var msg []byte
		_, async = req.Env["BABL_ASYNC"]
		delete(req.Env, "BABL_ASYNC") // worker needs to process job synchronously
		if len(req.Env) == 0 {
			req.Env = map[string]string{}
		}

		// Ignore all incoming messages from Kafka to flush the topic
		if KafkaFlush {
			str := "Topic Flush in process; ignoring this message"
			l.Warn(str)
			res.Exitcode = -6
			res.Stderr = []byte(str)
			status = "flush"
		} else if IsRequestCancelled(req.Id) {
			str := "Request cancelled; this job is ignored"
			l.Warn(str)
			res.Exitcode = -7
			res.Stderr = []byte(str)
			status = "cancel"
		} else {
			// Processing message
			var err error
			res, err = IO(req, MaxKafkaMessageSize)
			Check(err)
			if res.Exitcode == 0 {
				status = "success"
			}
		}
		msg, err = proto.Marshal(res)
		Check(err)

		if !async {
			n := strings.LastIndex(data.Key, ".")
			host := data.Key[:n]
			skey := data.Key[n+1:]
			stopic := "supervisor." + host
			kafka.SendMessage(producer, skey, stopic, &msg)
		}

		data.Processed <- status
	}
}

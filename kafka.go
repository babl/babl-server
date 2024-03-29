package main

import (
	"os"
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
		Processing = true
		req := &pbm.BinRequest{}
		err := proto.Unmarshal(data.Value, req)
		Check(err)
		l := log.WithFields(log.Fields{"rid": FmtRid(req.Id)})
		l.WithFields(log.Fields{"key": data.Key, "code": "req-started"}).Info("Request received in module's topic/group")

		async := false
		status := "error"
		res := &pbm.BinReply{Id: req.Id, Module: req.Module}
		var msg []byte
		_, async = req.Env["BABL_ASYNC"]
		delete(req.Env, "BABL_ASYNC") // worker needs to process job synchronously
		if len(req.Env) == 0 {
			req.Env = map[string]string{}
		}

		if KafkaFlush { // Ignore all incoming messages from Kafka to flush the topic
			str := "Topic Flush in process; ignoring this message"
			l.WithFields(log.Fields{"code": "req-flushed"}).Warn(str)
			res.Exitcode = -6
			res.Stderr = []byte(str)
			status = "flush"
		} else if IsRequestCancelled(req.Id) {
			str := "Request cancelled; this job is ignored"
			l.WithFields(log.Fields{"code": "req-execution-canceled"}).Warn(str)
			res.Exitcode = -7
			res.Stderr = []byte(str)
			res.Status = pbm.BinReply_EXECUTION_CANCELED
			status = "cancel"
		} else {
			// Processing message
			var err error
			res, err = IO(req, MaxKafkaMessageSize)
			Check(err)
			if res.Exitcode == 0 {
				res.Status = pbm.BinReply_SUCCESS
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
			l.WithFields(log.Fields{"code": "reply-enqueued"}).Info("Module replied")
		}
		if ShouldRestart() {
			log.WithFields(log.Fields{"code": "req-restart", "module": ModuleName}).Warn("Instance will restart now!")
			os.Exit(0)
		}
		Processing = false
		data.Processed <- status
	}
}

package main

import (
	"strconv"
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
		log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request received in module's topic/group")

		ridStr := SplitLast(data.Key, ".")
		rid, err := strconv.ParseUint(ridStr, 10, 64)
		Check(err)

		l := log.WithFields(log.Fields{"rid": rid})

		// Ignore all incoming messages from Kafka to flush the topic
		if KafkaFlush {
			l.Warn("Topic Flush in process; ignoring this message")
			data.Processed <- "flush"
			continue
		}

		async := false
		res := "error"
		var msg []byte
		method := SplitLast(data.Topic, ".")
		switch method {
		case "IO":
			in := &pbm.BinRequest{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			_, async = in.Env["BABL_ASYNC"]
			delete(in.Env, "BABL_ASYNC") // worker needs to process job synchronously
			if len(in.Env) == 0 {
				in.Env = map[string]string{}
			}
			in.Env["BABL_RID"] = ridStr

			var out *pbm.BinReply
			if !IsRequestCancelled(rid) {
				var err error
				out, err = IO(in, MaxKafkaMessageSize)
				Check(err)
				if out.Exitcode == 0 {
					res = "success"
				}
			} else {
				str := "Request cancelled; this job is ignored"
				l.Warn(str)
				out = &pbm.BinReply{
					Id:       rid,
					Module:   ModuleName,
					Exitcode: -7,
					Stderr:   []byte(str),
				}
				res = "cancel"
			}
			msg, err = proto.Marshal(out)
			Check(err)
		case "Ping":
			in := &pbm.Empty{}
			err := proto.Unmarshal(data.Value, in)
			Check(err)
			out, err := Ping(in)
			Check(err)
			res = "success"
			msg, err = proto.Marshal(out)
			Check(err)
		}

		if !async {
			n := strings.LastIndex(data.Key, ".")
			host := data.Key[:n]
			skey := data.Key[n+1:]
			stopic := "supervisor." + host
			kafka.SendMessage(producer, skey, stopic, &msg)
		}

		data.Processed <- res
	}
}

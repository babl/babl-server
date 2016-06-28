package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/bsm/sarama-cluster.v2"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server/kafka"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"github.com/larskluge/babl/shared"
)

const clientID = "babl-server"

type server struct{}

var debug bool
var command string

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(moduleName, cmd, address, kafkaBrokers string, dbg bool) {
	debug = dbg
	command = cmd

	if !shared.CheckModuleName(moduleName) {
		log.WithFields(log.Fields{"module": moduleName}).Fatal("Module name format incorrect")
	}

	log.Warn("Start module")
	module := shared.NewModule(moduleName, debug)

	brokers := strings.Split(kafkaBrokers, ",")
	client := kafka.NewClient(brokers, clientID, debug)
	clientgroup := kafka.NewClientGroup(brokers, clientID, debug)
	defer (*client).Close()
	defer (*clientgroup).Close()

	producer := kafka.NewProducer(brokers, clientID+".producer")
	defer func() {
		log.Infof("Producer: Close Producer")
		err := (*producer).Close()
		check(err)
	}()

	go registerModule(producer, moduleName)
	go work(clientgroup, producer, kafkaBrokers, []string{module.KafkaTopicName("IO"), module.KafkaTopicName("Ping")})

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}

func work(clientgroup *cluster.Client, producer *sarama.SyncProducer, brokers string, topics []string) {
	ch := make(chan kafka.ConsumerData)
	go kafka.ConsumeGroup(clientgroup, topics, ch)

	for {
		log.WithFields(log.Fields{"topics": topics}).Debug("Work")

		data, _ := <-ch
		log.WithFields(log.Fields{"key": data.Key}).Debug("Request recieved in module's topic/group")

		in := &pbm.BinRequest{}
		err := proto.Unmarshal(data.Value, in)
		check(err)
		out, err := IO(in)
		check(err)
		msg, err := proto.Marshal(out)
		check(err)

		n := strings.LastIndex(data.Key, ".")
		host := data.Key[:n]
		skey := data.Key[n+1:]
		stopic := "supervisor." + host
		kafka.SendMessage(producer, skey, stopic, &msg)
	}
}

func registerModule(producer *sarama.SyncProducer, mod string) {
	now := []byte(time.Now().UTC().String())
	kafka.SendMessage(producer, mod, "modules", &now)
}

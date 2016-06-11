package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"github.com/larskluge/babl/shared"
	"github.com/nneves/kafka-tools/bkconsumer"
	"github.com/nneves/kafka-tools/bkproducer"
)

type server struct{}

var command string

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(moduleName, cmd, address, kafkaBrokers string) {
	if !shared.CheckModuleName(moduleName) {
		log.WithFields(log.Fields{"module": moduleName}).Fatal("Module name format incorrect")
	}
	log.Warn("Start module")
	command = cmd
	module := shared.NewModule(moduleName, false)

	go work(module.KafkaTopicName("IO"))
	go work(module.KafkaTopicName("Ping"))

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}

func work(topic string) {
	key, value := bkconsumer.Consumer(topic)
	in := &pbm.BinRequest{}
	err := proto.Unmarshal(value, in)
	check(err)
	out, err := IO(in)
	check(err)
	msg, err := proto.Marshal(out)
	check(err)
	inboxTopic := "inbox." + key
	bkproducer.Producer(key, inboxTopic, msg)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

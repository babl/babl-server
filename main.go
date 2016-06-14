package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"github.com/larskluge/babl/shared"
	"github.com/nneves/kafka-tools/bkconsumergroups"
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

	go registerModule(moduleName)
	go work([]string{module.KafkaTopicName("IO"), module.KafkaTopicName("Ping")})

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}

func work(topics []string) {
	for {
		log.Infof("Work on topics %q", topics)
		key, value := bkconsumergroups.ConsumerGroups(strings.Join(topics, ","))
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
}

func registerModule(mod string) {
	now := time.Now().UTC().String()
	bkproducer.Producer(mod, "modules", []byte(now))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/larskluge/babl-server-kafka/kafka"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"github.com/larskluge/babl/shared"
)

type server struct{}

var debug bool

var command string

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(moduleName, cmd, address, kafkaBrokers string, clidebug bool) {
	if !shared.CheckModuleName(moduleName) {
		log.WithFields(log.Fields{"module": moduleName}).Fatal("Module name format incorrect")
	}
	log.Warn("Start module")
	command = cmd
	module := shared.NewModule(moduleName, debug)
	debug = clidebug

	go registerModule(moduleName)
	go work([]string{module.KafkaTopicName("IO"), module.KafkaTopicName("Ping")})

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	//kafka.ConsumerClose()
	kafka.ConsumerGroupsClose()
}

func work(topics []string) {
	for {
		log.Infof("Work on topics %q", topics)
		kafka.ConsumerGroupsConfig(kafka.ConsumerGroupsOptions{
			//BufferSize: 512,
			Verbose: debug,
		})
		key, value := kafka.ConsumerGroups(strings.Join(topics, ","))
		in := &pbm.BinRequest{}
		err := proto.Unmarshal(value, in)
		check(err)
		out, err := IO(in)
		check(err)
		msg, err := proto.Marshal(out)
		check(err)
		inboxTopic := "inbox." + key

		kafka.Producer(key, inboxTopic, msg, kafka.ProducerOptions{Verbose: debug})
	}
}

func registerModule(mod string) {
	now := time.Now().UTC().String()
	kafka.Producer(mod, "modules", []byte(now), kafka.ProducerOptions{Verbose: debug})
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

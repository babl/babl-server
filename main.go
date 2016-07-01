package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl-server/kafka"
	"github.com/larskluge/babl/shared"
)

const clientID = "babl-server"

var debug bool
var command string

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(moduleName, cmd, address string, kafkaBrokers []string, dbg bool) {
	debug = dbg
	command = cmd
	if !shared.CheckModuleName(moduleName) {
		log.WithFields(log.Fields{"module": moduleName}).Fatal("Module name format incorrect")
	}
	module := shared.NewModule(moduleName)
	module.SetDebug(debug)

	if len(kafkaBrokers) == 0 {
		log.Warn("Start module with GRPC access only")
	} else {
		log.Warn("Start module with GRPC access & Kafka support")
		clientgroup := kafka.NewClientGroup(kafkaBrokers, clientID, debug)
		defer (*clientgroup).Close()

		producer := kafka.NewProducer(kafkaBrokers, clientID+".producer")
		defer func() {
			log.Infof("Producer: Close Producer")
			err := (*producer).Close()
			check(err)
		}()

		go registerModule(producer, moduleName)
		go startWorker(clientgroup, producer, []string{module.KafkaTopicName("IO"), module.KafkaTopicName("Ping")})
	}

	startGrpcServer(address, module)
}

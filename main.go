package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl-server/kafka"
	"github.com/larskluge/babl/bablmodule"
)

const Version = "0.5.1"
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
	if !bablmodule.CheckModuleName(moduleName) {
		log.WithFields(log.Fields{"module": moduleName}).Fatal("Module name format incorrect")
	}
	module := bablmodule.New(moduleName)
	module.SetDebug(debug)

	interfaces := "GRPC"
	if len(kafkaBrokers) > 0 {
		interfaces += ",Kafka"
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

	log.WithFields(log.Fields{"version": Version, "interfaces": interfaces}).Warn("Start module")
	startGrpcServer(address, module)
}

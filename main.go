package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	"github.com/larskluge/babl/bablmodule"
	"github.com/larskluge/babl/bablutils"
)

const (
	Version  = "0.6.4"
	clientID = "babl-server"

	MaxKafkaMessageSize = 1024 * 512        // 512kb
	MaxGrpcMessageSize  = 1024 * 1024 * 100 // 100mb
)

var (
	debug           bool   // set by cli.go
	command         string // set by cli.go
	StorageEndpoint string // set by cli.go
	ModuleName      string // set by cli.go
)

func main() {
	bablutils.PrintPlainVersionAndExit(os.Args, Version)
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(address string, kafkaBrokers []string) {
	if !bablmodule.CheckModuleName(ModuleName) {
		log.WithFields(log.Fields{"module": ModuleName}).Fatal("Module name format incorrect")
	}
	module := bablmodule.New(ModuleName)
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
			Check(err)
		}()

		go registerModule(producer, ModuleName)
		go startWorker(clientgroup, producer, []string{module.KafkaTopicName("IO"), module.KafkaTopicName("Ping")})
	}

	log.WithFields(log.Fields{"version": Version, "interfaces": interfaces, "debug": debug}).Warn("Start module")
	startGrpcServer(address, module)
}

package main

import (
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
	"github.com/larskluge/babl/bablmodule"
	"github.com/larskluge/babl/bablutils"
)

const (
	Version  = "0.7.4"
	clientID = "babl-server"

	MaxKafkaMessageSize = 1024 * 100        // 100kb
	MaxGrpcMessageSize  = 1024 * 1024 * 100 // 100mb
)

var (
	debug           bool          // set by cli.go
	command         string        // set by cli.go
	CommandTimeout  time.Duration // set by cli.go
	StorageEndpoint string        // set by cli.go
	ModuleName      string        // set by cli.go
	KafkaFlush      bool          // set by cli.go
)

func main() {
	u := bablutils.NewUpgrade(clientID, os.Args)
	u.Update(Version, os.Getenv("BABL_DESIRED_SERVER_VERSION"))
	bablutils.PrintPlainVersionAndExit(os.Args, Version)
	app := configureCli()
	app.Run(os.Args)
}

func run(address string, kafkaBrokers []string) {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	if debug {
		log.SetLevel(log.DebugLevel)
	}

	if !bablmodule.CheckModuleName(ModuleName) {
		log.WithFields(log.Fields{"module": ModuleName}).Fatal("Module name format incorrect")
	}
	module := bablmodule.New(ModuleName)
	module.SetDebug(debug)

	interfaces := "GRPC"
	if len(kafkaBrokers) > 0 {
		interfaces += ",Kafka"

		client := kafka.NewClient(kafkaBrokers, clientID, debug)
		defer (*client).Close()

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
		go listenToMetadata(client)
	}

	log.WithFields(log.Fields{"version": Version, "interfaces": interfaces, "debug": debug}).Warn("Start module")
	startGrpcServer(address, module)
}

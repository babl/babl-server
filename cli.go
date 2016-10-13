package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/larskluge/babl/bablutils"
	"github.com/urfave/cli"
)

func configureCli() (app *cli.App) {
	app = cli.NewApp()
	app.Usage = "Server for a Babl Module"
	app.Version = Version
	app.Action = defaultAction
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "module, m",
			Usage:  "Module to serve",
			EnvVar: "BABL_MODULE",
		},
		cli.StringFlag{
			Name:   "cmd",
			Usage:  "Command to be executed",
			Value:  "cat",
			EnvVar: "BABL_COMMAND",
		},
		cli.StringFlag{
			Name:   "cmd-timeout",
			Usage:  "Max execution duration for command to complete, before its killed",
			Value:  "30s",
			EnvVar: "BABL_COMMAND_TIMEOUT",
		},
		cli.IntFlag{
			Name:   "port",
			Usage:  "Port for server to be started on",
			EnvVar: "PORT",
			Value:  4444,
		},
		cli.StringFlag{
			Name:   "kafka-brokers, kb",
			Usage:  "Comma separated list of kafka brokers",
			EnvVar: "BABL_KAFKA_BROKERS",
		},
		cli.BoolFlag{
			Name:   "kafka-flush",
			Usage:  "For debugging only: error all incoming messages from Kafka to flush the topic",
			EnvVar: "BABL_KAFKA_FLUSH",
		},
		cli.StringFlag{
			Name:   "storage",
			Usage:  "Endpoint for Babl storage",
			EnvVar: "BABL_STORAGE",
			Value:  "babl.sh:4443",
		},
		cli.BoolFlag{
			Name:   "debug",
			Usage:  "Enable debug mode & verbose logging",
			EnvVar: "BABL_DEBUG",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "upgrade",
			Usage: "Upgrades the server to the latest available version",
			Action: func(_ *cli.Context) {
				m := bablutils.NewUpgrade("babl-server", []string{})
				m.Upgrade(Version)
			},
		},
	}
	return
}

func defaultAction(c *cli.Context) error {
	ModuleName = c.String("module")
	if ModuleName == "" {
		cli.ShowAppHelp(c)
		os.Exit(1)
	} else {
		address := fmt.Sprintf(":%d", c.Int("port"))

		command = c.String("cmd")
		var err error
		if CommandTimeout, err = time.ParseDuration(c.String("cmd-timeout")); err != nil {
			panic("cmd-timeout: Command timeout not a valid duration")
		}
		debug = c.GlobalBool("debug")
		StorageEndpoint = c.String("storage")

		kb := c.String("kafka-brokers")
		brokers := []string{}
		if kb != "" {
			brokers = strings.Split(kb, ",")
		}

		KafkaFlush = c.Bool("kafka-flush")

		run(address, brokers)
	}
	return nil
}

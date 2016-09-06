package main

import (
	"fmt"
	"os"
	"strings"

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
				m := bablutils.NewUpgrade("babl-server")
				m.Upgrade(Version)
			},
		},
	}
	return
}

func defaultAction(c *cli.Context) error {
	module := c.String("module")
	if module == "" {
		cli.ShowAppHelp(c)
		os.Exit(1)
	} else {
		address := fmt.Sprintf(":%d", c.Int("port"))

		command = c.String("cmd")
		debug = c.GlobalBool("debug")
		StorageEndpoint = c.String("storage")

		kb := c.String("kafka-brokers")
		brokers := []string{}
		if kb != "" {
			brokers = strings.Split(kb, ",")
		}

		run(module, address, brokers)
	}
	return nil
}

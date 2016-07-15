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
		command = c.String("cmd")
		address := fmt.Sprintf(":%d", c.Int("port"))
		debug := c.GlobalBool("debug")

		kb := c.String("kafka-brokers")
		brokers := []string{}
		if kb != "" {
			brokers = strings.Split(kb, ",")
		}

		run(module, command, address, brokers, debug)
	}
	return nil
}

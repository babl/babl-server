package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func configureCli() (app *cli.App) {
	app = cli.NewApp()
	app.Usage = "Server for a Babl Module"
	app.Version = "0.4.0"
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
			Name:  "kafka-brokers, kb",
			Usage: "Comma separated list of kafka brokers",
			Value: "127.0.0.1:9092",
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
		brokers := c.String("kafka-brokers")
		run(module, command, address, brokers)
	}
	return nil
}

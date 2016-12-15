package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

func scheduleRestart() {
	log.WithFields(log.Fields{"module": ModuleName, "interval": RestartTimeout.String()}).Info("Scheduled Restart Activated")

	time.AfterFunc(RestartTimeout, func() {
		log.WithFields(log.Fields{"module": ModuleName}).Info("Restarting on Schedule")
		GracefulRestart()
	})
}

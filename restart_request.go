package main

import (
	"regexp"

	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
	pb "github.com/larskluge/babl/protobuf/messages"
)

var (
	HostMatch = regexp.MustCompile("^" + Hostname())
	Restart   = false
)

func ShouldRestart() bool {
	return Restart
}

func GracefulRestart() {
	Restart = true
}

func handleRestartRequest(req *pb.RestartRequest) {
	log.WithFields(log.Fields{"instance": req.InstanceId, "hostname": Hostname()}).Info("Restart request received")
	if HostMatch.MatchString(req.InstanceId) {
		log.WithFields(log.Fields{"instance": req.InstanceId, "hostname": Hostname()}).Info("Instance will graceful restart!")
		GracefulRestart()
	}
}

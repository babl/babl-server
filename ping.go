package main

import (
	log "github.com/Sirupsen/logrus"
	pb "github.com/larskluge/babl/protobuf/messages"
)

func handlePingRequest(echo *pb.EchoRequest) {
	log.Error("Ping requested; not implemented") // TODO implement ping reponse
}

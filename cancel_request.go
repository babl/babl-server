package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
	pb "github.com/larskluge/babl/protobuf/messages"
	"github.com/muesli/cache2go"
)

var (
	CancelledRequestsCache = cache2go.Cache("cancelled-requests")
)

func IsRequestCancelled(rid uint64) {
	CancelledRequestsCache.Exists(rid)
}

func handleCancelRequest(req *pb.CancelRequest) {
	log.WithFields(log.Fields{"rid": req.RequestId}).Info("Cancel request received")

	CancelledRequestsCache.Add(req.RequestId, 15*time.Minute, true)
}

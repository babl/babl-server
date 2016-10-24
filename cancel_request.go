package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
	. "github.com/larskluge/babl-server/utils"
	pb "github.com/larskluge/babl/protobuf/messages"
	"github.com/muesli/cache2go"
)

var CancelledRequestsCache = cache2go.Cache("cancelled-requests")

func IsRequestCancelled(rid uint64) bool {
	return CancelledRequestsCache.Exists(rid)
}

func handleCancelRequest(req *pb.CancelRequest) {
	log.WithFields(log.Fields{"rid": FmtRid(req.RequestId)}).Info("Cancel request received")

	CancelledRequestsCache.Add(req.RequestId, 15*time.Minute, true)
}

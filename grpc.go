//go:generate go-bindata data/...

package main

import (
	"crypto/tls"
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl/bablmodule"
	pb "github.com/larskluge/babl/protobuf"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type server struct{}

func startGrpcServer(address string, module *bablmodule.Module) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "address": address}).Fatal("Failed to listen at port")
	}

	certPEMBlock, _ := Asset("data/server.pem")
	keyPEMBlock, _ := Asset("data/server.key")
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		panic(err)
	}
	creds := credentials.NewServerTLSFromCert(&cert)
	opts := []grpc.ServerOption{grpc.Creds(creds)}

	maxMsgSize := 1024 * 1024 * 100 // 100 MB max message size
	opts = append(opts, grpc.MaxMsgSize(maxMsgSize))

	s := grpc.NewServer(opts...)
	pb.RegisterBinaryServer((*module).GrpcServiceName(), s, &server{})
	s.Serve(lis)
}

func (s *server) IO(ctx context.Context, in *pbm.BinRequest) (*pbm.BinReply, error) {
	return IO(in)
}

func (s *server) Ping(ctx context.Context, in *pbm.Empty) (*pbm.Pong, error) {
	return Ping(in)
}

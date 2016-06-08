//go:generate go-bindata data/...

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	pbm "github.com/larskluge/babl/protobuf/messages"
	"github.com/nneves/kafka-tools/bkconsumer"
	"github.com/nneves/kafka-tools/bkproducer"
)

type server struct{}

var command string

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	for {
		key, value := bkconsumer.Consumer("larskluge.image-resize")
		in := &pbm.BinRequest{}
		err := proto.Unmarshal(value, in)
		check(err)
		out, err := IO(in)
		check(err)
		msg, err := proto.Marshal(out)
		check(err)
		topic := "inbox." + key
		bkproducer.Producer(key, topic, msg)
	}
}

func IO(in *pbm.BinRequest) (*pbm.BinReply, error) {
	start := time.Now()

	command = "/bin/cat"
	cmd := exec.Command(command)
	env := os.Environ()
	cmd.Env = []string{} // {"FOO=BAR"}

	vars := []string{}
	for k, v := range in.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		vars = append(vars, k)
	}
	cmd.Env = append(cmd.Env, env...)
	cmd.Env = append(cmd.Env, "BABL_VARS="+strings.Join(vars, ","))

	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("cmd.StdinPipe")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("cmd.StdoutPipe")
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("cmd.StderrPipe")
	}
	err = cmd.Start()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("cmd.Start")
	}

	stdin.Write(in.Stdin)
	stdin.Close()
	outBytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("ioutil.ReadAll[stdout]")
	}
	errBytes, err := ioutil.ReadAll(stderr)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("ioutil.ReadAll[stderr]")
	}

	res := pbm.BinReply{
		Stdout:   outBytes,
		Stderr:   errBytes,
		Exitcode: 0,
	}

	if err := cmd.Wait(); err != nil {
		res.Exitcode = 255
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0

			// This works on both Unix and Windows. Although package
			// syscall is generally platform dependent, WaitStatus is
			// defined for both Unix and Windows and in both cases has
			// an ExitStatus() method with the same signature.
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				res.Exitcode = int32(status.ExitStatus())
			}
		} else {
			log.WithFields(log.Fields{"error": err}).Error("cmd.Wait")
		}
	}

	status := 500
	if res.Exitcode == 0 {
		status = 200
	}

	elapsed := float64(time.Since(start).Seconds() * 1000)

	fields := log.Fields{
		"stdin":       len(in.Stdin),
		"stdout":      len(res.Stdout),
		"stderr":      len(res.Stderr),
		"exitcode":    res.Exitcode,
		"status":      status,
		"duration_ms": elapsed,
	}
	if status != 200 {
		fields["error"] = string(res.Stderr)
	}
	l := log.WithFields(fields)
	if status == 200 {
		l.Info("call")
	} else {
		l.Error("call")
	}

	return &res, nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

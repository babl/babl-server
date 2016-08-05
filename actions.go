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
	pbm "github.com/larskluge/babl/protobuf/messages"
)

func IO(in *pbm.BinRequest) (*pbm.BinReply, error) {
	start := time.Now()
	res := pbm.BinReply{Exitcode: 0}

	done := make(chan bool, 1)
	_, async := in.Env["BABL_ASYNC"]
	if async {
		done <- true
	}

	go func() {
		cmd := exec.Command(command)
		env := os.Environ()
		cmd.Env = []string{} // {"FOO=BAR"}

		audit := in.Env["AUDIT"]
		delete(in.Env, "AUDIT")

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
		res.Stdout, err = ioutil.ReadAll(stdout)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("ioutil.ReadAll(stdout)")
		}
		res.Stderr, err = ioutil.ReadAll(stderr)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("ioutil.ReadAll(stderr)")
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
			"audit":        audit,
			"stdin_bytes":  len(in.Stdin),
			"stdout_bytes": len(res.Stdout),
			"stderr_bytes": len(res.Stderr),
			"stderr":       string(res.Stderr),
			"exitcode":     res.Exitcode,
			"status":       status,
			"duration_ms":  elapsed,
		}
		if async {
			fields["mode"] = "async"
		} else {
			fields["mode"] = "sync"
		}
		l := log.WithFields(fields)
		if status == 200 {
			l.Info("call")
		} else {
			l.Error("call")
		}

		done <- true
	}()

	<-done

	return &res, nil
}

func Ping(in *pbm.Empty) (*pbm.Pong, error) {
	log.Info("ping")
	res := pbm.Pong{Val: "pong"}
	return &res, nil
}

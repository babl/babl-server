package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/goware/prefixer"
	. "github.com/larskluge/babl-server/utils"
	"github.com/larskluge/babl-storage/download"
	"github.com/larskluge/babl-storage/upload"
	pbm "github.com/larskluge/babl/protobuf/messages"
)

func IO(req *pbm.BinRequest, maxReplySize int) (*pbm.BinReply, error) {
	steps := []int{}
	start := time.Now()
	steps = append(steps, 1)
	res := pbm.BinReply{Id: req.Id, Module: req.Module, Exitcode: 0}
	steps = append(steps, 2)

	l := log.WithFields(log.Fields{"rid": FmtRid(req.Id)})
	steps = append(steps, 3)

	done := make(chan bool, 1)
	steps = append(steps, 4)
	_, async := req.Env["BABL_ASYNC"]
	steps = append(steps, 5)
	if async {
		steps = append(steps, 6)
		done <- true
		steps = append(steps, 7)
	}

	go func() {
		steps = append(steps, 8)
		cmd := exec.Command(command)
		steps = append(steps, 9)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		steps = append(steps, 10)
		env := os.Environ()
		steps = append(steps, 11)
		cmd.Env = []string{} // {"FOO=BAR"}
		steps = append(steps, 12)

		vars := []string{}
		steps = append(steps, 13)
		for k, v := range req.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			vars = append(vars, k)
		}
		steps = append(steps, 14)
		cmd.Env = append(cmd.Env, env...)
		steps = append(steps, 15)
		cmd.Env = append(cmd.Env, "BABL_VARS="+strings.Join(vars, ","))
		steps = append(steps, 16)

		payload := req.Stdin
		steps = append(steps, 17)
		if len(payload) <= 0 && req.PayloadUrl != "" {
			steps = append(steps, 18)
			start := time.Now()
			steps = append(steps, 19)
			l.WithFields(log.Fields{"payload_url": req.PayloadUrl}).Info("Downloading external payload")
			steps = append(steps, 20)
			var err error
			payload, err = download.Download(req.PayloadUrl)
			steps = append(steps, 21)
			elapsed := float64(time.Since(start).Seconds() * 1000)
			steps = append(steps, 22)
			l := l.WithFields(log.Fields{"duration_ms": elapsed})
			if err != nil {
				l.WithError(err).Fatal("Payload download failed")
			}
			steps = append(steps, 23)
			l.WithFields(log.Fields{"payload_size": len(payload)}).Info("Payload download successful")
			steps = append(steps, 24)
		}
		stdinBytes := len(payload)
		steps = append(steps, 25)

		stdin, err := cmd.StdinPipe()
		steps = append(steps, 26)
		if err != nil {
			l.WithError(err).Error("cmd.StdinPipe")
		}
		steps = append(steps, 27)
		stdout, err := cmd.StdoutPipe()
		steps = append(steps, 28)
		if err != nil {
			l.WithError(err).Error("cmd.StdoutPipe")
		}
		steps = append(steps, 29)
		stderr, err := cmd.StderrPipe()
		steps = append(steps, 30)
		if err != nil {
			l.WithError(err).Error("cmd.StderrPipe")
		}
		steps = append(steps, 31)

		var stderrBuf bytes.Buffer
		// FIXME: prefixer breaks realtime reading of stderr
		stderrCopy := io.TeeReader(prefixer.New(stderr, ModuleName+": "), &stderrBuf)
		steps = append(steps, 32)

		steps = append(steps, 33)
		stderrCopied := make(chan bool, 1)
		steps = append(steps, 34)
		go func() {
			steps = append(steps, 35)
			req := bufio.NewScanner(stderrCopy)
			steps = append(steps, 36)
			for req.Scan() {
				l.Debug(req.Text())
			}
			steps = append(steps, 37)
			if err := req.Err(); err != nil {
				l.WithError(err).Warn("Copy module exec stderr stream to logs failed")
			}
			steps = append(steps, 38)
			stderrCopied <- true
			steps = append(steps, 39)
		}()

		steps = append(steps, 40)

		// write to stdin non-blocking so external process can start consuming data
		// before buffer is full and everything blocks up
		go func() {
			steps = append(steps, 41)
			stdin.Write(payload)
			steps = append(steps, 42)
			payload = nil
			steps = append(steps, 43)
			stdin.Close()
			steps = append(steps, 44)
		}()

		steps = append(steps, 45)
		err = cmd.Start()
		if err != nil {
			l.WithError(err).Error("cmd.Start")
		}

		steps = append(steps, 46)
		timer := time.AfterFunc(CommandTimeout, func() {
			steps = append(steps, 47)
			l.Errorf("Process calculation timed out after %s, killing process group", CommandTimeout)
			pgid, err := syscall.Getpgid(cmd.Process.Pid)
			steps = append(steps, 48)
			if err == nil {
				steps = append(steps, 49)
				syscall.Kill(-pgid, 15) // note the minus sign
			}
			// cmd.Process.Kill()
		})

		steps = append(steps, 50)
		res.Stdout, err = ioutil.ReadAll(stdout)
		steps = append(steps, 51)
		if err != nil {
			l.WithError(err).Error("ioutil.ReadAll(stdout)")
		}
		steps = append(steps, 52)
		<-stderrCopied
		steps = append(steps, 53)
		res.Stderr = stderrBuf.Bytes()
		steps = append(steps, 54)

		if err := cmd.Wait(); err != nil {
			steps = append(steps, 55)
			res.Exitcode = 255
			steps = append(steps, 56)
			if exiterr, ok := err.(*exec.ExitError); ok {
				steps = append(steps, 57)
				// The program has exited with an exit code != 0

				// This works on both Unix and Windows. Although package
				// syscall is generally platform dependent, WaitStatus is
				// defined for both Unix and Windows and in both cases has
				// an ExitStatus() method with the same signature.
				if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
					steps = append(steps, 58)
					res.Exitcode = int32(status.ExitStatus())
				}
			} else {
				steps = append(steps, 59)
				l.WithError(err).Error("cmd.Wait")
			}
		}

		steps = append(steps, 60)
		timer.Stop()
		steps = append(steps, 61)

		stdoutBytes := len(res.Stdout)
		steps = append(steps, 62)
		if len(res.Stdout) > maxReplySize {
			steps = append(steps, 63)
			start := time.Now()
			steps = append(steps, 64)
			up, err := upload.New(StorageEndpoint, bytes.NewReader(res.Stdout))
			steps = append(steps, 65)
			l := l.WithFields(log.Fields{"blob_url": up.Url})
			steps = append(steps, 66)
			if err != nil {
				l.WithError(err).Fatal("Payload upload failed")
			}
			steps = append(steps, 67)
			go func() {
				steps = append(steps, 68)
				up.WaitForCompletion()
				steps = append(steps, 69)
				elapsed := float64(time.Since(start).Seconds() * 1000)
				steps = append(steps, 70)
				l.WithFields(log.Fields{"duration_ms": elapsed}).Info("Payload upload done")
				steps = append(steps, 71)
			}()
			steps = append(steps, 72)
			res.Stdout = []byte{}
			res.PayloadUrl = up.Url
			steps = append(steps, 73)
		}

		steps = append(steps, 74)
		status := 500
		if res.Exitcode == 0 {
			status = 200
		}

		steps = append(steps, 75)
		elapsed := float64(time.Since(start).Seconds() * 1000)

		steps = append(steps, 76)
		fields := log.Fields{
			"stdin_bytes":  stdinBytes,
			"stdout_bytes": stdoutBytes,
			"stderr_bytes": len(res.Stderr),
			"stderr":       string(res.Stderr),
			"exitcode":     res.Exitcode,
			"status":       status,
			"duration_ms":  elapsed,
		}
		steps = append(steps, 77)
		if async {
			fields["mode"] = "async"
		} else {
			fields["mode"] = "sync"
		}
		steps = append(steps, 78)
		l = l.WithFields(fields)
		if status == 200 {
			l.Info("call")
		} else {
			l.Error("call")
		}

		steps = append(steps, 79)
		done <- true
		steps = append(steps, 80)
	}()

	steps = append(steps, 81)
	select {
	case <-done:
	case <-time.After(CommandTimeout + 15*time.Second):
		l.WithFields(log.Fields{"steps": steps}).Fatal("Something went terribly wrong :)")
	}
	steps = append(steps, 82)

	defer func() {
		steps = append(steps, 99)
	}()
	return &res, nil
}

func Ping(in *pbm.Empty) (*pbm.Pong, error) {
	log.Info("ping")
	res := pbm.Pong{Val: "pong"}
	return &res, nil
}

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
	start := time.Now()
	res := pbm.BinReply{Id: req.Id, Module: req.Module, Exitcode: 0}

	l := log.WithFields(log.Fields{"rid": FmtRid(req.Id)})

	done := make(chan bool, 1)
	_, async := req.Env["BABL_ASYNC"]
	if async {
		done <- true
	}

	go func() {
		cmd := exec.Command(command)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		env := os.Environ()
		cmd.Env = []string{} // {"FOO=BAR"}
		vars := []string{}
		for k, v := range req.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			vars = append(vars, k)
		}
		cmd.Env = append(cmd.Env, env...)
		cmd.Env = append(cmd.Env, "BABL_VARS="+strings.Join(vars, ","))

		payload := req.Stdin
		if len(payload) <= 0 && req.PayloadUrl != "" {
			start := time.Now()
			l.WithFields(log.Fields{"payload_url": req.PayloadUrl, "code": "req-downloading"}).Info("Downloading external payload")
			var err error
			payload, err = download.Download(req.PayloadUrl)
			elapsed := float64(time.Since(start).Seconds() * 1000)
			l := l.WithFields(log.Fields{"duration_ms": elapsed})
			if err != nil {
				l.WithError(err).Fatal("Payload download failed")
			}
			l.WithFields(log.Fields{"payload_size": len(payload), "code": "req-downloaded"}).Info("Payload download successful")
		}
		stdinBytes := len(payload)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			l.WithError(err).Error("cmd.StdinPipe")
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			l.WithError(err).Error("cmd.StdoutPipe")
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			l.WithError(err).Error("cmd.StderrPipe")
		}

		var stderrBuf bytes.Buffer
		// FIXME: prefixer breaks realtime reading of stderr
		stderrCopy := io.TeeReader(prefixer.New(stderr, ModuleName+": "), &stderrBuf)
		stderrCopied := make(chan bool, 1)
		go func() {
			req := bufio.NewScanner(stderrCopy)
			for req.Scan() {
				l.Debug(req.Text())
			}
			if err := req.Err(); err != nil {
				l.WithError(err).Warn("Copy module exec stderr stream to logs failed")
			}
			stderrCopied <- true
		}()
		// write to stdin non-blocking so external process can start consuming data
		// before buffer is full and everything blocks up
		go func() {
			stdin.Write(payload)
			payload = nil
			stdin.Close()
		}()

		err = cmd.Start()
		if err != nil {
			l.WithError(err).Error("cmd.Start")
		}

		timer := time.AfterFunc(CommandTimeout, func() {
			res.Status = pbm.BinReply_EXECUTION_TIMEOUT
			res.Error = fmt.Sprintf("Process calculation timed out after %s, killing process group", CommandTimeout)
			l.Error(res.Error)
			pgid, err := syscall.Getpgid(cmd.Process.Pid)
			if err == nil {
				syscall.Kill(-pgid, 15) // note the minus sign
			}
			// cmd.Process.Kill()
		})

		res.Stdout, err = ioutil.ReadAll(stdout)
		if err != nil {
			l.WithError(err).Error("ioutil.ReadAll(stdout)")
		}
		<-stderrCopied
		res.Stderr = stderrBuf.Bytes()

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
				l.WithError(err).Error("cmd.Wait")
			}
		}

		timer.Stop()
		stdoutBytes := len(res.Stdout)
		if len(res.Stdout) > maxReplySize {
			start := time.Now()
			l.WithFields(log.Fields{"payload_size": stdoutBytes, "code": "reply-uploading"}).Info("Uploading payload")
			up, err := upload.New(StorageEndpoint, bytes.NewReader(res.Stdout))
			l := l.WithFields(log.Fields{"blob_url": up.Url})
			if err != nil {
				l.WithError(err).Fatal("Payload upload failed")
			}
			go func() {
				up.WaitForCompletion()
				elapsed := float64(time.Since(start).Seconds() * 1000)
				l.WithFields(log.Fields{"duration_ms": elapsed, "payload_size": stdoutBytes, "code": "reply-uploaded"}).Info("Payload upload done")
			}()
			res.Stdout = []byte{}
			res.PayloadUrl = up.Url
		}

		status := 500
		if res.Exitcode == 0 {
			status = 200
		} else {
			if res.Status != pbm.BinReply_EXECUTION_TIMEOUT {
				res.Status = pbm.BinReply_ERROR
			}
		}

		elapsed := float64(time.Since(start).Seconds() * 1000)
		fields := log.Fields{
			"stdin_bytes":  stdinBytes,
			"stdout_bytes": stdoutBytes,
			"stderr_bytes": len(res.Stderr),
			"stderr":       string(res.Stderr),
			"exitcode":     res.Exitcode,
			"status":       status,
			"duration_ms":  elapsed,
			"code":         "req-executed",
		}
		if async {
			fields["mode"] = "async"
		} else {
			fields["mode"] = "sync"
		}
		l = l.WithFields(fields)
		if status == 200 {
			l.Info("call")
		} else {
			l.Error("call")
		}
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(CommandTimeout + 15*time.Second):
		l.Fatal("Something went terribly wrong")
	}
	return &res, nil
}

func Ping(in *pbm.Empty) (*pbm.Pong, error) {
	log.Info("ping")
	res := pbm.Pong{Val: "pong"}
	return &res, nil
}

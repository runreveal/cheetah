package journald

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"golang.org/x/exp/slog"
)

type Journald struct {
	msgC chan kawa.MsgAck[types.Event]
}

func New() *Journald {
	return &Journald{
		msgC: make(chan kawa.MsgAck[types.Event]),
	}
}

func (s *Journald) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *Journald) recvLoop(ctx context.Context) error {
	// Open file to check and save high watermark
	hwmFile, err := os.OpenFile("/tmp/kawad-journald-hwm", os.O_RDWR|os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return err
	}
	defer hwmFile.Close()

	// Read high watermark from file
	bts, err := io.ReadAll(hwmFile)
	if err != nil {
		return err
	}

	// Save high watermark to file
	ack := func(cursor string) {
		var err error
		defer func() {
			if err != nil {
				slog.Error(fmt.Sprintf("writing high watermark: %+v", err))
			}
		}()
		err = hwmFile.Truncate(0)
		if err != nil {
			return
		}
		_, err = hwmFile.Seek(0, 0)
		if err != nil {
			return
		}
		_, err = hwmFile.WriteString(cursor)
		if err != nil {
			return
		}
	}

	args := []string{
		"journalctl", "-b", "-af", "-o", "json",
	}
	if len(bts) > 0 {
		// Resume reading from the location of a previous invocation
		args = append(args, "--after-cursor", string(bts))
	} else {
		// Read all logs for this boot
		args = append(args, "--since", "1970-01-01 00:00:00")
	}
	slog.Debug(fmt.Sprintf("running: `%s`", strings.Join(args, " ")))

	cmd := exec.Command(args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	var wg sync.WaitGroup

	slog.Info("reading journald")

loop:
	for scanner.Scan() {
		bts := make([]byte, len(scanner.Bytes()))
		copy(bts, scanner.Bytes())

		// Parse timestamp from log
		log := autoGeneratedJournal{}
		var ts time.Time
		if err := json.Unmarshal(bts, &log); err != nil {
			slog.Error(fmt.Sprintf("unmarshaling: %+v", err))
			continue
		} else {
			ts, err = parseUnixMicroseconds(log.RealtimeTimestamp)
			if err != nil {
				slog.Error(fmt.Sprintf("parsing timestamp: %+v", err))
			}
		}

		wg.Add(1)
		select {
		case s.msgC <- kawa.MsgAck[types.Event]{
			Msg: kawa.Message[types.Event]{
				Value: types.Event{
					Timestamp:  ts,
					SourceType: "journald",
					RawLog:     bts,
				},
			},
			Ack: func() {
				ack(log.Cursor)
				wg.Done()
			},
		}:
		case <-ctx.Done():
			break loop
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning: %+w", err)
	}

	slog.Info("waiting for journald to exit")

	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	// We've received all the logs
	case <-c:
	case <-ctx.Done():
		return ctx.Err()
	}
	return cmd.Wait()
}

func (s *Journald) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.Msg, pass.Ack, nil
	}
}

func parseUnixMicroseconds(s string) (time.Time, error) {
	microseconds, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	// Convert microseconds to seconds and remainder microseconds
	sec := microseconds / 1e6
	nsec := (microseconds % 1e6) * 1e3

	// Create a new time.Time value
	return time.Unix(sec, nsec), nil
}

type journalMsg []byte

func (jm *journalMsg) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return errors.New("unexpected end of JSON input for journalMsg")
	}
	var err error
	switch b[0] {
	case '"':
		var s string
		err = json.Unmarshal(b, &s)
		if err != nil {
			return err
		}
		*jm = []byte(s)
	case '[':
		var bts []byte
		err = json.Unmarshal(b, &bts)
		if err != nil {
			return err
		}
		*jm = bts
	default:
		err = fmt.Errorf("unexpected character in journalMsg: %s. expecting string or list", string(b[0]))
	}
	return err
}

// There are other fields, but these should be on just about every journald event
type autoGeneratedJournal struct {
	Message journalMsg `json:"MESSAGE"`
	// Unix Timestamp in Microseconds since epoch as string
	RealtimeTimestamp string `json:"__REALTIME_TIMESTAMP"`
	SyslogIdentifier  string `json:"SYSLOG_IDENTIFIER"`
	Hostname          string `json:"_HOSTNAME"`
	Cursor            string `json:"__CURSOR"`

	// BootID             string `json:"_BOOT_ID"`
	// CapEffective       string `json:"_CAP_EFFECTIVE"`
	// Cmdline            string `json:"_CMDLINE"`
	// Comm               string `json:"_COMM"`
	// Exe                string `json:"_EXE"`
	// Gid                string `json:"_GID"`
	// MachineID          string `json:"_MACHINE_ID"`
	// MonotonicTimestamp string `json:"__MONOTONIC_TIMESTAMP"`
	// Pid                string `json:"_PID"`
	// Priority           string `json:"PRIORITY"`
	// SelinuxContext     string `json:"_SELINUX_CONTEXT"`
	// SyslogFacility     string `json:"SYSLOG_FACILITY"`
	// SystemdCgroup      string `json:"_SYSTEMD_CGROUP"`
	// SystemdSlice       string `json:"_SYSTEMD_SLICE"`
	// SystemdUnit        string `json:"_SYSTEMD_UNIT"`
	// Transport          string `json:"_TRANSPORT"`
	// UID                string `json:"_UID"`
}

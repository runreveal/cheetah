//go:build windows
// +build windows

package main

import (
	"github.com/runreveal/flow"
	"github.com/runreveal/flow/internal/sources/windows"
	"github.com/runreveal/flow/internal/types"
	"github.com/runreveal/lib/loader"
	"golang.org/x/exp/slog"
	// We could register and configure these in a separate package
	// using the init() function.
	// That would make it easy to "dynamically" enable and disable them at
	// compile time since it would simply be updating the import list.
)

func init() {
	loader.Register("eventlog", func() loader.Builder[flow.Source[types.Event]] {
		return &EventLogConfig{}
	})
}

type EventLogConfig struct {
	Channel string `json:"channel"`
	Query   string `json:"query"`
}

func (c *EventLogConfig) Configure() (flow.Source[types.Event], error) {
	slog.Info("configuring event log")
	return windows.NewEventLogSource(windows.EventLogCfg{
		Channel: c.Channel,
		Query:   c.Query,
	}), nil
}

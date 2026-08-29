package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// NonTTYReporter emits periodic single-line log messages for non-interactive environments.
type NonTTYReporter struct {
	mu          sync.Mutex
	out         io.Writer
	interval    time.Duration
	lastLogTime time.Time
}

// NewNonTTYReporter creates a new NonTTYReporter with the specified update interval.
func NewNonTTYReporter(out io.Writer, interval time.Duration) *NonTTYReporter {
	if out == nil {
		out = io.Discard
	}
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &NonTTYReporter{
		out:      out,
		interval: interval,
	}
}

// OnEvent formats and logs a progress event if interval has elapsed or if it is a finish event.
func (r *NonTTYReporter) OnEvent(ev ProgressEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if ev.Type == EventFinish || now.Sub(r.lastLogTime) >= r.interval {
		r.lastLogTime = now
		phase := strings.ToUpper(ev.Phase)
		if phase == "" {
			phase = "TASK"
		}

		if ev.Total > 0 {
			pct := float64(ev.Current) / float64(ev.Total) * 100.0
			fmt.Fprintf(r.out, "[%s] [%s] %d/%d (%4.1f%%) | %.1f p/s | Queue: %d | Saved: %s tokens\n",
				now.Format("15:04:05"), phase, ev.Current, ev.Total, pct, ev.Speed, ev.QueueDepth, FormatNumber(ev.TokensSaved),
			)
		} else {
			// Indeterminate formatting: (N items)
			fmt.Fprintf(r.out, "[%s] [%s] %d items | %.1f p/s | Queue: %d | Saved: %s tokens\n",
				now.Format("15:04:05"), phase, ev.Current, ev.Speed, ev.QueueDepth, FormatNumber(ev.TokensSaved),
			)
		}
	}
}

// NDJSONReporter encodes progress events into newline-delimited JSON streams.
type NDJSONReporter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewNDJSONReporter creates a new NDJSONReporter.
func NewNDJSONReporter(out io.Writer) *NDJSONReporter {
	if out == nil {
		out = io.Discard
	}
	return &NDJSONReporter{
		enc: json.NewEncoder(out),
	}
}

// OnEvent encodes the progress event to JSON with trailing newline.
func (r *NDJSONReporter) OnEvent(ev ProgressEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enc.Encode(ev)
}

package ui

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNonTTYReporter_PeriodicLog(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewNonTTYReporter(&buf, 10*time.Millisecond)

	ev1 := ProgressEvent{
		Type:        EventProgress,
		Phase:       "crawling",
		Current:     50,
		Total:       100,
		Speed:       12.5,
		QueueDepth:  20,
		TokensSaved: 1000,
	}
	reporter.OnEvent(ev1)

	// Sleep slightly to trigger interval
	time.Sleep(15 * time.Millisecond)

	ev2 := ProgressEvent{
		Type:        EventProgress,
		Phase:       "crawling",
		Current:     75,
		Total:       100,
		Speed:       15.0,
		QueueDepth:  10,
		TokensSaved: 2000,
	}
	reporter.OnEvent(ev2)

	out := buf.String()
	if !strings.Contains(out, "50/100") && !strings.Contains(out, "75/100") {
		t.Fatalf("expected progress logs, got: %s", out)
	}
}

func TestNDJSONReporter(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewNDJSONReporter(&buf)

	ev := ProgressEvent{
		JobID:       "job-123",
		Type:        EventProgress,
		Phase:       "crawling",
		Current:     10,
		Total:       50,
		Speed:       5.2,
		QueueDepth:  15,
		TokensSaved: 500,
	}

	err := reporter.OnEvent(ev)
	if err != nil {
		t.Fatalf("unexpected error encoding NDJSON: %v", err)
	}

	var decoded ProgressEvent
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("failed to decode emitted JSON: %v", err)
	}

	if decoded.JobID != "job-123" || decoded.Current != 10 || decoded.Total != 50 {
		t.Fatalf("mismatched decoded event: %+v", decoded)
	}
}

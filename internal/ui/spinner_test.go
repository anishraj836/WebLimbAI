package ui

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestSpinner_Lifecycle(t *testing.T) {
	var buf bytes.Buffer
	sp := NewSpinner(&buf, "Fetching page...")
	sp.Start()
	time.Sleep(50 * time.Millisecond)
	sp.Update("Extracting markdown...")
	time.Sleep(50 * time.Millisecond)
	sp.Success("Extracted 2,140 tokens")

	out := buf.String()
	if !strings.Contains(out, "Fetching page...") {
		t.Errorf("expected initial message, got: %s", out)
	}
	if !strings.Contains(out, "Extracted 2,140 tokens") {
		t.Errorf("expected success message, got: %s", out)
	}
}

func TestSpinner_Error(t *testing.T) {
	var buf bytes.Buffer
	sp := NewSpinner(&buf, "Fetching page...")
	sp.Start()
	time.Sleep(20 * time.Millisecond)
	sp.Error("Failed to fetch page")

	out := buf.String()
	if !strings.Contains(out, "Failed to fetch page") {
		t.Errorf("expected error message, got: %s", out)
	}
}

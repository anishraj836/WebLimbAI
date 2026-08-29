package ui

import (
	"testing"
	"time"
)

func TestStripANSI(t *testing.T) {
	colored := "\033[32m\033[1mSuccess\033[0m \033[34mProcessing\033[0m"
	plain := StripANSI(colored)
	if plain != "Success Processing" {
		t.Fatalf("expected 'Success Processing', got %q", plain)
	}
}

func TestVisibleLen(t *testing.T) {
	colored := "\033[36m=== Crawler Job ===\033[0m"
	length := VisibleLen(colored)
	if length != 19 {
		t.Fatalf("expected visible length 19, got %d", length)
	}
}

func TestTruncateMiddle(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		ellipsis string
		expected string
	}{
		{"https://example.com/very/long/path/to/resource.html", 25, "...", "https://ex...source.html"},
		{"short", 10, "...", "short"},
		{"exact_length", 12, "...", "exact_length"},
		{"abcdef", 4, "...", "a..."},
		{"", 5, "...", ""},
	}

	for _, tt := range tests {
		result := TruncateMiddle(tt.input, tt.maxLen, tt.ellipsis)
		if len([]rune(result)) > tt.maxLen {
			t.Errorf("TruncateMiddle(%q, %d) exceeded maxLen: %q (len %d)", tt.input, tt.maxLen, result, len([]rune(result)))
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
	}

	for _, tt := range tests {
		result := FormatBytes(tt.bytes)
		if result != tt.expected {
			t.Errorf("FormatBytes(%d) = %q, expected %q", tt.bytes, result, tt.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d        time.Duration
		expected string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "500ms"},
		{14200 * time.Millisecond, "14.2s"},
		{65 * time.Second, "1m05s"},
		{3665 * time.Second, "1h01m05s"},
	}

	for _, tt := range tests {
		result := FormatDuration(tt.d)
		if result != tt.expected {
			t.Errorf("FormatDuration(%v) = %q, expected %q", tt.d, result, tt.expected)
		}
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		n        int64
		expected string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{684200, "684,200"},
		{123456789, "123,456,789"},
		{-5000, "-5,000"},
	}

	for _, tt := range tests {
		result := FormatNumber(tt.n)
		if result != tt.expected {
			t.Errorf("FormatNumber(%d) = %q, expected %q", tt.n, result, tt.expected)
		}
	}
}

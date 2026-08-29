//go:build windows

package ui

import (
	"os"
)

// GetTerminalDimensions queries columns and rows on Windows.
func GetTerminalDimensions(fallbackWidth, fallbackHeight int) (int, int) {
	if fallbackWidth <= 0 {
		fallbackWidth = 80
	}
	if fallbackHeight <= 0 {
		fallbackHeight = 24
	}
	// Fallback to standard dimensions on Windows console
	return fallbackWidth, fallbackHeight
}

// WatchTerminalResize is a no-op on Windows to prevent undefined syscall.SIGWINCH errors.
func WatchTerminalResize(sigChan chan<- os.Signal) {
	// No-op on Windows
}

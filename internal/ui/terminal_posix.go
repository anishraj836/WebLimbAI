//go:build !windows

package ui

import (
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"
)

// GetTerminalDimensions queries columns and rows on POSIX systems via ioctl.
func GetTerminalDimensions(fallbackWidth, fallbackHeight int) (int, int) {
	if fallbackWidth <= 0 {
		fallbackWidth = 80
	}
	if fallbackHeight <= 0 {
		fallbackHeight = 24
	}

	ws, err := unix.IoctlGetWinsize(int(os.Stdout.Fd()), unix.TIOCGWINSZ)
	if err == nil && ws.Col > 0 && ws.Row > 0 {
		return int(ws.Col), int(ws.Row)
	}
	return fallbackWidth, fallbackHeight
}

// WatchTerminalResize registers a channel for SIGWINCH window resize events on POSIX.
func WatchTerminalResize(sigChan chan<- os.Signal) {
	signal.Notify(sigChan, syscall.SIGWINCH)
}

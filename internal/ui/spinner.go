package ui

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Spinner manages a lightweight Braille spinner for single-operation tasks (e.g. scrape, search).
type Spinner struct {
	out          io.Writer
	message      string
	mu           sync.Mutex
	ticker       *time.Ticker
	stopChan     chan struct{}
	doneChan     chan struct{}
	stopOnce     sync.Once
	active       bool
	isTTY        bool
	enableColors bool
	frameIdx     int
}

// NewSpinner creates a new Spinner instance.
func NewSpinner(out io.Writer, initialMessage string) *Spinner {
	if out == nil {
		out = os.Stdout
	}
	isTTY := false
	if f, ok := out.(*os.File); ok {
		isTTY = IsTTY(f)
	}
	enableColors := isTTY && os.Getenv("NO_COLOR") == ""

	return &Spinner{
		out:          out,
		message:      initialMessage,
		stopChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
		isTTY:        isTTY,
		enableColors: enableColors,
	}
}

// Start begins spinning the Braille frames on an 80ms interval.
func (s *Spinner) Start() {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return
	}
	s.active = true

	if !s.isTTY {
		// Non-TTY fallback: print single initial message
		if s.message != "" {
			fmt.Fprintf(s.out, "-> %s\n", s.message)
		}
		s.mu.Unlock()
		return
	}

	fmt.Fprint(s.out, CursorHide)
	s.ticker = time.NewTicker(80 * time.Millisecond)
	s.mu.Unlock()

	go func() {
		defer close(s.doneChan)
		defer func() {
			if s.isTTY {
				fmt.Fprint(s.out, CursorShow)
			}
		}()

		for {
			select {
			case <-s.stopChan:
				return
			case <-s.ticker.C:
				s.mu.Lock()
				if !s.active {
					s.mu.Unlock()
					return
				}
				frame := BrailleSpinnerFrames[s.frameIdx%len(BrailleSpinnerFrames)]
				s.frameIdx++

				cCyan := ""
				cReset := ""
				if s.enableColors {
					cCyan = FgCyan
					cReset = Reset
				}

				fmt.Fprintf(s.out, "%s%s%s%s%s %s", CarriageReturn, ClearLine, cCyan, frame, cReset, s.message)
				s.mu.Unlock()
			}
		}
	}()
}

// Update changes the spinner's displayed message.
func (s *Spinner) Update(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.message = msg
	if !s.isTTY {
		fmt.Fprintf(s.out, "-> %s\n", msg)
	}
}

// Success stops the spinner and prints a green checkmark success message.
func (s *Spinner) Success(msg string) {
	s.stopWithStatus(IconSuccess, FgGreen, msg)
}

// Error stops the spinner and prints a red cross error message.
func (s *Spinner) Error(msg string) {
	s.stopWithStatus(IconError, FgRed, msg)
}

func (s *Spinner) stopWithStatus(icon, colorCode, msg string) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.active = false
		if s.ticker != nil {
			s.ticker.Stop()
		}
		close(s.stopChan)
		s.mu.Unlock()

		if s.isTTY {
			<-s.doneChan
		}

		cColor := ""
		cReset := ""
		if s.enableColors {
			cColor = colorCode
			cReset = Reset
		}

		if msg == "" {
			msg = s.message
		}

		if s.isTTY {
			fmt.Fprintf(s.out, "%s%s%s%s%s %s\n", CarriageReturn, ClearLine, cColor, icon, cReset, msg)
		} else {
			fmt.Fprintf(s.out, "%s %s\n", icon, msg)
		}
	})
}

// Stop cleanly terminates the spinner without an icon message.
func (s *Spinner) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.active = false
		if s.ticker != nil {
			s.ticker.Stop()
		}
		close(s.stopChan)
		s.mu.Unlock()

		if s.isTTY {
			<-s.doneChan
			fmt.Fprint(s.out, CarriageReturn+ClearLine+CursorShow)
		}
	})
}

package ui

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mattn/go-isatty"
)

// IsTTY returns true if the specified file descriptor is an interactive terminal.
func IsTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

// ShouldUseInteractiveUI determines if rich ANSI progress rendering should be enabled.
func ShouldUseInteractiveUI(w io.Writer, noProgress, quiet bool) bool {
	if quiet || noProgress {
		return false
	}
	if os.Getenv("TERM") == "dumb" || os.Getenv("CI") == "true" || os.Getenv("NO_COLOR") != "" {
		return false
	}
	if f, ok := w.(*os.File); ok {
		return IsTTY(f)
	}
	return false
}

// ANSI Escape Sequences
const (
	CursorHide     = "\033[?25l"
	CursorShow     = "\033[?25h"
	ClearLine      = "\033[2K"
	ClearToEnd     = "\033[K"
	CarriageReturn = "\r"
	CursorUpFmt    = "\033[%dA"
	CursorDownFmt  = "\033[%dB"

	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Underline = "\033[4m"

	FgBlack   = "\033[30m"
	FgRed     = "\033[31m"
	FgGreen   = "\033[32m"
	FgYellow  = "\033[33m"
	FgBlue    = "\033[34m"
	FgMagenta = "\033[35m"
	FgCyan    = "\033[36m"
	FgWhite   = "\033[37m"
	FgGray    = "\033[90m"
)

// Unicode & Status Icons
const (
	IconSuccess = "✓"
	IconCached  = "⚡"
	IconError   = "✗"
	IconFolder  = "📁"
	IconSearch  = "🔍"
	IconBrain   = "🧠"
	IconDisk    = "💾"
)

var (
	BrailleSpinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	BlockFull            = "█"
	BlockEmpty           = "░"
	ansiRegex            = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
)

// StripANSI strips all ANSI escape sequences to calculate exact visible terminal width.
func StripANSI(str string) string {
	return ansiRegex.ReplaceAllString(str, "")
}

// VisibleLen calculates visible rune width of string stripped of ANSI codes.
func VisibleLen(str string) int {
	return utf8.RuneCountInString(StripANSI(str))
}

// TruncateMiddle truncates a long string to maxLen with a middle ellipsis, respecting width.
func TruncateMiddle(s string, maxLen int, ellipsis string) string {
	if maxLen <= 0 {
		return ""
	}
	sLen := utf8.RuneCountInString(s)
	if sLen <= maxLen {
		return s
	}
	if ellipsis == "" {
		ellipsis = "..."
	}
	eLen := utf8.RuneCountInString(ellipsis)
	if maxLen <= eLen {
		runes := []rune(s)
		return string(runes[:maxLen])
	}

	avail := maxLen - eLen
	half := avail / 2
	runes := []rune(s)
	left := string(runes[:half])
	right := string(runes[sLen-(avail-half):])
	return left + ellipsis + right
}

// FormatBytes converts raw byte count into human-readable representation.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatDuration converts a duration into concise human-readable time (e.g. 1h2m, 14.2s, 128ms).
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%02ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh%02dm%02ds", hours, mins, secs)
}

// FormatNumber adds thousands separators to integer values.
func FormatNumber(n int64) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return sign + str
	}

	var sb strings.Builder
	sb.WriteString(sign)
	rem := len(str) % 3
	if rem > 0 {
		sb.WriteString(str[:rem])
		if len(str) > rem {
			sb.WriteString(",")
		}
	}
	for i := rem; i < len(str); i += 3 {
		sb.WriteString(str[i : i+3])
		if i+3 < len(str) {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

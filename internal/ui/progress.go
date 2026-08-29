package ui

import (
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ProgressConfig configures the composite terminal progress bar display.
type ProgressConfig struct {
	Writer         io.Writer
	Title          string
	Total          int64
	RefreshRate    time.Duration // Default: 50ms (20 FPS)
	ShowSpeed      bool
	ShowETA        bool
	ShowQueue      bool
	ShowTokens     bool
	ShowBytes      bool
	ShowActiveItem bool
	EnableColors   bool
}

// CompositeProgress manages live multi-line terminal rendering with EMA speed smoothing and sub-second ETA.
type CompositeProgress struct {
	cfg       ProgressConfig
	out       io.Writer
	startTime time.Time
	ticker    *time.Ticker
	stopChan  chan struct{}
	doneChan  chan struct{}
	stopOnce  sync.Once
	eventChan chan ProgressEvent

	mu            sync.RWMutex
	current       int64
	total         int64
	bytesRead     int64
	cacheHits     int64
	errorsCount   int64
	tokensSaved   int64
	queueDepth    int64
	activeItem    string
	status        string
	phase         string
	spinnerIdx    int
	pulseIdx      int
	renderedLines int
	finished      bool

	// EMA Speed smoothing
	lastCurrent    int64
	lastBytes      int64
	lastSampleTime time.Time
	speedEMA       float64
	bytesSpeedEMA  float64
	alpha          float64
}

// NewCompositeProgress initializes a new composite progress bar renderer.
func NewCompositeProgress(cfg ProgressConfig) *CompositeProgress {
	if cfg.Writer == nil {
		cfg.Writer = os.Stdout
	}
	if cfg.RefreshRate <= 0 {
		cfg.RefreshRate = 50 * time.Millisecond
	}
	if cfg.EnableColors && os.Getenv("NO_COLOR") != "" {
		cfg.EnableColors = false
	}

	return &CompositeProgress{
		cfg:            cfg,
		out:            cfg.Writer,
		total:          cfg.Total,
		phase:          "crawling",
		stopChan:       make(chan struct{}),
		doneChan:       make(chan struct{}),
		eventChan:      make(chan ProgressEvent, 512),
		alpha:          0.20,
		startTime:      time.Now(),
		lastSampleTime: time.Now(),
	}
}

// Start begins the rate-limited rendering loop and installs signal hooks.
func (p *CompositeProgress) Start() {
	p.startTime = time.Now()
	p.lastSampleTime = p.startTime
	p.ticker = time.NewTicker(p.cfg.RefreshRate)

	if f, ok := p.out.(*os.File); ok && IsTTY(f) {
		fmt.Fprint(p.out, CursorHide)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	WatchTerminalResize(sigChan) // Platform-specific SIGWINCH registration

	go func() {
		defer close(p.doneChan)
		defer func() {
			signal.Stop(sigChan)
			if f, ok := p.out.(*os.File); ok && IsTTY(f) {
				fmt.Fprint(p.out, CursorShow)
			}
		}()

		for {
			select {
			case <-p.stopChan:
				p.render(true)
				return
			case sig := <-sigChan:
				if sig == os.Interrupt || sig == syscall.SIGTERM {
					if f, ok := p.out.(*os.File); ok && IsTTY(f) {
						fmt.Fprint(p.out, CursorShow+"\n")
					}
					return
				}
				// Window resize (SIGWINCH): reset renderedLines for clean frame redraw
				p.mu.Lock()
				p.renderedLines = 0
				p.mu.Unlock()
				p.render(false)
			case ev, ok := <-p.eventChan:
				if !ok {
					return
				}
				p.handleEvent(ev)
			case <-p.ticker.C:
				p.updateMath()
				p.render(false)
			}
		}
	}()
}

// Stop idempotently halts rendering, renders final state, and restores cursor visibility.
func (p *CompositeProgress) Stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.finished = true
		p.mu.Unlock()
		if p.ticker != nil {
			p.ticker.Stop()
		}
		close(p.stopChan)
		<-p.doneChan
	})
}

// Send enqueues a progress event for rendering.
func (p *CompositeProgress) Send(ev ProgressEvent) {
	select {
	case p.eventChan <- ev:
	default:
		// Non-blocking drop to prevent producer stall
	}
}

// SetTotal updates the total expected items.
func (p *CompositeProgress) SetTotal(total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total = total
}

// Increment advances the current counter by 1.
func (p *CompositeProgress) Increment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current++
}

// AddCurrent advances the current counter by n.
func (p *CompositeProgress) AddCurrent(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current += n
}

// AddCacheHit increments the 304 cache hit counter.
func (p *CompositeProgress) AddCacheHit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cacheHits++
}

// AddTokensSaved adds to the total tokens saved counter.
func (p *CompositeProgress) AddTokensSaved(tokens int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tokensSaved += tokens
}

// AddBytesRead adds to the total bytes read counter.
func (p *CompositeProgress) AddBytesRead(b int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bytesRead += b
}

// AddError increments the error counter.
func (p *CompositeProgress) AddError() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errorsCount++
}

// SetActiveItem sets the currently processing item/URL.
func (p *CompositeProgress) SetActiveItem(item string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeItem = item
}

// SetPhase sets the active phase name (e.g. "crawling", "indexing", "seeding").
func (p *CompositeProgress) SetPhase(phase string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phase = phase
}

// GetStats returns current snapshot counters for summary output.
func (p *CompositeProgress) GetStats() (current, total, cacheHits, errorsCount, tokensSaved int64, speed float64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current, p.total, p.cacheHits, p.errorsCount, p.tokensSaved, p.speedEMA
}

func (p *CompositeProgress) handleEvent(ev ProgressEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ev.Current > 0 {
		p.current = ev.Current
	}
	if ev.Total > 0 {
		p.total = ev.Total
	}
	if ev.BytesRead > 0 {
		p.bytesRead = ev.BytesRead
	}
	if ev.CacheHits > 0 {
		p.cacheHits = ev.CacheHits
	}
	if ev.ErrorsCount > 0 {
		p.errorsCount = ev.ErrorsCount
	}
	if ev.TokensSaved > 0 {
		p.tokensSaved = ev.TokensSaved
	}
	if ev.QueueDepth > 0 {
		p.queueDepth = ev.QueueDepth
	}
	if ev.ActiveItem != "" {
		p.activeItem = ev.ActiveItem
	}
	if ev.Status != "" {
		p.status = ev.Status
	}
	if ev.Phase != "" {
		p.phase = ev.Phase
	}
	if ev.Speed > 0 {
		p.speedEMA = ev.Speed
	}
	if ev.BytesSpeed > 0 {
		p.bytesSpeedEMA = ev.BytesSpeed
	}
}

func (p *CompositeProgress) updateMath() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	dt := now.Sub(p.lastSampleTime).Seconds()
	if dt <= 0 {
		return
	}

	deltaItems := float64(p.current - p.lastCurrent)
	if deltaItems < 0 {
		deltaItems = 0
	}
	instantSpeed := deltaItems / dt

	if p.speedEMA <= 0 {
		p.speedEMA = instantSpeed
	} else {
		p.speedEMA = p.alpha*instantSpeed + (1.0-p.alpha)*p.speedEMA
	}

	deltaBytes := float64(p.bytesRead - p.lastBytes)
	if deltaBytes < 0 {
		deltaBytes = 0
	}
	instantBytesSpeed := deltaBytes / dt
	if p.bytesSpeedEMA <= 0 {
		p.bytesSpeedEMA = instantBytesSpeed
	} else {
		p.bytesSpeedEMA = p.alpha*instantBytesSpeed + (1.0-p.alpha)*p.bytesSpeedEMA
	}

	p.lastCurrent = p.current
	p.lastBytes = p.bytesRead
	p.lastSampleTime = now
}

// CalculateETA computes sub-second accurate remaining time using EMA speed with cold-start protection.
func (p *CompositeProgress) calculateETA() time.Duration {
	if p.total <= 0 || p.current >= p.total {
		return 0
	}

	remaining := float64(p.total - p.current)
	elapsed := time.Since(p.startTime).Seconds()

	// Cold start protection (first 1.5s): use overall average speed
	if elapsed < 1.5 || p.speedEMA <= 0.01 {
		if elapsed > 0 && p.current > 0 {
			avgSpeed := float64(p.current) / elapsed
			return time.Duration((remaining / avgSpeed) * float64(time.Second))
		}
		return 0
	}

	etaSec := remaining / p.speedEMA
	if etaSec > 86400 {
		etaSec = 86400 // 24-hour safety clamp
	}
	return time.Duration(etaSec * float64(time.Second))
}

// CalculateETAExported exposes ETA calculation for testing.
func (p *CompositeProgress) CalculateETAExported() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.calculateETA()
}

// SetSpeedEMA sets the EMA speed directly for testing.
func (p *CompositeProgress) SetSpeedEMA(speed float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.speedEMA = speed
}

func (p *CompositeProgress) render(isFinal bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	rawWidth, _ := GetTerminalDimensions(80, 24)
	// Enforce 4-column right safety margin to prevent emoji 2-cell wrap overflow
	safeWidth := rawWidth - 4
	if safeWidth < 36 {
		safeWidth = 36
	}

	cReset := ""
	cBold := ""
	cCyan := ""
	cGreen := ""
	cGray := ""
	cYellow := ""
	cRed := ""
	cBlue := ""
	cMagenta := ""

	if p.cfg.EnableColors {
		cReset = Reset
		cBold = Bold
		cCyan = FgCyan
		cGreen = FgGreen
		cGray = FgGray
		cYellow = FgYellow
		cRed = FgRed
		cBlue = FgBlue
		cMagenta = FgMagenta
	}

	var sb strings.Builder
	if p.renderedLines > 0 {
		sb.WriteString(fmt.Sprintf(CursorUpFmt, p.renderedLines))
	}

	linesCount := 0

	// Line 1: Header Bar
	headerTitle := p.cfg.Title
	if headerTitle == "" {
		headerTitle = "WebLimbAI Task"
	}
	headerStr := fmt.Sprintf("%s=== %s %s", cCyan+cBold, headerTitle, cReset)
	availWidth := safeWidth - VisibleLen(headerStr)
	if availWidth > 0 {
		headerStr += cGray + strings.Repeat("=", availWidth) + cReset
	}
	sb.WriteString(CarriageReturn + ClearToEnd + headerStr + "\n")
	linesCount++

	// Line 2: Progress Bar / Indeterminate State
	barWidth := 20
	var barStr string
	var metricsStr string

	if p.total > 0 {
		pct := float64(p.current) / float64(p.total) * 100.0
		if pct > 100.0 {
			pct = 100.0
		}
		filled := int(math.Round((pct / 100.0) * float64(barWidth)))
		if filled > barWidth {
			filled = barWidth
		}
		empty := barWidth - filled

		barStr = cGreen + strings.Repeat(BlockFull, filled) + cGray + strings.Repeat(BlockEmpty, empty) + cReset
		metricsStr = fmt.Sprintf(" %5.1f%% (%d/%d)", pct, p.current, p.total)

		if p.cfg.ShowSpeed {
			metricsStr += fmt.Sprintf(" | %s%.1f p/s%s", cCyan, p.speedEMA, cReset)
		}
		if p.cfg.ShowETA && pct < 100.0 {
			eta := p.calculateETA()
			if eta > 0 {
				metricsStr += fmt.Sprintf(" | ETA: %s%s%s", cYellow, FormatDuration(eta), cReset)
			}
		}
	} else {
		// Indeterminate Mode (total <= 0)
		p.pulseIdx++
		pos := p.pulseIdx % (barWidth + 4)
		var barBuilder strings.Builder
		for i := 0; i < barWidth; i++ {
			if i >= pos-4 && i <= pos {
				barBuilder.WriteString(cCyan + "=" + cReset)
			} else {
				barBuilder.WriteString(cGray + "-" + cReset)
			}
		}
		barStr = barBuilder.String()
		metricsStr = fmt.Sprintf(" (%d items)", p.current)
		if p.cfg.ShowSpeed && p.speedEMA > 0 {
			metricsStr += fmt.Sprintf(" | %s%.1f p/s%s", cCyan, p.speedEMA, cReset)
		}
	}

	line2 := fmt.Sprintf("  [%s]%s", barStr, metricsStr)
	sb.WriteString(CarriageReturn + ClearToEnd + line2 + "\n")
	linesCount++

	// Line 3: Statistics Row
	phaseAction := "processed"
	switch strings.ToLower(p.phase) {
	case "crawling", "crawl":
		phaseAction = "crawled"
	case "indexing", "index":
		phaseAction = "indexed"
	case "seeding", "seed":
		phaseAction = "seeded"
	case "scraping", "scrape":
		phaseAction = "scraped"
	}

	statsParts := []string{
		fmt.Sprintf("%s%s %d %s%s", cGreen, IconSuccess, p.current, phaseAction, cReset),
	}
	if p.cacheHits > 0 {
		statsParts = append(statsParts, fmt.Sprintf("%s%s %d cached (304)%s", cYellow, IconCached, p.cacheHits, cReset))
	}
	if p.cfg.ShowQueue && p.queueDepth > 0 {
		statsParts = append(statsParts, fmt.Sprintf("%s%s %d queued%s", cCyan, IconFolder, p.queueDepth, cReset))
	}
	if p.errorsCount > 0 {
		statsParts = append(statsParts, fmt.Sprintf("%s%s %d err%s", cRed, IconError, p.errorsCount, cReset))
	}
	if p.cfg.ShowBytes && p.bytesRead > 0 {
		statsParts = append(statsParts, fmt.Sprintf("%s%s %s%s", cBlue, IconDisk, FormatBytes(p.bytesRead), cReset))
	}
	if p.cfg.ShowTokens && p.tokensSaved > 0 {
		statsParts = append(statsParts, fmt.Sprintf("%s%s %s saved%s", cMagenta, IconBrain, FormatNumber(p.tokensSaved), cReset))
	}

	line3 := "  " + strings.Join(statsParts, " | ")
	sb.WriteString(CarriageReturn + ClearToEnd + line3 + "\n")
	linesCount++

	// Line 4: Active URL / File Ticker
	if p.cfg.ShowActiveItem && p.activeItem != "" && !isFinal {
		spinnerGlyph := BrailleSpinnerFrames[p.spinnerIdx%len(BrailleSpinnerFrames)]
		p.spinnerIdx++

		itemStr := TruncateMiddle(p.activeItem, safeWidth-16, "...")
		line4 := fmt.Sprintf("  %s%s%s %sActive:%s %s", cCyan, spinnerGlyph, cReset, cGray, cReset, itemStr)
		sb.WriteString(CarriageReturn + ClearToEnd + line4 + "\n")
		linesCount++
	}

	p.renderedLines = linesCount
	fmt.Fprint(p.out, sb.String())
}

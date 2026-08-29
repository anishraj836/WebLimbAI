package ui

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestETACalculation_SubSecondPrecision(t *testing.T) {
	var buf bytes.Buffer
	prog := NewCompositeProgress(ProgressConfig{
		Writer:      &buf,
		Total:       100,
		RefreshRate: 10 * time.Millisecond,
	})

	prog.startTime = time.Now().Add(-5 * time.Second)
	prog.current = 50
	prog.total = 100
	prog.SetSpeedEMA(10.0) // 10 items/sec, remaining = 50 -> ETA should be 5 seconds

	eta := prog.CalculateETAExported()
	if eta < 4900*time.Millisecond || eta > 5100*time.Millisecond {
		t.Fatalf("expected ETA approx 5s, got %v", eta)
	}

	// Test fractional sub-second ETA
	prog.current = 95
	prog.total = 100
	prog.SetSpeedEMA(20.0) // 20 items/sec, remaining = 5 -> ETA = 0.25s (250ms)
	etaSub := prog.CalculateETAExported()
	if etaSub < 240*time.Millisecond || etaSub > 260*time.Millisecond {
		t.Fatalf("expected fractional ETA approx 250ms, got %v", etaSub)
	}
}

func TestIndeterminateFormatting(t *testing.T) {
	var buf bytes.Buffer
	prog := NewCompositeProgress(ProgressConfig{
		Writer:      &buf,
		Total:       0, // indeterminate
		RefreshRate: 10 * time.Millisecond,
	})

	prog.current = 42
	prog.render(false)

	out := buf.String()
	if !strings.Contains(out, "(42 items)") {
		t.Fatalf("expected indeterminate '(42 items)' in output, got: %s", out)
	}
}

func TestCompositeProgress_IdempotentStop(t *testing.T) {
	var buf bytes.Buffer
	prog := NewCompositeProgress(ProgressConfig{
		Writer:      &buf,
		Total:       50,
		RefreshRate: 10 * time.Millisecond,
	})

	prog.Start()
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prog.Stop()
		}()
	}
	wg.Wait()

	// Calling stop once more should also be completely safe
	prog.Stop()
}

func TestEventBroadcaster_HighConcurrency(t *testing.T) {
	b := NewEventBroadcaster(512)
	defer b.Close()

	const numSubscribers = 10
	const numWorkers = 50
	const eventsPerWorker = 1000

	subs := make([]<-chan ProgressEvent, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subs[i] = b.Subscribe()
	}

	var subWg sync.WaitGroup
	subWg.Add(numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subCh := subs[i]
		go func() {
			defer subWg.Done()
			for range subCh {
				// drain events
			}
		}()
	}

	var workerWg sync.WaitGroup
	workerWg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer workerWg.Done()
			for e := 0; e < eventsPerWorker; e++ {
				b.Publish(ProgressEvent{
					JobID:   fmt.Sprintf("job-%d", workerID),
					Type:    EventProgress,
					Phase:   "crawling",
					Current: int64(e),
					Total:   eventsPerWorker,
				})
			}
		}()
	}

	workerWg.Wait()
	for i := 0; i < numSubscribers; i++ {
		b.Unsubscribe(subs[i])
	}
	subWg.Wait()
}

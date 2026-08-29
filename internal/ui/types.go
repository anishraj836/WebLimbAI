package ui

import (
	"sync"
	"time"
)

// EventType categorizes progress event lifecycle stages.
type EventType string

const (
	EventStart    EventType = "start"
	EventProgress EventType = "progress"
	EventLog      EventType = "log"
	EventError    EventType = "error"
	EventFinish   EventType = "finish"
	EventTick     EventType = "tick"
)

// ProgressEvent represents a strongly-typed, JSON-serializable progress snapshot.
type ProgressEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	JobID       string                 `json:"job_id,omitempty"`
	Type        EventType              `json:"type"`
	Phase       string                 `json:"phase,omitempty"` // "crawling", "indexing", "seeding", "scraping"
	Current     int64                  `json:"current"`
	Total       int64                  `json:"total"`
	Percent     float64                `json:"percent"`
	Speed       float64                `json:"speed"` // items / second
	BytesRead   int64                  `json:"bytes_read,omitempty"`
	BytesSpeed  float64                `json:"bytes_speed,omitempty"` // bytes / second
	ETA         time.Duration          `json:"eta,omitempty"`
	ETASec      float64                `json:"eta_seconds,omitempty"`
	Message     string                 `json:"message,omitempty"`
	Status      string                 `json:"status,omitempty"`      // "ok", "cached", "error", "skipped", "deleted"
	ActiveItem  string                 `json:"active_item,omitempty"` // Current URL or file path
	QueueDepth  int64                  `json:"queue_depth,omitempty"`
	CacheHits   int64                  `json:"cache_hits,omitempty"`
	TokensSaved int64                  `json:"tokens_saved,omitempty"`
	ErrorsCount int64                  `json:"errors_count,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// EventBroadcaster coordinates non-blocking fan-out distribution to multiple subscribers.
type EventBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[chan ProgressEvent]int // subscriber channel -> buffer capacity
	bufferSize  int
	closed      bool
}

// NewEventBroadcaster initializes a new broadcaster with the given channel buffer size.
func NewEventBroadcaster(bufferSize int) *EventBroadcaster {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	return &EventBroadcaster{
		subscribers: make(map[chan ProgressEvent]int),
		bufferSize:  bufferSize,
	}
}

// Subscribe creates a new buffered channel for receiving events.
func (b *EventBroadcaster) Subscribe() <-chan ProgressEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		ch := make(chan ProgressEvent)
		close(ch)
		return ch
	}
	ch := make(chan ProgressEvent, b.bufferSize)
	b.subscribers[ch] = b.bufferSize
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (b *EventBroadcaster) Unsubscribe(ch <-chan ProgressEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subscribers {
		if sub == ch {
			delete(b.subscribers, sub)
			close(sub)
			return
		}
	}
}

// Publish broadcasts an event to all subscribers using non-blocking send.
func (b *EventBroadcaster) Publish(ev ProgressEvent) {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// Non-blocking drop protects producer routines against slow consumers
		}
	}
}

// Close terminates the broadcaster and closes all subscriber channels.
func (b *EventBroadcaster) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
}

// SubscribersCount returns the number of active subscribers.
func (b *EventBroadcaster) SubscribersCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

// IsClosed returns whether the broadcaster is closed.
func (b *EventBroadcaster) IsClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

package storage

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Asynchronous, batched document writes.
//
// Crawl workers used to commit every page to SQLite themselves, each waiting
// for the store's write mutex and its own WAL commit. With async writes
// enabled, SaveCrawledDocument / UpsertCrawledDocument only enqueue the write
// and return; a single background goroutine drains the queue and commits
// writes in batches (one transaction per batch when the store supports it).
//
// Guarantees:
//   - Writes are applied in the order they were enqueued (one writer goroutine,
//     FIFO queue), so a later write to the same URL always wins.
//   - GetCrawledDocumentByURL sees a queued upsert before it is committed
//     (read-your-writes), so conditional-request metadata isn't lost.
//   - Flush returns once every write enqueued before the call is committed.
//     Rebuilds, listings and shutdown flush first.
//   - The queue is bounded: when it is full, enqueue blocks (backpressure)
//     instead of growing memory without limit.
//
// The trade-off: a write error is no longer returned to the caller. It is
// logged and counted in Stats, and the batch is retried one write at a time
// so a single bad document can't drop its neighbours.

// BatchWrite is one queued write.
type BatchWrite struct {
	Doc    CrawledDocument
	TTL    time.Duration
	Upsert bool // true: full UpsertDocument; false: Save

	seq int64 // enqueue order, used to clear read-your-writes entries
}

// BatchWriter is implemented by stores that can commit many writes in one
// transaction.
type BatchWriter interface {
	WriteBatch(ctx context.Context, ops []BatchWrite) error
}

// AsyncWriterConfig tunes batching.
type AsyncWriterConfig struct {
	QueueSize int           // max queued writes before enqueue blocks (default 4096)
	BatchSize int           // max writes per transaction (default 256)
	MaxDelay  time.Duration // max time a write waits for its batch to fill (default 50ms)
}

// AsyncWriterStats reports writer activity.
type AsyncWriterStats struct {
	Enqueued int64  `json:"enqueued"`
	Written  int64  `json:"written"`
	Batches  int64  `json:"batches"`
	Failed   int64  `json:"failed"`
	Pending  int    `json:"pending"`
	LastErr  string `json:"last_error,omitempty"`
}

type flushRequest struct {
	done chan struct{}
}

// AsyncWriter queues document writes and commits them in the background.
type AsyncWriter struct {
	store DocumentStore
	cfg   AsyncWriterConfig

	queue   chan BatchWrite
	flushes chan flushRequest
	stop    chan struct{}
	stopped chan struct{}

	closeOnce sync.Once
	closeMu   sync.RWMutex // Enqueue holds it shared; Close takes it exclusively
	closed    atomic.Bool

	// pending holds the newest queued upsert per URL for read-your-writes.
	pendingMu sync.RWMutex
	pending   map[string]*pendingDoc

	enqueued atomic.Int64
	written  atomic.Int64
	batches  atomic.Int64
	failed   atomic.Int64
	lastErr  atomic.Value // string
}

type pendingDoc struct {
	doc CrawledDocument
	seq int64
}

// NewAsyncWriter starts a background writer for store.
func NewAsyncWriter(store DocumentStore, cfg AsyncWriterConfig) *AsyncWriter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 50 * time.Millisecond
	}
	w := &AsyncWriter{
		store:   store,
		cfg:     cfg,
		queue:   make(chan BatchWrite, cfg.QueueSize),
		flushes: make(chan flushRequest),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
		pending: make(map[string]*pendingDoc),
	}
	go w.run()
	return w
}

// Enqueue queues a write. It returns false (and writes nothing) if the writer
// is closed; callers then fall back to a synchronous write.
func (w *AsyncWriter) Enqueue(op BatchWrite) bool {
	// Holding closeMu shared guarantees Close can't stop the writer between
	// our closed-check and the send, which would strand the write.
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	if w.closed.Load() {
		return false
	}
	op.seq = w.enqueued.Add(1)
	if op.Upsert {
		w.pendingMu.Lock()
		w.pending[op.Doc.URL] = &pendingDoc{doc: op.Doc, seq: op.seq}
		w.pendingMu.Unlock()
	}
	w.queue <- op // blocks when full: backpressure
	return true
}

// Pending returns the newest queued, not yet committed upsert for url.
func (w *AsyncWriter) Pending(url string) (*CrawledDocument, bool) {
	w.pendingMu.RLock()
	defer w.pendingMu.RUnlock()
	p, ok := w.pending[url]
	if !ok {
		return nil, false
	}
	doc := p.doc
	return &doc, true
}

// Flush blocks until every write enqueued before the call is committed, or
// ctx is done.
func (w *AsyncWriter) Flush(ctx context.Context) error {
	if w.closed.Load() {
		return nil
	}
	req := flushRequest{done: make(chan struct{})}
	select {
	case w.flushes <- req:
	case <-w.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes everything queued and stops the writer. Safe to call twice.
func (w *AsyncWriter) Close() {
	w.closeOnce.Do(func() {
		w.closeMu.Lock()
		w.closed.Store(true)
		w.closeMu.Unlock()
		close(w.stop)
		<-w.stopped
	})
}

// Stats returns counters for monitoring and tests.
func (w *AsyncWriter) Stats() AsyncWriterStats {
	st := AsyncWriterStats{
		Enqueued: w.enqueued.Load(),
		Written:  w.written.Load(),
		Batches:  w.batches.Load(),
		Failed:   w.failed.Load(),
		Pending:  len(w.queue),
	}
	if v, ok := w.lastErr.Load().(string); ok {
		st.LastErr = v
	}
	return st
}

func (w *AsyncWriter) run() {
	defer close(w.stopped)

	batch := make([]BatchWrite, 0, w.cfg.BatchSize)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerArmed := false

	commit := func() {
		if timerArmed {
			timer.Stop()
			timerArmed = false
		}
		if len(batch) == 0 {
			return
		}
		w.commit(batch)
		batch = batch[:0]
	}
	// drain moves everything currently queued into batches.
	drain := func() {
		for {
			select {
			case op := <-w.queue:
				batch = append(batch, op)
				if len(batch) >= w.cfg.BatchSize {
					commit()
				}
			default:
				return
			}
		}
	}

	for {
		select {
		case op := <-w.queue:
			batch = append(batch, op)
			if len(batch) >= w.cfg.BatchSize {
				commit()
			} else if !timerArmed {
				timer.Reset(w.cfg.MaxDelay)
				timerArmed = true
			}
		case <-timer.C:
			timerArmed = false
			commit()
		case req := <-w.flushes:
			drain()
			commit()
			close(req.done)
		case <-w.stop:
			drain()
			commit()
			return
		}
	}
}

func (w *AsyncWriter) commit(batch []BatchWrite) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batched := false
	if bw, ok := w.store.(BatchWriter); ok {
		if err := bw.WriteBatch(ctx, batch); err == nil {
			w.written.Add(int64(len(batch)))
			batched = true
		}
	}
	if !batched {
		// No batch support, or the transaction failed: write one by one so a
		// single bad document can't drop the rest of the batch.
		for i := range batch {
			if err := w.writeOne(ctx, &batch[i]); err != nil {
				w.recordFailure(&batch[i], err)
			} else {
				w.written.Add(1)
			}
		}
	}
	w.batches.Add(1)
	w.clearPending(batch)
}

func (w *AsyncWriter) writeOne(ctx context.Context, op *BatchWrite) error {
	if op.Upsert {
		return w.store.UpsertDocument(ctx, &op.Doc, op.TTL)
	}
	return w.store.Save(ctx, &op.Doc, op.TTL)
}

func (w *AsyncWriter) recordFailure(op *BatchWrite, err error) {
	w.failed.Add(1)
	msg := op.Doc.URL + ": " + err.Error()
	w.lastErr.Store(msg)
	log.Printf("[Storage] async write failed: %s", msg)
}

// clearPending drops read-your-writes entries for URLs whose newest queued
// upsert has now been committed. Entries replaced by a newer enqueue stay.
func (w *AsyncWriter) clearPending(batch []BatchWrite) {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	for i := range batch {
		if !batch[i].Upsert {
			continue
		}
		url := batch[i].Doc.URL
		if p, ok := w.pending[url]; ok && p.seq == batch[i].seq {
			delete(w.pending, url)
		}
	}
}

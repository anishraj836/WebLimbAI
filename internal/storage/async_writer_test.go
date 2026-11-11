package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestSQLite(t *testing.T) *SQLiteStore {
	t.Helper()
	st, err := NewSQLiteStore(filepath.Join(t.TempDir(), "docs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestAsyncWriterOrderingAndFlush(t *testing.T) {
	st := newTestSQLite(t)
	w := NewAsyncWriter(st, AsyncWriterConfig{BatchSize: 7, MaxDelay: 5 * time.Millisecond})
	defer w.Close()

	for i := 0; i < 50; i++ {
		w.Enqueue(BatchWrite{Upsert: true, Doc: CrawledDocument{URL: "https://a.dev/p", Title: fmt.Sprintf("v%d", i), CleanBody: "body"}})
	}
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc, err := st.GetByURL(context.Background(), "https://a.dev/p")
	if err != nil || doc == nil {
		t.Fatalf("doc missing after flush: %v", err)
	}
	if doc.Title != "v49" {
		t.Fatalf("last write must win, got %q", doc.Title)
	}
	if _, ok := w.Pending("https://a.dev/p"); ok {
		t.Fatalf("pending entry not cleared after commit")
	}
}

func TestAsyncWriterReadYourWrites(t *testing.T) {
	st := newTestSQLite(t)
	prev := GetGlobalStore()
	SetGlobalStore(st)
	defer SetGlobalStore(prev)
	w := EnableAsyncWrites(AsyncWriterConfig{BatchSize: 10000, MaxDelay: time.Hour})
	defer DisableAsyncWrites()

	doc := &CrawledDocument{URL: "https://b.dev/x", Title: "queued", CleanBody: "body", ETag: `"abc"`}
	if err := UpsertCrawledDocument(context.Background(), doc, 0); err != nil {
		t.Fatal(err)
	}

	// Not committed yet (huge batch, long delay)...
	if got, _ := st.GetByURL(context.Background(), doc.URL); got != nil {
		t.Fatalf("write committed too early")
	}
	// ...but readers through the repository already see it.
	got, err := GetCrawledDocumentByURL(context.Background(), doc.URL)
	if err != nil || got == nil || got.ETag != `"abc"` {
		t.Fatalf("read-your-writes failed: %+v %v", got, err)
	}

	// Whole-table reads flush first.
	docs, err := GetCrawledDocuments(context.Background())
	if err != nil || len(docs) != 1 {
		t.Fatalf("expected listing to include the queued write, got %d (%v)", len(docs), err)
	}
	if s := w.Stats(); s.Written != 1 || s.Pending != 0 {
		t.Fatalf("unexpected stats after flush: %+v", s)
	}
}

func TestAsyncWriterBatchesWrites(t *testing.T) {
	st := newTestSQLite(t)
	w := NewAsyncWriter(st, AsyncWriterConfig{BatchSize: 256, MaxDelay: 20 * time.Millisecond})

	const n = 2000
	for i := 0; i < n; i++ {
		w.Enqueue(BatchWrite{Upsert: i%2 == 0, Doc: CrawledDocument{URL: fmt.Sprintf("https://c.dev/%d", i), Title: "t", CleanBody: "b"}})
	}
	w.Close() // Close must flush

	s := w.Stats()
	if s.Written != n || s.Failed != 0 {
		t.Fatalf("expected %d written, got %+v", n, s)
	}
	if s.Batches >= n/10 {
		t.Fatalf("writes were not batched: %d batches for %d writes", s.Batches, n)
	}
	count, err := st.GetDocumentsCount(context.Background())
	if err != nil || count != n {
		t.Fatalf("expected %d rows, got %d (%v)", n, count, err)
	}
	if w.Enqueue(BatchWrite{Doc: CrawledDocument{URL: "https://c.dev/late"}}) {
		t.Fatalf("enqueue after Close must be refused")
	}
}

// failingStore rejects one URL and has no batch support.
type failingStore struct {
	*MemoryStore
	bad string
}

func (f *failingStore) UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if doc.URL == f.bad {
		return errors.New("boom")
	}
	return f.MemoryStore.UpsertDocument(ctx, doc, ttl)
}

func TestAsyncWriterIsolatesFailures(t *testing.T) {
	fs := &failingStore{MemoryStore: NewMemoryStore(), bad: "https://d.dev/bad"}
	w := NewAsyncWriter(fs, AsyncWriterConfig{BatchSize: 10, MaxDelay: time.Millisecond})
	for _, u := range []string{"https://d.dev/1", "https://d.dev/bad", "https://d.dev/2"} {
		w.Enqueue(BatchWrite{Upsert: true, Doc: CrawledDocument{URL: u, Title: "t", CleanBody: "b"}})
	}
	w.Close()

	s := w.Stats()
	if s.Written != 2 || s.Failed != 1 || s.LastErr == "" {
		t.Fatalf("expected 2 written and 1 failed, got %+v", s)
	}
	if doc, _ := fs.GetByURL(context.Background(), "https://d.dev/2"); doc == nil {
		t.Fatalf("a failure dropped a neighbouring write")
	}
}

func TestAsyncWriterConcurrentProducers(t *testing.T) {
	st := newTestSQLite(t)
	w := NewAsyncWriter(st, AsyncWriterConfig{QueueSize: 64, BatchSize: 32, MaxDelay: 2 * time.Millisecond})

	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				w.Enqueue(BatchWrite{Upsert: true, Doc: CrawledDocument{URL: fmt.Sprintf("https://e.dev/%d/%d", p, i), Title: "t", CleanBody: "b"}})
				if i%50 == 0 {
					_ = w.Flush(context.Background())
				}
			}
		}(p)
	}
	wg.Wait()
	w.Close()

	if s := w.Stats(); s.Written != 1600 || s.Failed != 0 {
		t.Fatalf("expected 1600 writes, got %+v", s)
	}
}

// BenchmarkPageWrites compares crawl-style page writes from 8 concurrent
// workers: committing each page inline versus enqueueing to the async writer
// (timed until everything is committed).
func BenchmarkPageWrites(b *testing.B) {
	body := make([]byte, 4096)
	for i := range body {
		body[i] = 'a' + byte(i%26)
	}
	newDoc := func(i int) *CrawledDocument {
		return &CrawledDocument{
			URL: fmt.Sprintf("https://bench.dev/page/%d", i), Title: "Page", CleanBody: string(body),
			ETag: `"e"`, ContentHash: "h", HTTPStatus: 200, OutboundLinks: []string{"https://bench.dev/a", "https://bench.dev/b"},
		}
	}

	b.Run("sync", func(b *testing.B) {
		st, err := NewSQLiteStore(filepath.Join(b.TempDir(), "docs.db"))
		if err != nil {
			b.Fatal(err)
		}
		defer st.Close()
		b.SetParallelism(8)
		var n int64
		var mu sync.Mutex
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.Lock()
				n++
				i := int(n)
				mu.Unlock()
				_ = st.UpsertDocument(context.Background(), newDoc(i), 0)
			}
		})
	})

	b.Run("async", func(b *testing.B) {
		st, err := NewSQLiteStore(filepath.Join(b.TempDir(), "docs.db"))
		if err != nil {
			b.Fatal(err)
		}
		defer st.Close()
		w := NewAsyncWriter(st, AsyncWriterConfig{})
		b.SetParallelism(8)
		var n int64
		var mu sync.Mutex
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.Lock()
				n++
				i := int(n)
				mu.Unlock()
				w.Enqueue(BatchWrite{Upsert: true, Doc: *newDoc(i)})
			}
		})
		w.Close() // include the time to commit everything
		b.StopTimer()
		if s := w.Stats(); s.Failed != 0 {
			b.Fatalf("failed writes: %+v", s)
		}
	})
}

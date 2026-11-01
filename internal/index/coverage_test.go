package index

import "testing"

func TestCoverageStats_CountsDocumentsAndHosts(t *testing.T) {
	e := NewEngine()

	empty := e.CoverageStats()
	if empty.Documents != 0 || empty.HostCount != 0 || empty.LastIndexedAt != nil {
		t.Fatalf("expected empty coverage, got %+v", empty)
	}

	e.IndexDocumentDirectly("https://go.dev/doc/tutorial", "Tutorial", "learn golang modules and packages", 0)
	e.IndexDocumentDirectly("https://go.dev/doc/effective_go", "Effective Go", "golang style and idioms", 0)
	e.IndexDocumentDirectly("https://docs.python.org/3/library/venv.html", "venv", "python virtual environments", 0)

	stats := e.CoverageStats()
	if stats.Documents != 3 {
		t.Fatalf("expected 3 documents, got %d", stats.Documents)
	}
	if stats.HostCount != 2 {
		t.Fatalf("expected 2 hosts, got %d", stats.HostCount)
	}
	if len(stats.TopHosts) != 2 || stats.TopHosts[0].Host != "go.dev" || stats.TopHosts[0].Documents != 2 {
		t.Fatalf("expected go.dev first with 2 docs, got %+v", stats.TopHosts)
	}
	if stats.LastIndexedAt == nil {
		t.Fatalf("expected last_indexed_at to be set after indexing")
	}
	if stats.LastSyncedFromStoreAt != nil {
		t.Fatalf("expected no store sync yet, got %v", stats.LastSyncedFromStoreAt)
	}

	e.MarkSyncedFromStore()
	if e.CoverageStats().LastSyncedFromStoreAt == nil {
		t.Fatalf("expected last_synced_from_store_at after MarkSyncedFromStore")
	}
}

func TestCoverageStats_SeparatesChunks(t *testing.T) {
	e := NewEngine()
	shard := e.getShard("https://example.com/a#chunk0")
	shard.mu.Lock()
	shard.urls["https://example.com/a#chunk0"] = "https://example.com/a"
	shard.mu.Unlock()
	parent := e.getShard("https://example.com/a")
	parent.mu.Lock()
	parent.urls["https://example.com/a"] = "https://example.com/a"
	parent.mu.Unlock()

	stats := e.CoverageStats()
	if stats.Documents != 1 || stats.Chunks != 1 {
		t.Fatalf("expected 1 document and 1 chunk, got %d docs / %d chunks", stats.Documents, stats.Chunks)
	}
}

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSQLiteStore_BasicCRUD(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_crud.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQLiteStore: %v", err)
	}
	defer store.Close()

	if store.DriverName() != "sqlite" {
		t.Fatalf("Expected driver name 'sqlite', got %s", store.DriverName())
	}

	ctx := context.Background()

	// 1. Save document
	doc1 := &CrawledDocument{
		URL:         "https://example.com/page1",
		Title:       "Example Page 1",
		CleanBody:   "This is clean body text for page 1.",
		TotalTokens: 8,
		SourceType:  "web_crawled",
		SourceURL:   "https://example.com/page1",
	}
	if err := store.Save(ctx, doc1, 0); err != nil {
		t.Fatalf("Failed to save doc1: %v", err)
	}

	// 2. GetByURL
	fetched, err := store.GetByURL(ctx, "https://example.com/page1")
	if err != nil {
		t.Fatalf("Failed to get doc1: %v", err)
	}
	if fetched == nil {
		t.Fatalf("Expected fetched doc1 to be non-nil")
	}
	if fetched.Title != "Example Page 1" || fetched.CleanBody != doc1.CleanBody || fetched.TotalTokens != 8 {
		t.Errorf("Mismatch in fetched doc: %+v", fetched)
	}
	if fetched.OutboundLinks == nil || len(fetched.OutboundLinks) != 0 {
		t.Errorf("Expected empty outbound links slice, got: %+v", fetched.OutboundLinks)
	}

	// 3. Upsert document with full metadata
	doc1Updated := &CrawledDocument{
		URL:           "https://example.com/page1",
		Title:         "Example Page 1 Updated",
		CleanBody:     "Updated clean body.",
		TotalTokens:   4,
		SourceType:    "cli_scraped",
		SourceURL:     "https://example.com/page1",
		ETag:          "etag-12345",
		LastModified:  "Wed, 21 Oct 2026 07:28:00 GMT",
		ContentHash:   "hash-abcdef",
		LastCrawledAt: time.Now(),
		HTTPStatus:    200,
		OutboundLinks: []string{"https://example.com/about", "https://example.com/contact"},
	}
	if err := store.UpsertDocument(ctx, doc1Updated, 0); err != nil {
		t.Fatalf("Failed to upsert doc1: %v", err)
	}

	fetchedUpdated, err := store.GetByURL(ctx, "https://example.com/page1")
	if err != nil {
		t.Fatalf("Failed to get updated doc1: %v", err)
	}
	if fetchedUpdated.Title != "Example Page 1 Updated" || fetchedUpdated.ETag != "etag-12345" || fetchedUpdated.HTTPStatus != 200 {
		t.Errorf("Mismatch in upserted doc: %+v", fetchedUpdated)
	}
	if len(fetchedUpdated.OutboundLinks) != 2 || fetchedUpdated.OutboundLinks[0] != "https://example.com/about" {
		t.Errorf("Mismatch in outbound links: %+v", fetchedUpdated.OutboundLinks)
	}

	// 4. List pagination
	doc2 := &CrawledDocument{
		URL:         "https://example.com/page2",
		Title:       "Example Page 2",
		CleanBody:   "Body 2",
		TotalTokens: 2,
		SourceType:  "web_crawled",
	}
	if err := store.Save(ctx, doc2, 0); err != nil {
		t.Fatalf("Failed to save doc2: %v", err)
	}

	listAll, err := store.List(ctx, 0, 0)
	if err != nil || len(listAll) != 2 {
		t.Fatalf("Expected 2 documents in list, got %d (err: %v)", len(listAll), err)
	}

	listLimit, err := store.List(ctx, 1, 0)
	if err != nil || len(listLimit) != 1 {
		t.Fatalf("Expected 1 document with limit 1, got %d", len(listLimit))
	}

	listOffset, err := store.List(ctx, 1, 1)
	if err != nil || len(listOffset) != 1 {
		t.Fatalf("Expected 1 document with offset 1, got %d", len(listOffset))
	}
	if listOffset[0].URL != "https://example.com/page2" {
		t.Errorf("Expected offset page2, got: %s", listOffset[0].URL)
	}

	// 5. DeleteCrawledDocument
	if err := store.DeleteCrawledDocument(ctx, "https://example.com/page1"); err != nil {
		t.Fatalf("Failed to delete doc1: %v", err)
	}
	deletedFetch, err := store.GetByURL(ctx, "https://example.com/page1")
	if err != nil || deletedFetch != nil {
		t.Errorf("Expected deleted doc1 to return nil, got %+v (err: %v)", deletedFetch, err)
	}
}

func TestSQLiteStore_InMemoryPool(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create in-memory store: %v", err)
	}
	defer store.Close()

	if !store.isMemory {
		t.Fatalf("Expected isMemory to be true")
	}

	ctx := context.Background()

	// Write document
	doc := &CrawledDocument{
		URL:         "https://memory.test/doc",
		Title:       "Memory Doc",
		CleanBody:   "In-memory body text",
		TotalTokens: 4,
	}
	if err := store.Save(ctx, doc, 0); err != nil {
		t.Fatalf("Failed to save in-memory doc: %v", err)
	}

	// Concurrently query from multiple goroutines to ensure MaxOpenConns(1) prevents isolated DB splits
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fetched, err := store.GetByURL(ctx, "https://memory.test/doc")
			if err != nil || fetched == nil {
				t.Errorf("Failed to read from in-memory pool concurrently: %v, doc: %+v", err, fetched)
			}
		}()
	}
	wg.Wait()
}

func TestSQLiteStore_TTLPruning(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 1. Save document with very short TTL
	docShort := &CrawledDocument{
		URL:         "https://example.com/short-ttl",
		Title:       "Short TTL",
		CleanBody:   "Short lived body",
		TotalTokens: 3,
	}
	// Expire 1 second in the future
	if err := store.Save(ctx, docShort, 1*time.Second); err != nil {
		t.Fatalf("Failed to save short-ttl doc: %v", err)
	}

	// 2. Save document with long TTL
	docLong := &CrawledDocument{
		URL:         "https://example.com/long-ttl",
		Title:       "Long TTL",
		CleanBody:   "Long lived body",
		TotalTokens: 3,
	}
	if err := store.Save(ctx, docLong, 1*time.Hour); err != nil {
		t.Fatalf("Failed to save long-ttl doc: %v", err)
	}

	// Verify both exist initially
	f1, _ := store.GetByURL(ctx, "https://example.com/short-ttl")
	f2, _ := store.GetByURL(ctx, "https://example.com/long-ttl")
	if f1 == nil || f2 == nil {
		t.Fatalf("Expected both documents to exist initially")
	}

	// Wait for short TTL to expire
	time.Sleep(1200 * time.Millisecond)

	// GetByURL should filter out expired doc
	f1Expired, err := store.GetByURL(ctx, "https://example.com/short-ttl")
	if err != nil {
		t.Fatalf("GetByURL error: %v", err)
	}
	if f1Expired != nil {
		t.Errorf("Expected short-ttl doc to be filtered out after expiration, got %+v", f1Expired)
	}

	// Long doc should still be accessible
	f2Active, _ := store.GetByURL(ctx, "https://example.com/long-ttl")
	if f2Active == nil {
		t.Errorf("Expected long-ttl doc to still be accessible")
	}

	// DeleteExpired Janitor execution
	deletedCount, err := store.DeleteExpired(ctx)
	if err != nil {
		t.Fatalf("DeleteExpired error: %v", err)
	}
	if deletedCount != 1 {
		t.Errorf("Expected 1 expired document deleted, got %d", deletedCount)
	}

	// Verify count is now 1
	count, err := store.GetDocumentsCount(ctx)
	if err != nil || count != 1 {
		t.Errorf("Expected document count 1, got %d (err: %v)", count, err)
	}
}

func TestSQLiteStore_NegativeTTL(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Negative TTL should expire immediately
	doc := &CrawledDocument{
		URL:         "https://example.com/negative-ttl",
		Title:       "Negative TTL",
		CleanBody:   "Immediately expired body",
		TotalTokens: 3,
	}
	if err := store.Save(ctx, doc, -10*time.Second); err != nil {
		t.Fatalf("Failed to save negative TTL doc: %v", err)
	}

	// Should not be visible via GetByURL
	fetched, err := store.GetByURL(ctx, "https://example.com/negative-ttl")
	if err != nil || fetched != nil {
		t.Errorf("Expected immediately expired document to not be returned, got %+v (err: %v)", fetched, err)
	}

	// Should be deleted by DeleteExpired
	deleted, err := store.DeleteExpired(ctx)
	if err != nil || deleted != 1 {
		t.Errorf("Expected 1 document pruned by DeleteExpired, got %d (err: %v)", deleted, err)
	}
}

func TestSQLiteStore_Aliases(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 1. Direct alias
	if err := store.SaveAlias(ctx, "http://example.com", "https://example.com"); err != nil {
		t.Fatalf("Failed to save alias: %v", err)
	}
	if canonical := store.GetAlias(ctx, "http://example.com"); canonical != "https://example.com" {
		t.Errorf("Expected canonical URL 'https://example.com', got %s", canonical)
	}

	// 2. Unknown alias returns self
	if unknown := store.GetAlias(ctx, "https://not-an-alias.org"); unknown != "https://not-an-alias.org" {
		t.Errorf("Expected unknown alias to return self, got %s", unknown)
	}

	// 3. Multi-hop alias: a -> b -> c
	_ = store.SaveAlias(ctx, "http://hop1.com", "https://hop2.com")
	_ = store.SaveAlias(ctx, "https://hop2.com", "https://hop3.com")
	if canonical := store.GetAlias(ctx, "http://hop1.com"); canonical != "https://hop3.com" {
		t.Errorf("Expected multi-hop alias to resolve to 'https://hop3.com', got %s", canonical)
	}

	// 4. Circular alias: cycle1 -> cycle2 -> cycle1
	_ = store.SaveAlias(ctx, "https://cycle1.com", "https://cycle2.com")
	_ = store.SaveAlias(ctx, "https://cycle2.com", "https://cycle1.com")
	// Cycle detection must return original URL without infinite looping
	if cycleRes := store.GetAlias(ctx, "https://cycle1.com"); cycleRes != "https://cycle1.com" {
		t.Errorf("Expected cycle detection to safely return start URL, got %s", cycleRes)
	}

	// 5. GetByURL resolving alias
	doc := &CrawledDocument{
		URL:         "https://example.com/canonical",
		Title:       "Canonical Page",
		CleanBody:   "Canonical content",
		TotalTokens: 2,
	}
	_ = store.Save(ctx, doc, 0)
	_ = store.SaveAlias(ctx, "http://example.com/alias", "https://example.com/canonical")

	byAlias, err := store.GetByURL(ctx, "http://example.com/alias")
	if err != nil || byAlias == nil {
		t.Fatalf("Expected to find document via registered alias: %v", err)
	}
	if byAlias.URL != "https://example.com/canonical" {
		t.Errorf("Mismatch in alias resolved document: %+v", byAlias)
	}
}

func TestSQLiteStore_COALESCEAndCorruptedJSON(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert row directly with NULLs and corrupt JSON
	rawQuery := `
	INSERT INTO crawled_pages (url, title, clean_body, total_tokens, source_type, source_url, etag, last_modified, content_hash, http_status, outbound_links)
	VALUES ('https://corrupted.com', 'Corrupt Test', 'Body', 2, NULL, NULL, NULL, NULL, NULL, NULL, '{malformed_json}');
	`
	if _, err := store.DB().ExecContext(ctx, rawQuery); err != nil {
		t.Fatalf("Failed to insert raw corrupt row: %v", err)
	}

	// 1. GetByURL must not error on NULL scans or malformed JSON
	doc, err := store.GetByURL(ctx, "https://corrupted.com")
	if err != nil {
		t.Fatalf("GetByURL failed on NULL/malformed row: %v", err)
	}
	if doc == nil {
		t.Fatalf("Expected non-nil document")
	}
	if doc.SourceType != "web_crawled" {
		t.Errorf("Expected COALESCE source_type 'web_crawled', got %s", doc.SourceType)
	}
	if doc.ETag != "" || doc.ContentHash != "" {
		t.Errorf("Expected empty string for NULL columns, got ETag=%s, Hash=%s", doc.ETag, doc.ContentHash)
	}
	if doc.OutboundLinks == nil {
		t.Errorf("Expected non-nil OutboundLinks slice on JSON decode error")
	}

	// 2. List must also handle NULLs gracefully
	docs, err := store.List(ctx, 0, 0)
	if err != nil || len(docs) != 1 {
		t.Fatalf("List failed on NULL row: %v (count: %d)", err, len(docs))
	}

	// 3. GetDocumentMetadataBySource
	bySource, err := store.GetDocumentMetadataBySource(ctx, "web_crawled", 10, 0)
	if err != nil || len(bySource) != 1 {
		t.Fatalf("GetDocumentMetadataBySource failed: %v", err)
	}
}

func TestSQLiteStore_Concurrency(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrency_test.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Pre-populate 50 seed documents
	for i := 0; i < 50; i++ {
		doc := &CrawledDocument{
			URL:         fmt.Sprintf("https://concurrent.test/page/%d", i),
			Title:       fmt.Sprintf("Concurrent Page %d", i),
			CleanBody:   fmt.Sprintf("Body content for page %d with concurrency test tokens.", i),
			TotalTokens: 10,
			SourceType:  "web_crawled",
			SourceURL:   fmt.Sprintf("https://concurrent.test/page/%d", i),
		}
		if err := store.Save(ctx, doc, 0); err != nil {
			t.Fatalf("Failed initial seed: %v", err)
		}
	}

	var wg sync.WaitGroup
	const readers = 100
	const writers = 20

	// Launch 100 concurrent readers
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for iter := 0; iter < 10; iter++ {
				idx := (readerID + iter) % 50
				url := fmt.Sprintf("https://concurrent.test/page/%d", idx)
				doc, err := store.GetByURL(ctx, url)
				if err != nil {
					t.Errorf("Concurrent read error on %s: %v", url, err)
					return
				}
				if doc != nil && doc.Title == "" {
					t.Errorf("Empty title in concurrent read for %s", url)
				}
			}
		}(r)
	}

	// Launch 20 concurrent writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for iter := 0; iter < 5; iter++ {
				url := fmt.Sprintf("https://concurrent.test/writer/%d/item/%d", writerID, iter)
				doc := &CrawledDocument{
					URL:           url,
					Title:         fmt.Sprintf("Writer %d Item %d", writerID, iter),
					CleanBody:     "Dynamic write payload.",
					TotalTokens:   4,
					SourceType:    "concurrent_writer",
					OutboundLinks: []string{"https://concurrent.test/page/0"},
				}
				if err := store.UpsertDocument(ctx, doc, 0); err != nil {
					t.Errorf("Concurrent write error on %s: %v", url, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// Verify total count
	total, err := store.GetDocumentsCount(ctx)
	if err != nil {
		t.Fatalf("GetDocumentsCount error: %v", err)
	}
	expected := int64(50 + (writers * 5))
	if total != expected {
		t.Errorf("Expected total count %d, got %d", expected, total)
	}
}

func TestSQLiteStore_FactoryAndRepositoryIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "factory_test.db")

	// 1. Test NewStore with sqlite aliases
	store, err := NewStore("sqlite", dbPath)
	if err != nil {
		t.Fatalf("NewStore(sqlite) failed: %v", err)
	}
	if store.DriverName() != "sqlite" {
		t.Errorf("Expected driver name 'sqlite', got %s", store.DriverName())
	}
	_ = store.Close()

	// 2. Test InitGlobalStore
	if err := InitGlobalStore("sqlite", dbPath); err != nil {
		t.Fatalf("InitGlobalStore failed: %v", err)
	}
	global := GetGlobalStore()
	if global.DriverName() != "sqlite" {
		t.Errorf("Expected global store driver 'sqlite', got %s", global.DriverName())
	}

	// 3. Test Repository facade methods
	ctx := context.Background()
	err = SaveCrawledDocument(ctx, "https://repo.test/doc1", "Repo Doc 1", "Body 1", 5, "repo_test", "https://repo.test/doc1")
	if err != nil {
		t.Fatalf("SaveCrawledDocument failed: %v", err)
	}

	doc, err := GetCrawledDocumentByURL(ctx, "https://repo.test/doc1")
	if err != nil || doc == nil || doc.Title != "Repo Doc 1" {
		t.Fatalf("GetCrawledDocumentByURL failed: %+v (err: %v)", doc, err)
	}

	count, err := GetDocumentsCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("GetDocumentsCount failed: %d (err: %v)", count, err)
	}

	_ = SaveURLAlias(ctx, "http://alias.repo.test", "https://repo.test/doc1")
	aliasDoc, err := GetCrawledDocumentByURL(ctx, "http://alias.repo.test")
	if err != nil || aliasDoc == nil || aliasDoc.URL != "https://repo.test/doc1" {
		t.Fatalf("Alias resolution via repository failed: %+v", aliasDoc)
	}

	CloseDB()

	// 4. Test InitDB with SQLite URL
	InitDB("sqlite://" + dbPath)
	if GetGlobalStore().DriverName() != "sqlite" {
		t.Errorf("Expected InitDB to initialize SQLiteStore")
	}
	CloseDB()
}

func BenchmarkSQLite_SaveGet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		b.Fatalf("Failed to initialize store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	doc := &CrawledDocument{
		URL:         "https://benchmark.test/doc",
		Title:       "Benchmark Document",
		CleanBody:   "High performance benchmarking document body with 10 words.",
		TotalTokens: 10,
		SourceType:  "benchmark",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			url := fmt.Sprintf("https://benchmark.test/doc/%d", i)
			d := *doc
			d.URL = url
			_ = store.Save(ctx, &d, 0)
			_, _ = store.GetByURL(ctx, url)
		}
	})
}

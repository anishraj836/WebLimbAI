package index

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crawler-monorepo/internal/storage"
)

func TestEngineIndexAndSearch(t *testing.T) {
	eng := NewEngine()

	termPositions := map[string][]int{
		"golang": {0},
		"engine": {1},
		"search": {2},
	}

	eng.IndexDocument("https://golang.org", "Go Language", "Golang engine for search indexing", termPositions, 5)

	hits := eng.SearchBM25("golang search", 5)
	if len(hits) == 0 {
		t.Fatalf("Expected BM25 search hits, got 0")
	}

	if hits[0].DocID != "https://golang.org" {
		t.Errorf("Expected doc ID 'https://golang.org', got '%s'", hits[0].DocID)
	}

	eng.IndexDocumentVector("https://golang.org", "Go Language", "Golang engine for search indexing")
	vecHits := eng.SearchVector("golang search", 5)
	if len(vecHits) == 0 {
		t.Fatalf("Expected Vector search hits, got 0")
	}

	if vecHits[0].DocID != "https://golang.org" {
		t.Errorf("Expected doc ID 'https://golang.org', got '%s'", vecHits[0].DocID)
	}
}

func TestTrieAutocomplete(t *testing.T) {
	trie := NewTrie()
	trie.Insert("golang", 10)
	trie.Insert("goroutine", 5)
	trie.Insert("google", 2)

	results := trie.SearchPrefix("go", 5)
	if len(results) != 3 {
		t.Fatalf("Expected 3 autocomplete results, got %d", len(results))
	}

	if results[0].Term != "golang" {
		t.Errorf("Expected top result 'golang', got '%s'", results[0].Term)
	}
}

func TestSnapshotSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()

	eng := NewEngine()
	termPositions := map[string][]int{"test": {0}}
	eng.IndexDocument("https://test.com", "Test Title", "Test Body", termPositions, 2)
	eng.IndexDocumentVector("https://test.com", "Test Title", "Test Body")

	err := eng.SaveSnapshot(tmpDir)
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "inverted_index.json")); err != nil {
		t.Errorf("Expected inverted_index.json to exist")
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "vector_index.json")); err != nil {
		t.Errorf("Expected vector_index.json to exist")
	}

	eng2 := NewEngine()
	err = eng2.LoadSnapshot(tmpDir)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	pl, exists := eng2.Inverted.GetPostingList("test")
	if !exists || len(pl.Entries) == 0 {
		t.Errorf("Expected inverted index to contain 'test' entry after restore")
	}

	// Verify temporary files are cleaned up
	if _, err := os.Stat(filepath.Join(tmpDir, "inverted_index.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("Expected inverted_index.json.tmp to not exist after successful save")
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "vector_index.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("Expected vector_index.json.tmp to not exist after successful save")
	}
}

func TestCosineSimilarity(t *testing.T) {
	u := []float64{1.0, 0.0, 0.0}
	v := []float64{1.0, 0.0, 0.0}
	sim := CosineSimilarity(u, v)
	if mathAbs(sim-1.0) > 1e-5 {
		t.Errorf("Expected similarity 1.0 for identical vectors, got %f", sim)
	}
}

func mathAbs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func TestIndexDocumentIncrementalByURL(t *testing.T) {
	originalStore := storage.GetGlobalStore()
	defer storage.SetGlobalStore(originalStore)
	storage.SetGlobalStore(storage.NewMemoryStore())

	eng := NewEngine()
	termPositions1 := map[string][]int{"golang": {0}}
	eng.IndexDocument("https://golang.org", "Go Language", "Golang initial doc", termPositions1, 3)

	ctx := context.Background()
	doc2URL := "https://example.com/inc"
	_ = storage.SaveCrawledDocument(ctx, doc2URL, "Incremental Document", "Golang concurrency incremental text", 4, "web_crawled", doc2URL)

	err := eng.IndexDocumentIncrementalByURL(ctx, doc2URL)
	if err != nil {
		t.Fatalf("IndexDocumentIncrementalByURL failed: %v", err)
	}

	hits := eng.SearchBM25("golang", 10)
	if len(hits) < 2 {
		t.Fatalf("Expected at least 2 BM25 hits after incremental index, got %d", len(hits))
	}

	foundDoc1 := false
	foundDoc2 := false
	for _, h := range hits {
		if h.DocID == "https://golang.org" {
			foundDoc1 = true
		}
		if h.DocID == doc2URL {
			foundDoc2 = true
		}
	}

	if !foundDoc1 || !foundDoc2 {
		t.Errorf("Expected both doc1 and doc2 in index after incremental add, got doc1=%v, doc2=%v", foundDoc1, foundDoc2)
	}
}

func TestURLAliasDeduplication(t *testing.T) {
	eng := NewEngine()
	targetURL := "http://example.com"
	canonicalURL := "https://example.com/canonical"

	eng.IndexDocumentDirectly(canonicalURL, "Canonical Page", "Content of canonical page with unique text", 6, targetURL)

	// Verify metadata lookup by canonical URL
	title1, _, _, exists1 := eng.GetDocumentMetadata(canonicalURL)
	if !exists1 || title1 != "Canonical Page" {
		t.Errorf("Expected canonical URL lookup to succeed, got title '%s', exists=%v", title1, exists1)
	}

	// Verify metadata lookup by target/alias URL resolves to canonical document
	title2, _, _, exists2 := eng.GetDocumentMetadata(targetURL)
	if !exists2 || title2 != "Canonical Page" {
		t.Errorf("Expected alias URL lookup to resolve to canonical document, got title '%s', exists=%v", title2, exists2)
	}

	// Verify no duplicate entries in metadata maps
	titles, _, _ := eng.GetMetadataMaps()
	if len(titles) != 1 {
		t.Errorf("Expected exactly 1 metadata entry (deduplicated), got %d entries", len(titles))
	}
}

func TestPostingListDeepCopyConcurrency(t *testing.T) {
	inv := NewInvertedIndex()
	inv.AddDocument("doc1", map[string][]int{"go": {1, 5, 10}}, 15)

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			inv.AddDocument("doc1", map[string][]int{"go": {i, i + 1}}, 20)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			if pl, exists := inv.GetPostingList("go"); exists && len(pl.Entries) > 0 {
				// Mutate returned positions slice locally to test isolation
				if len(pl.Entries[0].Positions) > 0 {
					pl.Entries[0].Positions[0] = 99999
				}
			}
		}
		done <- true
	}()

	<-done
	<-done
}

func TestVectorIndexSaveSnapshotDeepCopyConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	vi := NewVectorIndex(3)
	_ = vi.AddVector("doc1", []float64{0.1, 0.2, 0.3})

	done := make(chan bool)
	go func() {
		for i := 0; i < 50; i++ {
			_ = vi.AddVector(filepath.Join("doc", string(rune('a'+i%26))), []float64{float64(i), 0.5, 0.9})
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 20; i++ {
			snapPath := filepath.Join(tmpDir, "vec_snap.json")
			_ = vi.SaveSnapshot(snapPath)
		}
		done <- true
	}()

	<-done
	<-done
}

func TestEmpiricalSemanticSimilarityAndScanScaling(t *testing.T) {
	// 1. Measure exact cosine similarity between semantic synonyms vs morphological variants
	vCar := GenerateFeatureVector("car", 128)
	vAutomobile := GenerateFeatureVector("automobile", 128)
	vVehicle := GenerateFeatureVector("vehicle", 128)
	vSportsCar := GenerateFeatureVector("sports car", 128)
	vCars := GenerateFeatureVector("cars", 128)

	simCarAuto := CosineSimilarity(vCar, vAutomobile)
	simCarVehicle := CosineSimilarity(vCar, vVehicle)
	simCarSports := CosineSimilarity(vCar, vSportsCar)
	simCarCars := CosineSimilarity(vCar, vCars)

	t.Logf("Empirical Cosine Similarity:")
	t.Logf("  'car' vs 'automobile': %.4f", simCarAuto)
	t.Logf("  'car' vs 'vehicle':    %.4f", simCarVehicle)
	t.Logf("  'car' vs 'sports car': %.4f", simCarSports)
	t.Logf("  'car' vs 'cars':       %.4f", simCarCars)

	// 2. Measure flat O(N) vector scan latency across 1k, 10k, and 50k vectors
	sizes := []int{1000, 10000, 50000}
	for _, size := range sizes {
		vi := NewVectorIndex(128)
		qVec := GenerateFeatureVector("search query test", 128)
		for i := 0; i < size; i++ {
			_ = vi.AddVector(string(rune(i)), GenerateFeatureVector(string(rune(i*37)), 128))
		}

		// Warm up and measure
		start := time.Now()
		runs := 50
		for r := 0; r < runs; r++ {
			_ = vi.SearchNearest(qVec, 10)
		}
		avgDuration := time.Since(start) / time.Duration(runs)
		t.Logf("Flat O(N) scan latency for N=%d vectors: %v/query", size, avgDuration)
	}
}

func TestVectorIndex_DeleteVector(t *testing.T) {
	vi := NewVectorIndex(3)
	vec := []float64{1.0, 0.0, 0.0}
	_ = vi.AddVector("doc1", vec)

	res := vi.SearchNearest(vec, 5)
	if len(res) == 0 || res[0].DocID != "doc1" {
		t.Fatalf("Expected doc1 in nearest results before deletion")
	}

	vi.DeleteVector("doc1")

	resAfter := vi.SearchNearest(vec, 5)
	if len(resAfter) != 0 {
		t.Errorf("Expected 0 results after deleting doc1, got %d", len(resAfter))
	}
}

func TestBM25_NegativeAndZeroTopK(t *testing.T) {
	eng := NewEngine()
	eng.IndexDocument("https://example.com/doc1", "Test Title", "Golang distributed indexing system", map[string][]int{"golang": {0}, "indexing": {2}}, 4)

	// SearchBM25 with topK <= 0 must return nil without panicking
	for _, k := range []int{-10, -1, 0} {
		hits := eng.SearchBM25("golang indexing", k)
		if hits != nil {
			t.Errorf("SearchBM25 with topK=%d expected nil, got %v", k, hits)
		}
	}

	// Direct RankDocuments call with topK <= 0 must return nil without panicking
	titles, urls, bodies := eng.GetMetadataMaps()
	for _, k := range []int{-5, -1, 0} {
		hits := RankDocuments("golang indexing", eng.GetInvertedIndex(), titles, urls, bodies, k)
		if hits != nil {
			t.Errorf("RankDocuments with topK=%d expected nil, got %v", k, hits)
		}
	}
}

func TestLoadSnapshot_EmptyJSONNilMapProtection(t *testing.T) {
	tmpDir := t.TempDir()
	emptySnapPath := filepath.Join(tmpDir, "empty_snapshot.json")
	if err := os.WriteFile(emptySnapPath, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write empty snapshot file: %v", err)
	}

	// 1. InvertedIndex LoadSnapshot from empty JSON
	inv := NewInvertedIndex()
	if err := inv.LoadSnapshot(emptySnapPath); err != nil {
		t.Fatalf("inv.LoadSnapshot on empty JSON failed: %v", err)
	}
	// AddDocument must not panic on nil map assignment
	inv.AddDocument("doc1", map[string][]int{"test": {0}}, 1)
	pl, exists := inv.GetPostingList("test")
	if !exists || len(pl.Entries) != 1 {
		t.Errorf("Expected posting list for 'test' after AddDocument on restored empty snapshot")
	}
	if length := inv.GetDocLength("doc1"); length != 1 {
		t.Errorf("Expected docLength 1 for doc1, got %d", length)
	}

	// 2. VectorIndex LoadSnapshot from empty JSON
	vi := NewVectorIndex(3)
	if err := vi.LoadSnapshot(emptySnapPath); err != nil {
		t.Fatalf("vi.LoadSnapshot on empty JSON failed: %v", err)
	}
	// AddVector must not panic on nil map assignment
	if err := vi.AddVector("doc1", []float64{1.0, 0.0, 0.0}); err != nil {
		t.Fatalf("vi.AddVector on restored empty snapshot failed: %v", err)
	}
	results := vi.SearchNearest([]float64{1.0, 0.0, 0.0}, 1)
	if len(results) != 1 || results[0].DocID != "doc1" {
		t.Errorf("Expected doc1 in vector search results, got %v", results)
	}
}

func TestLoadSnapshot_ZeroByteFileHandling(t *testing.T) {
	tmpDir := t.TempDir()
	zeroByteSnapPath := filepath.Join(tmpDir, "zero_byte_snapshot.json")
	if err := os.WriteFile(zeroByteSnapPath, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to write zero byte snapshot file: %v", err)
	}

	// 1. InvertedIndex LoadSnapshot from 0-byte file succeeds and allows subsequent AddDocument
	inv := NewInvertedIndex()
	if err := inv.LoadSnapshot(zeroByteSnapPath); err != nil {
		t.Fatalf("inv.LoadSnapshot on 0-byte file failed: %v", err)
	}
	inv.AddDocument("doc1", map[string][]int{"golang": {0, 1}}, 2)
	pl, exists := inv.GetPostingList("golang")
	if !exists || len(pl.Entries) != 1 {
		t.Errorf("Expected posting list for 'golang' after AddDocument on restored 0-byte snapshot")
	}
	if length := inv.GetDocLength("doc1"); length != 2 {
		t.Errorf("Expected docLength 2 for doc1, got %d", length)
	}

	// 2. VectorIndex LoadSnapshot from 0-byte file succeeds and allows subsequent AddVector
	vi := NewVectorIndex(3)
	if err := vi.LoadSnapshot(zeroByteSnapPath); err != nil {
		t.Fatalf("vi.LoadSnapshot on 0-byte file failed: %v", err)
	}
	if err := vi.AddVector("doc1", []float64{0.5, 0.5, 0.5}); err != nil {
		t.Fatalf("vi.AddVector on restored 0-byte snapshot failed: %v", err)
	}
	results := vi.SearchNearest([]float64{0.5, 0.5, 0.5}, 1)
	if len(results) != 1 || results[0].DocID != "doc1" {
		t.Errorf("Expected doc1 in vector search results after 0-byte snapshot restore, got %v", results)
	}
}

func TestLoadFromDB_PurgeExpiredAndTrieIdempotency(t *testing.T) {
	ctx := context.Background()
	permURL := "https://example.com/perm-test"
	tempURL := "https://example.com/temp-test"

	// Use a fresh MemoryStore to avoid file-based state interference across tests
	originalStore := storage.GetGlobalStore()
	defer storage.SetGlobalStore(originalStore)
	storage.SetGlobalStore(storage.NewMemoryStore())

	// 1. Save permanent and short-lived documents
	_ = storage.SaveCrawledDocumentWithTTL(ctx, permURL, "Permanent Doc", "Golang permanent search indexing document", 5, "test", permURL, 1*time.Hour)
	_ = storage.SaveCrawledDocumentWithTTL(ctx, tempURL, "Temporary Doc", "Golang temporary expiring indexing document", 5, "test", tempURL, 300*time.Millisecond)

	eng := NewEngine()

	// 2. Load from DB initially
	if err := eng.LoadFromDB(ctx); err != nil {
		t.Fatalf("LoadFromDB failed: %v", err)
	}

	permHits := eng.SearchBM25("permanent", 5)
	if len(permHits) == 0 || permHits[0].DocID != permURL {
		t.Errorf("Expected perm document indexed, got %v", permHits)
	}
	tempHits := eng.SearchBM25("temporary", 5)
	if len(tempHits) == 0 || tempHits[0].DocID != tempURL {
		t.Errorf("Expected temp document indexed, got %v", tempHits)
	}

	// Measure autocomplete frequency for 'golang'
	trieRes1 := eng.GetTrie().SearchPrefix("golang", 5)
	if len(trieRes1) == 0 {
		t.Fatalf("Expected autocomplete result for 'golang'")
	}
	initialFreq := trieRes1[0].Frequency

	// 3. LoadFromDB second time -> MUST be idempotent, no frequency doubling
	if err := eng.LoadFromDB(ctx); err != nil {
		t.Fatalf("LoadFromDB second run failed: %v", err)
	}
	trieRes2 := eng.GetTrie().SearchPrefix("golang", 5)
	if len(trieRes2) == 0 {
		t.Fatalf("Expected autocomplete result for 'golang' after second LoadFromDB")
	}
	if trieRes2[0].Frequency != initialFreq {
		t.Errorf("Autocomplete frequency changed after second LoadFromDB: expected %d, got %d", initialFreq, trieRes2[0].Frequency)
	}

	// 4. Expire temp doc, delete from DB with polling, and reload
	for i := 0; i < 30; i++ {
		time.Sleep(25 * time.Millisecond)
		del, _ := storage.DeleteExpiredDocuments(ctx)
		if del >= 1 {
			break
		}
	}

	if err := eng.LoadFromDB(ctx); err != nil {
		t.Fatalf("LoadFromDB after expiration purge failed: %v", err)
	}

	// Verify temp doc is purged from inverted index and metadata shards
	tempHitsAfter := eng.SearchBM25("temporary", 5)
	if len(tempHitsAfter) != 0 {
		t.Errorf("Expected expired temp doc to be purged, but found hits: %v", tempHitsAfter)
	}
	_, _, _, exists := eng.GetDocumentMetadata(tempURL)
	if exists {
		t.Errorf("Expected expired temp doc to be purged from metadata shards, but still exists")
	}

	// Permanent doc still exists
	permHitsAfter := eng.SearchBM25("permanent", 5)
	if len(permHitsAfter) == 0 || permHitsAfter[0].DocID != permURL {
		t.Errorf("Expected permanent doc to remain indexed after purge, got %v", permHitsAfter)
	}
}

func TestEngine_ThreadSafeAccessors(t *testing.T) {
	eng := NewEngine()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i++
				docURL := "https://example.com/doc" + string(rune('a'+i%26))
				eng.IndexDocumentDirectly(docURL, "Concurrent Title", "Concurrent document body test indexing", 5)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Reader goroutines accessing SearchBM25, GetInvertedIndex, GetTrie, GetVectorIndex
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_ = eng.SearchBM25("concurrent test", 5)
					_, _, _ = eng.GetInvertedIndex().GetStats()
					_ = eng.GetTrie().SearchPrefix("co", 5)
					_ = eng.GetVectorIndex().SearchNearest([]float64{0.1, 0.2, 0.3}, 5)
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
}

func TestFunctionalOptionsAndInterfaces(t *testing.T) {
	customEmbedder := NewSubwordEmbedder(64)
	eng := NewEngine(
		WithEmbedder(customEmbedder),
		WithDimensions(64),
	)

	if eng.ActiveEmbedder.Dimensions() != 64 {
		t.Errorf("expected embedder dimensions 64, got %d", eng.ActiveEmbedder.Dimensions())
	}
	if eng.GetVectorIndex().dimensions != 64 {
		t.Errorf("expected vector index dimensions 64, got %d", eng.GetVectorIndex().dimensions)
	}

	// Verify interface conformance at runtime
	var _ Searcher = eng
	var _ MetadataReader = eng
	var _ VectorStore = eng.GetVectorIndex()
	var _ Autocompleter = eng.GetTrie()
	var _ Embedder = customEmbedder

	// Test ExtractHighlightedSnippet
	snippet := ExtractHighlightedSnippet("The quick brown fox jumps over the lazy dog", []string{"brown", "fox"}, 100)
	if snippet == "" || !strings.Contains(snippet, "<mark>brown</mark>") {
		t.Errorf("expected highlighted snippet containing <mark>brown</mark>, got %s", snippet)
	}
}

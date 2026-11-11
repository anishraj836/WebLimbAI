package index

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var shardTestVocab = []string{
	"golang", "goroutine", "channel", "mutex", "vector", "index", "search", "crawler",
	"kafka", "raft", "sqlite", "postgres", "memory", "token", "parser", "network",
	"latency", "shard", "lock", "cache", "query", "ranking", "bm25", "fusion",
}

func randomDoc(r *rand.Rand, n int) (map[string][]int, int) {
	tp := make(map[string][]int)
	for i := 0; i < n; i++ {
		term := shardTestVocab[r.Intn(len(shardTestVocab))]
		tp[term] = append(tp[term], i)
	}
	return tp, n
}

func buildInverted(shards, docs int, seed int64) *InvertedIndex {
	r := rand.New(rand.NewSource(seed))
	idx := NewInvertedIndexWithShards(shards)
	for d := 0; d < docs; d++ {
		tp, n := randomDoc(r, 20+r.Intn(200))
		idx.AddDocument(fmt.Sprintf("https://example.com/doc/%d", d), tp, n)
	}
	return idx
}

func scoresEqual(t *testing.T, a, b map[string]float64) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("score maps differ in size: %d vs %d", len(a), len(b))
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || math.Abs(va-vb) > 1e-9 {
			t.Fatalf("score mismatch for %s: %v vs %v", k, va, vb)
		}
	}
}

// Sharding must not change BM25: corpus statistics are global.
func TestShardedBM25MatchesSingleShard(t *testing.T) {
	one := buildInverted(1, 500, 42)
	many := buildInverted(64, 500, 42)
	for _, q := range [][]string{{"golang"}, {"vector", "index"}, {"raft", "kafka", "sqlite"}, {"missing"}} {
		s1, m1 := one.scoreTerms(q)
		s64, m64 := many.scoreTerms(q)
		scoresEqual(t, s1, s64)
		if len(m1) != len(m64) {
			t.Fatalf("match counts differ for %v", q)
		}
	}
	d1, a1, v1 := one.GetStats()
	d64, a64, v64 := many.GetStats()
	if d1 != d64 || a1 != a64 || v1 != v64 {
		t.Fatalf("stats differ: (%d %v %d) vs (%d %v %d)", d1, a1, v1, d64, a64, v64)
	}
}

func TestInvertedReplaceDropsStaleTerms(t *testing.T) {
	idx := NewInvertedIndexWithShards(8)
	idx.AddDocument("doc", map[string][]int{"alpha": {0}, "beta": {1}}, 2)
	idx.AddDocument("doc", map[string][]int{"gamma": {0}}, 1)

	if _, ok := idx.GetPostingList("alpha"); ok {
		t.Fatalf("stale term alpha survived a replace")
	}
	if pl, ok := idx.GetPostingList("gamma"); !ok || pl.DocumentFrequency != 1 {
		t.Fatalf("gamma missing after replace")
	}
	if docs, _, _ := idx.GetStats(); docs != 1 {
		t.Fatalf("expected 1 document after replace, got %d", docs)
	}

	idx.DeleteDocument("doc")
	if docs, _, vocab := idx.GetStats(); docs != 0 || vocab != 0 {
		t.Fatalf("expected empty index after delete, got docs=%d vocab=%d", docs, vocab)
	}
}

func TestInvertedSnapshotAcrossShardCounts(t *testing.T) {
	src := buildInverted(64, 200, 7)
	path := filepath.Join(t.TempDir(), "inv.json")
	if err := src.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	dst := NewInvertedIndexWithShards(4)
	if err := dst.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	s1, _ := src.scoreTerms([]string{"goroutine", "lock"})
	s2, _ := dst.scoreTerms([]string{"goroutine", "lock"})
	scoresEqual(t, s1, s2)

	// A loaded index must still support deletes (docTerms rebuilt).
	dst.DeleteDocument("https://example.com/doc/3")
	if dst.GetDocLength("https://example.com/doc/3") != 0 {
		t.Fatalf("delete after snapshot load failed")
	}
}

func TestShardedVectorSearchMatchesSingleShard(t *testing.T) {
	for _, prec := range []VectorPrecision{PrecisionFloat32, PrecisionFloat64, PrecisionInt8} {
		one := NewVectorIndexWithShards(32, prec, 1)
		many := NewVectorIndexWithShards(32, prec, 64)
		r := rand.New(rand.NewSource(1))
		for d := 0; d < 300; d++ {
			vec := make([]float64, 32)
			for i := range vec {
				vec[i] = r.Float64() - 0.3
			}
			id := fmt.Sprintf("doc-%d", d)
			if err := one.AddVector(id, vec); err != nil {
				t.Fatal(err)
			}
			_ = many.AddVector(id, vec)
		}
		q := make([]float64, 32)
		for i := range q {
			q[i] = r.Float64()
		}
		a, b := one.SearchNearest(q, 10), many.SearchNearest(q, 10)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("%s: results differ\n%v\n%v", prec, a, b)
		}
		if one.Len() != 300 || many.Len() != 300 {
			t.Fatalf("%s: expected 300 vectors", prec)
		}
	}
}

func TestShardedVectorPrecisionAndSnapshot(t *testing.T) {
	vi := NewVectorIndexWithShards(8, PrecisionFloat32, 16)
	for d := 0; d < 50; d++ {
		vec := []float64{float64(d), 1, 2, 3, 4, 5, 6, float64(d % 3)}
		_ = vi.AddVector(fmt.Sprintf("d%d", d), vec)
	}
	if err := vi.SetPrecision(PrecisionInt8); err != nil {
		t.Fatal(err)
	}
	if vi.Len() != 50 || vi.GetPrecision() != PrecisionInt8 {
		t.Fatalf("precision change lost vectors: len=%d prec=%s", vi.Len(), vi.GetPrecision())
	}
	path := filepath.Join(t.TempDir(), "vec.json")
	if err := vi.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	re := NewVectorIndexWithShards(8, PrecisionFloat32, 2)
	if err := re.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	if re.Len() != 50 || re.GetPrecision() != PrecisionInt8 {
		t.Fatalf("snapshot reload: len=%d prec=%s", re.Len(), re.GetPrecision())
	}
	vi.DeleteVector("d7")
	if _, ok := vi.GetSimilarity("d7", []float64{1, 1, 1, 1, 1, 1, 1, 1}); ok {
		t.Fatalf("deleted vector still present")
	}
}

func TestShardedTrieMatchesSingleShard(t *testing.T) {
	one, many := NewTrieWithShards(1), NewTrieWithShards(64)
	words := []string{"go", "goroutine", "golang", "google", "gopher", "graph", "g", "grpc", "zebra", "zoo", "z"}
	for i, w := range words {
		one.Insert(w, i+1)
		many.Insert(w, i+1)
	}
	for _, p := range []string{"g", "go", "gor", "z", "zo", "x"} {
		a, b := one.SearchPrefix(p, 5), many.SearchPrefix(p, 5)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("prefix %q: %v vs %v", p, a, b)
		}
	}
	if one.NodeCount() != many.NodeCount() && many.NodeCount() < one.NodeCount() {
		t.Fatalf("node count unexpectedly low")
	}
}

func TestEngineShardCountOptionAndEnv(t *testing.T) {
	e := NewEngine(WithIndexShards(10))
	if e.ShardCount() != 16 || e.Inverted.ShardCount() != 16 || e.Vector.ShardCount() != 16 {
		t.Fatalf("expected 16 shards everywhere, got %d/%d/%d", e.ShardCount(), e.Inverted.ShardCount(), e.Vector.ShardCount())
	}
	t.Setenv(IndexShardsEnv, "3")
	if got := NewEngine().ShardCount(); got != 4 {
		t.Fatalf("env override: expected 4 shards, got %d", got)
	}
	os.Unsetenv(IndexShardsEnv)
}

func TestMetadataViewInvalidatesOnWrite(t *testing.T) {
	e := NewEngine()
	e.IndexDocumentDirectly("https://a.dev/x", "X", "alpha beta gamma", 3)
	t1, _, _ := e.GetMetadataMaps()
	t2, _, _ := e.GetMetadataMaps()
	if fmt.Sprintf("%p", t1) != fmt.Sprintf("%p", t2) {
		t.Fatalf("expected cached view to be reused when nothing changed")
	}
	e.IndexDocumentDirectly("https://a.dev/y", "Y", "delta epsilon", 2)
	t3, _, _ := e.GetMetadataMaps()
	if _, ok := t3["https://a.dev/y"]; !ok {
		t.Fatalf("view not refreshed after a write")
	}
	e.DeleteDocument("https://a.dev/x")
	t4, _, _ := e.GetMetadataMaps()
	if _, ok := t4["https://a.dev/x"]; ok {
		t.Fatalf("view not refreshed after a delete")
	}
}

// Many workers indexing, searching and deleting at once; run with -race.
func TestEngineConcurrentIndexingAndSearch(t *testing.T) {
	e := NewEngine()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 40; i++ {
				words := make([]string, 30)
				for j := range words {
					words[j] = shardTestVocab[r.Intn(len(shardTestVocab))]
				}
				url := fmt.Sprintf("https://site%d.dev/page/%d", w, i)
				e.IndexDocumentDirectly(url, "Page", strings.Join(words, " "), 30)
				_ = e.SearchBM25("golang vector", 5)
				_ = e.SearchVector("golang vector", 5)
				_ = e.GetTrie().SearchPrefix("go", 5)
				if i%5 == 0 {
					e.DeleteDocument(url)
				}
			}
		}(w)
	}
	wg.Wait()

	docs, _, _ := e.GetInvertedIndex().GetStats()
	if docs != 8*32 {
		t.Fatalf("expected %d documents after concurrent run, got %d", 8*32, docs)
	}
	hits := e.SearchBM25("golang", 1000)
	urls := make([]string, 0, len(hits))
	for _, h := range hits {
		urls = append(urls, h.URL)
	}
	sort.Strings(urls)
	for _, u := range urls {
		var w, i int
		if _, err := fmt.Sscanf(u, "https://site%d.dev/page/%d", &w, &i); err == nil && i%5 == 0 {
			t.Fatalf("deleted page %s still searchable", u)
		}
	}
}

// BenchmarkConcurrentIndexing measures indexing throughput with many writers.
// Compare -bench=ConcurrentIndexing/shards=1 with shards=64 to see the effect
// of lock striping on write contention.
func BenchmarkConcurrentIndexing(b *testing.B) {
	for _, shards := range []int{1, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			inv := NewInvertedIndexWithShards(shards)
			trie := NewTrieWithShards(shards)
			vec := NewVectorIndexWithShards(128, PrecisionFloat32, shards)
			docs := make([]map[string][]int, 256)
			r := rand.New(rand.NewSource(3))
			for i := range docs {
				docs[i], _ = randomDoc(r, 300)
			}
			v := make([]float64, 128)
			for i := range v {
				v[i] = float64(i%7) + 0.5
			}
			var counter atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := counter.Add(1)
					id := fmt.Sprintf("doc-%d", n)
					tp := docs[n%int64(len(docs))]
					inv.AddDocument(id, tp, 300)
					for term, pos := range tp {
						trie.Insert(term, len(pos))
					}
					_ = vec.AddVector(id, v)
				}
			})
		})
	}
}

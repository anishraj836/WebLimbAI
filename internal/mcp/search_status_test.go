package mcp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/crawler-monorepo/internal/crawler"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/search"
)

func TestDecideRetrievalStatus(t *testing.T) {
	withDocs := index.CoverageStats{Documents: 3}
	idle := IngestionStatus{}
	crawling := IngestionStatus{CrawlActivity: crawler.CrawlActivity{CrawlActive: true}}
	stale := IngestionStatus{IndexMayBeStale: true}
	strong := MatchQuality{LexicalMatches: 2}
	weak := MatchQuality{Weak: true}

	tests := []struct {
		name      string
		coverage  index.CoverageStats
		ingestion IngestionStatus
		quality   MatchQuality
		want      string
	}{
		{"empty index", index.CoverageStats{}, idle, weak, StatusEmpty},
		{"empty index but crawl running", index.CoverageStats{}, crawling, weak, StatusPartial},
		{"crawl running beats everything", withDocs, crawling, strong, StatusPartial},
		{"stale index", withDocs, stale, strong, StatusStale},
		{"weak match", withDocs, idle, weak, StatusWeakMatch},
		{"complete", withDocs, idle, strong, StatusComplete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, hint := decideRetrievalStatus(tt.coverage, tt.ingestion, tt.quality)
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
			if hint == "" {
				t.Fatalf("expected a non-empty hint for %q", got)
			}
		})
	}
}

func TestBuildMatchQuality(t *testing.T) {
	one := 1
	if q := buildMatchQuality(nil); !q.Weak {
		t.Fatalf("no hits should be weak")
	}
	vectorOnly := []search.HybridSearchHit{{VectorRank: &one, VectorSim: 0.4}}
	if q := buildMatchQuality(vectorOnly); !q.Weak || q.SemanticMatches != 1 || q.TopVectorSimilarity != 0.4 {
		t.Fatalf("vector-only hits should be weak, got %+v", q)
	}
	lexical := []search.HybridSearchHit{{BM25Rank: &one, VectorRank: &one, VectorSim: 0.2}}
	if q := buildMatchQuality(lexical); q.Weak || q.LexicalMatches != 1 {
		t.Fatalf("lexical hits should not be weak, got %+v", q)
	}
}

func TestBuildIngestionStatus_StaleWhenOtherProcessIndexedAfterSync(t *testing.T) {
	dataDir := t.TempDir()
	SetDataDir(dataDir)
	defer SetDataDir("")

	eng := index.NewEngine()
	eng.MarkSyncedFromStore()
	later := time.Now().Add(time.Second)

	dir := filepath.Join(dataDir, "crawl_status")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(crawler.CrawlStatusFile{PID: os.Getpid() + 100001, UpdatedAt: time.Now(), LastPageIndexedAt: &later})
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.json", os.Getpid()+100001)), data, 0o644); err != nil {
		t.Fatal(err)
	}

	st := buildIngestionStatus(eng)
	if !st.IndexMayBeStale || st.NextSyncAt == nil {
		t.Fatalf("expected stale index with next_sync_at, got %+v", st)
	}
	if st.CrawlActive {
		t.Fatalf("idle heartbeat should not count as an active crawl")
	}
}

// End-to-end through the JSON-RPC handler: the tool result must be a JSON
// object with hits plus retrieval_status, and must flip to "partial" while a
// crawl heartbeat is live.
func TestMCPHybridSearch_ReturnsRetrievalStatus(t *testing.T) {
	dataDir := t.TempDir()
	SetDataDir(dataDir)
	defer SetDataDir("")

	index.GlobalEngine.IndexDocumentDirectly("https://go.dev/doc/status-test", "Status Test", "golang goroutines channels status test page", 0)
	defer index.GlobalEngine.DeleteDocument("https://go.dev/doc/status-test")

	call := func() HybridSearchResponse {
		t.Helper()
		req := `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"agent_limbs_hybrid_search","arguments":{"query":"golang goroutines"}}}`
		respBytes, err := HandleRPCMessage([]byte(req), nil)
		if err != nil {
			t.Fatalf("HandleRPCMessage failed: %v", err)
		}
		var resp struct {
			Result CallToolResult `json:"result"`
		}
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Result.IsError || len(resp.Result.Content) == 0 {
			t.Fatalf("unexpected tool error: %+v", resp.Result)
		}
		var out HybridSearchResponse
		if err := json.Unmarshal([]byte(resp.Result.Content[0].Text), &out); err != nil {
			t.Fatalf("tool text is not the expected JSON object: %v\n%s", err, resp.Result.Content[0].Text)
		}
		return out
	}

	out := call()
	if out.RetrievalStatus != StatusComplete {
		t.Fatalf("expected complete, got %q (hint: %s)", out.RetrievalStatus, out.Hint)
	}
	if out.ResultCount == 0 || len(out.Hits) != out.ResultCount || out.Coverage.Documents == 0 {
		t.Fatalf("expected hits and coverage, got %+v", out)
	}
	if !out.Ingestion.StatusKnown || out.Ingestion.CrawlActive {
		t.Fatalf("expected known, idle ingestion, got %+v", out.Ingestion)
	}

	dir := filepath.Join(dataDir, "crawl_status")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pid := os.Getpid() + 100002
	data, _ := json.Marshal(crawler.CrawlStatusFile{
		PID:        pid,
		UpdatedAt:  time.Now(),
		ActiveJobs: []crawler.ActiveCrawlJob{{ID: "job", SeedURL: "https://go.dev", PagesCrawled: 4, MaxPages: 50}},
	})
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.json", pid)), data, 0o644); err != nil {
		t.Fatal(err)
	}

	out = call()
	if out.RetrievalStatus != StatusPartial || !out.Ingestion.CrawlActive || len(out.Ingestion.ActiveJobs) != 1 {
		t.Fatalf("expected partial with one active job, got status %q, ingestion %+v", out.RetrievalStatus, out.Ingestion)
	}
}

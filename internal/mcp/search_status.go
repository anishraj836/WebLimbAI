package mcp

import (
	"sync"
	"time"

	"github.com/crawler-monorepo/internal/crawler"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/search"
)

// Retrieval status values returned by agent_limbs_hybrid_search. They tell the
// calling agent whether it can trust the result set as final.
const (
	// StatusComplete: no ingestion is pending and the index is in sync with the
	// store, so results reflect everything that has been scraped or crawled.
	StatusComplete = "complete"
	// StatusPartial: a crawl is still running, so more relevant pages may arrive.
	StatusPartial = "partial"
	// StatusStale: another process indexed pages after this server's last sync
	// with the store, so some stored pages are not searchable here yet.
	StatusStale = "stale"
	// StatusWeakMatch: ingestion is done but no result matched the query's
	// words, so the indexed pages probably do not cover this topic.
	StatusWeakMatch = "weak_match"
	// StatusEmpty: nothing is indexed yet.
	StatusEmpty = "empty"
)

var (
	statusDirMu   sync.RWMutex
	statusDataDir string
)

// SetDataDir tells the MCP handlers where crawl heartbeat files live
// (<dataDir>/crawl_status). An empty dir disables crawl-activity reporting.
func SetDataDir(dir string) {
	statusDirMu.Lock()
	statusDataDir = dir
	statusDirMu.Unlock()
}

func getDataDir() string {
	statusDirMu.RLock()
	defer statusDirMu.RUnlock()
	return statusDataDir
}

// IngestionStatus reports crawl activity visible to this server.
type IngestionStatus struct {
	crawler.CrawlActivity
	// IndexMayBeStale is true when another process indexed pages after this
	// server last synced its in-memory index with the store.
	IndexMayBeStale bool       `json:"index_may_be_stale"`
	NextSyncAt      *time.Time `json:"next_sync_at,omitempty"`
}

// MatchQuality summarizes how well the fused results match the query.
type MatchQuality struct {
	LexicalMatches      int     `json:"lexical_matches"`
	SemanticMatches     int     `json:"semantic_matches"`
	TopVectorSimilarity float64 `json:"top_vector_similarity"`
	Weak                bool    `json:"weak"`
}

// HybridSearchResponse is the JSON payload of agent_limbs_hybrid_search.
type HybridSearchResponse struct {
	RetrievalStatus string                   `json:"retrieval_status"`
	Hint            string                   `json:"hint"`
	Query           string                   `json:"query"`
	ResultCount     int                      `json:"result_count"`
	Hits            []search.HybridSearchHit `json:"hits"`
	MatchQuality    MatchQuality             `json:"match_quality"`
	Coverage        index.CoverageStats      `json:"coverage"`
	Ingestion       IngestionStatus          `json:"ingestion"`
}

// syncInterval is how often the TTL janitor rebuilds the index from the store.
// It is a variable so the CLI can report the interval it actually configured.
var syncInterval = 15 * time.Minute

// SetSyncInterval records the janitor interval used to estimate next_sync_at.
func SetSyncInterval(d time.Duration) {
	if d > 0 {
		syncInterval = d
	}
}

func buildIngestionStatus(eng *index.Engine) IngestionStatus {
	status := IngestionStatus{
		CrawlActivity: crawler.ReadCrawlActivity(getDataDir(), crawler.StatusStaleAfter),
	}
	lastSync := eng.LastSyncedFromStoreAt()
	if other := status.LastPageIndexedAt; other != nil && !lastSync.IsZero() && other.After(lastSync) {
		status.IndexMayBeStale = true
		next := lastSync.Add(syncInterval)
		status.NextSyncAt = &next
	}
	return status
}

func buildMatchQuality(hits []search.HybridSearchHit) MatchQuality {
	q := MatchQuality{}
	for i, h := range hits {
		if h.BM25Rank != nil {
			q.LexicalMatches++
		}
		if h.VectorRank != nil {
			q.SemanticMatches++
		}
		if i == 0 || h.VectorSim > q.TopVectorSimilarity {
			q.TopVectorSimilarity = h.VectorSim
		}
	}
	// The default embedder is a hashed-subword model, so vector-only matches are
	// often noise. Treat "no hit contains any query word" as a weak match.
	q.Weak = len(hits) == 0 || q.LexicalMatches == 0
	return q
}

// decideRetrievalStatus picks the single status an agent should act on, in
// priority order: still crawling > index behind store > weak match > complete.
func decideRetrievalStatus(coverage index.CoverageStats, ingestion IngestionStatus, quality MatchQuality) (string, string) {
	switch {
	case coverage.Documents == 0 && ingestion.CrawlActive:
		return StatusPartial, "Nothing is indexed yet, but a crawl is running. Wait a few seconds and search again."
	case coverage.Documents == 0:
		return StatusEmpty, "Nothing is indexed yet. Use agent_limbs_scrape on relevant pages first."
	case ingestion.CrawlActive:
		return StatusPartial, "A crawl is still running, so more relevant pages may be indexed soon. Use these results provisionally and search again later for a complete answer."
	case ingestion.IndexMayBeStale:
		return StatusStale, "Pages were crawled by another process after this server last synced, so some stored pages are not searchable here until the next sync. Scrape the specific pages you need to make them searchable now."
	case quality.Weak:
		return StatusWeakMatch, "Ingestion is finished, but no result contains the query's words. The indexed pages probably don't cover this topic. Scrape more relevant pages before relying on these results."
	default:
		return StatusComplete, "Ingestion is finished and these are the best matches among all indexed pages. Pages that were never scraped or crawled are not covered."
	}
}

// BuildHybridSearchResponse wraps fused hits with coverage and ingestion
// metadata so an agent can tell final results from partial ones.
func BuildHybridSearchResponse(eng *index.Engine, query string, hits []search.HybridSearchHit) HybridSearchResponse {
	if hits == nil {
		hits = []search.HybridSearchHit{}
	}
	coverage := eng.CoverageStats()
	ingestion := buildIngestionStatus(eng)
	quality := buildMatchQuality(hits)
	status, hint := decideRetrievalStatus(coverage, ingestion, quality)

	return HybridSearchResponse{
		RetrievalStatus: status,
		Hint:            hint,
		Query:           query,
		ResultCount:     len(hits),
		Hits:            hits,
		MatchQuality:    quality,
		Coverage:        coverage,
		Ingestion:       ingestion,
	}
}

package index

import (
	"net/url"
	"sort"
	"strings"
	"time"
)

// HostCount reports how many parent documents from a single host are indexed.
type HostCount struct {
	Host      string `json:"host"`
	Documents int    `json:"documents"`
}

// CoverageStats describes what the in-memory index currently holds, so callers
// (e.g. LLM agents) can judge whether a result set is likely to be complete.
type CoverageStats struct {
	Documents             int         `json:"documents"`
	Chunks                int         `json:"chunks"`
	HostCount             int         `json:"host_count"`
	TopHosts              []HostCount `json:"top_hosts"`
	LastIndexedAt         *time.Time  `json:"last_indexed_at,omitempty"`
	LastSyncedFromStoreAt *time.Time  `json:"last_synced_from_store_at,omitempty"`
}

const maxReportedHosts = 10

// markIndexed records that the in-memory index changed just now.
func (e *Engine) markIndexed() {
	e.lastIndexedAt.Store(time.Now().UnixNano())
}

// MarkSyncedFromStore records that the in-memory index was (re)built from the
// persistent store just now. Pages written to the store by other processes
// after this moment are not visible until the next sync.
func (e *Engine) MarkSyncedFromStore() {
	now := time.Now().UnixNano()
	e.lastSyncAt.Store(now)
	e.lastIndexedAt.Store(now)
}

// LastIndexedAt returns when the in-memory index last changed (zero if never).
func (e *Engine) LastIndexedAt() time.Time {
	return unixNanoToTime(e.lastIndexedAt.Load())
}

// LastSyncedFromStoreAt returns when the index was last rebuilt from the store (zero if never).
func (e *Engine) LastSyncedFromStoreAt() time.Time {
	return unixNanoToTime(e.lastSyncAt.Load())
}

func unixNanoToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// CoverageStats walks the metadata shards (read locks only, no body copies)
// and summarizes how many documents, chunks and hosts are indexed.
func (e *Engine) CoverageStats() CoverageStats {
	stats := CoverageStats{TopHosts: []HostCount{}}
	hosts := make(map[string]int)

	for i := 0; i < 64; i++ {
		shard := &e.shards[i]
		shard.mu.RLock()
		for docID, docURL := range shard.urls {
			if strings.Contains(docID, "#chunk") {
				stats.Chunks++
				continue
			}
			stats.Documents++
			if u, err := url.Parse(docURL); err == nil && u.Hostname() != "" {
				hosts[strings.ToLower(u.Hostname())]++
			} else {
				hosts["(local)"]++
			}
		}
		shard.mu.RUnlock()
	}

	stats.HostCount = len(hosts)
	for h, n := range hosts {
		stats.TopHosts = append(stats.TopHosts, HostCount{Host: h, Documents: n})
	}
	sort.Slice(stats.TopHosts, func(i, j int) bool {
		if stats.TopHosts[i].Documents != stats.TopHosts[j].Documents {
			return stats.TopHosts[i].Documents > stats.TopHosts[j].Documents
		}
		return stats.TopHosts[i].Host < stats.TopHosts[j].Host
	})
	if len(stats.TopHosts) > maxReportedHosts {
		stats.TopHosts = stats.TopHosts[:maxReportedHosts]
	}

	if t := e.LastIndexedAt(); !t.IsZero() {
		stats.LastIndexedAt = &t
	}
	if t := e.LastSyncedFromStoreAt(); !t.IsZero() {
		stats.LastSyncedFromStoreAt = &t
	}
	return stats
}

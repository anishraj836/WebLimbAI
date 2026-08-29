package index

import (
	"strings"
	"sync"
)

// Metadata Sharding & Alias Resolution

type MetadataShard struct {
	mu     sync.RWMutex
	titles map[string]string
	urls   map[string]string
	bodies map[string]string
}

func getShardIndex(docID string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(docID); i++ {
		h ^= uint32(docID[i])
		h *= 16777619
	}
	return int(h % 64)
}

func (e *Engine) getShard(docID string) *MetadataShard {
	return &e.shards[getShardIndex(docID)]
}

func (e *Engine) AddAlias(aliasURL, canonicalURL string) {
	if aliasURL == "" || canonicalURL == "" || aliasURL == canonicalURL {
		return
	}
	e.aliasesMu.Lock()
	if e.aliases == nil {
		e.aliases = make(map[string]string)
	}
	e.aliases[aliasURL] = canonicalURL
	e.aliasesMu.Unlock()
}

func (e *Engine) ResolveURL(targetURL string) string {
	e.aliasesMu.RLock()
	defer e.aliasesMu.RUnlock()
	if canonical, ok := e.aliases[targetURL]; ok && canonical != "" {
		return canonical
	}
	return targetURL
}

func (e *Engine) GetDocumentMetadata(docID string) (title, url, body string, exists bool) {
	canonicalID := e.ResolveURL(docID)
	shard := e.getShard(canonicalID)
	shard.mu.RLock()
	title, exists = shard.titles[canonicalID]
	url = shard.urls[canonicalID]
	body = shard.bodies[canonicalID]
	shard.mu.RUnlock()

	if body == "" && strings.Contains(canonicalID, "#chunk") {
		baseURL := canonicalID[:strings.Index(canonicalID, "#chunk")]
		parentShard := e.getShard(baseURL)
		parentShard.mu.RLock()
		body = parentShard.bodies[baseURL]
		parentShard.mu.RUnlock()
	}
	return
}

func (e *Engine) SetDocumentMetadata(docID, title, url, body string) {
	canonicalID := e.ResolveURL(docID)
	shard := e.getShard(canonicalID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.titles[canonicalID] = title
	shard.urls[canonicalID] = url
	shard.bodies[canonicalID] = body
}

func (e *Engine) GetMetadataMaps() (titles, urls, bodies map[string]string) {
	titles = make(map[string]string)
	urls = make(map[string]string)
	bodies = make(map[string]string)

	for i := 0; i < 64; i++ {
		e.shards[i].mu.RLock()
		for k, v := range e.shards[i].titles {
			titles[k] = v
		}
		for k, v := range e.shards[i].urls {
			urls[k] = v
		}
		for k, v := range e.shards[i].bodies {
			bodies[k] = v
		}
		e.shards[i].mu.RUnlock()
	}

	return titles, urls, bodies
}

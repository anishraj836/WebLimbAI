package index

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

// Metadata Sharding & Alias Resolution
//
// Every per-document structure in the engine (metadata, inverted index,
// vectors) is split into N shards, N a power of two (default 64). A document
// ID is mapped to its shard with 32-bit FNV-1a, masked to N. This is lock
// striping: concurrent crawl workers indexing different documents take
// different locks, so there is no single global write lock on the indexing
// path. The shard count is fixed for the lifetime of an index; the index is
// rebuilt from storage on start-up, so changing it between runs is free.

// DefaultIndexShards is the shard count used when none is configured.
const DefaultIndexShards = 64

const maxIndexShards = 1024

// IndexShardsEnv overrides the shard count for engines created without
// WithIndexShards (including GlobalEngine). Values are rounded up to a power
// of two and capped at 1024.
const IndexShardsEnv = "LIGHTLIMBS_INDEX_SHARDS"

func normalizeShardCount(n int) int {
	if n <= 0 {
		return DefaultIndexShards
	}
	if n > maxIndexShards {
		n = maxIndexShards
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func shardCountFromEnv() int {
	if v := strings.TrimSpace(os.Getenv(IndexShardsEnv)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return normalizeShardCount(n)
		}
	}
	return DefaultIndexShards
}

// fnv1a32 is the 32-bit FNV-1a hash used to pick a document's shard.
func fnv1a32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

type MetadataShard struct {
	mu     sync.RWMutex
	titles map[string]string
	urls   map[string]string
	bodies map[string]string
	chunks map[string][]string // parent URL -> chunk IDs owned by it
}

func newMetadataShards(n int) []MetadataShard {
	shards := make([]MetadataShard, n)
	for i := range shards {
		shards[i].titles = make(map[string]string)
		shards[i].urls = make(map[string]string)
		shards[i].bodies = make(map[string]string)
		shards[i].chunks = make(map[string][]string)
	}
	return shards
}

func (e *Engine) shardIndex(docID string) int {
	return int(fnv1a32(docID) & e.shardMask)
}

func (e *Engine) getShard(docID string) *MetadataShard {
	return &e.shards[e.shardIndex(docID)]
}

// ShardCount returns the number of lock stripes used by this engine.
func (e *Engine) ShardCount() int {
	return len(e.shards)
}

// putMetadata stores one document's metadata under its shard lock.
func (e *Engine) putMetadata(docID, title, url, body string) {
	shard := e.getShard(docID)
	shard.mu.Lock()
	shard.titles[docID] = title
	shard.urls[docID] = url
	if body != "" {
		shard.bodies[docID] = body
	} else {
		delete(shard.bodies, docID)
	}
	shard.mu.Unlock()
	e.metaVersion.Add(1)
}

// deleteMetadata removes one document's metadata and returns the chunk IDs
// it owned, if it was a parent page.
func (e *Engine) deleteMetadata(docID string) []string {
	shard := e.getShard(docID)
	shard.mu.Lock()
	delete(shard.titles, docID)
	delete(shard.urls, docID)
	delete(shard.bodies, docID)
	chunks := shard.chunks[docID]
	delete(shard.chunks, docID)
	shard.mu.Unlock()
	e.metaVersion.Add(1)
	return chunks
}

func (e *Engine) setChunkIDs(parentURL string, chunkIDs []string) {
	shard := e.getShard(parentURL)
	shard.mu.Lock()
	shard.chunks[parentURL] = chunkIDs
	shard.mu.Unlock()
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
	shard.titles[canonicalID] = title
	shard.urls[canonicalID] = url
	shard.bodies[canonicalID] = body
	shard.mu.Unlock()
	e.metaVersion.Add(1)
}

// GetMetadataMaps returns merged title, URL and body maps for all documents.
//
// The maps are a cached, read-only view: callers must not modify them. The
// view is rebuilt only after the metadata changes, so repeated searches over
// an idle index don't copy every document on every query.
func (e *Engine) GetMetadataMaps() (titles, urls, bodies map[string]string) {
	version := e.metaVersion.Load()

	e.metaCacheMu.Lock()
	defer e.metaCacheMu.Unlock()
	if c := e.metaCache; c != nil && c.version == version {
		return c.titles, c.urls, c.bodies
	}

	titles = make(map[string]string)
	urls = make(map[string]string)
	bodies = make(map[string]string)
	for i := range e.shards {
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
	// Labelled with the version read before the copy: if a write landed
	// during the copy, the version has moved on and the next call rebuilds.
	e.metaCache = &metadataView{version: version, titles: titles, urls: urls, bodies: bodies}
	return titles, urls, bodies
}

type metadataView struct {
	version uint64
	titles  map[string]string
	urls    map[string]string
	bodies  map[string]string
}

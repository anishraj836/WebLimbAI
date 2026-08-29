package index

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/crawler-monorepo/common/stemmer"
	"github.com/crawler-monorepo/common/stopwords"
	"github.com/crawler-monorepo/internal/storage"
	"github.com/crawler-monorepo/internal/chunker"
)

// Compile-time interface compliance assertions
var (
	_ Searcher       = (*Engine)(nil)
	_ MetadataReader = (*Engine)(nil)
	_ VectorStore    = (*VectorIndex)(nil)
	_ Autocompleter  = (*Trie)(nil)
)

// Engine is the central hybrid search and indexing engine.
type Engine struct {
	mu             sync.RWMutex
	shards         [64]MetadataShard
	Inverted       *InvertedIndex
	Trie           *Trie
	Vector         *VectorIndex
	ActiveEmbedder Embedder
	aliasesMu      sync.RWMutex
	aliases        map[string]string
	parentChunks   map[string][]string
	chunkOpts      chunker.ChunkingOptions
}

// IndexEngine is a type alias for Engine.
type IndexEngine = Engine

// GlobalEngine is the singleton default Engine instance.
var GlobalEngine = NewEngine()

// Option represents a functional configuration option for Engine.
type Option func(*Engine)

// WithEmbedder configures a custom Embedder for the engine.
func WithEmbedder(emb Embedder) Option {
	return func(e *Engine) {
		if emb != nil {
			e.ActiveEmbedder = emb
			e.Vector = NewVectorIndex(emb.Dimensions())
		}
	}
}

// WithDimensions sets the vector index dimensions for the engine.
func WithDimensions(dim int) Option {
	return func(e *Engine) {
		if dim > 0 {
			e.Vector = NewVectorIndex(dim)
		}
	}
}

func WithChunkingOptions(opts chunker.ChunkingOptions) Option {
	return func(e *Engine) {
		e.chunkOpts = opts
	}
}

// NewEngine initializes and returns an Engine with optional functional configurations.
func NewEngine(opts ...Option) *Engine {
	active := NewEmbedderFromEnv()
	e := &Engine{
		Inverted:       NewInvertedIndex(),
		Trie:           NewTrie(),
		Vector:         NewVectorIndex(active.Dimensions()),
		ActiveEmbedder: active,
		aliases:        make(map[string]string),
		parentChunks:   make(map[string][]string),
		chunkOpts:      chunker.DefaultChunkingOptions(),
	}
	for i := 0; i < 64; i++ {
		e.shards[i].titles = make(map[string]string)
		e.shards[i].urls = make(map[string]string)
		e.shards[i].bodies = make(map[string]string)
	}

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}

// NewIndexEngine creates a new Engine instance.
func NewIndexEngine(opts ...Option) *Engine {
	return NewEngine(opts...)
}

func (e *Engine) IndexDocument(url, title, cleanBody string, termPositions map[string][]int, totalTokens int) {
	e.IndexDocumentWithSource(url, title, cleanBody, termPositions, totalTokens, "web_crawled", url)
}

func (e *Engine) IndexDocumentWithSource(url, title, cleanBody string, termPositions map[string][]int, totalTokens int, sourceType, sourceURL string) {
	e.DeleteDocument(url) // Purge old first (atomic replacement)

	docID := url

	shard := e.getShard(docID)
	shard.mu.Lock()
	shard.titles[docID] = title
	shard.urls[docID] = url
	shard.bodies[docID] = cleanBody
	shard.mu.Unlock()

	e.mu.RLock()
	e.Inverted.AddDocument(docID, termPositions, totalTokens)
	for term, positions := range termPositions {
		e.Trie.Insert(term, len(positions))
	}
	e.mu.RUnlock()

	e.IndexDocumentVector(docID, title, cleanBody)

	// Chunking
	e.mu.RLock()
	opts := e.chunkOpts
	e.mu.RUnlock()

	chunks := chunker.ChunkDocument(url, title, cleanBody, opts)
	
	var chunkIDs []string
	for _, c := range chunks {
		chunkIDs = append(chunkIDs, c.ID)
	}
	
	e.mu.Lock()
	if e.parentChunks == nil {
		e.parentChunks = make(map[string][]string)
	}
	e.parentChunks[url] = chunkIDs
	e.mu.Unlock()

	for _, c := range chunks {
		cShard := e.getShard(c.ID)
		cShard.mu.Lock()
		cShard.titles[c.ID] = title + " (Chunk " + fmt.Sprint(c.ChunkIndex) + ")"
		cShard.urls[c.ID] = url
		cShard.mu.Unlock()
		
		cRawTokens := strings.Fields(strings.ToLower(c.Content))
		cTermPositions := make(map[string][]int)
		for idx, raw := range cRawTokens {
			clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
			if clean == "" || stopwords.IsStopword(clean) {
				continue
			}
			stemmed := stemmer.Stem(clean)
			cTermPositions[stemmed] = append(cTermPositions[stemmed], idx)
		}
		
		e.mu.RLock()
		e.Inverted.AddDocument(c.ID, cTermPositions, c.Tokens)
		for term, positions := range cTermPositions {
			e.Trie.Insert(term, len(positions))
		}
		e.mu.RUnlock()
		
		e.IndexDocumentVector(c.ID, title, c.Content)
	}
	
	_ = storage.SaveCrawledDocument(context.Background(), url, title, cleanBody, totalTokens, sourceType, sourceURL)
}

func (e *Engine) IndexDocumentVector(docID, title, body string) {
	e.mu.RLock()
	embedder := e.ActiveEmbedder
	vectorIdx := e.Vector
	e.mu.RUnlock()

	if embedder == nil || vectorIdx == nil {
		return
	}

	vec, err := embedder.Embed(context.Background(), title+" "+body)
	if err != nil || len(vec) == 0 {
		return
	}
	_ = vectorIdx.AddVector(docID, vec)
}

func (e *Engine) LoadFromDB(ctx context.Context) error {
	docs, err := storage.GetCrawledDocuments(ctx)
	if err != nil {
		return err
	}

	newInv := NewInvertedIndex()
	newTrie := NewTrie()

	e.mu.RLock()
	opts := e.chunkOpts
	dim := 384
	if e.Vector != nil && e.Vector.dimensions > 0 {
		dim = e.Vector.dimensions
	} else if e.ActiveEmbedder != nil {
		dim = e.ActiveEmbedder.Dimensions()
	}
	embedder := e.ActiveEmbedder
	e.mu.RUnlock()

	newVector := NewVectorIndex(dim)
	newParentChunks := make(map[string][]string)

	var newShards [64]MetadataShard
	for i := 0; i < 64; i++ {
		newShards[i].titles = make(map[string]string)
		newShards[i].urls = make(map[string]string)
		newShards[i].bodies = make(map[string]string)
	}

	for _, d := range docs {
		rawTokens := strings.Fields(strings.ToLower(d.CleanBody))
		termPositions := make(map[string][]int)
		for idx, raw := range rawTokens {
			clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
			if clean == "" || stopwords.IsStopword(clean) {
				continue
			}
			stemmed := stemmer.Stem(clean)
			termPositions[stemmed] = append(termPositions[stemmed], idx)
		}

		sIdx := getShardIndex(d.URL)
		newShards[sIdx].titles[d.URL] = d.Title
		newShards[sIdx].urls[d.URL] = d.URL
		newShards[sIdx].bodies[d.URL] = d.CleanBody

		totalTokens := d.TotalTokens
		if totalTokens <= 0 {
			totalTokens = len(d.CleanBody) / 4
		}
		newInv.AddDocument(d.URL, termPositions, totalTokens)
		for term, positions := range termPositions {
			newTrie.Insert(term, len(positions))
		}

		if embedder != nil {
			vec, err := embedder.Embed(ctx, d.Title+" "+d.CleanBody)
			if err == nil && len(vec) > 0 {
				_ = newVector.AddVector(d.URL, vec)
			}
		}
		
		// Chunking during load
		chunks := chunker.ChunkDocument(d.URL, d.Title, d.CleanBody, opts)
		
		var chunkIDs []string
		for _, c := range chunks {
			chunkIDs = append(chunkIDs, c.ID)
			
			cShardIdx := getShardIndex(c.ID)
			newShards[cShardIdx].titles[c.ID] = d.Title + " (Chunk " + fmt.Sprint(c.ChunkIndex) + ")"
			newShards[cShardIdx].urls[c.ID] = d.URL
			
			cRawTokens := strings.Fields(strings.ToLower(c.Content))
			cTermPositions := make(map[string][]int)
			for idx, raw := range cRawTokens {
				clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
				if clean == "" || stopwords.IsStopword(clean) {
					continue
				}
				stemmed := stemmer.Stem(clean)
				cTermPositions[stemmed] = append(cTermPositions[stemmed], idx)
			}
			
			newInv.AddDocument(c.ID, cTermPositions, c.Tokens)
			for term, positions := range cTermPositions {
				newTrie.Insert(term, len(positions))
			}
			
			if embedder != nil {
				cvec, err := embedder.Embed(ctx, d.Title+" "+c.Content)
				if err == nil && len(cvec) > 0 {
					_ = newVector.AddVector(c.ID, cvec)
				}
			}
		}
		newParentChunks[d.URL] = chunkIDs

	}

	e.mu.Lock()
	e.parentChunks = newParentChunks
	e.Inverted = newInv
	e.Trie = newTrie
	e.Vector = newVector
	for i := 0; i < 64; i++ {
		e.shards[i].mu.Lock()
		e.shards[i].titles = newShards[i].titles
		e.shards[i].urls = newShards[i].urls
		e.shards[i].bodies = newShards[i].bodies
		e.shards[i].mu.Unlock()
	}
	e.mu.Unlock()

	return nil
}

func (e *Engine) StartTTLJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				_, _ = storage.DeleteExpiredDocuments(ctx)
				_ = e.LoadFromDB(ctx)
			}
		}
	}()
}

func (e *Engine) IndexDocumentIncrementalByURL(ctx context.Context, targetURL string) error {
	canonicalURL := e.ResolveURL(targetURL)
	doc, err := storage.GetCrawledDocumentByURL(ctx, canonicalURL)
	if err != nil {
		return err
	}
	if doc == nil {
		return fmt.Errorf("document not found for url: %s", targetURL)
	}

	rawTokens := strings.Fields(strings.ToLower(doc.CleanBody))
	termPositions := make(map[string][]int)
	for idx, raw := range rawTokens {
		clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
		if clean == "" || stopwords.IsStopword(clean) {
			continue
		}
		stemmed := stemmer.Stem(clean)
		termPositions[stemmed] = append(termPositions[stemmed], idx)
	}

	shard := e.getShard(doc.URL)
	shard.mu.Lock()
	shard.titles[doc.URL] = doc.Title
	shard.urls[doc.URL] = doc.URL
	shard.bodies[doc.URL] = doc.CleanBody
	shard.mu.Unlock()

	e.mu.RLock()
	inv := e.Inverted
	trie := e.Trie
	e.mu.RUnlock()

	inv.AddDocument(doc.URL, termPositions, doc.TotalTokens)
	for term, positions := range termPositions {
		trie.Insert(term, len(positions))
	}
	e.IndexDocumentVector(doc.URL, doc.Title, doc.CleanBody)

	return nil
}

func (e *Engine) IndexDocumentDirectly(docURL, title, cleanBody string, totalTokens int, aliasURL ...string) {
	if docURL == "" || cleanBody == "" {
		return
	}
	if len(aliasURL) > 0 && aliasURL[0] != "" && aliasURL[0] != docURL {
		e.AddAlias(aliasURL[0], docURL)
		_ = storage.SaveURLAlias(context.Background(), aliasURL[0], docURL)
	}
	if totalTokens <= 0 {
		totalTokens = len(cleanBody) / 4
	}

	rawTokens := strings.Fields(strings.ToLower(cleanBody))
	termPositions := make(map[string][]int)
	for idx, raw := range rawTokens {
		clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
		if clean == "" || stopwords.IsStopword(clean) {
			continue
		}
		stemmed := stemmer.Stem(clean)
		termPositions[stemmed] = append(termPositions[stemmed], idx)
	}

	shard := e.getShard(docURL)
	shard.mu.Lock()
	shard.titles[docURL] = title
	shard.urls[docURL] = docURL
	shard.bodies[docURL] = cleanBody
	shard.mu.Unlock()

	e.mu.RLock()
	inv := e.Inverted
	trie := e.Trie
	e.mu.RUnlock()

	inv.AddDocument(docURL, termPositions, totalTokens)
	for term, positions := range termPositions {
		trie.Insert(term, len(positions))
	}
	e.IndexDocumentVector(docURL, title, cleanBody)
}

func (e *Engine) GetInvertedIndex() *InvertedIndex {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Inverted
}

func (e *Engine) GetTrie() *Trie {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Trie
}

func (e *Engine) GetVectorIndex() *VectorIndex {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Vector
}

func (e *Engine) SearchBM25(query string, topK int) []SearchHit {
	titles, urls, bodies := e.GetMetadataMaps()
	e.mu.RLock()
	inv := e.Inverted
	e.mu.RUnlock()
	return RankDocuments(query, inv, titles, urls, bodies, topK)
}

func (e *Engine) SearchVector(query string, topK int) []VectorSearchResult {
	e.mu.RLock()
	embedder := e.ActiveEmbedder
	vectorIdx := e.Vector
	e.mu.RUnlock()

	if embedder == nil || vectorIdx == nil {
		return nil
	}
	queryVec, err := embedder.Embed(context.Background(), query)
	if err != nil || len(queryVec) == 0 {
		return nil
	}
	return vectorIdx.SearchNearest(queryVec, topK)
}

// DeleteDocument removes a document from the local metadata shards, inverted index, and vector index.
func (e *Engine) DeleteDocument(docURL string) {
	e.mu.Lock()
	chunks := e.parentChunks[docURL]
	delete(e.parentChunks, docURL)
	e.mu.Unlock()

	shard := e.getShard(docURL)
	shard.mu.Lock()
	delete(shard.titles, docURL)
	delete(shard.urls, docURL)
	delete(shard.bodies, docURL)
	shard.mu.Unlock()

	e.mu.RLock()
	inv := e.Inverted
	vectorIdx := e.Vector
	e.mu.RUnlock()

	if inv != nil {
		inv.DeleteDocument(docURL)
		for _, cID := range chunks {
			inv.DeleteDocument(cID)
		}
	}
	if vectorIdx != nil {
		vectorIdx.DeleteVector(docURL)
		for _, cID := range chunks {
			vectorIdx.DeleteVector(cID)
		}
	}
	
	// Also delete chunks from shards
	for _, cID := range chunks {
		cShard := e.getShard(cID)
		cShard.mu.Lock()
		delete(cShard.titles, cID)
		delete(cShard.urls, cID)
		delete(cShard.bodies, cID)
		cShard.mu.Unlock()
	}
}


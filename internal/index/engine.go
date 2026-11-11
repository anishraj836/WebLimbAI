package index

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	shards         []MetadataShard
	shardMask      uint32
	Inverted       *InvertedIndex
	Trie           *Trie
	Vector         *VectorIndex
	ActiveEmbedder Embedder
	aliasesMu      sync.RWMutex
	aliases        map[string]string
	chunkOpts      chunker.ChunkingOptions

	// Cached merged view of the metadata shards (see GetMetadataMaps).
	metaVersion atomic.Uint64
	metaCacheMu sync.Mutex
	metaCache   *metadataView
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
			e.Vector = NewVectorIndexWithShards(emb.Dimensions(), PrecisionFloat32, e.ShardCount())
		}
	}
}

// WithDimensions sets the vector index dimensions for the engine.
func WithDimensions(dim int) Option {
	return func(e *Engine) {
		if dim > 0 {
			e.Vector = NewVectorIndexWithShards(dim, PrecisionFloat32, e.ShardCount())
		}
	}
}

// WithIndexShards sets the number of lock stripes (rounded up to a power of
// two, default 64) for the metadata, inverted index, trie and vector index.
// It resets the engine to empty, so it belongs in NewEngine's options.
func WithIndexShards(n int) Option {
	return func(e *Engine) {
		e.resetShards(normalizeShardCount(n))
	}
}

// resetShards replaces every per-document structure with empty ones split
// into n stripes, keeping the vector dimensions and precision.
func (e *Engine) resetShards(n int) {
	dim, prec := 0, PrecisionFloat32
	if e.Vector != nil {
		dim, prec = e.Vector.Dimensions(), e.Vector.GetPrecision()
	} else if e.ActiveEmbedder != nil {
		dim = e.ActiveEmbedder.Dimensions()
	}
	e.shards = newMetadataShards(n)
	e.shardMask = uint32(n - 1)
	e.Inverted = NewInvertedIndexWithShards(n)
	e.Trie = NewTrieWithShards(n)
	e.Vector = NewVectorIndexWithShards(dim, prec, n)
	e.metaVersion.Add(1)
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
		ActiveEmbedder: active,
		aliases:        make(map[string]string),
		chunkOpts:      chunker.DefaultChunkingOptions(),
	}
	e.resetShards(shardCountFromEnv())

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
	e.putMetadata(docID, title, url, cleanBody)

	// e.mu is only read-locked here: it keeps LoadFromDB from swapping the
	// indexes mid-write. Writers don't exclude each other on it; they only
	// contend on the shard that owns their document.
	e.mu.RLock()
	e.Inverted.AddDocument(docID, termPositions, totalTokens)
	for term, positions := range termPositions {
		e.Trie.Insert(term, len(positions))
	}
	opts := e.chunkOpts
	e.mu.RUnlock()

	e.IndexDocumentVector(docID, title, cleanBody)

	chunks := chunker.ChunkDocument(url, title, cleanBody, opts)
	chunkIDs := make([]string, 0, len(chunks))
	for _, c := range chunks {
		chunkIDs = append(chunkIDs, c.ID)
	}
	e.setChunkIDs(url, chunkIDs)

	for _, c := range chunks {
		e.putMetadata(c.ID, title+" (Chunk "+fmt.Sprint(c.ChunkIndex)+")", url, "")

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

	e.mu.RLock()
	n := len(e.shards)
	opts := e.chunkOpts
	dim := 384
	if e.Vector != nil && e.Vector.dimensions > 0 {
		dim = e.Vector.dimensions
	} else if e.ActiveEmbedder != nil {
		dim = e.ActiveEmbedder.Dimensions()
	}
	embedder := e.ActiveEmbedder
	e.mu.RUnlock()

	newInv := NewInvertedIndexWithShards(n)
	newTrie := NewTrieWithShards(n)
	newVector := NewVectorIndexWithShards(dim, PrecisionFloat32, n)
	newShards := newMetadataShards(n)
	mask := uint32(n - 1)

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

		sIdx := fnv1a32(d.URL) & mask
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
			
			cShardIdx := fnv1a32(c.ID) & mask
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
		newShards[sIdx].chunks[d.URL] = chunkIDs

	}

	e.mu.Lock()
	e.Inverted = newInv
	e.Trie = newTrie
	e.Vector = newVector
	for i := range e.shards {
		e.shards[i].mu.Lock()
		e.shards[i].titles = newShards[i].titles
		e.shards[i].urls = newShards[i].urls
		e.shards[i].bodies = newShards[i].bodies
		e.shards[i].chunks = newShards[i].chunks
		e.shards[i].mu.Unlock()
	}
	e.mu.Unlock()
	e.metaVersion.Add(1)

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

	e.putMetadata(doc.URL, doc.Title, doc.URL, doc.CleanBody)

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

	e.putMetadata(docURL, title, docURL, cleanBody)

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

// DeleteDocument removes a document and its chunks from the metadata shards,
// inverted index and vector index. Each removal locks only the owning shard.
func (e *Engine) DeleteDocument(docURL string) {
	chunks := e.deleteMetadata(docURL)

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
	for _, cID := range chunks {
		e.deleteMetadata(cID)
	}
}

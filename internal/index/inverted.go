package index

import (
	"bytes"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// Inverted Index
//
// The index is document-partitioned: each document's postings live in one of
// N shards chosen by FNV-1a(docID). Writers lock only the shard that owns the
// document, so concurrent crawl workers indexing different pages don't share a
// lock. BM25 needs corpus-wide statistics (document count, total length and
// per-term document frequency); counts are kept in atomics and document
// frequency is summed across shards at query time, so scores are identical to
// a single unsharded index.

type PostingEntry struct {
	DocID         string `json:"doc_id"`
	TermFrequency int    `json:"term_frequency"`
	Positions     []int  `json:"positions"`
}

type PostingList struct {
	Term              string         `json:"term"`
	DocumentFrequency int            `json:"document_frequency"`
	Entries           []PostingEntry `json:"entries"`
	docIndex          map[string]int
}

type invShard struct {
	mu         sync.RWMutex
	postings   map[string]*PostingList
	docLengths map[string]int
	docTerms   map[string][]string // docID -> terms it contributed, for O(terms) deletes
}

type InvertedIndex struct {
	shards         []invShard
	mask           uint32
	totalDocLength atomic.Int64
	totalDocuments atomic.Int64
}

func NewInvertedIndex() *InvertedIndex {
	return NewInvertedIndexWithShards(DefaultIndexShards)
}

// NewInvertedIndexWithShards creates an index split into n lock stripes
// (rounded up to a power of two).
func NewInvertedIndexWithShards(n int) *InvertedIndex {
	n = normalizeShardCount(n)
	idx := &InvertedIndex{
		shards: make([]invShard, n),
		mask:   uint32(n - 1),
	}
	for i := range idx.shards {
		idx.shards[i].reset()
	}
	return idx
}

func (s *invShard) reset() {
	s.postings = make(map[string]*PostingList)
	s.docLengths = make(map[string]int)
	s.docTerms = make(map[string][]string)
}

// ShardCount returns the number of lock stripes.
func (idx *InvertedIndex) ShardCount() int {
	return len(idx.shards)
}

func (idx *InvertedIndex) shardFor(docID string) *invShard {
	return &idx.shards[fnv1a32(docID)&idx.mask]
}

func (pl *PostingList) ensureDocIndex() {
	if pl.docIndex == nil {
		pl.docIndex = make(map[string]int, len(pl.Entries))
		for i, entry := range pl.Entries {
			pl.docIndex[entry.DocID] = i
		}
	}
}

// removeDoc deletes docID from the posting list in O(1) by swapping the last
// entry into its slot. Entry order carries no meaning for scoring.
func (pl *PostingList) removeDoc(docID string) {
	pl.ensureDocIndex()
	pos, found := pl.docIndex[docID]
	if !found {
		return
	}
	last := len(pl.Entries) - 1
	if pos != last {
		pl.Entries[pos] = pl.Entries[last]
		pl.docIndex[pl.Entries[pos].DocID] = pos
	}
	pl.Entries[last] = PostingEntry{}
	pl.Entries = pl.Entries[:last]
	delete(pl.docIndex, docID)
	pl.DocumentFrequency = len(pl.Entries)
}

// removeLocked drops docID from the shard. Caller holds s.mu for writing.
func (idx *InvertedIndex) removeLocked(s *invShard, docID string) bool {
	length, exists := s.docLengths[docID]
	if !exists {
		return false
	}
	for _, term := range s.docTerms[docID] {
		if pl, ok := s.postings[term]; ok {
			pl.removeDoc(docID)
			if len(pl.Entries) == 0 {
				delete(s.postings, term)
			}
		}
	}
	delete(s.docTerms, docID)
	delete(s.docLengths, docID)
	idx.totalDocuments.Add(-1)
	idx.totalDocLength.Add(-int64(length))
	return true
}

// AddDocument indexes (or replaces) a document. Only the owning shard is locked.
func (idx *InvertedIndex) AddDocument(docID string, termPositions map[string][]int, docLength int) {
	s := idx.shardFor(docID)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Replace semantics: drop the previous version so terms that disappeared
	// from the document don't leave stale postings behind.
	idx.removeLocked(s, docID)

	s.docLengths[docID] = docLength
	idx.totalDocuments.Add(1)
	idx.totalDocLength.Add(int64(docLength))

	terms := make([]string, 0, len(termPositions))
	for term, positions := range termPositions {
		pl, exists := s.postings[term]
		if !exists {
			pl = &PostingList{
				Term:     term,
				Entries:  make([]PostingEntry, 0, 1),
				docIndex: make(map[string]int),
			}
			s.postings[term] = pl
		}
		pl.ensureDocIndex()
		pl.docIndex[docID] = len(pl.Entries)
		pl.Entries = append(pl.Entries, PostingEntry{
			DocID:         docID,
			TermFrequency: len(positions),
			Positions:     positions,
		})
		pl.DocumentFrequency = len(pl.Entries)
		terms = append(terms, term)
	}
	s.docTerms[docID] = terms
}

// GetPostingList returns a copy of a term's postings merged across shards.
func (idx *InvertedIndex) GetPostingList(term string) (*PostingList, bool) {
	cp := &PostingList{Term: term}
	found := false
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		if pl, ok := s.postings[term]; ok {
			found = true
			for _, entry := range pl.Entries {
				var posCopy []int
				if entry.Positions != nil {
					posCopy = make([]int, len(entry.Positions))
					copy(posCopy, entry.Positions)
				}
				cp.Entries = append(cp.Entries, PostingEntry{
					DocID:         entry.DocID,
					TermFrequency: entry.TermFrequency,
					Positions:     posCopy,
				})
			}
		}
		s.mu.RUnlock()
	}
	if !found {
		return nil, false
	}
	cp.DocumentFrequency = len(cp.Entries)
	return cp, true
}

// DocumentFrequency returns how many documents contain term, across all shards.
func (idx *InvertedIndex) DocumentFrequency(term string) int {
	df := 0
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		if pl, ok := s.postings[term]; ok {
			df += len(pl.Entries)
		}
		s.mu.RUnlock()
	}
	return df
}

func (idx *InvertedIndex) GetDocLength(docID string) int {
	s := idx.shardFor(docID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.docLengths[docID]
}

func (idx *InvertedIndex) GetStats() (totalDocs int64, avgDocLength float64, vocabularySize int) {
	totalDocs = idx.totalDocuments.Load()
	if totalDocs > 0 {
		avgDocLength = float64(idx.totalDocLength.Load()) / float64(totalDocs)
	}
	vocab := make(map[string]struct{})
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for term := range s.postings {
			vocab[term] = struct{}{}
		}
		s.mu.RUnlock()
	}
	vocabularySize = len(vocab)
	return
}

// scoreTerms computes BM25 scores and match counts for the given stemmed
// query terms. Document frequencies are summed across shards first, then each
// shard is scored under its own read lock.
func (idx *InvertedIndex) scoreTerms(terms []string) (map[string]float64, map[string]int) {
	totalDocs := idx.totalDocuments.Load()
	if totalDocs <= 0 {
		return nil, nil
	}
	avgDocLen := float64(idx.totalDocLength.Load()) / float64(totalDocs)

	idfs := make([]float64, len(terms))
	for t, term := range terms {
		idfs[t] = ComputeIDF(totalDocs, idx.DocumentFrequency(term))
	}

	docScores := make(map[string]float64)
	docMatchCounts := make(map[string]int)
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for t, term := range terms {
			pl, ok := s.postings[term]
			if !ok || idfs[t] <= 0 {
				continue
			}
			for _, entry := range pl.Entries {
				docLen := s.docLengths[entry.DocID]
				docScores[entry.DocID] += ComputeBM25Score(entry.TermFrequency, docLen, avgDocLen, idfs[t])
				docMatchCounts[entry.DocID]++
			}
		}
		s.mu.RUnlock()
	}
	return docScores, docMatchCounts
}

type indexSnapshot struct {
	Postings       map[string]*PostingList `json:"postings"`
	DocLengths     map[string]int          `json:"doc_lengths"`
	TotalDocLength int64                   `json:"total_doc_length"`
	TotalDocuments int64                   `json:"total_documents"`
}

// SaveSnapshot writes the index in the same single-index JSON format as
// before sharding, so existing snapshot files stay compatible.
func (idx *InvertedIndex) SaveSnapshot(filePath string) error {
	docLengthsCopy := make(map[string]int)
	postingsCopy := make(map[string]*PostingList)
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for k, v := range s.docLengths {
			docLengthsCopy[k] = v
		}
		for term, pl := range s.postings {
			merged, ok := postingsCopy[term]
			if !ok {
				merged = &PostingList{Term: term}
				postingsCopy[term] = merged
			}
			for _, entry := range pl.Entries {
				var posCopy []int
				if entry.Positions != nil {
					posCopy = make([]int, len(entry.Positions))
					copy(posCopy, entry.Positions)
				}
				merged.Entries = append(merged.Entries, PostingEntry{
					DocID:         entry.DocID,
					TermFrequency: entry.TermFrequency,
					Positions:     posCopy,
				})
			}
		}
		s.mu.RUnlock()
	}
	for _, pl := range postingsCopy {
		sort.Slice(pl.Entries, func(a, b int) bool { return pl.Entries[a].DocID < pl.Entries[b].DocID })
		pl.DocumentFrequency = len(pl.Entries)
	}

	snap := indexSnapshot{
		Postings:       postingsCopy,
		DocLengths:     docLengthsCopy,
		TotalDocLength: idx.totalDocLength.Load(),
		TotalDocuments: idx.totalDocuments.Load(),
	}

	tmpPath := filePath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if tmpFile != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if err := json.NewEncoder(tmpFile).Encode(snap); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	tmpFile = nil

	return os.Rename(tmpPath, filePath)
}

// lockAll / unlockAll take every shard's write lock, for whole-index swaps.
func (idx *InvertedIndex) lockAll() {
	for i := range idx.shards {
		idx.shards[i].mu.Lock()
	}
}

func (idx *InvertedIndex) unlockAll() {
	for i := len(idx.shards) - 1; i >= 0; i-- {
		idx.shards[i].mu.Unlock()
	}
}

func (idx *InvertedIndex) LoadSnapshot(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	var snap indexSnapshot
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
	}

	idx.lockAll()
	defer idx.unlockAll()

	for i := range idx.shards {
		idx.shards[i].reset()
	}
	for docID, length := range snap.DocLengths {
		idx.shardFor(docID).docLengths[docID] = length
	}
	for term, pl := range snap.Postings {
		if pl == nil {
			continue
		}
		for _, entry := range pl.Entries {
			s := idx.shardFor(entry.DocID)
			spl, ok := s.postings[term]
			if !ok {
				spl = &PostingList{Term: term, docIndex: make(map[string]int)}
				s.postings[term] = spl
			}
			spl.docIndex[entry.DocID] = len(spl.Entries)
			spl.Entries = append(spl.Entries, entry)
			spl.DocumentFrequency = len(spl.Entries)
			s.docTerms[entry.DocID] = append(s.docTerms[entry.DocID], term)
		}
	}
	idx.totalDocLength.Store(snap.TotalDocLength)
	idx.totalDocuments.Store(snap.TotalDocuments)
	return nil
}

// DeleteDocument removes a document's postings. It touches only the owning
// shard and only the terms the document contained, instead of scanning the
// whole vocabulary.
func (idx *InvertedIndex) DeleteDocument(docID string) {
	s := idx.shardFor(docID)
	s.mu.Lock()
	defer s.mu.Unlock()
	idx.removeLocked(s, docID)
}

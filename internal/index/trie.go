package index

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// Trie Autocomplete
//
// Terms are spread over N independent tries keyed by their first two runes,
// so inserts from concurrent indexing workers only contend when they touch
// terms with the same two-letter prefix bucket. A prefix of two or more runes
// is answered by one shard; a one-rune prefix is answered by merging all.

type TrieNode struct {
	children  map[rune]*TrieNode
	isEnd     bool
	frequency int
	term      string
}

func newTrieNode() *TrieNode {
	return &TrieNode{
		children: make(map[rune]*TrieNode),
	}
}

type AutocompleteResult struct {
	Term      string `json:"term"`
	Frequency int    `json:"frequency"`
}

type trieShard struct {
	mu   sync.RWMutex
	root *TrieNode
}

type Trie struct {
	shards    []trieShard
	mask      uint32
	nodeCount atomic.Int64
}

func NewTrie() *Trie {
	return NewTrieWithShards(DefaultIndexShards)
}

// NewTrieWithShards creates a trie split into n lock stripes.
func NewTrieWithShards(n int) *Trie {
	n = normalizeShardCount(n)
	t := &Trie{
		shards: make([]trieShard, n),
		mask:   uint32(n - 1),
	}
	for i := range t.shards {
		t.shards[i].root = newTrieNode()
	}
	return t
}

// shardKey returns the first two runes of term (or one, for a 1-rune term).
func shardKey(term string) string {
	r1, n1 := utf8.DecodeRuneInString(term)
	if r1 == utf8.RuneError && n1 == 0 {
		return ""
	}
	_, n2 := utf8.DecodeRuneInString(term[n1:])
	return term[:n1+n2]
}

func (t *Trie) shardFor(term string) *trieShard {
	return &t.shards[fnv1a32(shardKey(term))&t.mask]
}

func (t *Trie) Insert(term string, freq int) {
	term = strings.TrimSpace(strings.ToLower(term))
	if term == "" || utf8.RuneCountInString(term) > 64 {
		return
	}

	s := t.shardFor(term)
	s.mu.Lock()
	defer s.mu.Unlock()

	curr := s.root
	for _, ch := range term {
		next, exists := curr.children[ch]
		if !exists {
			next = newTrieNode()
			curr.children[ch] = next
			t.nodeCount.Add(1)
		}
		curr = next
	}
	curr.isEnd = true
	curr.frequency += freq
	curr.term = term
}

// collect gathers up to limit terms under prefix from one shard.
func (s *trieShard) collect(prefix string, limit int, out []AutocompleteResult) []AutocompleteResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	curr := s.root
	for _, ch := range prefix {
		next, exists := curr.children[ch]
		if !exists {
			return out
		}
		curr = next
	}

	var dfs func(node *TrieNode)
	dfs = func(node *TrieNode) {
		if node == nil || len(out) >= limit {
			return
		}
		if node.isEnd {
			out = append(out, AutocompleteResult{Term: node.term, Frequency: node.frequency})
			if len(out) >= limit {
				return
			}
		}
		for _, child := range node.children {
			dfs(child)
			if len(out) >= limit {
				return
			}
		}
	}
	dfs(curr)
	return out
}

func (t *Trie) SearchPrefix(prefix string, topK int) []AutocompleteResult {
	prefix = strings.TrimSpace(strings.ToLower(prefix))
	if prefix == "" || topK <= 0 {
		return nil
	}

	limit := topK * 10
	if limit < 1000 {
		limit = 1000
	}

	var results []AutocompleteResult
	if utf8.RuneCountInString(prefix) >= 2 {
		results = t.shardFor(prefix).collect(prefix, limit, nil)
	} else {
		for i := range t.shards {
			results = t.shards[i].collect(prefix, len(results)+limit, results)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Frequency == results[j].Frequency {
			return results[i].Term < results[j].Term
		}
		return results[i].Frequency > results[j].Frequency
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results
}

func (t *Trie) NodeCount() int {
	return int(t.nodeCount.Load())
}

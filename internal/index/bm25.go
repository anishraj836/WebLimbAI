package index

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/crawler-monorepo/common/stemmer"
	"github.com/crawler-monorepo/common/stopwords"
)

// BM25 Ranking Calculations (Robertson-Spärck Jones IDF math)

const (
	k1 = 1.2
	b  = 0.75
)

type SearchHit struct {
	DocID      string  `json:"doc_id"`
	Score      float64 `json:"score"`
	Title      string  `json:"title"`
	URL        string  `json:"url"`
	Snippet    string  `json:"snippet"`
	MatchCount int     `json:"match_count"`
}

func ComputeIDF(totalDocs int64, docFreq int) float64 {
	if totalDocs <= 0 || docFreq <= 0 {
		return 0.0
	}
	N := float64(totalDocs)
	n := float64(docFreq)
	num := N - n + 0.5
	den := n + 0.5
	if num < 0 {
		num = 0.5
	}
	return math.Log((num / den) + 1.0)
}

func ComputeBM25Score(tf int, docLen int, avgDocLen float64, idf float64) float64 {
	if tf <= 0 || idf <= 0 {
		return 0.0
	}
	if avgDocLen <= 0 {
		avgDocLen = 1.0
	}
	lenNorm := 1.0 - b + b*(float64(docLen)/avgDocLen)
	num := float64(tf) * (k1 + 1.0)
	den := float64(tf) + k1*lenNorm
	return idf * (num / den)
}

func (inv *InvertedIndex) RankDocuments(
	query string,
	docTitles map[string]string,
	docURLs map[string]string,
	docBodies map[string]string,
	topK int,
) []SearchHit {
	return RankDocuments(query, inv, docTitles, docURLs, docBodies, topK)
}

func RankDocuments(
	query string,
	invIndex *InvertedIndex,
	docTitles map[string]string,
	docURLs map[string]string,
	docBodies map[string]string,
	topK int,
) []SearchHit {
	if strings.TrimSpace(query) == "" || topK <= 0 {
		return nil
	}

	rawTokens := strings.Fields(strings.ToLower(query))
	var stemmedTokens []string
	var queryTerms []string

	for _, t := range rawTokens {
		t = strings.Trim(t, ".,!?:;\"'()[]{}")
		if t == "" || stopwords.IsStopword(t) {
			continue
		}
		stemmed := stemmer.Stem(t)
		stemmedTokens = append(stemmedTokens, stemmed)
		queryTerms = append(queryTerms, t)
	}

	if len(stemmedTokens) == 0 {
		return nil
	}

	invIndex.mu.RLock()
	totalDocs := invIndex.totalDocuments
	totalDocLength := invIndex.totalDocLength
	if totalDocs == 0 {
		invIndex.mu.RUnlock()
		return nil
	}
	avgDocLen := float64(totalDocLength) / float64(totalDocs)

	docScores := make(map[string]float64)
	docMatchCounts := make(map[string]int)

	for _, term := range stemmedTokens {
		pl, exists := invIndex.postings[term]
		if !exists {
			continue
		}

		idf := ComputeIDF(totalDocs, pl.DocumentFrequency)
		for _, entry := range pl.Entries {
			docLen := invIndex.docLengths[entry.DocID]
			score := ComputeBM25Score(entry.TermFrequency, docLen, avgDocLen, idf)
			docScores[entry.DocID] += score
			docMatchCounts[entry.DocID]++
		}
	}
	invIndex.mu.RUnlock()

	// Aggregate hits by base URL to eliminate duplicate chunk hits while retaining highest score and best snippet
	urlHits := make(map[string]SearchHit)
	for docID, score := range docScores {
		rawURL := docURLs[docID]
		baseURL := rawURL
		if baseURL == "" {
			baseURL = docID
		}
		if idx := strings.Index(baseURL, "#chunk"); idx != -1 {
			baseURL = baseURL[:idx]
		}

		body := docBodies[docID]
		if body == "" && docID != baseURL {
			body = docBodies[baseURL]
		}
		snippet := GenerateHighlightedSnippet(body, queryTerms, 180)

		title := docTitles[docID]
		if title == "" {
			title = docTitles[baseURL]
			if title == "" {
				title = baseURL
			}
		}

		roundedScore := math.Round(score*1000) / 1000
		existing, found := urlHits[baseURL]
		if !found || roundedScore > existing.Score {
			urlHits[baseURL] = SearchHit{
				DocID:      baseURL,
				Score:      roundedScore,
				Title:      title,
				URL:        baseURL,
				Snippet:    snippet,
				MatchCount: docMatchCounts[docID],
			}
		}
	}

	var hits []SearchHit
	for _, hit := range urlHits {
		hits = append(hits, hit)
	}

	sort.Slice(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})

	if len(hits) > topK {
		hits = hits[:topK]
	}

	return hits
}

// ExtractHighlightedSnippet extracts and highlights snippet matches in document text.
func ExtractHighlightedSnippet(body string, queryTerms []string, maxLen int) string {
	return GenerateHighlightedSnippet(body, queryTerms, maxLen)
}

func GenerateHighlightedSnippet(body string, queryTerms []string, maxLen int) string {
	if strings.TrimSpace(body) == "" {
		return ""
	}

	words := strings.Fields(body)
	if len(words) == 0 {
		return ""
	}

	matchIdx := 0
	lowerBody := strings.ToLower(body)
	firstMatchPos := len(lowerBody)

	for _, q := range queryTerms {
		pos := strings.Index(lowerBody, strings.ToLower(q))
		if pos != -1 && pos < firstMatchPos {
			firstMatchPos = pos
		}
	}

	if firstMatchPos != len(lowerBody) {
		runningLen := 0
		for i, w := range words {
			runningLen += len(w) + 1
			if runningLen >= firstMatchPos {
				matchIdx = i
				break
			}
		}
	}

	startIdx := matchIdx - 5
	if startIdx < 0 {
		startIdx = 0
	}
	endIdx := startIdx + 25
	if endIdx > len(words) {
		endIdx = len(words)
	}

	snippetWords := make([]string, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		w := words[i]
		cleanW := strings.Trim(strings.ToLower(w), ".,!?:;\"'()[]{}")
		matched := false
		for _, q := range queryTerms {
			if cleanW == strings.ToLower(q) || stemmer.Stem(cleanW) == stemmer.Stem(q) {
				matched = true
				break
			}
		}

		if matched {
			snippetWords = append(snippetWords, fmt.Sprintf("<mark>%s</mark>", w))
		} else {
			snippetWords = append(snippetWords, w)
		}
	}

	result := strings.Join(snippetWords, " ")
	if startIdx > 0 {
		result = "..." + result
	}
	if endIdx < len(words) {
		result = result + "..."
	}

	runes := []rune(result)
	if len(runes) > maxLen {
		cut := maxLen
		sub := string(runes[:cut])
		lastOpen := strings.LastIndex(sub, "<")
		lastClose := strings.LastIndex(sub, ">")
		if lastOpen > lastClose {
			cut = lastOpen
			sub = string(runes[:cut])
		}
		markOpenCount := strings.Count(sub, "<mark>")
		markCloseCount := strings.Count(sub, "</mark>")
		if markOpenCount > markCloseCount {
			sub += "</mark>"
		}
		result = sub + "..."
	}

	return result
}

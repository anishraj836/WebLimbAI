// Command token_savings_live measures how many cl100k_base BPE tokens the
// clean_rag extractor saves on real documentation pages.
//
//	go run ./scripts/benchmarks/token_savings_live            # default page set
//	go run ./scripts/benchmarks/token_savings_live <url> ...  # your own pages
//
// Raw and cleaned text are both counted with the same tokenizer. The command
// refuses to run if the tokenizer can't load, because the fallback counts
// words and would produce a meaningless percentage.
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/crawler-monorepo/internal/extractor"
)

var defaultPages = []string{
	"https://go.dev/doc/tutorial/getting-started",
	"https://go.dev/doc/effective_go",
	"https://docs.python.org/3/library/venv.html",
	"https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/map",
	"https://kubernetes.io/docs/concepts/overview/",
	"https://react.dev/learn",
	"https://docs.docker.com/get-started/docker-overview/",
	"https://www.postgresql.org/docs/current/tutorial-select.html",
}

type result struct {
	url        string
	rawBytes   int
	rawTokens  int
	cleanTok   int
	reduction  float64
	fetchError string
}

func main() {
	if err := extractor.TokenizerStatus(); err != nil {
		fmt.Fprintf(os.Stderr, "cl100k_base tokenizer unavailable: %v\nRefusing to report word counts as tokens.\n", err)
		os.Exit(1)
	}

	pages := defaultPages
	if len(os.Args) > 1 {
		pages = os.Args[1:]
	}

	client := &http.Client{Timeout: 20 * time.Second}
	var results []result
	for _, u := range pages {
		r := result{url: u}
		req, _ := http.NewRequest(http.MethodGet, u, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (token-savings benchmark)")
		resp, err := client.Do(req)
		if err != nil {
			r.fetchError = err.Error()
			results = append(results, r)
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			r.fetchError = fmt.Sprintf("status %d %v", resp.StatusCode, err)
			results = append(results, r)
			continue
		}
		md, _, _ := extractor.ConvertHTMLToMarkdown(u, raw, "clean_rag")
		r.rawBytes = len(raw)
		r.rawTokens = extractor.CountBPETokens(string(raw))
		r.cleanTok = extractor.CountBPETokens(md)
		if r.rawTokens > 0 {
			r.reduction = float64(r.rawTokens-r.cleanTok) / float64(r.rawTokens) * 100
		}
		results = append(results, r)
	}

	fmt.Println("Token savings: cl100k_base BPE tokens, raw HTML vs clean_rag Markdown")
	fmt.Printf("Measured %s\n\n", time.Now().Format("2006-01-02 15:04 MST"))
	fmt.Printf("%-80s %10s %10s %10s\n", "page", "raw tok", "clean tok", "saved")
	var reductions []float64
	var totalRaw, totalClean int
	for _, r := range results {
		if r.fetchError != "" {
			fmt.Printf("%-80s  fetch failed: %s\n", r.url, r.fetchError)
			continue
		}
		fmt.Printf("%-80s %10d %10d %9.1f%%\n", r.url, r.rawTokens, r.cleanTok, r.reduction)
		reductions = append(reductions, r.reduction)
		totalRaw += r.rawTokens
		totalClean += r.cleanTok
	}
	if len(reductions) == 0 {
		fmt.Println("\nNo pages measured.")
		os.Exit(1)
	}
	sort.Float64s(reductions)
	median := reductions[len(reductions)/2]
	if len(reductions)%2 == 0 {
		median = (reductions[len(reductions)/2-1] + reductions[len(reductions)/2]) / 2
	}
	fmt.Printf("\npages: %d   min: %.1f%%   median: %.1f%%   max: %.1f%%   overall: %.1f%%\n",
		len(reductions), reductions[0], median, reductions[len(reductions)-1],
		float64(totalRaw-totalClean)/float64(totalRaw)*100)
}

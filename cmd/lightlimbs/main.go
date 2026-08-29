package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/crawler-monorepo/agent-service/api"
	"github.com/crawler-monorepo/common/config"
	"github.com/crawler-monorepo/common/logger"
	appMiddleware "github.com/crawler-monorepo/common/middleware"
	"github.com/crawler-monorepo/common/ratelimit"
	"github.com/crawler-monorepo/common/stemmer"
	"github.com/crawler-monorepo/common/stopwords"
	"github.com/crawler-monorepo/common/tracing"
	"github.com/crawler-monorepo/common/utils"
	"github.com/crawler-monorepo/internal/auth"
	"github.com/crawler-monorepo/internal/cluster"
	"github.com/crawler-monorepo/internal/crawler"
	"github.com/crawler-monorepo/internal/extractor"
	"github.com/crawler-monorepo/internal/chunker"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/mcp"
	"github.com/crawler-monorepo/internal/search"
	"github.com/crawler-monorepo/internal/storage"
	"github.com/crawler-monorepo/internal/ui"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

var (
	chunkWindow int = 512
	chunkOverlap int = 64
	version         = "v1.5.0-entropy"
	saveTriggerChan = make(chan string, 100)
	saveTriggerOnce sync.Once
)

func triggerDebouncedSave(dataDir string) {
	saveTriggerOnce.Do(func() {
		go func() {
			var timer *time.Timer
			var latestDir string
			for {
				select {
				case dir, ok := <-saveTriggerChan:
					if !ok {
						return
					}
					latestDir = dir
					if timer == nil {
						timer = time.NewTimer(300 * time.Millisecond)
					} else {
						timer.Reset(300 * time.Millisecond)
					}
				case <-func() <-chan time.Time {
					if timer == nil {
						return nil
					}
					return timer.C
				}():
					if latestDir != "" {
						saveStorage(latestDir)
						latestDir = ""
					}
					timer = nil
				}
			}
		}()
	})

	select {
	case saveTriggerChan <- dataDir:
	default:
	}
}

type EmbeddedServer struct {
	httpClient   *crawler.Client
	jobManager   *crawler.JobManager
	dataDir      string
	clusterCoord *cluster.ClusterCoordinator
	raftNode     *cluster.RaftNode
}

func NewEmbeddedServer(dataDir string) *EmbeddedServer {
	return NewEmbeddedServerWithClient(dataDir, crawler.NewClient())
}

func NewEmbeddedServerWithClient(dataDir string, httpClient *crawler.Client) *EmbeddedServer {
	if dataDir == "" {
		dataDir = "data"
	}
	if httpClient == nil {
		httpClient = crawler.NewClient()
	}
	return &EmbeddedServer{
		httpClient: httpClient,
		jobManager: crawler.NewJobManager(httpClient, dataDir),
		dataDir:    dataDir,
	}
}

func (s *EmbeddedServer) SetCluster(coord *cluster.ClusterCoordinator, raftNode *cluster.RaftNode) {
	s.clusterCoord = coord
	s.raftNode = raftNode
}

func (s *EmbeddedServer) JobManager() *crawler.JobManager {
	return s.jobManager
}

func (s *EmbeddedServer) HTTPClient() *crawler.Client {
	return s.httpClient
}

func (s *EmbeddedServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","mode":"single_binary_embedded"}`))
}

type ScrapeRequest struct {
	URL        string `json:"url"`
	Mode       string `json:"mode,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type ScrapeResponse struct {
	URL           string  `json:"url"`
	Title         string  `json:"title"`
	Markdown      string  `json:"markdown"`
	TokenEstimate int     `json:"token_estimate"`
	LatencyMs     float64 `json:"latency_ms"`
}

func (s *EmbeddedServer) ScrapeHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	var req ScrapeRequest

	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"Invalid JSON body"}`, http.StatusBadRequest)
			return
		}
	} else {
		req.URL = r.URL.Query().Get("url")
		req.Mode = r.URL.Query().Get("mode")
		if ttlStr := r.URL.Query().Get("ttl_seconds"); ttlStr != "" {
			req.TTLSeconds, _ = strconv.Atoi(ttlStr)
		}
	}

	if req.URL == "" {
		http.Error(w, `{"error":"URL parameter required"}`, http.StatusBadRequest)
		return
	}

	if req.Mode == "" {
		req.Mode = "clean_rag"
	}
	if req.Mode != "clean_rag" && req.Mode != "preserve_links" && req.Mode != "raw" {
		http.Error(w, `{"error":"Invalid mode parameter"}`, http.StatusBadRequest)
		return
	}

	normURL, err := utils.NormalizeURL(req.URL)
	if err != nil {
		http.Error(w, `{"error":"Invalid URL format"}`, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	fetchURL := utils.TransformGitHubURL(normURL)
	res, err := s.httpClient.Fetch(ctx, fetchURL)
	if err != nil {
		http.Error(w, `{"error":"Failed to fetch URL: `+err.Error()+`"}`, http.StatusBadGateway)
		return
	}
	defer res.Response.Body.Close()

	limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
	htmlBytes, err := io.ReadAll(limitedBody)
	if err != nil {
		http.Error(w, `{"error":"Failed to read response body"}`, http.StatusInternalServerError)
		return
	}

	contentType := res.Response.Header.Get("Content-Type")
	mdText, tokens, title, extractErr := extractor.ExtractDocumentText(res.FinalURL, contentType, htmlBytes, req.Mode)
	if extractErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": extractErr.Error()})
		return
	}

	bodyForIndexing := mdText
	docTitle := title
	if cleanDoc, _ := extractor.ProcessRawHTML(res.FinalURL, htmlBytes); cleanDoc != nil && cleanDoc.Body != "" {
		bodyForIndexing = cleanDoc.Body
		if cleanDoc.Title != "" {
			docTitle = cleanDoc.Title
		}
	}

	rawTokens := strings.Fields(strings.ToLower(bodyForIndexing))
	termPositions := make(map[string][]int)
	for idx, raw := range rawTokens {
		clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
		if clean == "" || stopwords.IsStopword(clean) {
			continue
		}
		stemmed := stemmer.Stem(clean)
		termPositions[stemmed] = append(termPositions[stemmed], idx)
	}

	index.GlobalEngine.IndexDocumentWithSource(res.FinalURL, docTitle, bodyForIndexing, termPositions, tokens, "embedded_scraped", res.FinalURL)

	if ttlDuration, hasTTL := api.ClampTTL(req.TTLSeconds); hasTTL {
		_ = storage.SaveCrawledDocumentWithTTL(
			r.Context(),
			res.FinalURL,
			docTitle,
			bodyForIndexing,
			tokens,
			"embedded_scraped",
			res.FinalURL,
			ttlDuration,
		)
	}

	triggerDebouncedSave(s.dataDir)

	latency := float64(time.Since(t0).Microseconds()) / 1000.0

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ScrapeResponse{
		URL:           res.FinalURL,
		Title:         title,
		Markdown:      mdText,
		TokenEstimate: tokens,
		LatencyMs:     latency,
	})
}

type SearchRequest struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k"`
	Mode  string `json:"mode,omitempty"`
}

type SearchResponse struct {
	Query           string                   `json:"query"`
	LatencyMs       float64                  `json:"latency_ms"`
	TotalHits       int                      `json:"total_hits"`
	Results         []search.HybridSearchHit `json:"results"`
	Degraded        bool                     `json:"degraded,omitempty"`
	ShardsQueried   int                      `json:"shards_queried,omitempty"`
	ShardsResponded int                      `json:"shards_responded,omitempty"`
}

func (s *EmbeddedServer) SearchHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	var req SearchRequest

	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON request payload"})
			return
		}
	} else {
		req.Query = r.URL.Query().Get("q")
		if req.Query == "" {
			req.Query = r.URL.Query().Get("query")
		}
		req.Mode = r.URL.Query().Get("mode")
		req.TopK, _ = strconv.Atoi(r.URL.Query().Get("top_k"))
	}

	if req.TopK <= 0 {
		req.TopK = 5
	}
	if req.TopK > 100 {
		req.TopK = 100
	}

	// Cluster Scatter-Gather delegation
	if s.clusterCoord != nil && req.Mode != "bm25" && req.Mode != "vector" {
		clusterResp, err := s.clusterCoord.ScatterGatherSearch(r.Context(), req.Query, req.TopK)
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(clusterResp)
			return
		}
	}

	titles, urls, bodies := index.GlobalEngine.GetMetadataMaps()
	bm25Hits := index.RankDocuments(
		req.Query,
		index.GlobalEngine.GetInvertedIndex(),
		titles,
		urls,
		bodies,
		req.TopK*2,
	)

	vecHits := index.GlobalEngine.SearchVector(req.Query, req.TopK*2)

	fusedHits := search.ReciprocalRankFusion(req.Query, bm25Hits, vecHits, req.TopK, titles, urls, bodies)

	if req.Mode == "bm25" {
		filtered := make([]search.HybridSearchHit, 0)
		for _, h := range fusedHits {
			if h.BM25Rank != nil && *h.BM25Rank > 0 {
				filtered = append(filtered, h)
			}
		}
		fusedHits = filtered
	} else if req.Mode == "vector" {
		filtered := make([]search.HybridSearchHit, 0)
		for _, h := range fusedHits {
			if h.VectorRank != nil && *h.VectorRank > 0 {
				filtered = append(filtered, h)
			}
		}
		fusedHits = filtered
	}

	latency := float64(time.Since(t0).Microseconds()) / 1000.0

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(SearchResponse{
		Query:     req.Query,
		LatencyMs: latency,
		TotalHits: len(fusedHits),
		Results:   fusedHits,
	})
}

func SecurityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedKey := os.Getenv("AGENT_API_KEY")
		if expectedKey != "" {
			apiKey := r.Header.Get("X-API-Key")
			if apiKey == "" {
				authHeader := r.Header.Get("Authorization")
				if strings.HasPrefix(authHeader, "Bearer ") {
					apiKey = strings.TrimPrefix(authHeader, "Bearer ")
				}
			}
			if apiKey == "" {
				apiKey = r.URL.Query().Get("api_key")
			}
			if apiKey == "" || subtle.ConstantTimeCompare([]byte(apiKey), []byte(expectedKey)) != 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"Unauthorized"}`))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

type AutocompleteResponse struct {
	Prefix      string                     `json:"prefix"`
	Suggestions []index.AutocompleteResult `json:"suggestions"`
}

func (s *EmbeddedServer) AutocompleteHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	limitStr := r.URL.Query().Get("limit")
	limit := 5
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}
	if limit > 50 {
		limit = 50
	}

	results := index.GlobalEngine.GetTrie().SearchPrefix(q, limit)
	if results == nil {
		results = make([]index.AutocompleteResult, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(AutocompleteResponse{
		Prefix:      q,
		Suggestions: results,
	})
}

func (s *EmbeddedServer) WebSearchHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	var req SearchRequest

	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024)
		_ = json.NewDecoder(r.Body).Decode(&req)
	} else {
		req.Query = r.URL.Query().Get("q")
		if req.Query == "" {
			req.Query = r.URL.Query().Get("query")
		}
		req.TopK, _ = strconv.Atoi(r.URL.Query().Get("top_k"))
	}

	if req.TopK <= 0 {
		req.TopK = 5
	}
	if req.TopK > 50 {
		req.TopK = 50
	}

	adapter := search.NewMetasearchAdapter(index.GlobalEngine)
	hits, err := adapter.Search(r.Context(), req.Query, req.TopK)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	latency := float64(time.Since(t0).Microseconds()) / 1000.0

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(SearchResponse{
		Query:     req.Query,
		LatencyMs: latency,
		TotalHits: len(hits),
		Results:   hits,
	})
}

func (s *EmbeddedServer) AgenticSearchHandler(w http.ResponseWriter, r *http.Request) {
	var req search.AgenticSearchRequest

	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024)
		_ = json.NewDecoder(r.Body).Decode(&req)
	} else {
		req.Query = r.URL.Query().Get("q")
		if req.Query == "" {
			req.Query = r.URL.Query().Get("query")
		}
		req.Model = r.URL.Query().Get("model")
		req.LLMApiKey = r.URL.Query().Get("llm_api_key")
		req.LLMBaseURL = r.URL.Query().Get("llm_base_url")
		req.TopK, _ = strconv.Atoi(r.URL.Query().Get("top_k"))
	}

	pipeline := search.NewAgenticPipeline(index.GlobalEngine)
	resp, err := pipeline.Execute(r.Context(), req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *EmbeddedServer) CrawlHandler(w http.ResponseWriter, r *http.Request) {
	var req crawler.CrawlRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON body: " + err.Error()})
		return
	}

	if req.URL == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "URL parameter required"})
		return
	}

	if os.Getenv("ENV") == "test" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1" {
		req.AllowLoopback = true
	}

	job, err := s.jobManager.StartCrawl(r.Context(), req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if req.Async {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id":  job.ID,
			"status":  job.GetStatus(),
			"message": "Crawl job started in background",
		})
		return
	}

	triggerDebouncedSave(s.dataDir)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(job)
}

func (s *EmbeddedServer) GetCrawlJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		jobID = chi.URLParam(r, "job_id")
	}

	job, found := s.jobManager.GetJob(jobID)
	if !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Crawl job not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(job)
}

func (s *EmbeddedServer) CancelCrawlJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		jobID = chi.URLParam(r, "job_id")
	}

	cancelled := s.jobManager.CancelJob(jobID)
	if !cancelled {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Crawl job not found or already completed"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"job_id":  jobID,
		"status":  "cancelled",
		"message": "Crawl job cancelled successfully",
	})
}

func (s *EmbeddedServer) CrawlJobStreamHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"Streaming not supported"}`, http.StatusBadRequest)
		return
	}

	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		jobID = chi.URLParam(r, "job_id")
	}
	if jobID == "" {
		jobID = r.URL.Query().Get("job_id")
	}

	job, found := s.jobManager.GetJob(jobID)
	if !found || job == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Crawl job not found"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	status := job.GetStatus()

	// If job is already completed/failed/cancelled, emit finish and return immediately
	if status != "running" {
		finalEv := ui.ProgressEvent{
			Timestamp:   time.Now(),
			JobID:       job.ID,
			Type:        ui.EventFinish,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      status,
		}
		data, _ := json.Marshal(finalEv)
		fmt.Fprintf(w, "event: finish\ndata: %s\n\n", data)
		flusher.Flush()
		return
	}

	events := job.SubscribeEvents()
	defer job.UnsubscribeEvents(events)

	// Send initial start snapshot
	initialEv := ui.ProgressEvent{
		Timestamp:   time.Now(),
		JobID:       job.ID,
		Type:        ui.EventStart,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      status,
	}
	data, _ := json.Marshal(initialEv)
	fmt.Fprintf(w, "event: start\ndata: %s\n\n", data)
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case ev, ok := <-events:
			if !ok {
				return
			}
			evBytes, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, evBytes)
			flusher.Flush()

			if ev.Type == ui.EventFinish {
				return
			}
		}
	}
}

func (s *EmbeddedServer) SchemaExtractHandler(w http.ResponseWriter, r *http.Request) {
	var req extractor.SchemaExtractRequest
	r.Body = http.MaxBytesReader(w, r.Body, 10*1024*1024)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON request payload"})
		return
	}

	if len(req.Schema) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Missing required schema parameter"})
		return
	}

	if req.HTML == "" && req.URL == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Either url or html parameter is required"})
		return
	}

	htmlContent := req.HTML
	if htmlContent == "" && req.URL != "" {
		normURL, err := utils.NormalizeURL(req.URL)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid URL format"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		fetchURL := utils.TransformGitHubURL(normURL)
		res, err := s.httpClient.Fetch(ctx, fetchURL)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch target URL: " + err.Error()})
			return
		}
		defer res.Response.Body.Close()

		limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
		bodyBytes, readErr := io.ReadAll(limitedBody)
		if readErr != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to read response body"})
			return
		}
		htmlContent = string(bodyBytes)
	}

	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	var llmProvider search.LLMProvider
	if req.LLMApiKey != "" || req.LLMBaseURL != "" {
		if req.LLMBaseURL != "" && strings.Contains(req.LLMBaseURL, "deepseek") {
			llmProvider = search.NewDeepSeekLLMProvider(req.LLMApiKey, req.LLMBaseURL, req.Model, allowLoopback)
		} else {
			llmProvider = search.NewOpenAILLMProvider(req.LLMApiKey, req.LLMBaseURL, req.Model, allowLoopback)
		}
	} else {
		llmProvider = search.NewLLMProviderFromEnv(allowLoopback)
	}

	result, extractErr := extractor.ExtractStructuredJSON(r.Context(), htmlContent, string(req.Schema), req.Prompt, llmProvider)
	if extractErr != nil {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(extractErr.Error(), "schema compilation error") || strings.Contains(extractErr.Error(), "malformed JSON") {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusUnprocessableEntity)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": extractErr.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func (s *EmbeddedServer) SetupRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(appMiddleware.RequestIDMiddleware)
	r.Use(tracing.TracingMiddleware)
	r.Use(auth.TenantMiddleware)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization, X-Tenant-ID, traceparent, X-Request-ID")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	r.Get("/health", s.HealthHandler)

	r.Group(func(r chi.Router) {
		r.Use(SecurityMiddleware)
		r.Use(ratelimit.RateLimiterMiddleware(50.0, 100.0))
		r.Post("/v1/scrape", s.ScrapeHandler)
		r.Get("/v1/scrape", s.ScrapeHandler)
		r.Post("/v1/search", s.SearchHandler)
		r.Get("/v1/search", s.SearchHandler)
		r.Post("/v1/web-search", s.WebSearchHandler)
		r.Get("/v1/web-search", s.WebSearchHandler)
		r.Post("/v1/agentic-search", s.AgenticSearchHandler)
		r.Get("/v1/agentic-search", s.AgenticSearchHandler)
		r.Get("/v1/autocomplete", s.AutocompleteHandler)

		// Stage 2 endpoints
		r.Post("/v1/crawl", s.CrawlHandler)
		r.Get("/v1/crawl/{id}", s.GetCrawlJobHandler)
		r.Get("/v1/crawl/{id}/stream", s.CrawlJobStreamHandler)
		r.Get("/v1/crawl/stream", s.CrawlJobStreamHandler)
		r.Get("/api/v1/crawl/stream", s.CrawlJobStreamHandler)
		r.Get("/api/v1/crawl/{id}/stream", s.CrawlJobStreamHandler)
		r.Delete("/v1/crawl/{id}", s.CancelCrawlJobHandler)
		r.Post("/v1/extract/schema", s.SchemaExtractHandler)
	})

	// Mount cluster & Raft RPC routes
	cluster.RegisterClusterHTTPHandlers(r, s.raftNode, s.clusterCoord)

	return r
}

func GetJanitorInterval() time.Duration {
	if env := os.Getenv("JANITOR_INTERVAL"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			return d
		}
	}
	return 15 * time.Minute
}

func setupStorage(dataDir, dbType, sqlitePath string) {
	if dataDir == "" {
		dataDir = "data"
	}
	_ = os.MkdirAll(dataDir, 0755)

	if dbType == "" {
		dbType = os.Getenv("DATABASE_DRIVER")
		if dbType == "" {
			if os.Getenv("DATABASE_URL") != "" {
				dbURL := os.Getenv("DATABASE_URL")
				if strings.HasPrefix(dbURL, "postgres://") || strings.HasPrefix(dbURL, "postgresql://") {
					dbType = "postgres"
				} else {
					dbType = "sqlite"
				}
			} else {
				dbType = "sqlite"
			}
		}
	}

	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "sqlite", "sqlite3", "db":
		path := sqlitePath
		if path == "" {
			if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" && (strings.HasPrefix(dbURL, "sqlite://") || strings.HasSuffix(dbURL, ".db") || strings.HasPrefix(dbURL, "file:")) {
				path = dbURL
			} else {
				path = filepath.Join(dataDir, "docs.db")
			}
		}
		if s, err := storage.NewSQLiteStore(path); err == nil {
			storage.SetGlobalStore(s)
		} else {
			storage.SetGlobalStore(storage.NewFileStore(""))
		}
	case "file", "json", "local":
		storage.SetGlobalStore(storage.NewFileStore(filepath.Join(dataDir, "crawled_pages.json")))
	case "memory", "mem":
		storage.SetGlobalStore(storage.NewMemoryStore())
	case "postgres", "postgresql", "pg":
		dbURL := os.Getenv("DATABASE_URL")
		if s, err := storage.NewPostgresStore(dbURL); err == nil {
			storage.SetGlobalStore(s)
		} else {
			storage.SetGlobalStore(storage.NewFileStore(""))
		}
	default:
		storage.SetGlobalStore(storage.NewStoreFromEnv())
	}
}

func initStorage(ctx context.Context, dataDir string) {
	if dataDir == "" {
		dataDir = "data"
	}
	_ = os.MkdirAll(dataDir, 0755)

	indexPath := filepath.Join(dataDir, "inverted_index.json")
	if _, err := os.Stat(indexPath); err == nil {
		if err := index.GlobalEngine.GetInvertedIndex().LoadSnapshot(indexPath); err == nil {
			logger.Log.Info("Loaded inverted index snapshot from file fallback: " + indexPath)
		}
	}

	vectorPath := filepath.Join(dataDir, "vector_index.json")
	if _, err := os.Stat(vectorPath); err == nil {
		if err := index.GlobalEngine.GetVectorIndex().LoadSnapshot(vectorPath); err == nil {
			logger.Log.Info("Loaded vector index snapshot from file fallback: " + vectorPath)
		}
	}

	docs, err := storage.GetCrawledDocuments(context.Background())
	if err == nil && len(docs) > 0 {
		for _, d := range docs {
			index.GlobalEngine.IndexDocumentDirectly(d.URL, d.Title, d.CleanBody, d.TotalTokens, d.SourceURL)
		}
		logger.Log.Info(fmt.Sprintf("Hydrated %d documents into memory from storage (%s)", len(docs), storage.GetGlobalStore().DriverName()))
	}

	if ctx == nil {
		ctx = context.Background()
	}
	index.GlobalEngine.StartTTLJanitor(ctx, GetJanitorInterval())
}

func saveStorage(dataDir string) {
	if dataDir == "" {
		dataDir = "data"
	}
	_ = os.MkdirAll(dataDir, 0755)

	indexPath := filepath.Join(dataDir, "inverted_index.json")
	_ = index.GlobalEngine.GetInvertedIndex().SaveSnapshot(indexPath)

	vectorPath := filepath.Join(dataDir, "vector_index.json")
	_ = index.GlobalEngine.GetVectorIndex().SaveSnapshot(vectorPath)

	_ = storage.GetGlobalStore().Close()
}

func loadSnapshotsForCLI(dataDir string) {
	if dataDir == "" {
		dataDir = "data"
	}
	_ = os.MkdirAll(dataDir, 0755)

	indexPath := filepath.Join(dataDir, "inverted_index.json")
	if _, err := os.Stat(indexPath); err == nil {
		_ = index.GlobalEngine.GetInvertedIndex().LoadSnapshot(indexPath)
	}

	vectorPath := filepath.Join(dataDir, "vector_index.json")
	if _, err := os.Stat(vectorPath); err == nil {
		_ = index.GlobalEngine.GetVectorIndex().LoadSnapshot(vectorPath)
	}

	docs, err := storage.GetCrawledDocuments(context.Background())
	if err == nil && len(docs) > 0 {
		for _, d := range docs {
			index.GlobalEngine.IndexDocumentDirectly(d.URL, d.Title, d.CleanBody, d.TotalTokens, d.SourceURL)
		}
	}
}

func parseInterleavedFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for len(args) > 0 {
		if args[0] == "--" {
			positional = append(positional, args[1:]...)
			break
		}
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		parsedArgs := fs.Args()
		if len(args) == len(parsedArgs) {
			// args[0] was a non-flag argument; consume it as positional
			positional = append(positional, parsedArgs[0])
			args = parsedArgs[1:]
		} else {
			// fs.Parse consumed flags. Check if it stopped at a "--" terminator.
			numConsumed := len(args) - len(parsedArgs)
			if numConsumed > 0 && args[numConsumed-1] == "--" {
				positional = append(positional, parsedArgs...)
				break
			}
			args = parsedArgs
		}
	}
	return positional, nil
}

func printHelp() {
	helpText := fmt.Sprintf(`WebLimbAI LightLimbs (%s) — High-Performance RAG Web Crawler & Hybrid Search Engine

Usage:
  lightlimbs <subcommand> [flags]

Available Subcommands:
  serve                               Run HTTP REST API server or stdio MCP server (Default)
  scrape <url>                        Direct DOM AST extraction to Markdown / JSON
  crawl <url>                         Recursive whole-domain BFS & adaptive entropy crawling
  extract <url> --schema <file.json>  Schema-guided structured LLM extraction
  search "<query>"                    Hybrid BM25 + Vector + RRF search from local index
  init-mcp                            1-Click AI IDE auto-configurator (Claude Desktop & Cursor)
  seed                                Seed 1,000+ SDE technical documents into local index
  version                             Display current WebLimbAI version & runtime diagnostics

Examples:
  lightlimbs                                            # Starts HTTP API daemon on :8080
  lightlimbs serve --port 9090                          # Starts HTTP server on port 9090
  lightlimbs serve --mcp                                # Starts stdio Model Context Protocol server
  lightlimbs scrape https://go.dev -j                   # Scrapes URL and prints structured JSON
  lightlimbs crawl https://docs.docker.com -a -d 2      # Adaptive entropy-guided documentation crawl
  lightlimbs extract https://example.com --schema s.json# Structured JSON extraction with schema
  lightlimbs search "GMP Scheduler"                     # Searches local indexed documents
  lightlimbs init-mcp                                   # Configures Claude Desktop & Cursor MCP JSON
  lightlimbs seed                                       # Seeds 1,000+ technical documents

Run 'lightlimbs <subcommand> --help' for details on each subcommand.
`, version)
	fmt.Fprint(os.Stderr, helpText)
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	port := fs.String("port", "", "HTTP port to bind (default 8080 or PORT env)")
	fs.StringVar(port, "p", "", "HTTP port to bind (shorthand)")
	dataDir := fs.String("data-dir", "", "Data directory for snapshots (default 'data' or DATA_DIR env)")
	fs.StringVar(dataDir, "d", "", "Data directory (shorthand)")
	chunkWindowFlag := fs.Int("chunk-window", 512, "Max tokens per chunk")
	chunkOverlapFlag := fs.Int("chunk-overlap", 64, "Overlap tokens between chunks")
	mcpMode := fs.Bool("mcp", false, "Start in stdio MCP server mode")
	_ = fs.Bool("readonly", false, "Start in readonly mode")
	clusterPeers := fs.String("cluster-peers", "", "Comma-separated peer URLs/hosts for distributed cluster")
	nodeID := fs.String("node-id", "", "Cluster node ID (default 'node-<port>')")
	shards := fs.Int("shards", 16, "Number of cluster partition shards (default 16)")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cOpts := chunker.DefaultChunkingOptions()
	if *chunkWindowFlag > 0 {
		cOpts.MaxTokens = *chunkWindowFlag
	}
	if *chunkOverlapFlag > 0 {
		cOpts.Overlap = *chunkOverlapFlag
	}
	index.WithChunkingOptions(cOpts)(index.GlobalEngine)

	if *port == "" {
		*port = os.Getenv("PORT")
		if *port == "" {
			*port = "8080"
		}
	}

	if *dataDir == "" {
		*dataDir = os.Getenv("DATA_DIR")
		if *dataDir == "" {
			*dataDir = "data"
		}
	}

	if *mcpMode {
		runStdioMCP(*dataDir)
		return
	}

	// Validate config for HTTP server
	if _, err := config.LoadAndValidate(); err != nil {
		logger.Log.Fatal("Configuration validation error", zap.Error(err))
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	setupStorage(*dataDir, *dbType, *sqlitePath)
	initStorage(rootCtx, *dataDir)

	server := NewEmbeddedServer(*dataDir)

	var raftNode *cluster.RaftNode
	var coord *cluster.ClusterCoordinator
	var stateMachine *cluster.StateMachine

	if *clusterPeers != "" || os.Getenv("CLUSTER_PEERS") != "" {
		peersRaw := *clusterPeers
		if peersRaw == "" {
			peersRaw = os.Getenv("CLUSTER_PEERS")
		}
		peerList := strings.Split(peersRaw, ",")
		var cleanPeers []string
		endpointMap := make(map[string]string)
		for _, p := range peerList {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				cleanPeers = append(cleanPeers, trimmed)
				endpointMap[trimmed] = trimmed
			}
		}

		if *nodeID == "" {
			*nodeID = os.Getenv("NODE_ID")
			if *nodeID == "" {
				*nodeID = "node-" + *port
			}
		}

		ring := cluster.NewHashRing(128)
		ring.AddNode(*nodeID)
		for _, p := range cleanPeers {
			ring.AddNode(p)
		}

		transport := cluster.NewHTTPRaftTransport(endpointMap)
		applyCh := make(chan cluster.ApplyMsg, 1000)

		raftCfg := cluster.DefaultRaftConfig(*nodeID, cleanPeers)
		raftNode = cluster.NewRaftNode(raftCfg, transport, applyCh)
		stateMachine = cluster.NewStateMachine(index.GlobalEngine, applyCh)
		coord = cluster.NewClusterCoordinator(*nodeID, ring, raftNode, index.GlobalEngine, transport, *shards)

		server.SetCluster(coord, raftNode)
		logger.Log.Info("Distributed Raft Cluster mode initialized",
			zap.String("node_id", *nodeID),
			zap.Int("peers", len(cleanPeers)),
			zap.Int("shards", *shards),
		)
	}

	router := server.SetupRouter()

	httpServer := &http.Server{
		Addr:              ":" + *port,
		Handler:           router,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Log.Info("WebLimbAI Server starting on port " + *port)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("Server error", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Log.Info("Shutting down server gracefully...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Log.Error("Server shutdown error", zap.Error(err))
	}

	if raftNode != nil {
		raftNode.Close()
	}
	if stateMachine != nil {
		stateMachine.Stop()
	}

	saveStorage(*dataDir)
}

const maxLineSize = 10 * 1024 * 1024

var errLineTooLong = errors.New("line length exceeded 10MB")

func readBoundedLine(reader *bufio.Reader, maxBytes int) ([]byte, error) {
	var line []byte
	for {
		chunk, isPrefix, err := reader.ReadLine()
		if err != nil {
			if len(line) == 0 && len(chunk) == 0 {
				return nil, err
			}
			if len(line)+len(chunk) > maxBytes {
				return nil, errLineTooLong
			}
			line = append(line, chunk...)
			return line, nil
		}
		if len(line)+len(chunk) > maxBytes {
			for isPrefix && err == nil {
				_, isPrefix, err = reader.ReadLine()
			}
			return nil, errLineTooLong
		}
		line = append(line, chunk...)
		if !isPrefix {
			break
		}
	}
	return line, nil
}

func runStdioMCP(dataDir string) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	initStorage(ctx, dataDir)
	defer saveStorage(dataDir)

	client := crawler.NewClient()
	reader := bufio.NewReaderSize(os.Stdin, 64*1024)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := readBoundedLine(reader, maxLineSize)
		if err != nil {
			if errors.Is(err, errLineTooLong) {
				errResp := map[string]interface{}{
					"jsonrpc": "2.0",
					"id":      nil,
					"error": map[string]interface{}{
						"code":    -32700,
						"message": "Parse error: line length exceeded 10MB",
					},
				}
				if respBytes, marshalErr := json.Marshal(errResp); marshalErr == nil {
					fmt.Println(string(respBytes))
				}
				continue
			}
			if errors.Is(err, io.EOF) || err == io.EOF {
				break
			}
			break
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		var rawMessage json.RawMessage
		if unmarshalErr := json.Unmarshal(trimmed, &rawMessage); unmarshalErr != nil {
			errResp := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      nil,
				"error": map[string]interface{}{
					"code":    -32700,
					"message": "Parse error: " + unmarshalErr.Error(),
				},
			}
			if respBytes, marshalErr := json.Marshal(errResp); marshalErr == nil {
				fmt.Println(string(respBytes))
			}
			continue
		}

		if len(rawMessage) == 0 {
			continue
		}

		respBytes, rpcErr := mcp.HandleRPCMessage(rawMessage, client)
		if rpcErr != nil {
			continue
		}

		if len(respBytes) > 0 {
			fmt.Println(string(respBytes))
		}
	}
}

type ScrapeCLIOutput struct {
	URL        string  `json:"url"`
	Title      string  `json:"title"`
	Markdown   string  `json:"markdown"`
	Tokens     int     `json:"tokens"`
	RawTokens  int     `json:"raw_tokens"`
	SavingsPct float64 `json:"savings_pct"`
}

func runScrape(args []string) {
	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("WEBLIMB_ALLOW_LOOPBACK") == "1" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	client := crawler.NewTestClient(allowLoopback)
	runScrapeWithClient(client, args)
}

func runScrapeWithClient(client *crawler.Client, args []string) {
	fs := flag.NewFlagSet("scrape", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	mode := fs.String("mode", "clean_rag", "Extraction mode (clean_rag, preserve_links, raw)")
	jsonOut := fs.Bool("json", false, "Output JSON directly to stdout")
	fs.BoolVar(jsonOut, "j", false, "Output JSON (shorthand)")
	outFile := fs.String("output", "", "Output file path (default: stdout)")
	fs.StringVar(outFile, "o", "", "Output file path (shorthand)")
	ttl := fs.Int("ttl", 604800, "Time-to-live in seconds [default: 604800 / 7 days]")
	noIndex := fs.Bool("no-index", false, "Do not index into local vector/BM25 database")
	dataDir := fs.String("data-dir", "data", "Snapshot data directory")
	fs.StringVar(dataDir, "d", "data", "Snapshot data directory (shorthand)")
	forceRefresh := fs.Bool("force-refresh", false, "Force refresh existing crawled documents")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")
	noProgress := fs.Bool("no-progress", false, "Disable progress spinner")
	quiet := fs.Bool("quiet", false, "Quiet mode")
	fs.BoolVar(quiet, "q", false, "Quiet mode (shorthand)")

	posArgs, err := parseInterleavedFlags(fs, args)
	if err != nil {
		os.Exit(1)
	}

	if len(posArgs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Missing target URL.")
		fmt.Fprintln(os.Stderr, "Usage: lightlimbs scrape <url> [flags]")
		fmt.Fprintln(os.Stderr, "Flags:")
		fmt.Fprintln(os.Stderr, "  -m, --mode <mode>       Extraction mode (clean_rag, preserve_links, raw) [default: clean_rag]")
		fmt.Fprintln(os.Stderr, "  -j, --json              Output structured JSON payload")
		fmt.Fprintln(os.Stderr, "  -o, --out <path>        Save markdown directly to a file")
		fmt.Fprintln(os.Stderr, "      --ttl <seconds>     Time-to-live in seconds [default: 604800 / 7 days]")
		fmt.Fprintln(os.Stderr, "      --no-index          Skip persisting to local search index")
		fmt.Fprintln(os.Stderr, "  -d, --data-dir <dir>    Snapshot storage directory [default: data]")
		fmt.Fprintln(os.Stderr, "      --db-type <type>    Storage backend (sqlite, file, postgres, memory) [default: sqlite]")
		fmt.Fprintln(os.Stderr, "      --sqlite-path <path>Path to SQLite database file [default: <data-dir>/docs.db]")
		fmt.Fprintln(os.Stderr, "      --no-progress       Disable progress spinner")
		fmt.Fprintln(os.Stderr, "  -q, --quiet             Quiet mode (suppress logs)")
		os.Exit(1)
	}

	rawURL := posArgs[0]
	normURL, err := utils.NormalizeURL(rawURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Invalid URL format %q: %v\n", rawURL, err)
		os.Exit(1)
	}

	setupStorage(*dataDir, *dbType, *sqlitePath)

	startTime := time.Now()
	fetchURL := utils.TransformGitHubURL(normURL)

	var sp *ui.Spinner
	if ui.ShouldUseInteractiveUI(os.Stderr, *noProgress, *quiet) && !*jsonOut {
		sp = ui.NewSpinner(os.Stderr, fmt.Sprintf("Fetching and extracting clean RAG Markdown for %s...", fetchURL))
		sp.Start()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if client == nil {
		allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
		client = crawler.NewTestClient(allowLoopback)
	}

	opts := crawler.FetchOptions{}
	if !*forceRefresh {
		if doc, err := storage.GetCrawledDocumentByURL(ctx, fetchURL); err == nil && doc != nil {
			opts.ETag = doc.ETag
			opts.LastModified = doc.LastModified
		}
	}
	res, err := client.FetchWithAuth(ctx, fetchURL, opts)
	if err != nil {
		if sp != nil {
			sp.Error(fmt.Sprintf("Failed to fetch %s: %v", fetchURL, err))
		} else {
			fmt.Fprintf(os.Stderr, "Error: Failed to fetch %s: %v\n", fetchURL, err)
		}
		os.Exit(1)
	}
	if res.NotModified {
		doc, _ := storage.GetCrawledDocumentByURL(ctx, fetchURL)
		if doc != nil {
			if sp != nil {
				sp.Success(fmt.Sprintf("Cached 304 Not Modified for %s", fetchURL))
			}
			if *jsonOut {
				payload := ScrapeCLIOutput{
					URL:        res.FinalURL,
					Title:      doc.Title,
					Markdown:   doc.CleanBody,
					Tokens:     doc.TotalTokens,
					RawTokens:  doc.TotalTokens,
					SavingsPct: 0,
				}
				jsonBytes, _ := json.MarshalIndent(payload, "", "  ")
				fmt.Println(string(jsonBytes))
			} else if *outFile == "" {
				fmt.Println(doc.CleanBody)
			}
			os.Exit(0)
		}
	}
	defer res.Response.Body.Close()

	if res.Response.StatusCode != 200 {
		if sp != nil {
			sp.Error(fmt.Sprintf("HTTP %d (%s) returned by %s", res.Response.StatusCode, res.Response.Status, res.FinalURL))
		} else {
			fmt.Fprintf(os.Stderr, "Error: HTTP %d (%s) returned by %s\n", res.Response.StatusCode, res.Response.Status, res.FinalURL)
		}
		os.Exit(1)
	}

	limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
	htmlBytes, err := io.ReadAll(limitedBody)
	if err != nil {
		if sp != nil {
			sp.Error(fmt.Sprintf("Failed to read response body: %v", err))
		} else {
			fmt.Fprintf(os.Stderr, "Error: Failed to read response body: %v\n", err)
		}
		os.Exit(1)
	}

	contentType := res.Response.Header.Get("Content-Type")
	mdText, tokens, title, extractErr := extractor.ExtractDocumentText(res.FinalURL, contentType, htmlBytes, *mode)
	if extractErr != nil {
		if sp != nil {
			sp.Error(fmt.Sprintf("Extraction failed: %v", extractErr))
		} else {
			fmt.Fprintf(os.Stderr, "Error: Extraction failed: %v\n", extractErr)
		}
		os.Exit(1)
	}

	rawTokens := len(strings.Fields(string(htmlBytes)))
	savingsPct := 0.0
	if rawTokens > 0 {
		savingsPct = float64(rawTokens-tokens) / float64(rawTokens) * 100.0
	}
	if savingsPct < 0 {
		savingsPct = 0
	}

	if !*noIndex {
		loadSnapshotsForCLI(*dataDir)

		bodyForIndexing := mdText
		docTitle := title
		if cleanDoc, _ := extractor.ProcessRawHTML(res.FinalURL, htmlBytes); cleanDoc != nil && cleanDoc.Body != "" {
			bodyForIndexing = cleanDoc.Body
			if cleanDoc.Title != "" {
				docTitle = cleanDoc.Title
			}
		}

		rawToks := strings.Fields(strings.ToLower(bodyForIndexing))
		termPositions := make(map[string][]int)
		for idx, raw := range rawToks {
			clean := strings.Trim(raw, ".,!?:;\"'()[]{}")
			if clean == "" || stopwords.IsStopword(clean) {
				continue
			}
			stemmed := stemmer.Stem(clean)
			termPositions[stemmed] = append(termPositions[stemmed], idx)
		}

		index.GlobalEngine.IndexDocumentWithSource(res.FinalURL, docTitle, bodyForIndexing, termPositions, tokens, "cli_scraped", res.FinalURL)

		hashBytes := sha256.Sum256([]byte(bodyForIndexing))
		contentHash := hex.EncodeToString(hashBytes[:])
		doc := &storage.CrawledDocument{
			URL:           res.FinalURL,
			Title:         docTitle,
			CleanBody:     bodyForIndexing,
			TotalTokens:   tokens,
			SourceType:    "cli_scraped",
			SourceURL:     res.FinalURL,
			ETag:          res.ETag,
			LastModified:  res.LastModified,
			ContentHash:   contentHash,
			LastCrawledAt: time.Now(),
			HTTPStatus:    res.Response.StatusCode,
		}
		ttlDuration, _ := api.ClampTTL(*ttl)
		_ = storage.UpsertCrawledDocument(context.Background(), doc, ttlDuration)

		saveStorage(*dataDir)
	}

	if sp != nil {
		sp.Success(fmt.Sprintf("Extracted %s tokens (%.1f%% savings) from %s in %s", ui.FormatNumber(int64(tokens)), savingsPct, res.FinalURL, ui.FormatDuration(time.Since(startTime))))
	}

	if *outFile != "" {
		if err := os.WriteFile(*outFile, []byte(mdText), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to write to %s: %v\n", *outFile, err)
			os.Exit(1)
		}
		if !*quiet {
			fmt.Fprintf(os.Stderr, "Extracted markdown saved to %s\n", *outFile)
		}
	}

	if *jsonOut {
		payload := ScrapeCLIOutput{
			URL:        res.FinalURL,
			Title:      title,
			Markdown:   mdText,
			Tokens:     tokens,
			RawTokens:  rawTokens,
			SavingsPct: math.Round(savingsPct*10) / 10,
		}
		jsonBytes, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Println(string(jsonBytes))
	} else if *outFile == "" {
		fmt.Println(mdText)
	}
}

func runSearch(args []string) {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	topK := fs.Int("top", 5, "Maximum results count")
	fs.IntVar(topK, "k", 5, "Maximum results count (shorthand)")
	mode := fs.String("mode", "hybrid", "Search mode (hybrid, bm25, vector)")
	dataDir := fs.String("data-dir", "data", "Snapshot data directory")
	fs.StringVar(dataDir, "d", "data", "Snapshot data directory (shorthand)")
	jsonOut := fs.Bool("json", false, "Output results as raw JSON array")
	fs.BoolVar(jsonOut, "j", false, "Output results as raw JSON array (shorthand)")
	snippetLen := fs.Int("snippet-len", 180, "Highlight snippet length")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")

	posArgs, err := parseInterleavedFlags(fs, args)
	if err != nil {
		os.Exit(1)
	}

	if len(posArgs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Missing search query.")
		fmt.Fprintln(os.Stderr, "Usage: lightlimbs search \"<query>\" [flags]")
		fmt.Fprintln(os.Stderr, "Flags:")
		fmt.Fprintln(os.Stderr, "  -k, --top <num>         Maximum results count [default: 5]")
		fmt.Fprintln(os.Stderr, "      --mode <mode>       Search mode (hybrid, bm25, vector) [default: hybrid]")
		fmt.Fprintln(os.Stderr, "  -j, --json              Output results as JSON array")
		fmt.Fprintln(os.Stderr, "  -d, --data-dir <dir>    Snapshot storage directory [default: data]")
		fmt.Fprintln(os.Stderr, "      --snippet-len <num> Snippet highlight length [default: 180]")
		fmt.Fprintln(os.Stderr, "      --db-type <type>    Storage backend (sqlite, file, postgres, memory) [default: sqlite]")
		fmt.Fprintln(os.Stderr, "      --sqlite-path <path>Path to SQLite database file [default: <data-dir>/docs.db]")
		os.Exit(1)
	}

	setupStorage(*dataDir, *dbType, *sqlitePath)

	query := strings.Join(posArgs, " ")
	loadSnapshotsForCLI(*dataDir)

	titles, urls, bodies := index.GlobalEngine.GetMetadataMaps()
	if len(titles) == 0 {
		if *jsonOut {
			fmt.Println("[]")
		} else {
			fmt.Fprintf(os.Stderr, "No indexed documents found in '%s'. Scrape web pages or run 'lightlimbs seed' first.\n", *dataDir)
		}
		return
	}

	if *topK <= 0 {
		*topK = 5
	}
	fetchK := *topK * 2
	if fetchK < 10 {
		fetchK = 10
	}

	bm25Hits := index.RankDocuments(
		query,
		index.GlobalEngine.GetInvertedIndex(),
		titles,
		urls,
		bodies,
		fetchK,
	)

	vecHits := index.GlobalEngine.SearchVector(query, fetchK)

	fusedHits := search.ReciprocalRankFusion(query, bm25Hits, vecHits, *topK, titles, urls, bodies)

	if *mode == "bm25" {
		var filtered []search.HybridSearchHit
		for _, h := range fusedHits {
			if h.BM25Rank != nil && *h.BM25Rank > 0 {
				filtered = append(filtered, h)
			}
		}
		fusedHits = filtered
	} else if *mode == "vector" {
		var filtered []search.HybridSearchHit
		for _, h := range fusedHits {
			if h.VectorRank != nil && *h.VectorRank > 0 {
				filtered = append(filtered, h)
			}
		}
		fusedHits = filtered
	}

	if *snippetLen > 0 {
		for i := range fusedHits {
			if len(fusedHits[i].Snippet) > *snippetLen {
				fusedHits[i].Snippet = fusedHits[i].Snippet[:*snippetLen] + "..."
			}
		}
	}

	if *jsonOut {
		if fusedHits == nil {
			fusedHits = []search.HybridSearchHit{}
		}
		resJSON, _ := json.MarshalIndent(fusedHits, "", "  ")
		fmt.Println(string(resJSON))
		return
	}

	if len(fusedHits) == 0 {
		fmt.Printf("No matching documents found for query %q.\n", query)
		return
	}

	fmt.Printf("🔍 Found %d result(s) for query %q:\n\n", len(fusedHits), query)
	for i, hit := range fusedHits {
		fmt.Printf("[%d] %s\n", i+1, hit.Title)
		fmt.Printf("    URL:   %s\n", hit.URL)
		fmt.Printf("    Score: %.6f\n", hit.RRFScore)
		if hit.Snippet != "" {
			fmt.Printf("    Text:  %s\n", hit.Snippet)
		}
		fmt.Println()
	}
}

func runInitMCP(args []string) {
	fs := flag.NewFlagSet("init-mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	editor := fs.String("editor", "all", "Target editor (all, cursor, claude)")
	fs.StringVar(editor, "e", "all", "Target editor (shorthand)")
	binPath := fs.String("binary-path", "", "Explicit agentlimbs binary path override")
	fs.StringVar(binPath, "b", "", "Binary path override (shorthand)")
	dryRun := fs.Bool("dry-run", false, "Preview JSON diffs without writing to disk")
	stdout := fs.Bool("stdout", false, "Print MCP server config block to stdout")
	global := fs.Bool("global", false, "Configure Cursor globally (~/.cursor/mcp.json)")
	workspace := fs.Bool("workspace", false, "Configure Cursor for current workspace (.cursor/mcp.json)")

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	opts := mcp.ConfigOptions{
		Editor:     *editor,
		BinaryPath: *binPath,
		DryRun:     *dryRun,
		Stdout:     *stdout,
		Global:     *global,
		Workspace:  *workspace,
	}

	res, err := mcp.ConfigureMCP(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: MCP configuration failed: %v\n", err)
		os.Exit(1)
	}

	if *stdout {
		fmt.Println(res.StdoutJSON)
		return
	}

	if *dryRun {
		fmt.Fprintln(os.Stderr, "Dry-Run Mode: Showing proposed MCP configurations without modifying files:")
		for path, diff := range res.DryRunDiffs {
			fmt.Fprintf(os.Stderr, "\nTarget File: %s\n%s\n", path, diff)
		}
		return
	}

	fmt.Fprintln(os.Stderr, "WebLimbAI LightLimbs MCP Auto-Configurator")
	fmt.Fprintf(os.Stderr, "   Executable Path: %s\n", res.ResolvedBinaryPath)
	if len(res.FilesCreated) > 0 {
		for _, f := range res.FilesCreated {
			fmt.Fprintf(os.Stderr, "   Created: %s\n", f)
		}
	}
	if len(res.FilesUpdated) > 0 {
		for _, f := range res.FilesUpdated {
			fmt.Fprintf(os.Stderr, "   Updated: %s\n", f)
		}
	}
	if len(res.BackupsCreated) > 0 {
		for _, b := range res.BackupsCreated {
			fmt.Fprintf(os.Stderr, "   Backup:  %s\n", b)
		}
	}
	for _, w := range res.Warnings {
		fmt.Fprintf(os.Stderr, "   Warning: %s\n", w)
	}
	fmt.Fprintln(os.Stderr, "\nMCP server configuration complete. Restart Claude Desktop or reload Cursor.")
}

type SDEDomain struct {
	Category string
	Topics   []string
}

func runSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	dataDir := fs.String("data-dir", "data", "Snapshot data directory")
	fs.StringVar(dataDir, "d", "data", "Snapshot data directory (shorthand)")
	quiet := fs.Bool("quiet", false, "Suppress progress output")
	fs.BoolVar(quiet, "q", false, "Quiet mode (shorthand)")
	noProgress := fs.Bool("no-progress", false, "Disable progress bar")
	jsonOut := fs.Bool("json", false, "Output JSON summary")
	fs.BoolVar(jsonOut, "j", false, "Output JSON summary (shorthand)")

	limit := fs.Int("limit", 0, "Limit number of seeded documents (0 for all)")
	fs.IntVar(limit, "l", 0, "Limit number of seeded documents (shorthand)")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	setupStorage(*dataDir, *dbType, *sqlitePath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	domains := []SDEDomain{
		{
			Category: "Data Structures & Algorithms",
			Topics: []string{
				"Dynamic Programming Memoization", "Red-Black Tree Self Balancing", "Trie Prefix Tree Autocomplete",
				"Segment Tree Range Query", "Disjoint Set Union Find Path Compression", "Monotonic Stack Next Greater Element",
				"Sliding Window Maximum", "Fenwick Tree Binary Indexed Tree", "A Star Pathfinding Algorithm", "Dijkstra Shortest Path Single Source",
				"Floyd Warshall All Pairs Shortest Path", "Tarjan Strongly Connected Components", "Kruskal Minimum Spanning Tree",
				"Prim Minimum Spanning Tree", "Topological Sort Directed Acyclic Graph", "KMP Knuth Morris Pratt String Matching",
				"Rabin Karp Rolling Hash Search", "B Tree Storage Node Balancing", "B Plus Tree Leaf Linked List", "AVL Tree Height Balance",
				"Skip List Concurrent Skip List", "LRU Cache Least Recently Used Doubly Linked List", "LFU Cache Least Frequently Used",
				"Hash Map Collision Resolution Chaining Open Addressing", "Bloom Filter Probabilistic Set Membership",
				"Count Min Sketch Frequency Estimation", "HyperLogLog Cardinality Estimation", "Spatial Index R Tree QuadTree",
				"Suffix Tree Suffix Array", "QuickSort Partitioning Median of Three", "MergeSort Stable Counting Inversions",
				"HeapSort Priority Queue Binary Heap", "Radix Sort LSD MSD Counting", "Bucket Sort Distribution",
				"Binary Search Lower Bound Upper Bound", "Ternary Search Unimodal Function", "Two Pointers Opposite Direction Same Direction",
				"Backtracking N Queens Sudoku Solver", "Greedy Algorithm Fractional Knapsack", "Branch and Bound Traveling Salesperson",
			},
		},
		{
			Category: "System Design & Distributed Systems",
			Topics: []string{
				"Consistent Hashing Virtual Nodes Ring", "CAP Theorem Consistency Availability Partition Tolerance",
				"Raft Consensus Protocol Leader Election Log Replication", "Paxos Distributed Consensus Protocol",
				"Vector Clocks Causal Ordering Logical Time", "Rate Limiter Token Bucket Leaky Bucket Fixed Window",
				"Distributed Locking Redis Redlock Zookeeper", "Message Broker Apache Kafka Consumer Groups Partitions",
				"Circuit Breaker Pattern State Transition Retry Exponential Backoff", "Event Sourcing CQRS Command Query Responsibility Segregation",
				"Distributed Tracing OpenTelemetry Jaeger Context Propagation", "API Gateway Reverse Proxy Nginx Envoy Routing",
				"Distributed Transaction Two Phase Commit 2PC Saga Pattern", "Distributed Cache Redis Cluster Eviction Policies",
				"Load Balancer Round Robin Weighted Least Connections", "Database Sharding Horizontal Partitioning Key Hash",
				"Read Replicas Master Slave Replication Lag", "Heartbeat Liveness Health Checks Failure Detection", "Gossip Protocol Cluster Membership SWIM",
				"Idempotency Key Deduplication Replay Protection", "Bulkhead Isolation Thread Pool Separation",
				"Write Through Cache Write Back Cache Write Around", "CDN Content Delivery Network Edge Caching Anycast",
				"Distributed File System HDFS Ceph Blob Storage", "Service Discovery Consul Eureka DNS Based",
				"SLA SLO SLI Service Level Agreement Availability Metrics", "Log Aggregation ELK Stack Fluentd Loki",
				"Database Connection Pooling PgBouncer HikariCP", "Distributed ID Generator Snowflake UUID ULID",
				"Graceful Degradation Fallback Response Rate Throttling",
			},
		},
		{
			Category: "Databases & Storage Engines",
			Topics: []string{
				"LSM Tree Log Structured Merge Tree SSTable MemTable", "B Plus Tree Storage Engine InnoDB WiredTiger",
				"WAL Write Ahead Logging Crash Recovery Journaling", "MVCC Multi Version Concurrency Control Snapshot Isolation",
				"PostgreSQL Indexing BTree GIN GiST BRIN Indexes", "Database Isolation Levels Read Uncommitted Read Committed Repeatable Read Serializable",
				"ACID Transactions Atomicity Consistency Isolation Durability", "Database Sharding Range Hash Directory Based",
				"Redis In Memory Data Structures Hash Set Sorted Set Bitmap", "NoSQL Document Store MongoDB Cassandra Key Value",
				"Graph Database Neo4j Cypher Property Graph", "Columnar Database ClickHouse Apache Parquet OLAP Analytics",
				"Query Optimization EXPLAIN ANALYZE Cost Based Optimizer", "Database Locking Shared Lock Exclusive Lock Intent Locks Row Level",
				"Deadlock Detection Lock Wait Timeout Wait For Graph", "Vector Database HNSW Faiss Milvus Embeddings",
				"Connection Pooling Max Open Connections Max Idle Lifetime", "Database Migration Schema Versioning Liquibase Flyway",
				"Replication Sync Async Semi Sync Replication", "CDC Change Data Capture Debezium Event Streaming",
			},
		},
		{
			Category: "Operating Systems & Low-Level Engineering",
			Topics: []string{
				"Process vs Thread Memory Layout Heap Stack Text Data", "Context Switching CPU Registers Translation Lookaside Buffer TLB",
				"Page Replacement Algorithms LRU Clock Second Chance FIFO", "Virtual Memory Page Tables Paging Segmentation Page Fault",
				"Inodes File Descriptors VFS Virtual File System", "Memory Allocator malloc free jemalloc tcmalloc Slab Allocator",
				"Mutex Semaphore Condition Variable Spinlock Mutex Contention", "Futex Fast Userspace Mutex Linux Kernel Locking",
				"Inter Process Communication Shared Memory Pipe Unix Domain Socket Signal", "Non Blocking IO epoll kqueue IO Multiplexing Select Poll",
				"Asynchronous IO io_uring Ring Buffers Completion Queue", "Linux System Calls sys_enter sys_exit Kernel Mode User Mode",
				"CPU Cache Lines L1 L2 L3 Cache False Sharing Cache Coherence MESI", "Zero Copy I O sendfile splice DMA Direct Memory Access",
				"Signal Handling SIGTERM SIGKILL SIGSEGV Signal Masks", "Cgroups v2 Resource Limits CPU Memory IO Quotas",
				"Namespaces PID Mount Network UTS IPC User Container Isolation", "Thread Synchronization Memory Barrier Atomic Load Store Acquire Release",
				"Process Scheduling O(1) Scheduler Completely Fair Scheduler CFS", "Memory Mapped Files mmap msync Page Cache Sync",
			},
		},
		{
			Category: "Networking & Distributed Protocols",
			Topics: []string{
				"TCP Three Way Handshake SYN SYN ACK ACK Congestion Control", "HTTP 2 Multiplexing Server Push Binary Framing Header Compression",
				"HTTP 3 QUIC Protocol UDP Fast Handshake Connection Migration", "TLS 1.3 Handshake Key Exchange Forward Secrecy Cipher Suites",
				"DNS Resolution Recursive Iterative A AAAA CNAME MX Records", "BGP Border Gateway Protocol Autonomous Systems Path Vector",
				"Subnetting CIDR IPv4 IPv6 Routing Tables Default Gateway", "Socket Programming Non Blocking Sockets epoll kqueue Event Loop",
				"WebSockets Full Duplex Bidirectional Upgrade Handshake", "gRPC Protocol Buffers HTTP 2 Streaming RPC Serialization",
				"NAT Network Address Translation STUN TURN ICE Hole Punching", "UDP Datagram Connectionless Low Latency Packet Loss",
				"TCP Window Size Sliding Window Window Scaling Flow Control", "SSL TLS Certificates CA Public Key Infrastructure PKI",
				"Reverse Proxy Nginx Proxy Pass Host Header Buffering", "ALPN Application Layer Protocol Negotiation TLS Extension",
				"Keep Alive Persistent Connections Connection Reuse Timeout", "CORS Cross Origin Resource Sharing Preflight Flight Headers",
				"SSRF Server Side Request Forgery Defense Egress Filtering", "Load Balancing Layer 4 TCP vs Layer 7 HTTP Reverse Proxying",
			},
		},
		{
			Category: "Go Engineering & Concurrency Systems",
			Topics: []string{
				"Go GMP Scheduler Goroutine M OS Thread P Processor Work Stealing", "Go Channels Buffered Unbuffered Select Case Deadlock Detection",
				"Go Garbage Collector Concurrent Mark Sweep Tri Color Abstraction", "Go Memory Management Small Large Allocations Span Arena mcache mcentral",
				"Go Mutex sync.Mutex sync.RWMutex Starvation Mode Normal Mode", "Go Atomic Operations sync/atomic CompareAndSwap Memory Ordering",
				"Go Escape Analysis Stack vs Heap Allocation Pointer Indirection", "Go Interface Dispatch Interface Table itab Dynamic Method Calls",
				"Go Context Cancellation Timeout Deadline Values Propagation", "Go Singleflight Coalescing Concurrent Duplicate Calls",
				"Go Sync Pool Reuse Garbage Collector Impact Memory Recycling", "Go Slices Header Pointer Length Capacity Resizing Growth",
				"Go Maps Hash Table Bucket Overflow Buckets Eviction Rehash", "Go Defer Statement Performance Cost Return Values Modification",
				"Go Error Handling Panic Recover Custom Error Wrapping", "Go Reflection reflect.Type reflect.Value Performance Impact",
				"Go Generics Type Parameters Type Constraints Performance Compiler", "Go Testing Benchmarks Allocations Profiling pprof",
				"Go Modules go.mod Direct Indirect Dependencies Vendor", "Go Build Tags Conditional Compilation Cross Compilation GOOS GOARCH",
			},
		},
		{
			Category: "Cloud Infrastructure, Kubernetes & DevOps",
			Topics: []string{
				"Kubernetes Pod Scheduling Affinity Anti Affinity NodeSelector Taints Tolerations", "Docker Container Namespaces Cgroups OverlayFS Image Layers",
				"Kafka Partitioning Key Hashing Log Segment Offsets Consumer Groups", "Redis Cluster Hash Slots Sharding Resharding Failover Sentinel",
				"Prometheus Metrics Counter Gauge Histogram Summary Scrape Targets", "Grafana Dashboard Visualization Alerting Metrics Observability",
				"Terraform Infrastructure as Code Provider State File HCL Declarative", "Envoy Proxy Service Mesh Sidecar mTLS Traffic Splitting Filter Chain", "CI CD Pipeline Github Actions Jenkins GitLab CI Docker Build Test Deploy",
				"Horizontal Pod Autoscaler HPA Metrics Server CPU Memory Custom Metrics", "Helm Chart Templates Values Release Rollback Package Manager",
				"Ingress Controller Nginx Traefik Routing Host Rules TLS Termination", "Container Registry Docker Hub ECR GCR Artifact Registry Scans",
				"StatefulSet Persistent Volume Claim PVC Dynamic Provisioning", "Service ClusterIP NodePort LoadBalancer Headless Service",
				"Network Policy Calico Flannel Pod to Pod Egress Ingress Rules", "Chaos Engineering Litmus Chaos Monkey Resiliency Testing",
				"Zero Downtime Deployment Rolling Update Blue Green Canary Release", "Secret Management Vault Kubernetes Secrets Sealed Secrets KMS",
				"Log Aggregation Vector Fluentbit ElasticSearch Kibana OpenSearch",
			},
		},
		{
			Category: "Object-Oriented Design & System Patterns",
			Topics: []string{
				"SOLID Principles Single Responsibility Open Closed Liskov Interface Segregation Dependency Inversion",
				"Factory Pattern Abstract Factory Object Instantiation Encapsulation", "Strategy Pattern Interface Interchangeable Algorithms Runtime Swap",
				"Observer Pattern Event Driven Publisher Subscriber Listener", "Decorator Pattern Dynamic Behavior Composition Wrapper Class",
				"Singleton Pattern Thread Safe Lazy Initialization Double Checked Locking", "Repository Pattern Data Access Abstraction Persistence Agnostic",
				"Dependency Injection IoC Inversion of Control Container Constructor Injection", "Domain Driven Design DDD Bounded Context Aggregate Root Entity Value Object",
				"Clean Architecture Hexagonal Architecture Ports and Adapters Layer Separation", "Command Pattern Encapsulate Request Undo Redo Queue Execution",
				"State Pattern State Transition Object Behavior Change", "Adapter Pattern Interface Compatibility Legacy Code Wrapper",
				"Facade Pattern Unified Simplified Interface Subsystem", "Proxy Pattern Virtual Proxy Protection Proxy Caching Proxy",
				"Template Method Pattern Algorithm Skeleton Subclass Override", "Chain of Responsibility Pattern Request Handling Pipeline Handler",
				"Flyweight Pattern Memory Savings Shared State Extrinsic State", "Bridge Pattern Decouple Abstraction Implementation",
				"Builder Pattern Fluent Interface Stepwise Complex Object Construction",
			},
		},
		{
			Category: "Security, Authentication & Cryptography",
			Topics: []string{
				"OAuth 2.0 Authorization Code Flow Access Token Refresh Token Scopes", "OpenID Connect OIDC Identity Layer ID Token JWT Claims Verification",
				"AES Symmetric Encryption AES GCM Authenticated Encryption Nonce", "RSA Public Key Asymmetric Cryptography Key Pair Signatures",
				"Password Hashing Argon2id bcrypt PBKDF2 Salt Work Factor", "SHA 256 Cryptographic Hash Collision Resistance Hash Digest",
				"CORS Security Allowed Origins Headers Methods Preflight Options", "CSRF Protection SameSite Cookies CSRF Tokens Double Submit Cookie",
				"XSS Cross Site Scripting Sanitization Content Security Policy CSP", "SQL Injection Defense Parameterized Queries Prepared Statements ORM",
				"Mutual TLS mTLS Client Certificates Peer Verification Handshake", "API Key Authentication Rate Limiting Header Query Parameter",
				"Zero Trust Security Model Identity Verification Least Privilege Microsegmentation", "JWT JSON Web Token Header Payload Signature Alg Verification",
				"Key Rotation Secret Management HSM KMS HashiCorp Vault", "WAF Web Application Firewall OWASP Top 10 Rule Engine",
				"DDoS Mitigation Anycast IP Rate Limiting SYN Cookies Scrubbing", "Security Headers HSTS X Content Type Options X Frame Options",
				"RBAC Role Based Access Control Permission Matrix Authorization", "ABAC Attribute Based Access Control Policy Engine Dynamic Evaluation",
			},
		},
		{
			Category: "Machine Learning Infrastructure & RAG Engineering",
			Topics: []string{
				"Transformer Architecture Self Attention Multi Head Attention Positional Encoding", "Attention Mechanism Query Key Value Softmax Scoring Context Vector",
				"Vector Embeddings Dense Feature Representations Cosine Similarity Euclidean Distance", "Reciprocal Rank Fusion RRF Sparse BM25 Dense Vector Score Merging",
				"HNSW Hierarchical Navigable Small World Graph Vector Index", "RAG Retrieval Augmented Generation Ingestion Chunking Embeddings Retrieval Prompting",
				"LLM Fine Tuning LoRA Low Rank Adaptation PEFT Parameter Efficient", "Quantization FP16 INT8 INT4 Model Compression Inference Acceleration",
				"Tokenization Byte Pair Encoding BPE Tiktoken WordPiece Subword", "Prompt Engineering System Prompt Chain of Thought Few Shot In Context",
				"Semantic Search BM25 Hybrid Keyword Vector Scoring", "Vector Store Milvus Qdrant Pinecone Weaviate Chroma Indexing",
				"Document Processor HTML Parsing Markdown Conversion Noise Stripping", "Model Context Protocol MCP JSON RPC Stdio Server Agent Integration",
				"Agent Function Calling JSON Schema Tool Registration Execution Loop", "LLM Inference Server vLLM Ollama TensorRT LLM Batching KV Cache",
				"Embedding Generation SentenceTransformers OpenAI Embeddings Feature Vector", "Reranking Cross Encoder BM25 Candidate Rescoring",
				"Text Chunking Fixed Size Overlap Semantic Header Based Chunking", "Evaluations RAGAS Faithfulness Context Precision Answer Relevance",
			},
		},
	}

	totalExpected := 0
	for _, domain := range domains {
		totalExpected += len(domain.Topics) * 5
	}
	if *limit > 0 && *limit < totalExpected {
		totalExpected = *limit
	}

	var prog *ui.CompositeProgress
	if ui.ShouldUseInteractiveUI(os.Stdout, *noProgress, *quiet) && !*jsonOut {
		prog = ui.NewCompositeProgress(ui.ProgressConfig{
			Writer:         os.Stdout,
			Title:          "Seeding SDE Technical Corpus",
			Total:          int64(totalExpected),
			RefreshRate:    50 * time.Millisecond,
			ShowSpeed:      true,
			ShowETA:        true,
			ShowTokens:     true,
			ShowActiveItem: true,
			EnableColors:   true,
		})
		prog.SetPhase("seeding")
		prog.Start()
	}

	totalIngested := 0
	startTime := time.Now()

	for _, domain := range domains {
		if ctx.Err() != nil {
			break
		}
		if *limit > 0 && totalIngested >= *limit {
			break
		}
		for _, topic := range domain.Topics {
			if ctx.Err() != nil {
				break
			}
			if *limit > 0 && totalIngested >= *limit {
				break
			}
			for v := 1; v <= 5; v++ {
				if ctx.Err() != nil {
					break
				}
				if *limit > 0 && totalIngested >= *limit {
					break
				}
				totalIngested++
				url := fmt.Sprintf("https://sde-knowledge.org/%s/%s/v%d", slugify(domain.Category), slugify(topic), v)
				title, cleanBody := generateSeedArticle(domain.Category, topic, v)
				totalTokens := len(strings.Fields(cleanBody))

				_ = storage.SaveCrawledDocument(ctx, url, title, cleanBody, totalTokens, "sde_corpus", url)
				index.GlobalEngine.IndexDocumentDirectly(url, title, cleanBody, totalTokens)

				if prog != nil {
					prog.Increment()
					prog.AddTokensSaved(int64(totalTokens))
					prog.SetActiveItem(topic)
				}
			}
		}
	}

	if prog != nil {
		prog.Stop()
	}

	saveStorage(*dataDir)
	duration := time.Since(startTime)

	if *jsonOut {
		totalDocs, avgLen, vocabSize := index.GlobalEngine.GetInvertedIndex().GetStats()
		resMap := map[string]interface{}{
			"documents_seeded": totalIngested,
			"duration_seconds": duration.Seconds(),
			"total_docs":       totalDocs,
			"avg_doc_len":      avgLen,
			"vocab_size":       vocabSize,
			"data_dir":         *dataDir,
		}
		jsonBytes, _ := json.MarshalIndent(resMap, "", "  ")
		fmt.Println(string(jsonBytes))
		return
	}

	if !*quiet {
		totalDocs, avgLen, vocabSize := index.GlobalEngine.GetInvertedIndex().GetStats()
		fmt.Fprintf(os.Stderr, "✅ Successfully seeded %d SDE corpus documents in %v!\n", totalIngested, duration)
		fmt.Fprintf(os.Stderr, "   - Total Indexed Documents: %d\n", totalDocs)
		fmt.Fprintf(os.Stderr, "   - Average Document Length: %.2f tokens\n", avgLen)
		fmt.Fprintf(os.Stderr, "   - Vocabulary Size:         %d unique terms\n", vocabSize)
		fmt.Fprintf(os.Stderr, "   - Snapshots saved to:      %s/\n", *dataDir)
	}
}

func generateSeedArticle(category, topic string, variation int) (string, string) {
	subtitles := map[int]string{
		1: "Foundational Architecture and Internal Mechanics",
		2: "Production Implementation and State Management",
		3: "Performance Benchmarking and Scale Optimization",
		4: "Fault Tolerance, Reliability, and Resiliency",
		5: "Advanced Distributed Patterns and Case Studies",
	}

	subtitle := subtitles[variation]
	if subtitle == "" {
		subtitle = fmt.Sprintf("Deep Dive Technical Guide (Part %d)", variation)
	}

	title := fmt.Sprintf("%s: %s — %s", category, topic, subtitle)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# %s\n\n", title))
	sb.WriteString("## Architectural Overview\n\n")
	sb.WriteString(fmt.Sprintf("In high-throughput systems engineering, `%s` represents a critical building block within the `%s` domain. ", topic, category))
	sb.WriteString(fmt.Sprintf("When designing scalable distributed topologies or core systems software, `%s` provides concrete structural guarantees around latency budgets, concurrency hazards, and resource boundaries.\n\n", topic))

	sb.WriteString("## Core Components & Structural Invariants\n\n")
	sb.WriteString(fmt.Sprintf("Implementing `%s` requires rigorous attention to memory alignment, lock contention, cache-line bouncing, and error boundary isolation.\n\n", topic))

	sb.WriteString("## Operational Trade-Offs & Complexity Analysis\n\n")
	sb.WriteString(fmt.Sprintf("Production deployment of `%s` involves evaluating time and space complexity against system-level operational overhead:\n\n", topic))
	sb.WriteString("- **Time Complexity**: Optimal amortized bounds ensure predictable tail latency (p99/p99.9).\n")
	sb.WriteString("- **Space Overhead**: Auxiliary heap allocations are controlled through pre-allocated buffers and object pooling.\n")

	return title, sb.String()
}

func slugify(s string) string {
	var res []rune
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			res = append(res, r)
		} else if r == ' ' || r == '-' || r == '_' || r == '&' {
			res = append(res, '-')
		}
	}
	return string(res)
}

func runCrawl(args []string) {
	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	client := crawler.NewTestClient(allowLoopback)
	runCrawlWithClient(client, args)
}

func runCrawlWithClient(client *crawler.Client, args []string) {
	fs := flag.NewFlagSet("crawl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	maxDepth := fs.Int("depth", 2, "Maximum link crawl depth")
	fs.IntVar(maxDepth, "d", 2, "Maximum link crawl depth (shorthand)")
	maxPages := fs.Int("max-pages", 50, "Maximum pages to crawl")
	fs.IntVar(maxPages, "p", 50, "Maximum pages to crawl (shorthand)")
	include := fs.String("include", "", "Comma-separated URL include patterns")
	fs.StringVar(include, "i", "", "Include patterns (shorthand)")
	exclude := fs.String("exclude", "", "Comma-separated URL exclude patterns")
	fs.StringVar(exclude, "e", "", "Exclude patterns (shorthand)")
	allowSubdoms := fs.Bool("subdomains", false, "Allow crawling subdomains")
	sitemap := fs.Bool("sitemap", true, "Enable sitemap discovery")
	concurrency := fs.Int("concurrency", 8, "Worker concurrency count")
	fs.IntVar(concurrency, "c", 8, "Worker concurrency (shorthand)")
	rateLimit := fs.Float64("rate-limit", 10.0, "Per-host rate limit in requests per second")
	fs.Float64Var(rateLimit, "r", 10.0, "Rate limit RPS (shorthand)")
	jsonOut := fs.Bool("json", false, "Output structured JSON summary")
	fs.BoolVar(jsonOut, "j", false, "Output JSON (shorthand)")
	asyncMode := fs.Bool("async", false, "Run crawl job asynchronously in background")
	adaptive := fs.Bool("adaptive", false, "Enable information-entropy adaptive priority crawl")
	fs.BoolVar(adaptive, "a", false, "Adaptive crawl (shorthand)")
	minPriority := fs.Float64("min-priority", 0.10, "Minimum entropy priority threshold for adaptive crawl")
	dataDir := fs.String("data-dir", "data", "Snapshot data directory")
	forceRefresh := fs.Bool("force-refresh", false, "Force refresh existing crawled documents")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")
	noProgress := fs.Bool("no-progress", false, "Disable live progress rendering")
	quiet := fs.Bool("quiet", false, "Quiet mode")
	fs.BoolVar(quiet, "q", false, "Quiet mode (shorthand)")

	posArgs, err := parseInterleavedFlags(fs, args)
	if err != nil {
		os.Exit(1)
	}

	if len(posArgs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Missing seed URL.")
		fmt.Fprintln(os.Stderr, "Usage: lightlimbs crawl <url> [flags]")
		os.Exit(1)
	}

	seedURL := posArgs[0]
	var incPatterns, excPatterns []string
	if *include != "" {
		incPatterns = strings.Split(*include, ",")
	}
	if *exclude != "" {
		excPatterns = strings.Split(*exclude, ",")
	}

	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("WEBLIMB_ALLOW_LOOPBACK") == "1" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	if client == nil {
		client = crawler.NewTestClient(allowLoopback)
	}

	setupStorage(*dataDir, *dbType, *sqlitePath)
	loadSnapshotsForCLI(*dataDir)
	jm := crawler.NewJobManager(client, *dataDir)
	defer jm.Close()

	req := crawler.CrawlRequest{
		URL:              seedURL,
		MaxDepth:         *maxDepth,
		MaxPages:         *maxPages,
		IncludePatterns:  incPatterns,
		ExcludePatterns:  excPatterns,
		AllowSubdomains:  *allowSubdoms,
		SitemapDiscovery: *sitemap,
		Concurrency:      *concurrency,
		RateLimitRPS:     *rateLimit,
		Async:            *asyncMode,
		AllowLoopback:    allowLoopback,
		Adaptive:         *adaptive,
		MinPriority:      *minPriority,
		ForceRefresh:     *forceRefresh,
	}

	var prog *ui.CompositeProgress
	var nonTTY *ui.NonTTYReporter

	if !*asyncMode && !*jsonOut {
		if ui.ShouldUseInteractiveUI(os.Stdout, *noProgress, *quiet) {
			prog = ui.NewCompositeProgress(ui.ProgressConfig{
				Writer:         os.Stdout,
				Title:          fmt.Sprintf("WebLimbAI Crawl (%s)", seedURL),
				Total:          int64(*maxPages),
				RefreshRate:    50 * time.Millisecond,
				ShowSpeed:      true,
				ShowETA:        true,
				ShowQueue:      true,
				ShowTokens:     true,
				ShowBytes:      true,
				ShowActiveItem: true,
				EnableColors:   true,
			})
			prog.SetPhase("crawling")
			prog.Start()
		} else if !*quiet && !*noProgress {
			nonTTY = ui.NewNonTTYReporter(os.Stderr, 3*time.Second)
		}
	}

	startTime := time.Now()
	job, err := jm.StartCrawl(context.Background(), req)
	if err != nil {
		if prog != nil {
			prog.Stop()
		}
		fmt.Fprintf(os.Stderr, "Error starting crawl: %v\n", err)
		os.Exit(1)
	}

	if prog != nil || nonTTY != nil {
		events := job.SubscribeEvents()
		go func() {
			for ev := range events {
				if prog != nil {
					prog.Send(ev)
				}
				if nonTTY != nil {
					nonTTY.OnEvent(ev)
				}
			}
		}()
	}

	if !*asyncMode {
		saveStorage(*dataDir)
	}

	if prog != nil {
		prog.Stop()
	}

	if *jsonOut {
		jsonBytes, _ := json.MarshalIndent(job, "", "  ")
		fmt.Println(string(jsonBytes))
		return
	}

	if *asyncMode {
		fmt.Printf("Crawl job %s started in background.\n", job.ID)
		return
	}

	if !*quiet {
		crawled := job.PagesCrawled.Load()
		cached := job.CacheHits.Load()
		okFetched := crawled - cached
		if okFetched < 0 {
			okFetched = 0
		}
		duration := time.Since(startTime)
		dbPath := *sqlitePath
		if dbPath == "" {
			dbPath = filepath.Join(*dataDir, "docs.db")
		}

		fmt.Println("\n+-------------------------------------------------------------------------------+")
		fmt.Printf("|                       WebLimbAI Crawl Complete (Job %s)                   |\n", ui.TruncateMiddle(job.ID, 12, "..."))
		fmt.Println("+-------------------------------------------------------------------------------+")
		fmt.Printf("|  • Pages Crawled:       %d total (%d fetched 200 OK, %d cached 304)\n", crawled, okFetched, cached)
		fmt.Printf("|  • Total Queued:        %d discovered links\n", job.PagesQueued.Load())
		fmt.Printf("|  • Raw Data Read:       %s\n", ui.FormatBytes(job.BytesRead.Load()))
		fmt.Printf("|  • Token Savings:       %s tokens\n", ui.FormatNumber(job.TokensSaved.Load()))
		fmt.Printf("|  • Wall Time:           %s\n", ui.FormatDuration(duration))
		fmt.Printf("|  • Errors Encountered:  %d errors\n", job.ErrorsCount.Load())
		fmt.Printf("|  • Snapshot DB:         %s\n", dbPath)
		fmt.Println("+-------------------------------------------------------------------------------+")
	}
}

func runExtract(args []string) {
	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("WEBLIMB_ALLOW_LOOPBACK") == "1" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	client := crawler.NewTestClient(allowLoopback)
	runExtractWithClient(client, args)
}

func runExtractWithClient(client *crawler.Client, args []string) {
	fs := flag.NewFlagSet("extract", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	schemaPath := fs.String("schema", "", "JSON Schema file path or raw JSON string")
	fs.StringVar(schemaPath, "s", "", "JSON Schema path/string (shorthand)")
	prompt := fs.String("prompt", "", "Additional extraction instructions")
	fs.StringVar(prompt, "p", "", "Extraction prompt (shorthand)")
	model := fs.String("model", "", "LLM model override")
	fs.StringVar(model, "m", "", "LLM model (shorthand)")
	outFile := fs.String("out", "", "Save JSON output to file")
	fs.StringVar(outFile, "o", "", "Output file (shorthand)")
	jsonOut := fs.Bool("json", true, "Output raw JSON")
	fs.BoolVar(jsonOut, "j", true, "Output JSON (shorthand)")

	posArgs, err := parseInterleavedFlags(fs, args)
	if err != nil {
		os.Exit(1)
	}

	if len(posArgs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Missing target URL or file path.")
		fmt.Fprintln(os.Stderr, "Usage: lightlimbs extract <url> --schema <file.json> [flags]")
		os.Exit(1)
	}

	if *schemaPath == "" {
		fmt.Fprintln(os.Stderr, "Error: Missing required --schema flag.")
		os.Exit(1)
	}

	schemaBytes := []byte(*schemaPath)
	if fileBytes, readErr := os.ReadFile(*schemaPath); readErr == nil {
		schemaBytes = fileBytes
	}

	target := posArgs[0]
	var htmlContent string
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		normURL, err := utils.NormalizeURL(target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Invalid URL: %v\n", err)
			os.Exit(1)
		}
		allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("WEBLIMB_ALLOW_LOOPBACK") == "1" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
		if client == nil {
			client = crawler.NewTestClient(allowLoopback)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		res, err := client.Fetch(ctx, utils.TransformGitHubURL(normURL))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching %s: %v\n", target, err)
			os.Exit(1)
		}
		defer res.Response.Body.Close()

		limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
		b, _ := io.ReadAll(limitedBody)
		htmlContent = string(b)
	} else if fileContent, err := os.ReadFile(target); err == nil {
		htmlContent = string(fileContent)
	} else {
		htmlContent = target
	}

	allowLoopback := os.Getenv("ENV") == "test" || os.Getenv("WEBLIMB_ALLOW_LOOPBACK") == "1" || os.Getenv("AGENTLIMBS_ALLOW_LOOPBACK") == "1"
	llm := search.NewLLMProviderFromEnv(allowLoopback)

	result, err := extractor.ExtractStructuredJSON(context.Background(), htmlContent, string(schemaBytes), *prompt, llm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error during extraction: %v\n", err)
		os.Exit(1)
	}

	jsonBytes, _ := json.MarshalIndent(result.Data, "", "  ")
	if *outFile != "" {
		_ = os.WriteFile(*outFile, jsonBytes, 0644)
		fmt.Fprintf(os.Stderr, "Extracted JSON saved to %s\n", *outFile)
	} else if *jsonOut {
		fmt.Println(string(jsonBytes))
	}
}

func main() {
	logger.InitLogger(os.Getenv("ENV"))
	defer logger.Sync()

	if len(os.Args) == 1 {
		runServe(nil)
		return
	}

	firstArg := os.Args[1]

	// Handle global help or version flags
	if firstArg == "-h" || firstArg == "--help" || firstArg == "help" {
		printHelp()
		return
	}

	if firstArg == "-v" || firstArg == "--version" || firstArg == "version" {
		provider := os.Getenv("EMBEDDING_PROVIDER")
		if provider == "" {
			provider = "default (128-D subword n-gram)"
		}
		fmt.Printf("WebLimbAI LightLimbs (%s)\n", version)
		fmt.Printf("  - Architecture: Embedded Single-Binary\n")
		fmt.Printf("  - Embedding Provider: %s\n", provider)
		fmt.Printf("  - Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		fmt.Printf("  - Go Runtime: %s\n", runtime.Version())
		return
	}

	// Backward compatibility: If first arg starts with "-", treat as `serve` flags
	if strings.HasPrefix(firstArg, "-") {
		runServe(os.Args[1:])
		return
	}

	// Dispatch to subcommands
	subcommandArgs := os.Args[2:]
	switch firstArg {
	case "serve":
		runServe(subcommandArgs)
	case "scrape":
		runScrape(subcommandArgs)
	case "crawl":
		runCrawl(subcommandArgs)
	case "extract":
		runExtract(subcommandArgs)
	case "search":
		runSearch(subcommandArgs)
	case "init-mcp":
		runInitMCP(subcommandArgs)
	case "index":
		runIndex(subcommandArgs)
	case "seed":
		runSeed(subcommandArgs)
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown subcommand %q\nRun 'lightlimbs --help' for usage.\n", firstArg)
		os.Exit(1)
	}
}
func runIndex(args []string) {
	fs := flag.NewFlagSet("index", flag.ExitOnError)
	includeStr := fs.String("include", "", "Comma-separated glob patterns to include")
	excludeStr := fs.String("exclude", "", "Comma-separated glob patterns to exclude")
	maxSizeMB := fs.Int64("max-size", 10, "Max file size in MB")
	dryRun := fs.Bool("dry-run", false, "Dry run, do not index")
	noIndex := fs.Bool("no-index", false, "Only process files, do not write to index")
	dataDir := fs.String("data-dir", "data", "Data directory")
	fs.StringVar(dataDir, "d", "data", "Data directory (shorthand)")
	dbType := fs.String("db-type", "sqlite", "Storage driver (sqlite, file, postgres, memory)")
	sqlitePath := fs.String("sqlite-path", "", "Path to SQLite database file (default: <data-dir>/docs.db)")
	noProgress := fs.Bool("no-progress", false, "Disable progress bar")
	quiet := fs.Bool("quiet", false, "Quiet mode")
	fs.BoolVar(quiet, "q", false, "Quiet mode (shorthand)")
	jsonOut := fs.Bool("json", false, "Output JSON summary")
	fs.BoolVar(jsonOut, "j", false, "Output JSON (shorthand)")

	fs.Parse(args)
	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: lightlimbs index <path> [flags]\n")
		os.Exit(1)
	}

	path := fs.Arg(0)

	var includes, excludes []string
	if *includeStr != "" {
		includes = strings.Split(*includeStr, ",")
	}
	if *excludeStr != "" {
		excludes = strings.Split(*excludeStr, ",")
	}

	broadcaster := ui.NewEventBroadcaster(512)
	defer broadcaster.Close()

	cfg := crawler.LocalIndexerConfig{
		BasePath:        path,
		MaxFileSize:     *maxSizeMB * 1024 * 1024,
		IncludePatterns: includes,
		ExcludePatterns: excludes,
		DryRun:          *dryRun,
		NoIndex:         *noIndex,
		Broadcaster:     broadcaster,
	}

	setupStorage(*dataDir, *dbType, *sqlitePath)
	loadSnapshotsForCLI(*dataDir)

	var prog *ui.CompositeProgress
	var nonTTY *ui.NonTTYReporter

	if !*jsonOut {
		if ui.ShouldUseInteractiveUI(os.Stdout, *noProgress, *quiet) {
			prog = ui.NewCompositeProgress(ui.ProgressConfig{
				Writer:         os.Stdout,
				Title:          fmt.Sprintf("Indexing %s", path),
				Total:          0, // indeterminate initially
				RefreshRate:    50 * time.Millisecond,
				ShowSpeed:      true,
				ShowActiveItem: true,
				EnableColors:   true,
			})
			prog.SetPhase("indexing")
			prog.Start()
		} else if !*quiet && !*noProgress {
			nonTTY = ui.NewNonTTYReporter(os.Stderr, 3*time.Second)
		}
	}

	if prog != nil || nonTTY != nil {
		events := broadcaster.Subscribe()
		go func() {
			for ev := range events {
				if prog != nil {
					prog.Send(ev)
				}
				if nonTTY != nil {
					nonTTY.OnEvent(ev)
				}
			}
		}()
	}

	start := time.Now()
	res, err := crawler.IndexDirectory(context.Background(), cfg, index.GlobalEngine, storage.GetGlobalStore())
	if err != nil {
		if prog != nil {
			prog.Stop()
		}
		fmt.Fprintf(os.Stderr, "Error indexing directory: %v\n", err)
		os.Exit(1)
	}

	if prog != nil {
		prog.Stop()
	}

	if !*dryRun && !*noIndex {
		saveStorage(*dataDir)
	}

	duration := time.Since(start)

	if *jsonOut {
		resMap := map[string]interface{}{
			"path":             path,
			"duration_seconds": duration.Seconds(),
			"files_processed":  res.FilesProcessed,
			"files_indexed":    res.FilesIndexed,
			"files_skipped":    res.FilesSkipped,
			"files_deleted":    res.FilesDeleted,
			"errors":           res.Errors,
		}
		jsonBytes, _ := json.MarshalIndent(resMap, "", "  ")
		fmt.Println(string(jsonBytes))
		return
	}

	if !*quiet {
		fmt.Printf("Finished in %v\n", duration)
		fmt.Printf("Files Processed: %d\n", res.FilesProcessed)
		fmt.Printf("Files Indexed:   %d\n", res.FilesIndexed)
		fmt.Printf("Files Skipped:   %d\n", res.FilesSkipped)
		fmt.Printf("Files Deleted:   %d\n", res.FilesDeleted)
		fmt.Printf("Errors:          %d\n", res.Errors)
	}
}

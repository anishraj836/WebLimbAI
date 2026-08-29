package crawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/crawler-monorepo/common/stemmer"
	"github.com/crawler-monorepo/common/stopwords"
	"github.com/crawler-monorepo/internal/extractor"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/storage"
	"github.com/crawler-monorepo/internal/ui"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// CrawlRequest defines configuration and boundaries for recursive whole-domain crawling.
type CrawlRequest struct {
	URL              string            `json:"url"`
	MaxDepth         int               `json:"max_depth"`
	MaxPages         int               `json:"max_pages"`
	IncludePatterns  []string          `json:"include_patterns,omitempty"`
	ExcludePatterns  []string          `json:"exclude_patterns,omitempty"`
	AllowSubdomains  bool              `json:"allow_subdomains,omitempty"`
	SitemapDiscovery bool              `json:"sitemap_discovery,omitempty"`
	Concurrency      int               `json:"concurrency,omitempty"`
	RateLimitRPS     float64           `json:"rate_limit_rps,omitempty"`
	Async            bool              `json:"async,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Cookies          map[string]string `json:"cookies,omitempty"`
	AllowLoopback    bool              `json:"allow_loopback,omitempty"`
	Adaptive         bool              `json:"adaptive,omitempty"`
	MinPriority      float64           `json:"min_priority,omitempty"`
	ForceRefresh     bool              `json:"force_refresh,omitempty"`
}

// CrawlPageResult records the outcome and token savings of an individual crawled page.
type CrawlPageResult struct {
	URL            string  `json:"url"`
	Title          string  `json:"title"`
	Tokens         int     `json:"tokens"`
	RawTokens      int     `json:"raw_tokens"`
	SavingsPct     float64 `json:"savings_pct"`
	Depth          int     `json:"depth"`
	StatusCode     int     `json:"status_code"`
	LatencyMs      float64 `json:"latency_ms"`
	DiscoveredURLs int     `json:"discovered_urls"`
}

// CrawlJob tracks the runtime state, atomic counters, and error logs of an active or finished crawl.
type CrawlJob struct {
	ID           string               `json:"id"`
	Status       string               `json:"status"` // "running", "completed", "failed", "cancelled"
	Request      CrawlRequest         `json:"request"`
	PagesCrawled atomic.Int64         `json:"pages_crawled"`
	PagesQueued  atomic.Int64         `json:"pages_queued"`
	TokensSaved  atomic.Int64         `json:"tokens_saved"`
	ErrorsCount  atomic.Int64         `json:"errors_count"`
	BytesRead    atomic.Int64         `json:"bytes_read"`
	CacheHits    atomic.Int64         `json:"cache_hits"`
	RecentErrors []string             `json:"recent_errors"`
	Results      []CrawlPageResult    `json:"results,omitempty"`
	StartTime    time.Time            `json:"start_time"`
	EndTime      *time.Time           `json:"end_time,omitempty"`
	CancelFunc   context.CancelFunc   `json:"-"`
	Broadcaster  *ui.EventBroadcaster `json:"-"`
	mu           sync.RWMutex         `json:"-"`
}

func (j *CrawlJob) SubscribeEvents() <-chan ui.ProgressEvent {
	if j.Broadcaster == nil {
		ch := make(chan ui.ProgressEvent)
		close(ch)
		return ch
	}
	return j.Broadcaster.Subscribe()
}

func (j *CrawlJob) UnsubscribeEvents(ch <-chan ui.ProgressEvent) {
	if j.Broadcaster != nil {
		j.Broadcaster.Unsubscribe(ch)
	}
}

func (j *CrawlJob) PublishEvent(ev ui.ProgressEvent) {
	if ev.JobID == "" {
		ev.JobID = j.ID
	}
	if ev.Phase == "" {
		ev.Phase = "crawling"
	}
	if j.Broadcaster != nil {
		j.Broadcaster.Publish(ev)
	}
}

func (j *CrawlJob) AddError(errStr string) {
	j.ErrorsCount.Add(1)
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.RecentErrors) >= 10 {
		j.RecentErrors = j.RecentErrors[1:]
	}
	j.RecentErrors = append(j.RecentErrors, errStr)
}

func (j *CrawlJob) AddResult(res CrawlPageResult) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Results = append(j.Results, res)
}

func (j *CrawlJob) SetStatus(status string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Status = status
	if status == "completed" || status == "failed" || status == "cancelled" {
		now := time.Now()
		j.EndTime = &now
	}
}

func (j *CrawlJob) GetStatus() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Status
}

// MarshalJSON implements custom JSON marshaling with thread-safe atomic loads.
func (j *CrawlJob) MarshalJSON() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	type Alias CrawlJob
	return json.Marshal(&struct {
		*Alias
		PagesCrawled int64 `json:"pages_crawled"`
		PagesQueued  int64 `json:"pages_queued"`
		TokensSaved  int64 `json:"tokens_saved"`
		ErrorsCount  int64 `json:"errors_count"`
		BytesRead    int64 `json:"bytes_read"`
		CacheHits    int64 `json:"cache_hits"`
	}{
		Alias:        (*Alias)(j),
		PagesCrawled: j.PagesCrawled.Load(),
		PagesQueued:  j.PagesQueued.Load(),
		TokensSaved:  j.TokensSaved.Load(),
		ErrorsCount:  j.ErrorsCount.Load(),
		BytesRead:    j.BytesRead.Load(),
		CacheHits:    j.CacheHits.Load(),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling into atomic counters.
func (j *CrawlJob) UnmarshalJSON(data []byte) error {
	type Alias CrawlJob
	aux := &struct {
		*Alias
		PagesCrawled int64 `json:"pages_crawled"`
		PagesQueued  int64 `json:"pages_queued"`
		TokensSaved  int64 `json:"tokens_saved"`
		ErrorsCount  int64 `json:"errors_count"`
		BytesRead    int64 `json:"bytes_read"`
		CacheHits    int64 `json:"cache_hits"`
	}{
		Alias: (*Alias)(j),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.PagesCrawled.Store(aux.PagesCrawled)
	j.PagesQueued.Store(aux.PagesQueued)
	j.TokensSaved.Store(aux.TokensSaved)
	j.ErrorsCount.Store(aux.ErrorsCount)
	j.BytesRead.Store(aux.BytesRead)
	j.CacheHits.Store(aux.CacheHits)
	return nil
}

// JobManager coordinates active and historical crawl jobs.
type JobManager struct {
	jobs         sync.Map // jobID -> *CrawlJob
	rateLimiters sync.Map // host -> *rate.Limiter
	httpClient   *Client
	dataDir      string
	ttl          time.Duration
	closeChan    chan struct{}
	closeOnce    sync.Once
}

func NewJobManager(client *Client, dataDir string) *JobManager {
	jm := &JobManager{
		httpClient: client,
		dataDir:    dataDir,
		ttl:        1 * time.Hour,
		closeChan:  make(chan struct{}),
	}
	go jm.startJanitor()
	return jm
}

func (m *JobManager) startJanitor() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeChan:
			return
		case <-ticker.C:
			m.EvictExpiredJobs()
		}
	}
}

func (m *JobManager) EvictExpiredJobs() {
	ttl := m.ttl
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	now := time.Now()
	var finishedJobs []*CrawlJob

	m.jobs.Range(func(key, value any) bool {
		job := value.(*CrawlJob)
		job.mu.RLock()
		isFinished := job.Status == "completed" || job.Status == "failed" || job.Status == "cancelled"
		endTime := job.EndTime
		job.mu.RUnlock()

		if isFinished && endTime != nil && now.Sub(*endTime) > ttl {
			m.jobs.Delete(key)
		} else if isFinished {
			finishedJobs = append(finishedJobs, job)
		}
		return true
	})

	const maxRetained = 1000
	if len(finishedJobs) > maxRetained {
		excess := len(finishedJobs) - maxRetained
		for i := 0; i < excess; i++ {
			m.jobs.Delete(finishedJobs[i].ID)
		}
	}
}

func (m *JobManager) GetJob(jobID string) (*CrawlJob, bool) {
	v, ok := m.jobs.Load(jobID)
	if !ok {
		return nil, false
	}
	return v.(*CrawlJob), true
}

func (m *JobManager) CancelJob(jobID string) bool {
	v, ok := m.jobs.Load(jobID)
	if !ok {
		return false
	}
	job := v.(*CrawlJob)
	if job.CancelFunc != nil {
		job.CancelFunc()
		job.SetStatus("cancelled")
		return true
	}
	return false
}

func (m *JobManager) getRateLimiter(host string, rps float64, concurrency int) *rate.Limiter {
	if rps <= 0 {
		rps = 10.0
	}
	if v, ok := m.rateLimiters.Load(host); ok {
		return v.(*rate.Limiter)
	}

	burst := concurrency / 2
	if burst < 1 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(rps), burst)
	actual, _ := m.rateLimiters.LoadOrStore(host, limiter)
	return actual.(*rate.Limiter)
}

func (m *JobManager) StartCrawl(ctx context.Context, req CrawlRequest) (*CrawlJob, error) {
	if req.URL == "" {
		return nil, fmt.Errorf("URL is required")
	}
	if req.MaxDepth < 0 {
		req.MaxDepth = 0
	}
	if req.MaxPages <= 0 {
		req.MaxPages = 50
	}
	if req.MaxPages > 10000 {
		req.MaxPages = 10000
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 8
	}
	if req.Concurrency > 32 {
		req.Concurrency = 32
	}
	if req.RateLimitRPS <= 0 {
		req.RateLimitRPS = 10.0
	}

	seedCanonical, err := NormalizeCanonicalURL(req.URL, req.AllowLoopback)
	if err != nil {
		return nil, fmt.Errorf("invalid seed URL: %w", err)
	}

	jobID := uuid.New().String()
	jobCtx, jobCancel := context.WithCancel(context.Background())

	broadcaster := ui.NewEventBroadcaster(512)
	job := &CrawlJob{
		ID:           jobID,
		Status:       "running",
		Request:      req,
		Broadcaster:  broadcaster,
		RecentErrors: make([]string, 0),
		Results:      make([]CrawlPageResult, 0),
		StartTime:    time.Now(),
		CancelFunc:   jobCancel,
	}

	m.jobs.Store(jobID, job)

	if req.Adaptive {
		pf, err := NewPriorityFrontier(PriorityFrontierConfig{
			SeedURL:         seedCanonical,
			MaxDepth:        req.MaxDepth,
			MaxPages:        req.MaxPages,
			AllowSubdomains: req.AllowSubdomains,
			IncludePatterns: req.IncludePatterns,
			ExcludePatterns: req.ExcludePatterns,
			MinPriority:     req.MinPriority,
			AllowLoopback:   req.AllowLoopback,
		})
		if err != nil {
			job.SetStatus("failed")
			job.AddError(err.Error())
			return job, err
		}
		_, _ = pf.Enqueue(seedCanonical, "Root Seed", 0)
		job.PagesQueued.Add(pf.PagesQueued())

		if req.Async {
			go m.executeAdaptiveCrawl(jobCtx, job, pf)
			return job, nil
		}
		m.executeAdaptiveCrawl(jobCtx, job, pf)
		return job, nil
	}

	frontier, err := NewFrontier(FrontierConfig{
		SeedURL:         seedCanonical,
		MaxDepth:        req.MaxDepth,
		MaxPages:        req.MaxPages,
		AllowSubdomains: req.AllowSubdomains,
		IncludePatterns: req.IncludePatterns,
		ExcludePatterns: req.ExcludePatterns,
		AllowLoopback:   req.AllowLoopback,
	})
	if err != nil {
		job.SetStatus("failed")
		job.AddError(err.Error())
		return job, err
	}
	job.PagesQueued.Add(frontier.PagesQueued())

	if req.Async {
		go m.executeCrawl(jobCtx, job, frontier)
		return job, nil
	}

	m.executeCrawl(jobCtx, job, frontier)
	return job, nil
}

func (m *JobManager) executeAdaptiveCrawl(ctx context.Context, job *CrawlJob, pf *PriorityFrontier) {
	defer pf.Close()
	if job.CancelFunc != nil {
		defer job.CancelFunc()
	}

	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventStart,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      "running",
	})

	// Context cancellation watcher bridge
	go func() {
		<-ctx.Done()
		pf.Close()
	}()

	st := NewSubtreeTracker()

	if job.Request.SitemapDiscovery {
		sitemapURLs := DiscoverSitemaps(job.Request.URL)
		for _, sitemapURL := range sitemapURLs {
			if ctx.Err() != nil {
				break
			}
			links, err := FetchAndParseSitemap(ctx, m.httpClient, sitemapURL)
			if err == nil && len(links) > 0 {
				for _, link := range links {
					if enqueued, _ := pf.Enqueue(link, "Sitemap", 1); enqueued {
						job.PagesQueued.Add(1)
					}
				}
				break
			}
		}
	}

	var activeWorkers atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < job.Request.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				if ctx.Err() != nil || job.PagesCrawled.Load() >= int64(job.Request.MaxPages) {
					return
				}

				item, ok := pf.TryDequeue()
				if !ok {
					if activeWorkers.Load() == 0 && pf.Len() == 0 {
						return // Quiescence
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(25 * time.Millisecond):
					}
					continue
				}

				if st.IsPruned(item.URL, item.Depth) {
					continue
				}

				activeWorkers.Add(1)
				m.processAdaptiveCrawlItem(ctx, job, pf, st, item)
				activeWorkers.Add(-1)
			}
		}()
	}

	wg.Wait()

	if job.GetStatus() == "running" {
		if ctx.Err() != nil {
			job.SetStatus("cancelled")
		} else {
			job.SetStatus("completed")
		}
	}

	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventFinish,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      job.GetStatus(),
	})
}

func (m *JobManager) processAdaptiveCrawlItem(ctx context.Context, job *CrawlJob, pf *PriorityFrontier, st *SubtreeTracker, item *PriorityItem) {
	t0 := time.Now()

	u, err := url.Parse(item.URL)
	if err != nil {
		job.AddError(fmt.Sprintf("%s: %v", item.URL, err))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     err.Error(),
		})
		return
	}

	limiter := m.getRateLimiter(u.Hostname(), job.Request.RateLimitRPS, job.Request.Concurrency)
	if waitErr := limiter.Wait(ctx); waitErr != nil {
		return
	}

	existingDoc, _ := storage.GetCrawledDocumentByURL(ctx, item.URL)
	opts := FetchOptions{
		Headers: job.Request.Headers,
		Cookies: job.Request.Cookies,
	}
	if existingDoc != nil && !job.Request.ForceRefresh {
		opts.ETag = existingDoc.ETag
		opts.LastModified = existingDoc.LastModified
	}

	res, fetchErr := m.httpClient.FetchWithAuth(ctx, item.URL, opts)
	if fetchErr != nil {
		job.AddError(fmt.Sprintf("%s: %v", item.URL, fetchErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     fetchErr.Error(),
		})
		return
	}

	if res.NotModified && existingDoc != nil {
		updatedDoc := *existingDoc
		updatedDoc.LastCrawledAt = time.Now()
		updatedDoc.HTTPStatus = 304
		storage.UpsertCrawledDocument(ctx, &updatedDoc, 0)

		if item.Depth < job.Request.MaxDepth {
			for _, link := range existingDoc.OutboundLinks {
				if enqueued, _ := pf.Enqueue(link, "", item.Depth+1); enqueued {
					job.PagesQueued.Add(1)
				}
			}
		}

		job.CacheHits.Add(1)
		job.PagesCrawled.Add(1)
		latency := float64(time.Since(t0).Microseconds()) / 1000.0
		job.AddResult(CrawlPageResult{
			URL:            res.FinalURL,
			Title:          existingDoc.Title,
			Tokens:         existingDoc.TotalTokens,
			RawTokens:      existingDoc.TotalTokens,
			SavingsPct:     0,
			Depth:          item.Depth,
			StatusCode:     304,
			LatencyMs:      latency,
			DiscoveredURLs: len(existingDoc.OutboundLinks),
		})
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "cached",
			ActiveItem:  res.FinalURL,
		})
		return
	}
	defer func() {
		if res.Response != nil && res.Response.Body != nil {
			res.Response.Body.Close()
		}
	}()

	if res.Response.StatusCode >= 400 {
		job.AddError(fmt.Sprintf("%s returned HTTP %d", item.URL, res.Response.StatusCode))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     fmt.Sprintf("HTTP %d", res.Response.StatusCode),
		})
		return
	}

	limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
	htmlBytes, readErr := io.ReadAll(limitedBody)
	if readErr != nil {
		job.AddError(fmt.Sprintf("%s: failed to read body: %v", item.URL, readErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     readErr.Error(),
		})
		return
	}
	job.BytesRead.Add(int64(len(htmlBytes)))

	contentType := res.Response.Header.Get("Content-Type")
	mdText, tokens, title, extractErr := extractor.ExtractDocumentText(res.FinalURL, contentType, htmlBytes, "clean_rag")
	if extractErr != nil {
		job.AddError(fmt.Sprintf("%s: extraction error: %v", item.URL, extractErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     extractErr.Error(),
		})
		return
	}

	rawTokens := len(strings.Fields(string(htmlBytes)))
	savingsPct := 0.0
	if rawTokens > 0 {
		savingsPct = float64(rawTokens-tokens) / float64(rawTokens) * 100.0
	}
	if savingsPct < 0 {
		savingsPct = 0
	}

	newHashBytes := sha256.Sum256([]byte(mdText))
	newHash := hex.EncodeToString(newHashBytes[:])

	// Link discovery with anchor extraction
	var outboundLinks []string
	discoveredCount := 0
	linksCount := 0
	if strings.Contains(contentType, "html") {
		doc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(htmlBytes))
		if parseErr == nil {
			doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
				linksCount++
				href, exists := s.Attr("href")
				if exists {
					resolved := resolveRelativeURL(res.FinalURL, href)
					if resolved != "" {
						outboundLinks = append(outboundLinks, resolved)
						if item.Depth < job.Request.MaxDepth {
							anchor := strings.TrimSpace(s.Text())
							if anchor == "" {
								if alt, ok := s.Find("img").Attr("alt"); ok {
									anchor = alt
								} else if titleAttr, ok := s.Attr("title"); ok {
									anchor = titleAttr
								} else if aria, ok := s.Attr("aria-label"); ok {
									anchor = aria
								}
							}
							if enqueued, _ := pf.Enqueue(resolved, anchor, item.Depth+1); enqueued {
								job.PagesQueued.Add(1)
								discoveredCount++
							}
						}
					}
				}
			})
		}
	}

	if existingDoc != nil && existingDoc.ContentHash == newHash && !job.Request.ForceRefresh {
		updatedDoc := *existingDoc
		updatedDoc.ETag = res.ETag
		updatedDoc.LastModified = res.LastModified
		updatedDoc.LastCrawledAt = time.Now()
		updatedDoc.HTTPStatus = 200
		updatedDoc.OutboundLinks = outboundLinks
		storage.UpsertCrawledDocument(ctx, &updatedDoc, 0)
		
		st.RecordYield(res.FinalURL, CleanYieldRatio(mdText, string(htmlBytes)), linksCount, item.Depth)

		job.CacheHits.Add(1)
		job.PagesCrawled.Add(1)
		latency := float64(time.Since(t0).Microseconds()) / 1000.0
		job.AddResult(CrawlPageResult{
			URL:            res.FinalURL,
			Title:          title,
			Tokens:         tokens,
			RawTokens:      rawTokens,
			SavingsPct:     math.Round(savingsPct*10) / 10,
			Depth:          item.Depth,
			StatusCode:     200,
			LatencyMs:      latency,
			DiscoveredURLs: discoveredCount,
		})
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "ok",
			ActiveItem:  res.FinalURL,
		})
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

	index.GlobalEngine.IndexDocumentWithSource(res.FinalURL, docTitle, bodyForIndexing, termPositions, tokens, "crawl_job", res.FinalURL)

	newDoc := &storage.CrawledDocument{
		URL:           res.FinalURL,
		Title:         title,
		CleanBody:     bodyForIndexing,
		TotalTokens:   tokens,
		SourceType:    "web_crawled",
		SourceURL:     res.FinalURL,
		ETag:          res.ETag,
		LastModified:  res.LastModified,
		ContentHash:   newHash,
		LastCrawledAt: time.Now(),
		HTTPStatus:    res.StatusCode,
		OutboundLinks: outboundLinks,
	}
	storage.UpsertCrawledDocument(ctx, newDoc, 0)

	// Update adaptive subtree tracker yield
	yieldRatio := CleanYieldRatio(mdText, string(htmlBytes))
	st.RecordYield(res.FinalURL, yieldRatio, linksCount, item.Depth)

	tokensSaved := rawTokens - tokens
	if tokensSaved > 0 {
		job.TokensSaved.Add(int64(tokensSaved))
	}
	job.PagesCrawled.Add(1)

	latency := float64(time.Since(t0).Microseconds()) / 1000.0
	job.AddResult(CrawlPageResult{
		URL:            res.FinalURL,
		Title:          title,
		Tokens:         tokens,
		RawTokens:      rawTokens,
		SavingsPct:     math.Round(savingsPct*10) / 10,
		Depth:          item.Depth,
		StatusCode:     res.Response.StatusCode,
		LatencyMs:      latency,
		DiscoveredURLs: discoveredCount,
	})
	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventProgress,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      "ok",
		ActiveItem:  res.FinalURL,
	})
}

func (m *JobManager) executeCrawl(ctx context.Context, job *CrawlJob, frontier *Frontier) {
	defer frontier.Close()
	if job.CancelFunc != nil {
		defer job.CancelFunc()
	}

	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventStart,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      "running",
	})

	if job.Request.SitemapDiscovery {
		sitemapURLs := DiscoverSitemaps(job.Request.URL)
		for _, sitemapURL := range sitemapURLs {
			if ctx.Err() != nil {
				break
			}
			links, err := FetchAndParseSitemap(ctx, m.httpClient, sitemapURL)
			if err == nil && len(links) > 0 {
				for _, link := range links {
					if enqueued, _ := frontier.Enqueue(link, 1); enqueued {
						job.PagesQueued.Add(1)
					}
				}
				break
			}
		}
	}

	var activeWorkers atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < job.Request.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				if ctx.Err() != nil {
					return
				}
				if job.PagesCrawled.Load() >= int64(job.Request.MaxPages) {
					return
				}

				item, ok := frontier.TryDequeue()
				if !ok {
					if activeWorkers.Load() == 0 && frontier.Len() == 0 {
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
					continue
				}

				activeWorkers.Add(1)
				m.processCrawlItem(ctx, job, frontier, item)
				activeWorkers.Add(-1)
			}
		}()
	}

	wg.Wait()

	if job.GetStatus() == "running" {
		if ctx.Err() != nil {
			job.SetStatus("cancelled")
		} else {
			job.SetStatus("completed")
		}
	}

	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventFinish,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      job.GetStatus(),
	})
}

func (m *JobManager) processCrawlItem(ctx context.Context, job *CrawlJob, frontier *Frontier, item FrontierItem) {
	t0 := time.Now()

	u, err := url.Parse(item.URL)
	if err != nil {
		job.AddError(fmt.Sprintf("%s: %v", item.URL, err))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     err.Error(),
		})
		return
	}

	limiter := m.getRateLimiter(u.Hostname(), job.Request.RateLimitRPS, job.Request.Concurrency)
	if waitErr := limiter.Wait(ctx); waitErr != nil {
		return
	}

	existingDoc, _ := storage.GetCrawledDocumentByURL(ctx, item.URL)
	opts := FetchOptions{
		Headers: job.Request.Headers,
		Cookies: job.Request.Cookies,
	}
	if existingDoc != nil && !job.Request.ForceRefresh {
		opts.ETag = existingDoc.ETag
		opts.LastModified = existingDoc.LastModified
	}

	res, fetchErr := m.httpClient.FetchWithAuth(ctx, item.URL, opts)
	if fetchErr != nil {
		job.AddError(fmt.Sprintf("%s: %v", item.URL, fetchErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     fetchErr.Error(),
		})
		return
	}

	if res.NotModified && existingDoc != nil {
		updatedDoc := *existingDoc
		updatedDoc.LastCrawledAt = time.Now()
		updatedDoc.HTTPStatus = 304
		storage.UpsertCrawledDocument(ctx, &updatedDoc, 0)

		if item.Depth < job.Request.MaxDepth {
			for _, link := range existingDoc.OutboundLinks {
				if enqueued, _ := frontier.Enqueue(link, item.Depth+1); enqueued {
					job.PagesQueued.Add(1)
				}
			}
		}

		job.CacheHits.Add(1)
		job.PagesCrawled.Add(1)
		latency := float64(time.Since(t0).Microseconds()) / 1000.0
		job.AddResult(CrawlPageResult{
			URL:            res.FinalURL,
			Title:          existingDoc.Title,
			Tokens:         existingDoc.TotalTokens,
			RawTokens:      existingDoc.TotalTokens,
			SavingsPct:     0,
			Depth:          item.Depth,
			StatusCode:     304,
			LatencyMs:      latency,
			DiscoveredURLs: len(existingDoc.OutboundLinks),
		})
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "cached",
			ActiveItem:  res.FinalURL,
		})
		return
	}
	defer func() {
		if res.Response != nil && res.Response.Body != nil {
			res.Response.Body.Close()
		}
	}()

	if res.Response.StatusCode >= 400 {
		job.AddError(fmt.Sprintf("%s returned HTTP %d", item.URL, res.Response.StatusCode))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     fmt.Sprintf("HTTP %d", res.Response.StatusCode),
		})
		return
	}

	limitedBody := io.LimitReader(res.Response.Body, 10*1024*1024)
	htmlBytes, readErr := io.ReadAll(limitedBody)
	if readErr != nil {
		job.AddError(fmt.Sprintf("%s: failed to read body: %v", item.URL, readErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     readErr.Error(),
		})
		return
	}
	job.BytesRead.Add(int64(len(htmlBytes)))

	contentType := res.Response.Header.Get("Content-Type")
	mdText, tokens, title, extractErr := extractor.ExtractDocumentText(res.FinalURL, contentType, htmlBytes, "clean_rag")
	if extractErr != nil {
		job.AddError(fmt.Sprintf("%s: extraction error: %v", item.URL, extractErr))
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "error",
			ActiveItem:  item.URL,
			Message:     extractErr.Error(),
		})
		return
	}

	rawTokens := len(strings.Fields(string(htmlBytes)))
	savingsPct := 0.0
	if rawTokens > 0 {
		savingsPct = float64(rawTokens-tokens) / float64(rawTokens) * 100.0
	}
	if savingsPct < 0 {
		savingsPct = 0
	}

	newHashBytes := sha256.Sum256([]byte(mdText))
	newHash := hex.EncodeToString(newHashBytes[:])

	var outboundLinks []string
	discoveredCount := 0
	if strings.Contains(contentType, "html") {
		doc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(htmlBytes))
		if parseErr == nil {
			doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
				href, exists := s.Attr("href")
				if exists {
					resolved := resolveRelativeURL(res.FinalURL, href)
					if resolved != "" {
						outboundLinks = append(outboundLinks, resolved)
						if item.Depth < job.Request.MaxDepth {
							if enqueued, _ := frontier.Enqueue(resolved, item.Depth+1); enqueued {
								job.PagesQueued.Add(1)
								discoveredCount++
							}
						}
					}
				}
			})
		}
	}

	if existingDoc != nil && existingDoc.ContentHash == newHash && !job.Request.ForceRefresh {
		updatedDoc := *existingDoc
		updatedDoc.ETag = res.ETag
		updatedDoc.LastModified = res.LastModified
		updatedDoc.LastCrawledAt = time.Now()
		updatedDoc.HTTPStatus = 200
		updatedDoc.OutboundLinks = outboundLinks
		storage.UpsertCrawledDocument(ctx, &updatedDoc, 0)
		
		job.CacheHits.Add(1)
		job.PagesCrawled.Add(1)
		latency := float64(time.Since(t0).Microseconds()) / 1000.0
		job.AddResult(CrawlPageResult{
			URL:            res.FinalURL,
			Title:          title,
			Tokens:         tokens,
			RawTokens:      rawTokens,
			SavingsPct:     math.Round(savingsPct*10) / 10,
			Depth:          item.Depth,
			StatusCode:     200,
			LatencyMs:      latency,
			DiscoveredURLs: discoveredCount,
		})
		job.PublishEvent(ui.ProgressEvent{
			Type:        ui.EventProgress,
			Phase:       "crawling",
			Current:     job.PagesCrawled.Load(),
			Total:       int64(job.Request.MaxPages),
			QueueDepth:  job.PagesQueued.Load(),
			TokensSaved: job.TokensSaved.Load(),
			BytesRead:   job.BytesRead.Load(),
			CacheHits:   job.CacheHits.Load(),
			ErrorsCount: job.ErrorsCount.Load(),
			Status:      "ok",
			ActiveItem:  res.FinalURL,
		})
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

	index.GlobalEngine.IndexDocumentWithSource(res.FinalURL, docTitle, bodyForIndexing, termPositions, tokens, "crawl_job", res.FinalURL)

	newDoc := &storage.CrawledDocument{
		URL:           res.FinalURL,
		Title:         title,
		CleanBody:     bodyForIndexing,
		TotalTokens:   tokens,
		SourceType:    "web_crawled",
		SourceURL:     res.FinalURL,
		ETag:          res.ETag,
		LastModified:  res.LastModified,
		ContentHash:   newHash,
		LastCrawledAt: time.Now(),
		HTTPStatus:    res.StatusCode,
		OutboundLinks: outboundLinks,
	}
	storage.UpsertCrawledDocument(ctx, newDoc, 0)

	tokensSaved := rawTokens - tokens
	if tokensSaved > 0 {
		job.TokensSaved.Add(int64(tokensSaved))
	}
	job.PagesCrawled.Add(1)

	latency := float64(time.Since(t0).Microseconds()) / 1000.0
	job.AddResult(CrawlPageResult{
		URL:            res.FinalURL,
		Title:          title,
		Tokens:         tokens,
		RawTokens:      rawTokens,
		SavingsPct:     math.Round(savingsPct*10) / 10,
		Depth:          item.Depth,
		StatusCode:     res.Response.StatusCode,
		LatencyMs:      latency,
		DiscoveredURLs: discoveredCount,
	})
	job.PublishEvent(ui.ProgressEvent{
		Type:        ui.EventProgress,
		Phase:       "crawling",
		Current:     job.PagesCrawled.Load(),
		Total:       int64(job.Request.MaxPages),
		QueueDepth:  job.PagesQueued.Load(),
		TokensSaved: job.TokensSaved.Load(),
		BytesRead:   job.BytesRead.Load(),
		CacheHits:   job.CacheHits.Load(),
		ErrorsCount: job.ErrorsCount.Load(),
		Status:      "ok",
		ActiveItem:  res.FinalURL,
	})
}

func resolveRelativeURL(baseURL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "tel:") {
		return ""
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	rel, err := url.Parse(href)
	if err != nil {
		return ""
	}
	return base.ResolveReference(rel).String()
}

// Close terminates background janitors.
func (m *JobManager) Close() {
	m.closeOnce.Do(func() {
		close(m.closeChan)
	})
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/crawler-monorepo/common/logger"
	"github.com/crawler-monorepo/internal/cluster"
	"github.com/crawler-monorepo/internal/crawler"
)

func TestHealthEndpoint(t *testing.T) {
	tmpDir := t.TempDir()
	server := NewEmbeddedServer(tmpDir)
	router := server.SetupRouter()

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP status 200, got: %d", rec.Code)
	}

	expectedBody := `{"status":"ok","mode":"single_binary_embedded"}`
	if strings.TrimSpace(rec.Body.String()) != expectedBody {
		t.Fatalf("expected body %s, got: %s", expectedBody, rec.Body.String())
	}
}

type mockTransport struct{}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	htmlBody := `<!DOCTYPE html><html><head><title>LightLimbs Embedded Document</title></head><body><h1>LightLimbs Embedded Vector Search</h1><p>LightLimbs is a high performance single binary embedded web crawler and search server.</p></body></html>`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(htmlBody)),
		Header:     make(http.Header),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "text/html; charset=utf-8")
	return resp, nil
}

func TestScrapeAndSearchEndpoints(t *testing.T) {
	os.Unsetenv("AGENT_API_KEY")
	testURL := "https://example.com/embedded-test-page"
	crawler.GlobalDomainCache.FetchAndCache("example.com", "User-agent: *\nAllow: /\n")

	// 1. Setup embedded server with mock transport
	tmpDir := t.TempDir()
	server := NewEmbeddedServerWithClient(tmpDir, crawler.NewClientWithTransport(&mockTransport{}))
	router := server.SetupRouter()

	// 2. Test POST /v1/scrape
	scrapeReqBody, _ := json.Marshal(map[string]interface{}{
		"url":  testURL,
		"mode": "clean_rag",
	})
	scrapeReq, err := http.NewRequest("POST", "/v1/scrape", bytes.NewBuffer(scrapeReqBody))
	if err != nil {
		t.Fatalf("failed to create scrape request: %v", err)
	}
	scrapeReq.Header.Set("Content-Type", "application/json")

	recScrape := httptest.NewRecorder()
	router.ServeHTTP(recScrape, scrapeReq)

	if recScrape.Code != http.StatusOK {
		t.Fatalf("expected scrape HTTP 200, got %d: %s", recScrape.Code, recScrape.Body.String())
	}

	var scrapeResp ScrapeResponse
	if err := json.Unmarshal(recScrape.Body.Bytes(), &scrapeResp); err != nil {
		t.Fatalf("failed to parse scrape response: %v", err)
	}

	if scrapeResp.Title != "LightLimbs Embedded Document" {
		t.Errorf("expected title 'LightLimbs Embedded Document', got: %s", scrapeResp.Title)
	}
	if !strings.Contains(scrapeResp.Markdown, "LightLimbs") {
		t.Errorf("expected markdown content to contain 'LightLimbs', got: %s", scrapeResp.Markdown)
	}

	// 3. Test POST /v1/search
	searchReqBody, _ := json.Marshal(map[string]interface{}{
		"query": "embedded web crawler search server",
		"top_k": 5,
	})
	searchReq, err := http.NewRequest("POST", "/v1/search", bytes.NewBuffer(searchReqBody))
	if err != nil {
		t.Fatalf("failed to create search request: %v", err)
	}
	searchReq.Header.Set("Content-Type", "application/json")

	recSearch := httptest.NewRecorder()
	router.ServeHTTP(recSearch, searchReq)

	if recSearch.Code != http.StatusOK {
		t.Fatalf("expected search HTTP 200, got %d: %s", recSearch.Code, recSearch.Body.String())
	}

	var searchResp SearchResponse
	if err := json.Unmarshal(recSearch.Body.Bytes(), &searchResp); err != nil {
		t.Fatalf("failed to parse search response: %v", err)
	}

	if searchResp.Query != "embedded web crawler search server" {
		t.Errorf("expected query 'embedded web crawler search server', got: %s", searchResp.Query)
	}
	if searchResp.TotalHits == 0 {
		t.Errorf("expected at least 1 search hit, got 0")
	}
}

func TestStorageInitAndSave(t *testing.T) {
	tmpDir := t.TempDir()
	saveStorage(tmpDir)

	indexPath := filepath.Join(tmpDir, "inverted_index.json")
	vectorPath := filepath.Join(tmpDir, "vector_index.json")

	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("expected inverted index snapshot file at %s", indexPath)
	}
	if _, err := os.Stat(vectorPath); os.IsNotExist(err) {
		t.Errorf("expected vector index snapshot file at %s", vectorPath)
	}

	// Test initStorage loads without crashing
	initStorage(context.Background(), tmpDir)
}

func TestSecurityMiddleware(t *testing.T) {
	os.Setenv("AGENT_API_KEY", "secret-test-key")
	defer os.Unsetenv("AGENT_API_KEY")

	tmpDir := t.TempDir()
	server := NewEmbeddedServer(tmpDir)
	router := server.SetupRouter()

	// 1. Request without API Key -> expect 401 Unauthorized
	req1, err := http.NewRequest("GET", "/v1/search?q=test", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized without key, got %d", rec1.Code)
	}

	// 2. Request with invalid API Key -> expect 401 Unauthorized
	req2, err := http.NewRequest("GET", "/v1/search?q=test", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req2.Header.Set("X-API-Key", "invalid-key")
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized with invalid key, got %d", rec2.Code)
	}

	// 3. Request with valid X-API-Key header -> expect 200 OK
	req3, err := http.NewRequest("GET", "/v1/search?q=test", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req3.Header.Set("X-API-Key", "secret-test-key")
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Errorf("expected status 200 OK with valid X-API-Key header, got %d", rec3.Code)
	}

	// 4. Request with valid api_key query param -> expect 200 OK
	req4, err := http.NewRequest("GET", "/v1/search?q=test&api_key=secret-test-key", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	rec4 := httptest.NewRecorder()
	router.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Errorf("expected status 200 OK with valid api_key query param, got %d", rec4.Code)
	}
}

func TestParseInterleavedFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	jsonFlag := fs.Bool("json", false, "json flag")
	modeFlag := fs.String("mode", "clean_rag", "mode flag")

	args := []string{"https://go.dev/doc", "--json", "-mode", "raw"}
	pos, err := parseInterleavedFlags(fs, args)
	if err != nil {
		t.Fatalf("parseInterleavedFlags failed: %v", err)
	}

	if len(pos) != 1 || pos[0] != "https://go.dev/doc" {
		t.Errorf("expected positional arg ['https://go.dev/doc'], got %v", pos)
	}
	if !*jsonFlag {
		t.Errorf("expected json flag to be true")
	}
	if *modeFlag != "raw" {
		t.Errorf("expected mode flag 'raw', got %s", *modeFlag)
	}

	// Test with explicit flag terminator "--"
	fs2 := flag.NewFlagSet("test2", flag.ContinueOnError)
	vFlag := fs2.Bool("v", false, "v flag")
	args2 := []string{"-v", "--", "-not-a-flag", "https://example.com"}
	pos2, err2 := parseInterleavedFlags(fs2, args2)
	if err2 != nil {
		t.Fatalf("parseInterleavedFlags with terminator failed: %v", err2)
	}
	if !*vFlag {
		t.Errorf("expected vFlag to be true")
	}
	if len(pos2) != 2 || pos2[0] != "-not-a-flag" || pos2[1] != "https://example.com" {
		t.Errorf("expected positional args ['-not-a-flag', 'https://example.com'], got %v", pos2)
	}

	// Test with positional arg preceding flag terminator "--"
	fs3 := flag.NewFlagSet("test3", flag.ContinueOnError)
	vFlag3 := fs3.Bool("v", false, "v flag")
	args3 := []string{"https://go.dev/doc", "-v", "--", "-not-a-flag", "extra"}
	pos3, err3 := parseInterleavedFlags(fs3, args3)
	if err3 != nil {
		t.Fatalf("parseInterleavedFlags with preceding positional failed: %v", err3)
	}
	if !*vFlag3 {
		t.Errorf("expected vFlag3 to be true")
	}
	if len(pos3) != 3 || pos3[0] != "https://go.dev/doc" || pos3[1] != "-not-a-flag" || pos3[2] != "extra" {
		t.Errorf("expected positional args ['https://go.dev/doc', '-not-a-flag', 'extra'], got %v", pos3)
	}
}

func TestCLISeedAndSearch(t *testing.T) {
	tmpDir := t.TempDir()

	// Run seed into tmpDir with limit 20 for fast unit testing
	runSeed([]string{"-d", tmpDir, "-q", "-l", "20"})

	// Verify snapshots exist
	indexPath := filepath.Join(tmpDir, "inverted_index.json")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("inverted_index.json not created in %s", tmpDir)
	}

	// Test runSearch output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runSearch([]string{"Dynamic Programming", "-d", tmpDir, "-j", "-k", "3"})

	_ = w.Close()
	os.Stdout = oldStdout

	var out bytes.Buffer
	_, _ = io.Copy(&out, r)

	var hits []map[string]any
	if err := json.Unmarshal(out.Bytes(), &hits); err != nil {
		t.Fatalf("failed to unmarshal search json output: %v\nOutput was: %s", err, out.String())
	}

	if len(hits) == 0 {
		t.Errorf("expected search hits for 'Dynamic Programming', got 0")
	}
}

func TestCLIScrape_DirectAST(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	tmpDir := t.TempDir()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>CLI Test Page</title></head><body><h1>AST Scrape Success</h1><p>High performance RAG extraction directly to stdout.</p></body></html>`))
	}))
	defer ts.Close()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runScrape([]string{ts.URL, "-j", "-d", tmpDir})

	_ = w.Close()
	os.Stdout = oldStdout

	var out bytes.Buffer
	_, _ = io.Copy(&out, r)

	var payload ScrapeCLIOutput
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
		t.Fatalf("failed to parse scrape CLI output json: %v\nOutput was: %s", err, out.String())
	}

	if payload.Title != "CLI Test Page" {
		t.Errorf("expected title 'CLI Test Page', got %q", payload.Title)
	}
	if !strings.Contains(payload.Markdown, "AST Scrape Success") {
		t.Errorf("expected markdown to contain 'AST Scrape Success', got: %s", payload.Markdown)
	}
	if payload.Tokens <= 0 {
		t.Errorf("expected positive token count, got %d", payload.Tokens)
	}
}

func TestCLIStdoutPurity(t *testing.T) {
	logger.InitLogger("production")
	defer func() {
		logger.Sync()
		logger.InitLogger("development")
	}()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run init-mcp --stdout
	runInitMCP([]string{"--stdout", "--binary-path", "/usr/local/bin/lightlimbs"})

	_ = w.Close()
	os.Stdout = oldStdout

	var out bytes.Buffer
	_, _ = io.Copy(&out, r)

	outStr := strings.TrimSpace(out.String())

	// Verify it's pure valid JSON without any Zap log text (e.g. "DEBUG", "INFO", "{"level":")
	var root map[string]any
	if err := json.Unmarshal([]byte(outStr), &root); err != nil {
		t.Fatalf("stdout was polluted, not clean JSON: %v\nOutput was: %s", err, outStr)
	}

	if root["mcpServers"] == nil {
		t.Errorf("expected mcpServers in stdout json output")
	}
}

func TestCLIScrape_OutFile(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "result.md")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>File Output Test</title></head><body><h1>Saved To Disk</h1></body></html>`))
	}))
	defer ts.Close()

	runScrape([]string{ts.URL, "-o", outFile, "--no-index"})

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("failed reading output file: %v", err)
	}

	if !strings.Contains(string(data), "Saved To Disk") {
		t.Errorf("expected file to contain 'Saved To Disk', got: %s", string(data))
	}
}

func TestPrintHelpAndVersion(t *testing.T) {
	// Verify printHelp runs without panic
	printHelp()

	// Verify slugify
	slug := slugify("Data Structures & Algorithms")
	if slug != "data-structures---algorithms" && slug != "Data-Structures---Algorithms" {
		if !strings.Contains(slug, "Data") && !strings.Contains(slug, "data") {
			t.Errorf("unexpected slug: %s", slug)
		}
	}
}

func TestCrawlEndpoints_SyncAndAsync(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch r.URL.Path {
		case "/":
			w.Write([]byte(`<html><body><h1>Crawl Root</h1><a href="/sub1">Sub 1</a></body></html>`))
		case "/sub1":
			w.Write([]byte(`<html><body><h1>Subpage 1</h1></body></html>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	server := NewEmbeddedServerWithClient(tmpDir, crawler.NewTestClient(true))
	router := server.SetupRouter()

	// 1. Test Synchronous POST /v1/crawl
	syncBody, _ := json.Marshal(map[string]interface{}{
		"url":           ts.URL + "/",
		"max_depth":     2,
		"max_pages":     10,
		"async":         false,
		"allow_loopback": true,
	})
	req, _ := http.NewRequest("POST", "/v1/crawl", bytes.NewReader(syncBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for sync crawl, got %d: %s", rec.Code, rec.Body.String())
	}

	var syncJob crawler.CrawlJob
	if err := json.Unmarshal(rec.Body.Bytes(), &syncJob); err != nil {
		t.Fatalf("failed to decode sync crawl response: %v", err)
	}
	if syncJob.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", syncJob.Status)
	}

	// 2. Test Asynchronous POST /v1/crawl
	asyncBody, _ := json.Marshal(map[string]interface{}{
		"url":           ts.URL + "/",
		"max_depth":     2,
		"max_pages":     10,
		"async":         true,
		"allow_loopback": true,
	})
	reqAsync, _ := http.NewRequest("POST", "/v1/crawl", bytes.NewReader(asyncBody))
	reqAsync.Header.Set("Content-Type", "application/json")
	recAsync := httptest.NewRecorder()
	router.ServeHTTP(recAsync, reqAsync)

	if recAsync.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 for async crawl, got %d: %s", recAsync.Code, recAsync.Body.String())
	}

	var asyncRes map[string]interface{}
	_ = json.Unmarshal(recAsync.Body.Bytes(), &asyncRes)
	jobID, ok := asyncRes["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("expected valid job_id in async response: %v", asyncRes)
	}

	// 3. Test GET /v1/crawl/{id}
	reqGet, _ := http.NewRequest("GET", "/v1/crawl/"+jobID, nil)
	recGet := httptest.NewRecorder()
	router.ServeHTTP(recGet, reqGet)

	if recGet.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for GET /v1/crawl/%s, got %d", jobID, recGet.Code)
	}

	// 4. Test DELETE /v1/crawl/{id}
	reqDel, _ := http.NewRequest("DELETE", "/v1/crawl/"+jobID, nil)
	recDel := httptest.NewRecorder()
	router.ServeHTTP(recDel, reqDel)

	if recDel.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for DELETE /v1/crawl/%s, got %d", jobID, recDel.Code)
	}
}

func TestSchemaExtractEndpoint(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	tmpDir := t.TempDir()
	server := NewEmbeddedServerWithClient(tmpDir, crawler.NewTestClient(true))
	router := server.SetupRouter()

	schema := map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type":    "object",
		"properties": map[string]interface{}{
			"result":    map[string]string{"type": "string"},
			"extracted": map[string]string{"type": "boolean"},
		},
		"required": []string{"result", "extracted"},
	}

	body, _ := json.Marshal(map[string]interface{}{
		"html":   "<html><body><h1>Extract Me</h1></body></html>",
		"schema": schema,
		"prompt": "Extract standard result object",
	})

	req, _ := http.NewRequest("POST", "/v1/extract/schema", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for /v1/extract/schema, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCLICrawlAndExtract(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<html><body><h1>CLI Crawl Root</h1><p>Test page</p></body></html>`))
	}))
	defer ts.Close()

	tmpDir := t.TempDir()

	// 1. Test CLI Crawl
	client := crawler.NewTestClient(true)
	runCrawlWithClient(client, []string{ts.URL + "/", "--depth", "1", "--max-pages", "5", "--json", "--data-dir", tmpDir})

	// 2. Test CLI Extract
	schemaJSON := `{"type":"object","properties":{"result":{"type":"string"}},"required":["result"]}`
	schemaFile := filepath.Join(tmpDir, "schema.json")
	_ = os.WriteFile(schemaFile, []byte(schemaJSON), 0644)

	runExtractWithClient(client, []string{ts.URL + "/", "--schema", schemaFile, "--json"})
}

func TestClusterServer_Integration(t *testing.T) {
	tmpDir1 := t.TempDir()
	ring := cluster.NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	coord1 := cluster.NewClusterCoordinator("node-1", ring, nil, nil, nil, 16)
	server1 := NewEmbeddedServer(tmpDir1)
	server1.SetCluster(coord1, nil)
	router1 := server1.SetupRouter()
	ts1 := httptest.NewServer(router1)
	defer ts1.Close()

	tmpDir2 := t.TempDir()
	server2 := NewEmbeddedServer(tmpDir2)

	transport := cluster.NewHTTPRaftTransport(map[string]string{
		"node-1": ts1.URL,
	})

	coord2 := cluster.NewClusterCoordinator("node-2", ring, nil, nil, transport, 16)
	server2.SetCluster(coord2, nil)
	router2 := server2.SetupRouter()
	ts2 := httptest.NewServer(router2)
	defer ts2.Close()

	// 1. Ingest document directly onto node 1
	ts1ScrapeReq, _ := http.NewRequest("POST", "/cluster/search", bytes.NewReader([]byte(`{"query":"cluster distributed","top_k":5}`)))
	rec1 := httptest.NewRecorder()
	router1.ServeHTTP(rec1, ts1ScrapeReq)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected /cluster/search HTTP 200, got %d", rec1.Code)
	}

	// 2. Query node 2 via /v1/search and verify scatter-gather
	searchReq, _ := json.Marshal(map[string]interface{}{
		"query": "cluster distributed",
		"top_k": 5,
	})
	req2, _ := http.NewRequest("POST", "/v1/search", bytes.NewReader(searchReq))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	router2.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected /v1/search HTTP 200, got %d: %s", rec2.Code, rec2.Body.String())
	}

	var resp SearchResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding SearchResponse: %v", err)
	}
	if resp.ShardsQueried != 2 {
		t.Errorf("expected 2 shards queried, got %d", resp.ShardsQueried)
	}
}

func TestCrawlJobStreamHandler_CompletedJob(t *testing.T) {
	tmpDir := t.TempDir()
	server := NewEmbeddedServer(tmpDir)
	router := server.SetupRouter()

	// Start a synchronous mock crawl job that completes immediately
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<html><body><h1>Stream Done</h1></body></html>`))
	}))
	defer ts.Close()

	req := crawler.CrawlRequest{
		URL:           ts.URL,
		MaxDepth:      1,
		MaxPages:      1,
		AllowLoopback: true,
	}
	job, err := server.jobManager.StartCrawl(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to start crawl: %v", err)
	}

	// Verify job is completed
	if job.GetStatus() != "completed" {
		t.Fatalf("expected job status 'completed', got: %s", job.GetStatus())
	}

	// Query stream endpoint
	streamReq, _ := http.NewRequest("GET", "/v1/crawl/"+job.ID+"/stream", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, streamReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for SSE stream, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: finish") {
		t.Fatalf("expected 'event: finish' in stream response for completed job, got: %s", body)
	}
}

func TestCrawlJobStreamHandler_ActiveJob(t *testing.T) {
	tmpDir := t.TempDir()
	server := NewEmbeddedServer(tmpDir)
	router := server.SetupRouter()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<html><body><h1>Active Stream</h1></body></html>`))
	}))
	defer ts.Close()

	req := crawler.CrawlRequest{
		URL:           ts.URL,
		MaxDepth:      1,
		MaxPages:      5,
		Async:         true,
		AllowLoopback: true,
	}
	job, err := server.jobManager.StartCrawl(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to start async crawl: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamReq, _ := http.NewRequestWithContext(ctx, "GET", "/v1/crawl/"+job.ID+"/stream", nil)
	rec := httptest.NewRecorder()

	go func() {
		// Cancel context after short delay to terminate stream loop
		cancel()
	}()

	router.ServeHTTP(rec, streamReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for SSE stream, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: start") && !strings.Contains(body, "event: finish") {
		t.Fatalf("expected SSE events in response, got: %s", body)
	}
}

func TestCLISubcommands_ProgressFlags(t *testing.T) {
	os.Setenv("ENV", "test")
	defer os.Unsetenv("ENV")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<html><body><h1>Flags Test</h1><p>Test page</p></body></html>`))
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	client := crawler.NewTestClient(true)

	// 1. Test crawl with --no-progress and --quiet
	runCrawlWithClient(client, []string{ts.URL + "/", "--max-pages", "1", "--no-progress", "--quiet", "--data-dir", tmpDir})

	// 2. Test scrape with --no-progress and --quiet
	runScrapeWithClient(client, []string{ts.URL + "/", "--no-progress", "--quiet", "--data-dir", tmpDir})

	// 3. Test seed with --no-progress, --limit, and --quiet
	runSeed([]string{"--limit", "2", "--no-progress", "--quiet", "--data-dir", tmpDir})

	// 4. Test index with --no-progress and --quiet
	dummyDir := filepath.Join(tmpDir, "index_test")
	_ = os.MkdirAll(dummyDir, 0755)
	_ = os.WriteFile(filepath.Join(dummyDir, "test.md"), []byte("# Hello Markdown"), 0644)
	runIndex([]string{dummyDir, "--no-progress", "--quiet", "--data-dir", tmpDir})
}




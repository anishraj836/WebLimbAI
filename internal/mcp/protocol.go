package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/crawler-monorepo/agent-service/api"
	"github.com/crawler-monorepo/internal/crawler"
	"github.com/crawler-monorepo/internal/extractor"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/search"
	"github.com/crawler-monorepo/internal/storage"
)

type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Tool struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	InputSchema ToolSchema `json:"inputSchema"`
}

type ToolSchema struct {
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties"`
	Required   []string            `json:"required,omitempty"`
}

type Property struct {
	Type        string              `json:"type"`
	Description string              `json:"description,omitempty"`
	Enum        []string            `json:"enum,omitempty"`
	Default     interface{}         `json:"default,omitempty"`
	Items       *Property           `json:"items,omitempty"`
	Properties  map[string]Property `json:"properties,omitempty"`
}

type CallToolParams struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments"`
}

type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type CallToolResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

func HandleRPCMessage(raw []byte, client *crawler.Client) (respBytes []byte, err error) {
	var reqID interface{}
	var hasID bool
	defer func() {
		if r := recover(); r != nil {
			if !hasID {
				respBytes = nil
				err = nil
				return
			}
			errResp := JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      reqID,
				Error:   &RPCError{Code: -32603, Message: fmt.Sprintf("Internal error: %v", r)},
			}
			respBytes, _ = json.Marshal(errResp)
			err = nil
		}
	}()

	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		errResp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   &RPCError{Code: -32700, Message: "Parse error: " + err.Error()},
		}
		return json.Marshal(errResp)
	}

	_, hasID = rawMap["id"]

	var rawReq struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      interface{}     `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}
	_ = json.Unmarshal(raw, &rawReq)

	reqID = rawReq.ID

	if rawReq.Method == "" || rawReq.JSONRPC != "2.0" {
		errResp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      rawReq.ID,
			Error:   &RPCError{Code: -32600, Message: "Invalid Request: jsonrpc must be '2.0' and method must be non-empty"},
		}
		return json.Marshal(errResp)
	}

	// JSON-RPC 2.0: Server MUST NOT reply to a Notification (id key is completely omitted from the request object)
	if !hasID {
		return nil, nil
	}

	req := JSONRPCRequest{
		JSONRPC: rawReq.JSONRPC,
		ID:      rawReq.ID,
		Method:  rawReq.Method,
		Params:  rawReq.Params,
	}

	switch req.Method {
	case "initialize":
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]interface{}{
					"tools": map[string]interface{}{},
				},
				"serverInfo": map[string]interface{}{
					"name":    "agent-limbs-mcp",
					"version": "1.0.0",
				},
			},
		}
		return json.Marshal(resp)

	case "tools/list":
		tools := []Tool{
			{
				Name:        "agent_limbs_scrape",
				Description: "Fast, low-latency DOM parser that extracts clean, token-reduced Markdown from static/SSR web pages (documentation, API references, technical blogs, wikis, articles). Use this to read documentation and articles in <5ms without browser overhead. (Note: optimized for text/content pages; does not execute client-side JavaScript for heavy SPAs).",
				InputSchema: ToolSchema{
					Type: "object",
					Properties: map[string]Property{
						"url":           {Type: "string", Description: "Target website URL to scrape (e.g. https://go.dev/doc/tutorial/getting-started)"},
						"mode":          {Type: "string", Description: "Extraction mode: 'clean_rag' (strips navbars/scripts/ads, default), 'preserve_links', or 'raw'", Enum: []string{"clean_rag", "preserve_links", "raw"}},
						"ttl_seconds":   {Type: "integer", Description: "Optional time-to-live for caching the scraped document in seconds"},
						"force_refresh": {Type: "boolean", Description: "Force refresh existing crawled documents"},
					},
					Required: []string{"url"},
				},
			},
			{
				Name:        "agent_limbs_hybrid_search",
				Description: "Sub-millisecond hybrid search (BM25 keyword matching + Int8 dense vector semantic similarity with Reciprocal Rank Fusion) over all crawled and indexed documentation.",
				InputSchema: ToolSchema{
					Type: "object",
					Properties: map[string]Property{
						"query": {Type: "string", Description: "The technical search query, topic, or question"},
						"limit": {Type: "integer", Description: "Maximum number of top-ranked results to return (default: 5)"},
					},
					Required: []string{"query"},
				},
			},
		}
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]interface{}{"tools": tools},
		}
		return json.Marshal(resp)

	case "tools/call":
		var params CallToolParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return json.Marshal(JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   &RPCError{Code: -32602, Message: "Invalid params"},
			})
		}

		ctx := context.Background()
		var toolResult CallToolResult

		switch params.Name {
		case "agent_limbs_scrape":
			targetURL, _ := params.Arguments["url"].(string)
			targetURL = strings.TrimSpace(targetURL)
			if targetURL == "" {
				toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: "Missing required argument 'url'"}}}
				break
			}

			parsedURL, err := url.Parse(targetURL)
			if err != nil || parsedURL.Scheme == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
				toolResult = CallToolResult{
					IsError: true,
					Content: []ToolContent{
						{Type: "text", Text: fmt.Sprintf("Invalid URL format: URL must begin with http:// or https:// (got '%s')", targetURL)},
					},
				}
				break
			}

			if client == nil {
				client = crawler.NewClient()
			}

			forceRefresh := false
			if val, ok := params.Arguments["force_refresh"]; ok && val != nil {
				if b, ok := val.(bool); ok {
					forceRefresh = b
				}
			}

			opts := crawler.FetchOptions{}
			if !forceRefresh {
				if doc, err := storage.GetCrawledDocumentByURL(ctx, targetURL); err == nil && doc != nil {
					opts.ETag = doc.ETag
					opts.LastModified = doc.LastModified
				}
			}

			res, err := client.FetchWithAuth(ctx, targetURL, opts)
			if err != nil || res == nil {
				toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Scrape failed: %v", err)}}}
				break
			}

			if res.NotModified {
				if doc, _ := storage.GetCrawledDocumentByURL(ctx, targetURL); doc != nil {
					toolResult = CallToolResult{
						Content: []ToolContent{
							{Type: "text", Text: fmt.Sprintf("Successfully scraped and indexed %s\nTitle: %s\n\nContent:\n%s", doc.URL, doc.Title, doc.CleanBody)},
						},
					}
					break
				}
			}

			if res.Response == nil {
				toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: "Scrape failed: no response"}}}
				break
			}

			if res.Response.StatusCode != 200 {
				if res.Response.Body != nil {
					res.Response.Body.Close()
				}
				toolResult = CallToolResult{
					IsError: true,
					Content: []ToolContent{
						{Type: "text", Text: fmt.Sprintf("Scrape rejected: Target URL returned HTTP status %d (%s). Document was not indexed.", res.Response.StatusCode, res.Response.Status)},
					},
				}
				break
			}

			bodyBytes, err := io.ReadAll(io.LimitReader(res.Response.Body, 10*1024*1024))
			res.Response.Body.Close()
			if err != nil {
				toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Failed to read body: %v", err)}}}
				break
			}

			mode := "clean_rag"
			if m, ok := params.Arguments["mode"].(string); ok && m != "" {
				mode = m
			}

			ttlSeconds := 0
			if val, ok := params.Arguments["ttl_seconds"]; ok && val != nil {
				switch v := val.(type) {
				case float64:
					ttlSeconds = int(v)
				case int:
					ttlSeconds = v
				case string:
					if parsed, err := strconv.Atoi(v); err == nil {
						ttlSeconds = parsed
					}
				}
			}

			contentType := res.Response.Header.Get("Content-Type")
			markdownContent, totalTokens, title, extractErr := extractor.ExtractDocumentText(targetURL, contentType, bodyBytes, mode)
			if extractErr != nil {
				toolResult = CallToolResult{
					IsError: true,
					Content: []ToolContent{
						{Type: "text", Text: extractErr.Error()},
					},
				}
				break
			}

			cleanMarkdown := strings.TrimSpace(markdownContent)
			wordCount := len(strings.Fields(cleanMarkdown))
			lowerTitle := strings.ToLower(title)
			lowerContent := strings.ToLower(cleanMarkdown)

			isSoft404 := wordCount < 15 ||
				strings.Contains(lowerTitle, "404 not found") ||
				strings.Contains(lowerTitle, "page not found") ||
				strings.Contains(lowerContent, "404 - page not found") ||
				strings.Contains(lowerContent, "404 page not found")

			if isSoft404 {
				toolResult = CallToolResult{
					IsError: true,
					Content: []ToolContent{
						{Type: "text", Text: fmt.Sprintf("Scrape rejected: Target URL '%s' returned soft 404 or empty content. Document was not indexed.", res.FinalURL)},
					},
				}
				break
			}

			hashBytes := sha256.Sum256([]byte(markdownContent))
			contentHash := hex.EncodeToString(hashBytes[:])
			doc := &storage.CrawledDocument{
				URL:           res.FinalURL,
				Title:         title,
				CleanBody:     markdownContent,
				TotalTokens:   totalTokens,
				SourceType:    "mcp_scraped",
				SourceURL:     targetURL,
				ETag:          res.ETag,
				LastModified:  res.LastModified,
				ContentHash:   contentHash,
				LastCrawledAt: time.Now(),
				HTTPStatus:    res.Response.StatusCode,
			}
			ttlDuration, _ := api.ClampTTL(ttlSeconds)
			_ = storage.UpsertCrawledDocument(context.Background(), doc, ttlDuration)
			if res.FinalURL != targetURL {
				index.GlobalEngine.AddAlias(targetURL, res.FinalURL)
				_ = storage.SaveURLAlias(context.Background(), targetURL, res.FinalURL)
			}
			index.GlobalEngine.IndexDocumentDirectly(res.FinalURL, title, markdownContent, totalTokens, targetURL)
			_ = index.GlobalEngine.IndexDocumentIncrementalByURL(ctx, res.FinalURL)

			displayText := markdownContent
			if len(displayText) > 300000 {
				displayText = displayText[:300000] + "\n\n... [Content truncated for MCP tool response payload display. Entire document has been indexed into memory and is searchable via agent_limbs_hybrid_search]"
			}

			toolResult = CallToolResult{
				Content: []ToolContent{
					{Type: "text", Text: fmt.Sprintf("Successfully scraped and indexed %s\nTitle: %s\n\nContent:\n%s", res.FinalURL, title, displayText)},
				},
			}

		case "agent_limbs_hybrid_search":
			query, _ := params.Arguments["query"].(string)
			if query == "" {
				toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: "Missing required argument 'query'"}}}
				break
			}

			limit := 5
			if val, ok := params.Arguments["limit"]; ok && val != nil {
				switch v := val.(type) {
				case float64:
					limit = int(v)
				case float32:
					limit = int(v)
				case int:
					limit = v
				case int64:
					limit = int(v)
				case string:
					if parsed, err := strconv.Atoi(v); err == nil {
						limit = parsed
					}
				case json.Number:
					if parsed, err := v.Int64(); err == nil {
						limit = int(parsed)
					}
				}
			}

			if limit < 0 {
				toolResult = CallToolResult{
					IsError: true,
					Content: []ToolContent{
						{Type: "text", Text: fmt.Sprintf("Invalid limit parameter: limit must be a non-negative integer (got %d)", limit)},
					},
				}
				break
			}

			if limit == 0 {
				toolResult = CallToolResult{
					Content: []ToolContent{
						{Type: "text", Text: "[]"},
					},
				}
				break
			}

			if limit > 100 {
				limit = 100
			}

			fetchK := limit
			if fetchK < 10 {
				fetchK = 10
			}

			titles, urls, bodies := index.GlobalEngine.GetMetadataMaps()
			if len(titles) == 0 {
				toolResult = CallToolResult{
					Content: []ToolContent{
						{Type: "text", Text: fmt.Sprintf("No indexed documents found in memory for query '%s'. Scrape target URLs first using agent_limbs_scrape.", query)},
					},
				}
				break
			}

			bm25Hits := index.GlobalEngine.SearchBM25(
				query,
				fetchK,
			)
			vectorHits := index.GlobalEngine.SearchVector(query, fetchK)
			fusedHits := search.ReciprocalRankFusion(query, bm25Hits, vectorHits, limit, titles, urls, bodies)
			if fusedHits == nil {
				fusedHits = []search.HybridSearchHit{}
			}

			resJSON, _ := json.MarshalIndent(fusedHits, "", "  ")
			toolResult = CallToolResult{
				Content: []ToolContent{
					{Type: "text", Text: string(resJSON)},
				},
			}

		default:
			toolResult = CallToolResult{IsError: true, Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Unknown tool: %s", params.Name)}}}
		}

		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  toolResult,
		}
		return json.Marshal(resp)

	default:
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32601, Message: "Method not found"},
		}
		return json.Marshal(resp)
	}
}

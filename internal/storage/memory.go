package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore implements DocumentStore in-memory (useful for testing and ephemeral workloads).
type MemoryStore struct {
	mu         sync.RWMutex
	docs       map[string]CrawledDocument
	docList    []string
	aliasMu    sync.RWMutex
	urlAliases map[string]string
	nextID     int
}

// Compile-time interface compliance assertion
var _ DocumentStore = (*MemoryStore)(nil)

// NewMemoryStore creates an in-memory document store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		docs:       make(map[string]CrawledDocument),
		docList:    make([]string, 0),
		urlAliases: make(map[string]string),
		nextID:     1,
	}
}

func (m *MemoryStore) DriverName() string {
	return "memory"
}

func (m *MemoryStore) Ping(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (m *MemoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs = make(map[string]CrawledDocument)
	m.docList = make([]string, 0)
	return nil
}

func (m *MemoryStore) Save(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot save nil or empty URL document")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var expiresAt *time.Time
	if ttl != 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	sourceType := doc.SourceType
	if sourceType == "" {
		sourceType = "web_crawled"
	}
	sourceURL := doc.SourceURL
	if sourceURL == "" {
		sourceURL = doc.URL
	}

	existing, exists := m.docs[doc.URL]
	docID := m.nextID
	createdAt := time.Now()
	if exists {
		docID = existing.ID
		if !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
	} else {
		m.nextID++
		m.docList = append(m.docList, doc.URL)
	}

	m.docs[doc.URL] = CrawledDocument{
		ID:          docID,
		URL:         doc.URL,
		Title:       doc.Title,
		CleanBody:   doc.CleanBody,
		TotalTokens: doc.TotalTokens,
		SourceType:  sourceType,
		SourceURL:   sourceURL,
		CreatedAt:   createdAt,
		ExpiresAt:   expiresAt,
	}

	return nil
}

func (m *MemoryStore) UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot upsert nil or empty URL document")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var expiresAt *time.Time
	if ttl != 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	sourceType := doc.SourceType
	if sourceType == "" {
		sourceType = "web_crawled"
	}
	sourceURL := doc.SourceURL
	if sourceURL == "" {
		sourceURL = doc.URL
	}

	existing, exists := m.docs[doc.URL]
	docID := m.nextID
	createdAt := time.Now()
	if exists {
		docID = existing.ID
		if !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
	} else {
		m.nextID++
		m.docList = append(m.docList, doc.URL)
	}

	m.docs[doc.URL] = CrawledDocument{
		ID:            docID,
		URL:           doc.URL,
		Title:         doc.Title,
		CleanBody:     doc.CleanBody,
		TotalTokens:   doc.TotalTokens,
		SourceType:    sourceType,
		SourceURL:     sourceURL,
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
		ETag:          doc.ETag,
		LastModified:  doc.LastModified,
		ContentHash:   doc.ContentHash,
		LastCrawledAt: doc.LastCrawledAt,
		HTTPStatus:    doc.HTTPStatus,
		OutboundLinks: doc.OutboundLinks,
	}

	return nil
}

func (m *MemoryStore) GetByURL(ctx context.Context, targetURL string) (*CrawledDocument, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if targetURL == "" {
		return nil, nil
	}

	canonicalURL := m.GetAlias(ctx, targetURL)

	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	for _, key := range []string{targetURL, canonicalURL} {
		if doc, ok := m.docs[key]; ok {
			if doc.ExpiresAt != nil && doc.ExpiresAt.Before(now) {
				continue
			}
			docCopy := doc
			return &docCopy, nil
		}
	}

	// Also check sourceURL
	for _, doc := range m.docs {
		if doc.ExpiresAt != nil && doc.ExpiresAt.Before(now) {
			continue
		}
		if doc.SourceURL == targetURL || doc.SourceURL == canonicalURL {
			docCopy := doc
			return &docCopy, nil
		}
	}

	return nil, nil
}

func (m *MemoryStore) List(ctx context.Context, limit, offset int) ([]CrawledDocument, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var validDocs []CrawledDocument
	for _, url := range m.docList {
		if doc, ok := m.docs[url]; ok {
			if doc.ExpiresAt == nil || doc.ExpiresAt.After(now) {
				validDocs = append(validDocs, doc)
			}
		}
	}

	if offset < 0 {
		offset = 0
	}
	if offset >= len(validDocs) {
		return []CrawledDocument{}, nil
	}

	validDocs = validDocs[offset:]
	if limit > 0 && limit < len(validDocs) {
		validDocs = validDocs[:limit]
	}

	return validDocs, nil
}

func (m *MemoryStore) DeleteExpired(ctx context.Context) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var deleted int64
	newDocList := make([]string, 0, len(m.docList))

	for _, url := range m.docList {
		if doc, ok := m.docs[url]; ok {
			if doc.ExpiresAt != nil && !doc.ExpiresAt.After(now) {
				delete(m.docs, url)
				deleted++
			} else {
				newDocList = append(newDocList, url)
			}
		}
	}
	m.docList = newDocList

	return deleted, nil
}

func (m *MemoryStore) SaveAlias(ctx context.Context, aliasURL, canonicalURL string) error {
	if aliasURL == "" || canonicalURL == "" || aliasURL == canonicalURL {
		return nil
	}
	m.aliasMu.Lock()
	defer m.aliasMu.Unlock()
	m.urlAliases[aliasURL] = canonicalURL
	return nil
}

func (m *MemoryStore) GetAlias(ctx context.Context, aliasURL string) string {
	m.aliasMu.RLock()
	defer m.aliasMu.RUnlock()
	
	visited := make(map[string]bool)
	current := aliasURL
	
	for {
		if visited[current] {
			return aliasURL // Circular reference detected
		}
		visited[current] = true
		
		canonical, exists := m.urlAliases[current]
		if !exists || canonical == "" {
			return current
		}
		current = canonical
	}
}

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// FileStore implements DocumentStore using local JSON file persistence with atomic writes.
type FileStore struct {
	mu         sync.RWMutex
	filePath   string
	aliasMu    sync.RWMutex
	urlAliases map[string]string
}

// Compile-time interface compliance assertion
var _ DocumentStore = (*FileStore)(nil)

// NewFileStore creates a new FileStore storing JSON documents at filePath.
func NewFileStore(filePath string) *FileStore {
	if filePath == "" {
		dir := "data"
		_ = os.MkdirAll(dir, 0755)
		filePath = filepath.Join(dir, "crawled_pages.json")
	} else {
		dir := filepath.Dir(filePath)
		if dir != "" && dir != "." {
			_ = os.MkdirAll(dir, 0755)
		}
	}

	return &FileStore{
		filePath:   filePath,
		urlAliases: make(map[string]string),
	}
}

func (f *FileStore) DriverName() string {
	return "file"
}

func (f *FileStore) Ping(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	dir := filepath.Dir(f.filePath)
	if dir != "" && dir != "." {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func (f *FileStore) Close() error {
	return nil
}

func (f *FileStore) loadLocked() ([]CrawledDocument, error) {
	data, err := os.ReadFile(f.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []CrawledDocument{}, nil
		}
		return nil, err
	}

	if len(data) == 0 {
		return []CrawledDocument{}, nil
	}

	var docs []CrawledDocument
	if err := json.Unmarshal(data, &docs); err != nil {
		return []CrawledDocument{}, nil
	}

	return docs, nil
}

func (f *FileStore) saveAtomicLocked(docs []CrawledDocument) error {
	data, err := json.MarshalIndent(docs, "", "  ")
	if err != nil {
		return fmt.Errorf("file storage marshal error: %w", err)
	}
	return atomicWriteFile(f.filePath, data, 0644)
}

func isEXDEV(err error) bool {
	if err == nil {
		return false
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		if errors.Is(linkErr.Err, syscall.EXDEV) {
			return true
		}
	}
	return false
}

func writeDirectly(dst string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func atomicWriteFile(filePath string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}

	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}

	if err := os.Rename(tmpName, filePath); err != nil {
		if isEXDEV(err) {
			return writeDirectly(filePath, data, perm)
		}
		return err
	}
	return nil
}

func (f *FileStore) Save(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot save nil or empty URL document")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	docs, err := f.loadLocked()
	if err != nil {
		docs = []CrawledDocument{}
	}

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

	found := false
	for i, d := range docs {
		if d.URL == doc.URL {
			docs[i].Title = doc.Title
			docs[i].CleanBody = doc.CleanBody
			docs[i].TotalTokens = doc.TotalTokens
			docs[i].SourceType = sourceType
			docs[i].SourceURL = sourceURL
			docs[i].ExpiresAt = expiresAt
			found = true
			break
		}
	}

	if !found {
		maxID := 0
		for _, d := range docs {
			if d.ID > maxID {
				maxID = d.ID
			}
		}
		newDoc := CrawledDocument{
			ID:          maxID + 1,
			URL:         doc.URL,
			Title:       doc.Title,
			CleanBody:   doc.CleanBody,
			TotalTokens: doc.TotalTokens,
			SourceType:  sourceType,
			SourceURL:   sourceURL,
			CreatedAt:   time.Now(),
			ExpiresAt:   expiresAt,
		}
		docs = append(docs, newDoc)
	}

	return f.saveAtomicLocked(docs)
}

func (f *FileStore) UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot upsert nil or empty URL document")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	docs, err := f.loadLocked()
	if err != nil {
		docs = []CrawledDocument{}
	}

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

	found := false
	for i, d := range docs {
		if d.URL == doc.URL {
			docs[i].Title = doc.Title
			docs[i].CleanBody = doc.CleanBody
			docs[i].TotalTokens = doc.TotalTokens
			docs[i].SourceType = sourceType
			docs[i].SourceURL = sourceURL
			docs[i].ExpiresAt = expiresAt
			docs[i].ETag = doc.ETag
			docs[i].LastModified = doc.LastModified
			docs[i].ContentHash = doc.ContentHash
			docs[i].LastCrawledAt = doc.LastCrawledAt
			docs[i].HTTPStatus = doc.HTTPStatus
			docs[i].OutboundLinks = doc.OutboundLinks
			found = true
			break
		}
	}

	if !found {
		maxID := 0
		for _, d := range docs {
			if d.ID > maxID {
				maxID = d.ID
			}
		}
		newDoc := CrawledDocument{
			ID:            maxID + 1,
			URL:           doc.URL,
			Title:         doc.Title,
			CleanBody:     doc.CleanBody,
			TotalTokens:   doc.TotalTokens,
			SourceType:    sourceType,
			SourceURL:     sourceURL,
			CreatedAt:     time.Now(),
			ExpiresAt:     expiresAt,
			ETag:          doc.ETag,
			LastModified:  doc.LastModified,
			ContentHash:   doc.ContentHash,
			LastCrawledAt: doc.LastCrawledAt,
			HTTPStatus:    doc.HTTPStatus,
			OutboundLinks: doc.OutboundLinks,
		}
		docs = append(docs, newDoc)
	}

	return f.saveAtomicLocked(docs)
}

func (f *FileStore) GetByURL(ctx context.Context, targetURL string) (*CrawledDocument, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if targetURL == "" {
		return nil, nil
	}

	canonicalURL := f.GetAlias(ctx, targetURL)

	f.mu.RLock()
	defer f.mu.RUnlock()

	docs, err := f.loadLocked()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	for _, d := range docs {
		if d.ExpiresAt != nil && d.ExpiresAt.Before(now) {
			continue
		}
		if d.URL == targetURL || d.URL == canonicalURL || d.SourceURL == targetURL || d.SourceURL == canonicalURL {
			docCopy := d
			return &docCopy, nil
		}
	}

	return nil, nil
}

func (f *FileStore) List(ctx context.Context, limit, offset int) ([]CrawledDocument, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	docs, err := f.loadLocked()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var validDocs []CrawledDocument
	for _, d := range docs {
		if d.ExpiresAt == nil || d.ExpiresAt.After(now) {
			validDocs = append(validDocs, d)
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

func (f *FileStore) DeleteExpired(ctx context.Context) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	docs, err := f.loadLocked()
	if err != nil {
		return 0, err
	}

	now := time.Now()
	var retained []CrawledDocument
	var deletedCount int64

	for _, d := range docs {
		if d.ExpiresAt != nil && !d.ExpiresAt.After(now) {
			deletedCount++
		} else {
			retained = append(retained, d)
		}
	}

	if deletedCount > 0 {
		if err := f.saveAtomicLocked(retained); err != nil {
			return 0, err
		}
	}

	return deletedCount, nil
}

func (f *FileStore) SaveAlias(ctx context.Context, aliasURL, canonicalURL string) error {
	if aliasURL == "" || canonicalURL == "" || aliasURL == canonicalURL {
		return nil
	}
	f.aliasMu.Lock()
	defer f.aliasMu.Unlock()
	f.urlAliases[aliasURL] = canonicalURL
	return nil
}

func (f *FileStore) GetAlias(ctx context.Context, aliasURL string) string {
	f.aliasMu.RLock()
	defer f.aliasMu.RUnlock()
	
	visited := make(map[string]bool)
	current := aliasURL
	
	for {
		if visited[current] {
			return aliasURL // Circular reference detected
		}
		visited[current] = true
		
		canonical, exists := f.urlAliases[current]
		if !exists || canonical == "" {
			return current
		}
		current = canonical
	}
}

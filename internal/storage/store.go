package storage

import (
	"context"
	"time"
)

// CrawledDocument represents a persisted web document with metadata.
type CrawledDocument struct {
	ID            int        `json:"id"`
	URL           string     `json:"url"`
	Title         string     `json:"title"`
	CleanBody     string     `json:"clean_body"`
	TotalTokens   int        `json:"total_tokens"`
	SourceType    string     `json:"source_type"`
	SourceURL     string     `json:"source_url"`
	CreatedAt     time.Time  `json:"created_at,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	ETag          string     `json:"etag,omitempty"`
	LastModified  string     `json:"last_modified,omitempty"`
	ContentHash   string     `json:"content_hash,omitempty"`
	LastCrawledAt time.Time  `json:"last_crawled_at,omitempty"`
	HTTPStatus    int        `json:"http_status,omitempty"`
	OutboundLinks []string   `json:"outbound_links,omitempty"`
}

// DocumentStore defines the pluggable storage interface for document persistence.
type DocumentStore interface {
	// DriverName returns the identifier of the storage driver (e.g. "postgres", "file", "memory").
	DriverName() string

	// Save persists a document with an optional TTL duration (0 = persistent).
	Save(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error

	// UpsertDocument updates or inserts a document with an optional TTL duration.
	UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error

	// GetByURL retrieves a single document by its canonical or alias URL.
	GetByURL(ctx context.Context, url string) (*CrawledDocument, error)

	// List returns stored documents up to limit starting from offset (limit <= 0 returns all).
	List(ctx context.Context, limit, offset int) ([]CrawledDocument, error)

	// DeleteExpired removes documents whose TTL has expired, returning the count deleted.
	DeleteExpired(ctx context.Context) (int64, error)

	// SaveAlias maps an alternate URL to a canonical URL.
	SaveAlias(ctx context.Context, aliasURL, canonicalURL string) error

	// GetAlias resolves an alias URL to its canonical URL (returns aliasURL if no mapping exists).
	GetAlias(ctx context.Context, aliasURL string) string

	// Ping checks the liveness of the underlying storage backend.
	Ping(ctx context.Context) error

	// Close flushes buffers and closes any open database or file handles.
	Close() error
}

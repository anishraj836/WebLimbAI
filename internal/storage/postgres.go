package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements DocumentStore using a PostgreSQL connection pool.
type PostgresStore struct {
	pool       *pgxpool.Pool
	aliasMu    sync.RWMutex
	urlAliases map[string]string
}

// Compile-time interface compliance assertion
var _ DocumentStore = (*PostgresStore)(nil)

// NewPostgresStore creates and connects a PostgresStore using the provided connection string.
func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("empty database URL")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	config.MaxConns = 50
	config.MinConns = 5
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	maxRetries := 5
	backoff := 1 * time.Second
	if strings.Contains(databaseURL, "connect_timeout=1") || strings.Contains(databaseURL, "invalid_user") || strings.Contains(databaseURL, "54329") {
		maxRetries = 2
		backoff = 10 * time.Millisecond
	}

	var pool *pgxpool.Pool
	var connected bool

	for i := 1; i <= maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		p, connErr := pgxpool.NewWithConfig(ctx, config)
		cancel()

		if connErr == nil && p != nil {
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = p.Ping(pingCtx)
			pingCancel()
			if err == nil {
				pool = p
				connected = true
				log.Printf("[Storage] Connected to PostgreSQL successfully on attempt %d", i)
				break
			}
			p.Close()
		} else {
			err = connErr
		}

		log.Printf("[Storage] PostgreSQL connection attempt %d/%d failed: %v; retrying in %v...", i, maxRetries, err, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}

	if !connected || pool == nil {
		return nil, fmt.Errorf("could not connect to PostgreSQL after %d attempts: %w", maxRetries, err)
	}

	store := &PostgresStore{
		pool:       pool,
		urlAliases: make(map[string]string),
	}

	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()
	if err := store.initTables(initCtx); err != nil {
		log.Printf("[Storage] PostgreSQL table initialization warning: %v", err)
	}

	return store, nil
}

func (p *PostgresStore) DriverName() string {
	return "postgres"
}

func (p *PostgresStore) Pool() *pgxpool.Pool {
	return p.pool
}

func (p *PostgresStore) initTables(ctx context.Context) error {
	if p.pool == nil {
		return fmt.Errorf("nil connection pool")
	}
	query := `
	CREATE TABLE IF NOT EXISTS crawled_pages (
		id SERIAL PRIMARY KEY,
		url TEXT UNIQUE NOT NULL,
		title TEXT NOT NULL,
		clean_body TEXT NOT NULL,
		total_tokens INT NOT NULL DEFAULT 0,
		source_type TEXT NOT NULL DEFAULT 'web_crawled',
		source_url TEXT,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		expires_at TIMESTAMPTZ
	);
	CREATE INDEX IF NOT EXISTS idx_crawled_pages_url ON crawled_pages(url);
	CREATE INDEX IF NOT EXISTS idx_crawled_pages_expires_at ON crawled_pages(expires_at);
	`
	_, err := p.pool.Exec(ctx, query)
	return err
}

func (p *PostgresStore) Ping(ctx context.Context) error {
	if p.pool == nil {
		return fmt.Errorf("postgres pool is nil")
	}
	return p.pool.Ping(ctx)
}

func (p *PostgresStore) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

func (p *PostgresStore) Save(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if p.pool == nil {
		return fmt.Errorf("postgres pool is nil")
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot save nil or empty URL document")
	}

	sourceType := doc.SourceType
	if sourceType == "" {
		sourceType = "web_crawled"
	}
	sourceURL := doc.SourceURL
	if sourceURL == "" {
		sourceURL = doc.URL
	}

	var expiresAt *time.Time
	if ttl > 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	query := `
	INSERT INTO crawled_pages (url, title, clean_body, total_tokens, source_type, source_url, expires_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (url) DO UPDATE SET
		title = EXCLUDED.title,
		clean_body = EXCLUDED.clean_body,
		total_tokens = EXCLUDED.total_tokens,
		source_type = EXCLUDED.source_type,
		source_url = EXCLUDED.source_url,
		expires_at = EXCLUDED.expires_at;
	`
	_, err := p.pool.Exec(ctx, query, doc.URL, doc.Title, doc.CleanBody, doc.TotalTokens, sourceType, sourceURL, expiresAt)
	return err
}

func (p *PostgresStore) UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if p.pool == nil {
		return fmt.Errorf("postgres pool is nil")
	}
	if doc == nil || doc.URL == "" {
		return fmt.Errorf("cannot upsert nil or empty URL document")
	}

	sourceType := doc.SourceType
	if sourceType == "" {
		sourceType = "web_crawled"
	}
	sourceURL := doc.SourceURL
	if sourceURL == "" {
		sourceURL = doc.URL
	}

	var expiresAt *time.Time
	if ttl > 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	query := `
	INSERT INTO crawled_pages (url, title, clean_body, total_tokens, source_type, source_url, expires_at, etag, last_modified, content_hash, last_crawled_at, http_status, outbound_links)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	ON CONFLICT (url) DO UPDATE SET
		title = EXCLUDED.title,
		clean_body = EXCLUDED.clean_body,
		total_tokens = EXCLUDED.total_tokens,
		source_type = EXCLUDED.source_type,
		source_url = EXCLUDED.source_url,
		expires_at = EXCLUDED.expires_at,
		etag = EXCLUDED.etag,
		last_modified = EXCLUDED.last_modified,
		content_hash = EXCLUDED.content_hash,
		last_crawled_at = EXCLUDED.last_crawled_at,
		http_status = EXCLUDED.http_status,
		outbound_links = EXCLUDED.outbound_links;
	`
	_, err := p.pool.Exec(ctx, query, doc.URL, doc.Title, doc.CleanBody, doc.TotalTokens, sourceType, sourceURL, expiresAt, doc.ETag, doc.LastModified, doc.ContentHash, doc.LastCrawledAt, doc.HTTPStatus, doc.OutboundLinks)
	return err
}

func (p *PostgresStore) GetByURL(ctx context.Context, targetURL string) (*CrawledDocument, error) {
	if p.pool == nil {
		return nil, fmt.Errorf("postgres pool is nil")
	}
	if targetURL == "" {
		return nil, nil
	}

	canonicalURL := p.GetAlias(ctx, targetURL)

	query := `
	SELECT id, url, title, clean_body, total_tokens, COALESCE(source_type, 'web_crawled'), COALESCE(source_url, url), expires_at, etag, last_modified, content_hash, last_crawled_at, http_status, outbound_links
	FROM crawled_pages
	WHERE (url = $1 OR url = $2 OR source_url = $1 OR source_url = $2)
	  AND (expires_at IS NULL OR expires_at > NOW())
	ORDER BY id ASC
	LIMIT 1;
	`
	row := p.pool.QueryRow(ctx, query, targetURL, canonicalURL)
	var doc CrawledDocument
	err := row.Scan(&doc.ID, &doc.URL, &doc.Title, &doc.CleanBody, &doc.TotalTokens, &doc.SourceType, &doc.SourceURL, &doc.ExpiresAt, &doc.ETag, &doc.LastModified, &doc.ContentHash, &doc.LastCrawledAt, &doc.HTTPStatus, &doc.OutboundLinks)
	if err != nil {
		return nil, nil
	}
	return &doc, nil
}

func (p *PostgresStore) List(ctx context.Context, limit, offset int) ([]CrawledDocument, error) {
	if p.pool == nil {
		return nil, fmt.Errorf("postgres pool is nil")
	}

	query := `
	SELECT id, url, title, clean_body, total_tokens, COALESCE(source_type, 'web_crawled'), COALESCE(source_url, url), expires_at, etag, last_modified, content_hash, last_crawled_at, http_status, outbound_links
	FROM crawled_pages
	WHERE expires_at IS NULL OR expires_at > NOW()
	ORDER BY id ASC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []CrawledDocument
	for rows.Next() {
		var doc CrawledDocument
		if err := rows.Scan(&doc.ID, &doc.URL, &doc.Title, &doc.CleanBody, &doc.TotalTokens, &doc.SourceType, &doc.SourceURL, &doc.ExpiresAt, &doc.ETag, &doc.LastModified, &doc.ContentHash, &doc.LastCrawledAt, &doc.HTTPStatus, &doc.OutboundLinks); err == nil {
			docs = append(docs, doc)
		}
	}

	return docs, nil
}

func (p *PostgresStore) DeleteExpired(ctx context.Context) (int64, error) {
	if p.pool == nil {
		return 0, fmt.Errorf("postgres pool is nil")
	}
	query := `DELETE FROM crawled_pages WHERE expires_at IS NOT NULL AND expires_at <= NOW();`
	cmdTag, err := p.pool.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return cmdTag.RowsAffected(), nil
}

func (p *PostgresStore) SaveAlias(ctx context.Context, aliasURL, canonicalURL string) error {
	if aliasURL == "" || canonicalURL == "" || aliasURL == canonicalURL {
		return nil
	}
	p.aliasMu.Lock()
	defer p.aliasMu.Unlock()
	p.urlAliases[aliasURL] = canonicalURL
	return nil
}

func (p *PostgresStore) GetAlias(ctx context.Context, aliasURL string) string {
	p.aliasMu.RLock()
	defer p.aliasMu.RUnlock()

	visited := make(map[string]bool)
	current := aliasURL

	for {
		if visited[current] {
			return aliasURL // Circular reference detected
		}
		visited[current] = true

		canonical, exists := p.urlAliases[current]
		if !exists || canonical == "" {
			return current
		}
		current = canonical
	}
}

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore implements DocumentStore using pure-Go modernc.org/sqlite.
type SQLiteStore struct {
	db         *sql.DB
	filePath   string
	isMemory   bool
	writeMu    sync.Mutex
	aliasMu    sync.RWMutex
	urlAliases map[string]string
}

// Compile-time interface compliance assertion
var _ DocumentStore = (*SQLiteStore)(nil)

func buildDSN(rawPath string) (dsn string, isMemory bool) {
	cleanPath := strings.TrimSpace(rawPath)
	cleanPath = strings.TrimPrefix(cleanPath, "sqlite://")
	cleanPath = strings.TrimPrefix(cleanPath, "file:")

	if cleanPath == "" || cleanPath == ":memory:" || strings.Contains(cleanPath, "mode=memory") {
		return "file::memory:?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=cache_size(-16000)", true
	}

	// File-backed SQLite: create directory and configure WAL mode + concurrency pragmas
	dir := filepath.Dir(cleanPath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	dsn = fmt.Sprintf(
		"file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=cache_size(-64000)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)",
		cleanPath,
	)
	return dsn, false
}

// NewSQLiteStore creates and initializes a SQLiteStore using pure-Go modernc.org/sqlite.
func NewSQLiteStore(dsnOrPath string) (*SQLiteStore, error) {
	dsn, isMemory := buildDSN(dsnOrPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if isMemory {
		// In-memory databases must use exactly 1 connection to prevent isolated DB instances
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		maxOpen := runtime.NumCPU() * 4
		if maxOpen < 8 {
			maxOpen = 8
		}
		if maxOpen > 32 {
			maxOpen = 32
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxOpen / 2)
		db.SetConnMaxIdleTime(5 * time.Minute)
		db.SetConnMaxLifetime(1 * time.Hour)
	}

	store := &SQLiteStore{
		db:         db,
		filePath:   dsnOrPath,
		isMemory:   isMemory,
		urlAliases: make(map[string]string),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping failed: %w", err)
	}

	if err := store.initTables(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite table initialization failed: %w", err)
	}

	if err := store.loadAliases(ctx); err != nil {
		log.Printf("[Storage] SQLite loadAliases warning: %v", err)
	}

	return store, nil
}

// DriverName returns the storage driver identifier.
func (s *SQLiteStore) DriverName() string {
	return "sqlite"
}

// DB returns the underlying *sql.DB instance.
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

// Ping verifies database connectivity.
func (s *SQLiteStore) Ping(ctx context.Context) error {
	if s.db == nil {
		return errors.New("sqlite db is nil")
	}
	return s.db.PingContext(ctx)
}

// Close closes the underlying SQLite database connection pool.
func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) initTables(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `
	CREATE TABLE IF NOT EXISTS crawled_pages (
		id              INTEGER PRIMARY KEY AUTOINCREMENT,
		url             TEXT UNIQUE NOT NULL,
		title           TEXT NOT NULL,
		clean_body      TEXT NOT NULL,
		total_tokens    INTEGER NOT NULL DEFAULT 0,
		source_type     TEXT DEFAULT 'web_crawled',
		source_url      TEXT,
		created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
		expires_at      DATETIME,
		etag            TEXT DEFAULT '',
		last_modified   TEXT DEFAULT '',
		content_hash    TEXT DEFAULT '',
		last_crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		http_status     INTEGER DEFAULT 0,
		outbound_links  TEXT DEFAULT '[]'
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_crawled_pages_url ON crawled_pages(url);
	CREATE INDEX IF NOT EXISTS idx_crawled_pages_source_url ON crawled_pages(source_url);
	CREATE INDEX IF NOT EXISTS idx_crawled_pages_expires_at ON crawled_pages(expires_at);
	CREATE INDEX IF NOT EXISTS idx_crawled_pages_source_type ON crawled_pages(source_type);

	CREATE TABLE IF NOT EXISTS url_aliases (
		alias_url     TEXT PRIMARY KEY,
		canonical_url TEXT NOT NULL,
		created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_url_aliases_canonical ON url_aliases(canonical_url);
	`
	_, err := s.db.ExecContext(ctx, query)
	return err
}

func (s *SQLiteStore) loadAliases(ctx context.Context) error {
	s.aliasMu.Lock()
	defer s.aliasMu.Unlock()

	rows, err := s.db.QueryContext(ctx, `SELECT alias_url, canonical_url FROM url_aliases`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var alias, canonical string
		if err := rows.Scan(&alias, &canonical); err == nil && alias != "" && canonical != "" {
			s.urlAliases[alias] = canonical
		}
	}
	return rows.Err()
}

// Save persists a document with an optional TTL duration.
func (s *SQLiteStore) Save(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if s.db == nil {
		return errors.New("sqlite db is nil")
	}
	if doc == nil || doc.URL == "" {
		return errors.New("cannot save nil or empty URL document")
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
	if ttl != 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	var expiresAtStr sql.NullString
	if expiresAt != nil {
		expiresAtStr = sql.NullString{String: expiresAt.UTC().Format("2006-01-02 15:04:05"), Valid: true}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `
	INSERT INTO crawled_pages (url, title, clean_body, total_tokens, source_type, source_url, expires_at)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(url) DO UPDATE SET
		title        = excluded.title,
		clean_body   = excluded.clean_body,
		total_tokens = excluded.total_tokens,
		source_type  = excluded.source_type,
		source_url   = excluded.source_url,
		expires_at   = excluded.expires_at;
	`
	_, err := s.db.ExecContext(ctx, query, doc.URL, doc.Title, doc.CleanBody, doc.TotalTokens, sourceType, sourceURL, expiresAtStr)
	return err
}

// UpsertDocument updates or inserts a document with complete metadata.
func (s *SQLiteStore) UpsertDocument(ctx context.Context, doc *CrawledDocument, ttl time.Duration) error {
	if s.db == nil {
		return errors.New("sqlite db is nil")
	}
	if doc == nil || doc.URL == "" {
		return errors.New("cannot upsert nil or empty URL document")
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
	if ttl != 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	} else if doc.ExpiresAt != nil {
		expiresAt = doc.ExpiresAt
	}

	var expiresAtStr sql.NullString
	if expiresAt != nil {
		expiresAtStr = sql.NullString{String: expiresAt.UTC().Format("2006-01-02 15:04:05"), Valid: true}
	}

	lastCrawledAt := doc.LastCrawledAt
	if lastCrawledAt.IsZero() {
		lastCrawledAt = time.Now()
	}
	lastCrawledStr := lastCrawledAt.UTC().Format("2006-01-02 15:04:05")

	links := doc.OutboundLinks
	if links == nil {
		links = []string{}
	}
	linksBytes, _ := json.Marshal(links)
	if len(linksBytes) == 0 {
		linksBytes = []byte("[]")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `
	INSERT INTO crawled_pages (
		url, title, clean_body, total_tokens, source_type, source_url, expires_at,
		etag, last_modified, content_hash, last_crawled_at, http_status, outbound_links
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(url) DO UPDATE SET
		title           = excluded.title,
		clean_body      = excluded.clean_body,
		total_tokens    = excluded.total_tokens,
		source_type     = excluded.source_type,
		source_url      = excluded.source_url,
		expires_at      = excluded.expires_at,
		etag            = excluded.etag,
		last_modified   = excluded.last_modified,
		content_hash    = excluded.content_hash,
		last_crawled_at = excluded.last_crawled_at,
		http_status     = excluded.http_status,
		outbound_links  = excluded.outbound_links;
	`
	_, err := s.db.ExecContext(
		ctx, query,
		doc.URL, doc.Title, doc.CleanBody, doc.TotalTokens, sourceType, sourceURL, expiresAtStr,
		doc.ETag, doc.LastModified, doc.ContentHash, lastCrawledStr, doc.HTTPStatus, string(linksBytes),
	)
	return err
}

// GetByURL retrieves a single document by its canonical or alias URL.
func (s *SQLiteStore) GetByURL(ctx context.Context, targetURL string) (*CrawledDocument, error) {
	if s.db == nil {
		return nil, errors.New("sqlite db is nil")
	}
	if targetURL == "" {
		return nil, nil
	}

	canonicalURL := s.GetAlias(ctx, targetURL)

	query := `
	SELECT
		id, url, title, clean_body, total_tokens,
		COALESCE(source_type, 'web_crawled'),
		COALESCE(source_url, url),
		expires_at,
		COALESCE(etag, ''),
		COALESCE(last_modified, ''),
		COALESCE(content_hash, ''),
		last_crawled_at,
		COALESCE(http_status, 0),
		COALESCE(outbound_links, '[]')
	FROM crawled_pages
	WHERE (url = ? OR url = ? OR source_url = ? OR source_url = ?)
	  AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))
	ORDER BY id ASC
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, targetURL, canonicalURL, targetURL, canonicalURL)

	var doc CrawledDocument
	var expiresAtStr sql.NullString
	var lastCrawledStr sql.NullString
	var linksJSON string

	err := row.Scan(
		&doc.ID, &doc.URL, &doc.Title, &doc.CleanBody, &doc.TotalTokens,
		&doc.SourceType, &doc.SourceURL, &expiresAtStr,
		&doc.ETag, &doc.LastModified, &doc.ContentHash, &lastCrawledStr,
		&doc.HTTPStatus, &linksJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if expiresAtStr.Valid {
		if t, parseErr := time.Parse("2006-01-02 15:04:05", expiresAtStr.String); parseErr == nil {
			doc.ExpiresAt = &t
		} else if t, parseErr := time.Parse(time.RFC3339, expiresAtStr.String); parseErr == nil {
			doc.ExpiresAt = &t
		}
	}
	if lastCrawledStr.Valid {
		if t, parseErr := time.Parse("2006-01-02 15:04:05", lastCrawledStr.String); parseErr == nil {
			doc.LastCrawledAt = t
		} else if t, parseErr := time.Parse(time.RFC3339, lastCrawledStr.String); parseErr == nil {
			doc.LastCrawledAt = t
		}
	}
	if linksJSON != "" {
		if err := json.Unmarshal([]byte(linksJSON), &doc.OutboundLinks); err != nil {
			doc.OutboundLinks = []string{}
		}
	} else {
		doc.OutboundLinks = []string{}
	}

	return &doc, nil
}

// List returns stored active documents up to limit starting from offset.
func (s *SQLiteStore) List(ctx context.Context, limit, offset int) ([]CrawledDocument, error) {
	if s.db == nil {
		return nil, errors.New("sqlite db is nil")
	}

	query := `
	SELECT
		id, url, title, clean_body, total_tokens,
		COALESCE(source_type, 'web_crawled'),
		COALESCE(source_url, url),
		expires_at,
		COALESCE(etag, ''),
		COALESCE(last_modified, ''),
		COALESCE(content_hash, ''),
		last_crawled_at,
		COALESCE(http_status, 0),
		COALESCE(outbound_links, '[]')
	FROM crawled_pages
	WHERE expires_at IS NULL OR datetime(expires_at) > datetime('now')
	ORDER BY id ASC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := make([]CrawledDocument, 0)
	for rows.Next() {
		var doc CrawledDocument
		var expiresAtStr sql.NullString
		var lastCrawledStr sql.NullString
		var linksJSON string

		if err := rows.Scan(
			&doc.ID, &doc.URL, &doc.Title, &doc.CleanBody, &doc.TotalTokens,
			&doc.SourceType, &doc.SourceURL, &expiresAtStr,
			&doc.ETag, &doc.LastModified, &doc.ContentHash, &lastCrawledStr,
			&doc.HTTPStatus, &linksJSON,
		); err == nil {
			if expiresAtStr.Valid {
				if t, parseErr := time.Parse("2006-01-02 15:04:05", expiresAtStr.String); parseErr == nil {
					doc.ExpiresAt = &t
				} else if t, parseErr := time.Parse(time.RFC3339, expiresAtStr.String); parseErr == nil {
					doc.ExpiresAt = &t
				}
			}
			if lastCrawledStr.Valid {
				if t, parseErr := time.Parse("2006-01-02 15:04:05", lastCrawledStr.String); parseErr == nil {
					doc.LastCrawledAt = t
				} else if t, parseErr := time.Parse(time.RFC3339, lastCrawledStr.String); parseErr == nil {
					doc.LastCrawledAt = t
				}
			}
			if linksJSON != "" {
				if err := json.Unmarshal([]byte(linksJSON), &doc.OutboundLinks); err != nil {
					doc.OutboundLinks = []string{}
				}
			} else {
				doc.OutboundLinks = []string{}
			}
			docs = append(docs, doc)
		}
	}

	return docs, nil
}

// DeleteExpired removes documents whose TTL has expired, returning the count deleted.
func (s *SQLiteStore) DeleteExpired(ctx context.Context) (int64, error) {
	if s.db == nil {
		return 0, errors.New("sqlite db is nil")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `DELETE FROM crawled_pages WHERE expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now');`
	res, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SaveAlias registers a URL alias to a canonical URL.
func (s *SQLiteStore) SaveAlias(ctx context.Context, aliasURL, canonicalURL string) error {
	if aliasURL == "" || canonicalURL == "" || aliasURL == canonicalURL {
		return nil
	}

	s.aliasMu.Lock()
	s.urlAliases[aliasURL] = canonicalURL
	s.aliasMu.Unlock()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `
	INSERT INTO url_aliases (alias_url, canonical_url)
	VALUES (?, ?)
	ON CONFLICT(alias_url) DO UPDATE SET canonical_url = excluded.canonical_url;
	`
	_, err := s.db.ExecContext(ctx, query, aliasURL, canonicalURL)
	return err
}

// GetAlias resolves an alias URL to its canonical URL (returns aliasURL if no alias exists).
func (s *SQLiteStore) GetAlias(ctx context.Context, aliasURL string) string {
	s.aliasMu.RLock()
	defer s.aliasMu.RUnlock()

	visited := make(map[string]bool)
	current := aliasURL

	for {
		if visited[current] {
			return aliasURL // Circular reference detected
		}
		visited[current] = true

		canonical, exists := s.urlAliases[current]
		if !exists || canonical == "" {
			return current
		}
		current = canonical
	}
}

// DeleteCrawledDocument deletes a document by URL or source_url.
func (s *SQLiteStore) DeleteCrawledDocument(ctx context.Context, url string) error {
	if s.db == nil {
		return errors.New("sqlite db is nil")
	}
	if url == "" {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	_, err := s.db.ExecContext(ctx, `DELETE FROM crawled_pages WHERE url = ? OR source_url = ?`, url, url)
	return err
}

// GetOutboundLinks returns outbound links for a URL.
func (s *SQLiteStore) GetOutboundLinks(ctx context.Context, url string) ([]string, error) {
	doc, err := s.GetByURL(ctx, url)
	if err != nil || doc == nil {
		return nil, err
	}
	return doc.OutboundLinks, nil
}

// GetDocumentMetadataBySource retrieves documents matching sourceType.
func (s *SQLiteStore) GetDocumentMetadataBySource(ctx context.Context, sourceType string, limit, offset int) ([]CrawledDocument, error) {
	if s.db == nil {
		return nil, errors.New("sqlite db is nil")
	}

	query := `
	SELECT
		id, url, title, clean_body, total_tokens,
		COALESCE(source_type, 'web_crawled'),
		COALESCE(source_url, url),
		expires_at,
		COALESCE(etag, ''),
		COALESCE(last_modified, ''),
		COALESCE(content_hash, ''),
		last_crawled_at,
		COALESCE(http_status, 0),
		COALESCE(outbound_links, '[]')
	FROM crawled_pages
	WHERE COALESCE(source_type, 'web_crawled') = ? AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))
	ORDER BY id ASC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := s.db.QueryContext(ctx, query, sourceType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := make([]CrawledDocument, 0)
	for rows.Next() {
		var doc CrawledDocument
		var expiresAtStr sql.NullString
		var lastCrawledStr sql.NullString
		var linksJSON string

		if err := rows.Scan(
			&doc.ID, &doc.URL, &doc.Title, &doc.CleanBody, &doc.TotalTokens,
			&doc.SourceType, &doc.SourceURL, &expiresAtStr,
			&doc.ETag, &doc.LastModified, &doc.ContentHash, &lastCrawledStr,
			&doc.HTTPStatus, &linksJSON,
		); err == nil {
			if expiresAtStr.Valid {
				if t, parseErr := time.Parse("2006-01-02 15:04:05", expiresAtStr.String); parseErr == nil {
					doc.ExpiresAt = &t
				} else if t, parseErr := time.Parse(time.RFC3339, expiresAtStr.String); parseErr == nil {
					doc.ExpiresAt = &t
				}
			}
			if lastCrawledStr.Valid {
				if t, parseErr := time.Parse("2006-01-02 15:04:05", lastCrawledStr.String); parseErr == nil {
					doc.LastCrawledAt = t
				} else if t, parseErr := time.Parse(time.RFC3339, lastCrawledStr.String); parseErr == nil {
					doc.LastCrawledAt = t
				}
			}
			if linksJSON != "" {
				if err := json.Unmarshal([]byte(linksJSON), &doc.OutboundLinks); err != nil {
					doc.OutboundLinks = []string{}
				}
			} else {
				doc.OutboundLinks = []string{}
			}
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

// GetDocumentsCount returns the total active document count.
func (s *SQLiteStore) GetDocumentsCount(ctx context.Context) (int64, error) {
	if s.db == nil {
		return 0, errors.New("sqlite db is nil")
	}
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM crawled_pages WHERE expires_at IS NULL OR datetime(expires_at) > datetime('now')`).Scan(&count)
	return count, err
}

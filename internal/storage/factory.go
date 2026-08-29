package storage

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	globalStoreMu sync.RWMutex
	globalStore   DocumentStore
)

// SetGlobalStore sets the process-wide active DocumentStore.
func SetGlobalStore(store DocumentStore) {
	globalStoreMu.Lock()
	defer globalStoreMu.Unlock()
	globalStore = store
}

// GetGlobalStore returns the process-wide active DocumentStore, initializing a default FileStore if none is set.
func GetGlobalStore() DocumentStore {
	globalStoreMu.RLock()
	if globalStore != nil {
		defer globalStoreMu.RUnlock()
		return globalStore
	}
	globalStoreMu.RUnlock()

	globalStoreMu.Lock()
	defer globalStoreMu.Unlock()
	if globalStore == nil {
		globalStore = NewFileStore("")
	}
	return globalStore
}

// NewStore creates a DocumentStore for the given driver name and connection string/path.
func NewStore(driver, dsn string) (DocumentStore, error) {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "sqlite", "sqlite3", "sqlite-pure", "docs.db", "db":
		if dsn == "" {
			dsn = "data/docs.db"
		}
		return NewSQLiteStore(dsn)
	case "postgres", "postgresql", "pg":
		if dsn == "" {
			return nil, fmt.Errorf("postgres driver requires a non-empty connection DSN")
		}
		return NewPostgresStore(dsn)
	case "file", "json", "local":
		return NewFileStore(dsn), nil
	case "memory", "mem":
		return NewMemoryStore(), nil
	default:
		return nil, fmt.Errorf("unsupported storage driver: %q (supported: sqlite, postgres, file, memory)", driver)
	}
}

// NewStoreFromEnv automatically initializes a DocumentStore using environment variables.
// If DATABASE_URL starts with postgres://, it attempts to connect to PostgreSQL.
// If DATABASE_URL starts with sqlite:// or ends in .db, it initializes SQLite.
// If connection fails or DATABASE_URL is empty, it attempts SQLite before falling back to FileStore.
func NewStoreFromEnv() DocumentStore {
	driver := os.Getenv("DATABASE_DRIVER")
	dbURL := os.Getenv("DATABASE_URL")

	if driver != "" {
		store, err := NewStore(driver, dbURL)
		if err == nil && store != nil {
			return store
		}
		log.Printf("[Storage] Failed to initialize driver %q: %v; falling back to sqlite/file storage", driver, err)
	}

	if dbURL != "" {
		if strings.HasPrefix(dbURL, "sqlite://") || strings.HasSuffix(dbURL, ".db") || strings.HasPrefix(dbURL, "file:") {
			store, err := NewSQLiteStore(dbURL)
			if err == nil && store != nil {
				return store
			}
			log.Printf("[Storage] SQLite connection failed: %v; falling back to file storage", err)
		}
		if strings.HasPrefix(dbURL, "postgres://") || strings.HasPrefix(dbURL, "postgresql://") {
			store, err := NewPostgresStore(dbURL)
			if err == nil && store != nil {
				return store
			}
			log.Printf("[Storage] PostgreSQL connection failed: %v; falling back to file storage", err)
		}
	}

	// Default embedded engine is SQLiteStore (falling back to FileStore if directory not writable)
	sqliteStore, err := NewSQLiteStore("data/docs.db")
	if err == nil && sqliteStore != nil {
		return sqliteStore
	}

	return NewFileStore("")
}

// InitGlobalStore initializes and sets the process-wide active DocumentStore for driver and dsn.
func InitGlobalStore(driver, dsn string) error {
	store, err := NewStore(driver, dsn)
	if err != nil {
		return err
	}
	SetGlobalStore(store)
	return nil
}


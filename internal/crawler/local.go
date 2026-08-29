package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/crawler-monorepo/common/stemmer"
	"github.com/crawler-monorepo/common/stopwords"
	"github.com/crawler-monorepo/internal/extractor"
	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/storage"
	"github.com/crawler-monorepo/internal/ui"
)

type LocalIndexerConfig struct {
	BasePath        string
	MaxFileSize     int64
	IncludePatterns []string
	ExcludePatterns []string
	DryRun          bool
	NoIndex         bool
	Concurrency     int
	Broadcaster     *ui.EventBroadcaster
}

type IndexResult struct {
	FilesProcessed int
	FilesIndexed   int
	FilesSkipped   int
	FilesDeleted   int
	Errors         int
}

// IndexDirectory crawls a local directory and indexes documents.
func IndexDirectory(ctx context.Context, cfg LocalIndexerConfig, eng *index.Engine, store storage.DocumentStore) (IndexResult, error) {
	var result IndexResult

	if cfg.Broadcaster != nil {
		cfg.Broadcaster.Publish(ui.ProgressEvent{
			Type:   ui.EventStart,
			Phase:  "indexing",
			Status: "running",
		})
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.NumCPU() * 2
		if cfg.Concurrency > 8 {
			cfg.Concurrency = 8
		}
	}
	if cfg.MaxFileSize <= 0 {
		cfg.MaxFileSize = 10 * 1024 * 1024 // default 10MB
	}

	absPath, err := filepath.Abs(cfg.BasePath)
	if err != nil {
		return result, fmt.Errorf("invalid base path: %w", err)
	}
	cfg.BasePath = absPath

	visited := make(map[string]bool)
	var mu sync.Mutex

	type job struct {
		path string
		info fs.FileInfo
	}

	jobs := make(chan job, 1000)
	
	var wg sync.WaitGroup
	var activeURLs sync.Map

	// Start workers
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				indexed, skip, doerr := processFileJob(ctx, j.path, j.info, cfg, eng, store)
				
				mu.Lock()
				result.FilesProcessed++
				status := "ok"
				if doerr != nil {
					result.Errors++
					status = "error"
				} else if skip {
					result.FilesSkipped++
					status = "skipped"
				} else if indexed {
					result.FilesIndexed++
					status = "indexed"
				}

				if cfg.Broadcaster != nil {
					errMsg := ""
					if doerr != nil {
						errMsg = doerr.Error()
					}
					cfg.Broadcaster.Publish(ui.ProgressEvent{
						Type:        ui.EventProgress,
						Phase:       "indexing",
						Current:     int64(result.FilesProcessed),
						Status:      status,
						ActiveItem:  j.path,
						ErrorsCount: int64(result.Errors),
						Message:     errMsg,
					})
				}
				mu.Unlock()

				if doerr == nil {
					fileURL := "file://" + j.path
					activeURLs.Store(fileURL, true)
				}
			}
		}()
	}

	err = filepath.Walk(cfg.BasePath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return nil // Skip gracefully
			}
			return err
		}

		// Hidden files/dirs
		name := info.Name()
		if strings.HasPrefix(name, ".") && name != "." && name != ".." {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if info.IsDir() {
			if name == "node_modules" || name == "vendor" || name == ".git" {
				return filepath.SkipDir
			}
			fileId := getFileIdentifier(info)
			if fileId != "" {
				mu.Lock()
				if visited[fileId] {
					mu.Unlock()
					return filepath.SkipDir
				}
				visited[fileId] = true
				mu.Unlock()
			} else {
				// fallback check using realpath
				realPath, _ := filepath.EvalSymlinks(path)
				if realPath != "" {
					mu.Lock()
					if visited[realPath] {
						mu.Unlock()
						return filepath.SkipDir
					}
					visited[realPath] = true
					mu.Unlock()
				}
			}
			return nil
		}

		// Symlink check
		if info.Mode()&os.ModeSymlink != 0 {
			realPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				return nil
			}
			rel, err := filepath.Rel(cfg.BasePath, realPath)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return nil // outside base directory boundary
			}
			info, err = os.Stat(realPath)
			if err != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
		}

		// Size check
		if info.Size() == 0 || info.Size() > cfg.MaxFileSize {
			return nil
		}

		// Pattern match
		if len(cfg.IncludePatterns) > 0 {
			matched := false
			for _, pat := range cfg.IncludePatterns {
				m, _ := filepath.Match(pat, name)
				if m {
					matched = true
					break
				}
			}
			if !matched {
				return nil
			}
		}
		for _, pat := range cfg.ExcludePatterns {
			m, _ := filepath.Match(pat, name)
			if m {
				return nil
			}
		}

		jobs <- job{path: path, info: info}
		return nil
	})

	close(jobs)
	wg.Wait()

	if err != nil {
		return result, err
	}

	// Deletion Reconciliation
	if !cfg.DryRun && !cfg.NoIndex {
		docs, err := store.List(ctx, 0, 0)
		if err == nil {
			normalizedBase := filepath.ToSlash(cfg.BasePath)
			basePrefix := "file://" + strings.TrimSuffix(normalizedBase, "/") + "/"
			baseDocURL := "file://" + normalizedBase
			for _, doc := range docs {
				if doc.URL == baseDocURL || strings.HasPrefix(doc.URL, basePrefix) {
					if _, ok := activeURLs.Load(doc.URL); !ok {
						// Not found in active urls, delete it
						eng.DeleteDocument(doc.URL)
						
						// Expire and delete from store
						doc.ExpiresAt = func(t time.Time) *time.Time { return &t }(time.Now().Add(-24 * time.Hour))
						_ = store.UpsertDocument(ctx, &doc, -24 * time.Hour)
						
						mu.Lock()
						result.FilesDeleted++
						mu.Unlock()
					}
				}
			}
			_, _ = store.DeleteExpired(ctx)
		}
	}

	if cfg.Broadcaster != nil {
		cfg.Broadcaster.Publish(ui.ProgressEvent{
			Type:        ui.EventFinish,
			Phase:       "indexing",
			Current:     int64(result.FilesProcessed),
			Status:      "completed",
			ErrorsCount: int64(result.Errors),
		})
	}

	return result, nil
}

func processFileJob(ctx context.Context, path string, info fs.FileInfo, cfg LocalIndexerConfig, eng *index.Engine, store storage.DocumentStore) (bool, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, false, err
	}

	fileURL := "file://" + filepath.ToSlash(path)
	modTime := info.ModTime().Format(time.RFC3339)
	hash := sha256.Sum256(data)
	contentHash := hex.EncodeToString(hash[:])

	if !cfg.DryRun {
		existingDoc, err := store.GetByURL(ctx, fileURL)
		if err == nil && existingDoc != nil {
			if existingDoc.ContentHash == contentHash {
				return false, true, nil // skip
			}
		}
	}

	title, body, tokens, err := extractor.ExtractLocalFile(path, data)
	if err != nil {
		return false, true, nil // skip unsupported
	}

	if cfg.DryRun || cfg.NoIndex {
		return false, false, nil
	}

	// Calculate term positions
	termPositions := make(map[string][]int)
	words := strings.Fields(strings.ToLower(body))
	for idx, w := range words {
		clean := strings.Trim(w, ".,!?;:\"()[]{}")
		if clean == "" || stopwords.IsStopword(clean) {
			continue
		}
		stemmed := stemmer.Stem(clean)
		termPositions[stemmed] = append(termPositions[stemmed], idx)
	}

	eng.IndexDocumentWithSource(fileURL, title, body, termPositions, tokens, "local_file", fileURL)

	doc := &storage.CrawledDocument{
		URL:          fileURL,
		Title:        title,
		CleanBody:    body,
		TotalTokens:  tokens,
		SourceType:   "local_file",
		SourceURL:    fileURL,
		LastModified: modTime,
		ContentHash:  contentHash,
	}
	_ = store.UpsertDocument(ctx, doc, 0)

	return true, false, nil
}

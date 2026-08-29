package crawler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/crawler-monorepo/internal/index"
	"github.com/crawler-monorepo/internal/storage"
)

func TestLocalIndexer(t *testing.T) {
	tempDir := t.TempDir()

	// Setup directories and files
	err := os.MkdirAll(filepath.Join(tempDir, "docs"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	err = os.MkdirAll(filepath.Join(tempDir, ".git"), 0755) // hidden dir
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(tempDir, "docs", "file1.md"), []byte("# Title1\nHello World"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(tempDir, "docs", "file2.txt"), []byte("Just text"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(tempDir, ".hidden.txt"), []byte("Hidden text"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(tempDir, "empty.txt"), []byte(""), 0644) // 0-byte file
	if err != nil {
		t.Fatal(err)
	}
    
	// Symlink pointing back to docs
	err = os.Symlink(filepath.Join(tempDir, "docs"), filepath.Join(tempDir, "docs_link"))
	if err != nil {
		t.Logf("symlink not supported, skipping symlink creation")
	}

	// External symlink
	externalDir := t.TempDir()
	err = os.WriteFile(filepath.Join(externalDir, "ext.txt"), []byte("External"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(externalDir, filepath.Join(tempDir, "ext_link"))
	if err != nil {
		t.Logf("symlink not supported, skipping external symlink creation")
	}

	store := storage.NewMemoryStore()
	eng := index.NewEngine()

	cfg := LocalIndexerConfig{
		BasePath:    tempDir,
		MaxFileSize: 10 * 1024 * 1024,
		Concurrency: 2,
	}

	ctx := context.Background()

	// First run
	res, err := IndexDirectory(ctx, cfg, eng, store)
	if err != nil {
		t.Fatalf("IndexDirectory failed: %v", err)
	}

	if res.FilesIndexed < 2 {
		t.Errorf("Expected at least 2 files indexed, got %d", res.FilesIndexed)
	}

	// Check if empty file was skipped
	if res.FilesSkipped > 0 {
		t.Logf("Files skipped: %d", res.FilesSkipped)
	}

	// Second run (Incremental)
	res2, err := IndexDirectory(ctx, cfg, eng, store)
	if err != nil {
		t.Fatalf("IndexDirectory second run failed: %v", err)
	}

	if res2.FilesIndexed != 0 {
		t.Errorf("Expected 0 files indexed on second run due to incremental check, got %d", res2.FilesIndexed)
	}

	if res2.FilesSkipped < 2 {
		t.Errorf("Expected files skipped on second run, got %d", res2.FilesSkipped)
	}

	// Test deletion reconciliation
	// Delete file1.md
	err = os.Remove(filepath.Join(tempDir, "docs", "file1.md"))
	if err != nil {
		t.Fatal(err)
	}

	// Third run
	res3, err := IndexDirectory(ctx, cfg, eng, store)
	if err != nil {
		t.Fatalf("IndexDirectory third run failed: %v", err)
	}

	if res3.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted, got %d", res3.FilesDeleted)
	}
}

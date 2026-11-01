package crawler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestStatus(t *testing.T, dataDir string, status CrawlStatusFile) string {
	t.Helper()
	dir := crawlStatusDir(dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%d.json", status.PID))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// otherPID returns a PID that is not this test process.
func otherPID(offset int) int { return os.Getpid() + 100000 + offset }

func TestReadCrawlActivity_NoDataDir(t *testing.T) {
	a := ReadCrawlActivity("", StatusStaleAfter)
	if a.StatusKnown || a.CrawlActive {
		t.Fatalf("expected unknown, inactive status, got %+v", a)
	}
}

func TestReadCrawlActivity_MissingDirIsIdle(t *testing.T) {
	a := ReadCrawlActivity(t.TempDir(), StatusStaleAfter)
	if !a.StatusKnown || a.CrawlActive || len(a.ActiveJobs) != 0 {
		t.Fatalf("expected known, idle status, got %+v", a)
	}
}

func TestReadCrawlActivity_LiveStaleOldAndSelf(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Now()
	indexedAt := now.Add(-time.Minute)

	writeTestStatus(t, dataDir, CrawlStatusFile{
		PID:               otherPID(1),
		UpdatedAt:         now,
		ActiveJobs:        []ActiveCrawlJob{{ID: "live", SeedURL: "https://go.dev", PagesCrawled: 3}},
		LastPageIndexedAt: &indexedAt,
	})
	writeTestStatus(t, dataDir, CrawlStatusFile{
		PID:        otherPID(2),
		UpdatedAt:  now.Add(-time.Minute), // stopped heartbeating mid-crawl
		ActiveJobs: []ActiveCrawlJob{{ID: "dead"}},
	})
	oldPath := writeTestStatus(t, dataDir, CrawlStatusFile{
		PID:       otherPID(3),
		UpdatedAt: now.Add(-48 * time.Hour),
	})
	writeTestStatus(t, dataDir, CrawlStatusFile{
		PID:        os.Getpid(), // our own writes are already in our index
		UpdatedAt:  now,
		ActiveJobs: []ActiveCrawlJob{{ID: "self"}},
	})

	a := ReadCrawlActivity(dataDir, StatusStaleAfter)
	if !a.CrawlActive {
		t.Fatalf("expected an active crawl")
	}
	if len(a.ActiveJobs) != 1 || a.ActiveJobs[0].ID != "live" {
		t.Fatalf("expected only the live job, got %+v", a.ActiveJobs)
	}
	if a.InterruptedJobs != 1 {
		t.Fatalf("expected 1 interrupted job, got %d", a.InterruptedJobs)
	}
	if a.LastPageIndexedAt == nil || !a.LastPageIndexedAt.Equal(indexedAt) {
		t.Fatalf("expected last_page_indexed_at %v, got %v", indexedAt, a.LastPageIndexedAt)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected status file older than 24h to be pruned")
	}
}

func TestJobManager_WritesStatusFile(t *testing.T) {
	dataDir := t.TempDir()
	jm := NewJobManager(NewTestClient(true), dataDir)
	defer jm.Close()

	jm.writeStatusFile()
	if _, err := os.Stat(crawlStatusDir(dataDir)); !os.IsNotExist(err) {
		t.Fatalf("expected no status file before EnableStatusFile")
	}
	jm.EnableStatusFile()

	job := &CrawlJob{
		ID:        "job-1",
		Status:    "running",
		Request:   CrawlRequest{URL: "https://go.dev", MaxPages: 50},
		StartTime: time.Now(),
	}
	job.PagesCrawled.Store(7)
	jm.jobs.Store(job.ID, job)
	jm.markPageIndexed()

	if running := jm.writeStatusFile(); !running {
		t.Fatalf("expected writeStatusFile to report a running job")
	}

	data, err := os.ReadFile(filepath.Join(crawlStatusDir(dataDir), fmt.Sprintf("%d.json", os.Getpid())))
	if err != nil {
		t.Fatalf("status file not written: %v", err)
	}
	var status CrawlStatusFile
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	if len(status.ActiveJobs) != 1 || status.ActiveJobs[0].PagesCrawled != 7 || status.ActiveJobs[0].MaxPages != 50 {
		t.Fatalf("unexpected active jobs: %+v", status.ActiveJobs)
	}
	if status.LastPageIndexedAt == nil {
		t.Fatalf("expected last_page_indexed_at in status file")
	}

	job.SetStatus("completed")
	if running := jm.writeStatusFile(); running {
		t.Fatalf("expected no running jobs after completion")
	}
}

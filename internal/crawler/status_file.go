package crawler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Crawl jobs run inside `lightlimbs serve` / `lightlimbs crawl`, while the MCP
// server is a separate stdio process. To let the MCP search tool tell an agent
// whether ingestion is still running, every process that owns a JobManager
// writes a small heartbeat file to <dataDir>/crawl_status/<pid>.json. Readers
// aggregate the files and ignore heartbeats that have gone quiet.

const (
	crawlStatusDirName = "crawl_status"
	// StatusHeartbeatInterval is how often a JobManager refreshes its status file.
	StatusHeartbeatInterval = 2 * time.Second
	// StatusStaleAfter is how old a heartbeat may be before its running jobs are
	// treated as dead (process crashed or was killed).
	StatusStaleAfter = 15 * time.Second
	// statusFileMaxAge is when idle status files from old processes are pruned.
	statusFileMaxAge = 24 * time.Hour
)

// ActiveCrawlJob is the public summary of a running job written to the status file.
type ActiveCrawlJob struct {
	ID           string    `json:"id"`
	SeedURL      string    `json:"seed_url"`
	PagesCrawled int64     `json:"pages_crawled"`
	PagesQueued  int64     `json:"pages_queued"`
	MaxPages     int       `json:"max_pages"`
	StartedAt    time.Time `json:"started_at"`
}

// CrawlStatusFile is the on-disk heartbeat written by one process.
type CrawlStatusFile struct {
	PID               int              `json:"pid"`
	UpdatedAt         time.Time        `json:"updated_at"`
	ActiveJobs        []ActiveCrawlJob `json:"active_jobs"`
	LastPageIndexedAt *time.Time       `json:"last_page_indexed_at,omitempty"`
}

// CrawlActivity aggregates all live status files visible from a data directory.
type CrawlActivity struct {
	// StatusKnown is false when no data directory is configured.
	StatusKnown bool `json:"status_known"`
	// CrawlActive is true while any live process reports a running job.
	CrawlActive bool             `json:"crawl_active"`
	ActiveJobs  []ActiveCrawlJob `json:"active_jobs"`
	// InterruptedJobs counts jobs whose process stopped heartbeating mid-crawl.
	InterruptedJobs int `json:"interrupted_jobs"`
	// LastPageIndexedAt is the newest page indexed by any other process.
	LastPageIndexedAt *time.Time `json:"last_page_indexed_at,omitempty"`
}

func crawlStatusDir(dataDir string) string {
	return filepath.Join(dataDir, crawlStatusDirName)
}

// EnableStatusFile turns on crawl_status heartbeat files for this manager.
// Long-running entry points (lightlimbs serve / crawl) enable it so a separate
// MCP server process can report crawl progress; library and test users of
// JobManager write nothing unless they opt in.
func (m *JobManager) EnableStatusFile() {
	m.statusOn.Store(true)
	m.kickStatus()
}

// activeJobs snapshots the running jobs held by this manager.
func (m *JobManager) activeJobs() []ActiveCrawlJob {
	jobs := []ActiveCrawlJob{}
	m.jobs.Range(func(_, value any) bool {
		job := value.(*CrawlJob)
		if job.GetStatus() != "running" {
			return true
		}
		jobs = append(jobs, ActiveCrawlJob{
			ID:           job.ID,
			SeedURL:      job.Request.URL,
			PagesCrawled: job.PagesCrawled.Load(),
			PagesQueued:  job.PagesQueued.Load(),
			MaxPages:     job.Request.MaxPages,
			StartedAt:    job.StartTime,
		})
		return true
	})
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].StartedAt.Before(jobs[j].StartedAt) })
	return jobs
}

// writeStatusFile atomically publishes this process's heartbeat. It returns
// whether any job was running. Errors are ignored: status is best-effort and
// must never break a crawl.
func (m *JobManager) writeStatusFile() bool {
	jobs := m.activeJobs()
	if m.dataDir == "" || !m.statusOn.Load() {
		return len(jobs) > 0
	}
	dir := crawlStatusDir(m.dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return len(jobs) > 0
	}

	status := CrawlStatusFile{
		PID:        os.Getpid(),
		UpdatedAt:  time.Now(),
		ActiveJobs: jobs,
	}
	if n := m.lastPageIndexedAt.Load(); n != 0 {
		t := time.Unix(0, n)
		status.LastPageIndexedAt = &t
	}

	data, err := json.Marshal(status)
	if err != nil {
		return len(jobs) > 0
	}
	final := filepath.Join(dir, fmt.Sprintf("%d.json", status.PID))
	tmp, err := os.CreateTemp(dir, ".status-*.tmp")
	if err != nil {
		return len(jobs) > 0
	}
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), final) != nil {
		_ = os.Remove(tmp.Name())
	}
	return len(jobs) > 0
}

// runStatusHeartbeat refreshes the status file while jobs run, plus one final
// write after the last job finishes so readers see the crawl as complete.
func (m *JobManager) runStatusHeartbeat() {
	defer close(m.statusDone)
	ticker := time.NewTicker(StatusHeartbeatInterval)
	defer ticker.Stop()
	wasRunning := false
	for {
		select {
		case <-m.closeChan:
			m.writeStatusFile()
			return
		case <-m.statusKick:
			wasRunning = m.writeStatusFile()
		case <-ticker.C:
			if m.hasRunningJobs() || wasRunning {
				wasRunning = m.writeStatusFile()
			}
		}
	}
}

func (m *JobManager) hasRunningJobs() bool {
	running := false
	m.jobs.Range(func(_, value any) bool {
		if value.(*CrawlJob).GetStatus() == "running" {
			running = true
			return false
		}
		return true
	})
	return running
}

// markPageIndexed records that a crawl job just added a page to the index.
func (m *JobManager) markPageIndexed() {
	m.lastPageIndexedAt.Store(time.Now().UnixNano())
}

// kickStatus requests an immediate status write without blocking.
func (m *JobManager) kickStatus() {
	select {
	case m.statusKick <- struct{}{}:
	default:
	}
}

// ReadCrawlActivity aggregates heartbeat files under dataDir. Files from the
// calling process are skipped (its own writes are already in its index), and
// running jobs from heartbeats older than staleAfter are reported as interrupted.
func ReadCrawlActivity(dataDir string, staleAfter time.Duration) CrawlActivity {
	activity := CrawlActivity{ActiveJobs: []ActiveCrawlJob{}}
	if dataDir == "" {
		return activity
	}
	activity.StatusKnown = true

	dir := crawlStatusDir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return activity
	}

	now := time.Now()
	self := os.Getpid()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var status CrawlStatusFile
		if json.Unmarshal(data, &status) != nil || status.PID == self {
			continue
		}

		age := now.Sub(status.UpdatedAt)
		if age > statusFileMaxAge {
			_ = os.Remove(path) // heartbeat from a long-gone process
			continue
		}
		if len(status.ActiveJobs) > 0 {
			if age <= staleAfter {
				activity.CrawlActive = true
				activity.ActiveJobs = append(activity.ActiveJobs, status.ActiveJobs...)
			} else {
				activity.InterruptedJobs += len(status.ActiveJobs)
			}
		}

		if t := status.LastPageIndexedAt; t != nil {
			if activity.LastPageIndexedAt == nil || t.After(*activity.LastPageIndexedAt) {
				tt := *t
				activity.LastPageIndexedAt = &tt
			}
		}
	}
	return activity
}

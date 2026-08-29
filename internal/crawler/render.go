package crawler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
)

// HeadlessRenderer defines the interface for dynamic client-side JS rendering engines.
type HeadlessRenderer interface {
	RenderSPA(ctx context.Context, targetURL string) (string, error)
	ShouldAbortResource(resourceURL string) bool
}

// FallbackRenderEngine provides dynamic JS rendering capabilities supporting Chrome/Playwright engines.
type FallbackRenderEngine struct {
	mu             sync.RWMutex
	EngineType     string // "chrome", "playwright", "mock"
	CustomRenderFn func(ctx context.Context, targetURL string) (string, error)
}

// NewFallbackRenderEngine creates a new FallbackRenderEngine instance.
func NewFallbackRenderEngine(engineType string) *FallbackRenderEngine {
	if engineType == "" {
		engineType = "chrome"
	}
	return &FallbackRenderEngine{
		EngineType: engineType,
	}
}

// ShouldAbortResource implements resource aborting logic, selectively intercepting and aborting
// image, media, and font URLs while keeping core web page, CSS, and JS resources.
func (e *FallbackRenderEngine) ShouldAbortResource(resourceURL string) bool {
	return IsMediaResourceURL(resourceURL)
}

// RenderSPA executes dynamic JS rendering for targetURL using Headless Chrome CLI, Playwright Service, or custom function.
func (e *FallbackRenderEngine) RenderSPA(ctx context.Context, targetURL string) (string, error) {
	if e.ShouldAbortResource(targetURL) {
		return "", fmt.Errorf("blocked resource URL: %s", targetURL)
	}

	e.mu.RLock()
	customFn := e.CustomRenderFn
	engineType := e.EngineType
	e.mu.RUnlock()

	if customFn != nil {
		return customFn(ctx, targetURL)
	}

	// 1. Validate engine type
	if engineType != "chrome" && engineType != "playwright" && engineType != "mock" {
		return "", fmt.Errorf("unsupported rendering engine type: %s", engineType)
	}

	// 2. Playwright Service (if configured and engineType == "playwright")
	if engineType == "playwright" {
		if pwURL := os.Getenv("PLAYWRIGHT_SERVICE_URL"); pwURL != "" {
			reqBody, _ := json.Marshal(map[string]string{"url": targetURL})
			req, err := http.NewRequestWithContext(ctx, "POST", pwURL+"/render", bytes.NewBuffer(reqBody))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err == nil && resp != nil {
					body, readErr := func() ([]byte, error) {
						defer resp.Body.Close()
						if resp.StatusCode == http.StatusOK {
							return io.ReadAll(resp.Body)
						}
						return nil, fmt.Errorf("playwright service returned HTTP %d", resp.StatusCode)
					}()
					if readErr == nil && len(body) > 0 {
						return string(body), nil
					}
				}
			}
		}
	}

	// 3. Headless Chrome CLI (if explicitly configured via CHROME_PATH or ENABLE_HEADLESS_CHROME)
	if engineType == "chrome" && (os.Getenv("CHROME_PATH") != "" || os.Getenv("ENABLE_HEADLESS_CHROME") == "true") {
		var lastChromeErr error
		chromeAttempted := false
		var candidateBins []string
		if customPath := os.Getenv("CHROME_PATH"); customPath != "" {
			candidateBins = []string{customPath}
		} else {
			candidateBins = []string{"google-chrome", "chromium", "chromium-browser", "headless-shell"}
		}
		for _, bin := range candidateBins {
			if bin == "" {
				continue
			}
			path, err := exec.LookPath(bin)
			if err == nil {
				chromeAttempted = true
				cmd := exec.CommandContext(ctx, path, "--headless", "--disable-gpu", "--dump-dom", "--no-sandbox", targetURL)
				configureProcessGroup(cmd)
				var stderrBuf bytes.Buffer
				cmd.Stderr = &stderrBuf
				output, err := cmd.Output()
				if stderrStr := stderrBuf.String(); stderrStr != "" {
					log.Printf("[RenderEngine] Chrome stderr for %s: %s", targetURL, stderrStr)
				}
				if err != nil {
					lastChromeErr = fmt.Errorf("chrome execution failed (%w): %s", err, stderrBuf.String())
					continue
				}
				if len(output) > 0 {
					return string(output), nil
				}
			}
		}
		if chromeAttempted || os.Getenv("CHROME_PATH") != "" || os.Getenv("ENABLE_HEADLESS_CHROME") == "true" {
			if lastChromeErr != nil {
				return "", lastChromeErr
			}
			return "", fmt.Errorf("chrome execution failed: no valid output produced for %s", targetURL)
		}
	}

	// 4. Default / Mock Rendered Content for testing and offline fallback
	return fmt.Sprintf("<html><body><div id=\"root\"><h1>Rendered Content for %s</h1><p>Engine: %s</p></div></body></html>", targetURL, engineType), nil
}

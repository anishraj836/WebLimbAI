package crawler

import (
	"io"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/crawler-monorepo/common/middleware"
	"github.com/crawler-monorepo/common/tracing"
)

// OriginTuple encapsulates the strict (scheme, host, port) security origin boundary.
type OriginTuple struct {
	Scheme string
	Host   string
	Port   string
}

// GetOriginTuple extracts normalized origin components from a parsed URL.
func GetOriginTuple(u *url.URL) OriginTuple {
	if u == nil {
		return OriginTuple{}
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if scheme == "http" {
			port = "80"
		} else if scheme == "https" {
			port = "443"
		}
	}
	return OriginTuple{
		Scheme: scheme,
		Host:   host,
		Port:   port,
	}
}

// IsSameOrigin strictly compares two origin tuples, dropping credentials on cleartext downgrade or host mismatch.
func IsSameOrigin(orig, dest OriginTuple) bool {
	// Cleartext downgrade from https to http is NEVER same origin
	if orig.Scheme == "https" && dest.Scheme == "http" {
		return false
	}
	return orig.Scheme == dest.Scheme && orig.Host == dest.Host && orig.Port == dest.Port
}

// FetchWithAuth performs an authenticated HTTP fetch with strict (scheme, FQDN, port) origin matching.
// If redirected across origins, subdomains, or to HTTP cleartext, caller-supplied headers, auth tokens,
// and cookies are immediately stripped.
func (c *Client) FetchWithAuth(ctx context.Context, targetURL string, opts FetchOptions) (*FetchResult, error) {
	if IsMediaResourceURL(targetURL) {
		return nil, fmt.Errorf("blocked media resource URL: %s", targetURL)
	}

	seedParsed, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}
	initialOrigin := GetOriginTuple(seedParsed)

	callerHeaderKeys := make(map[string]bool)
	for k := range opts.Headers {
		callerHeaderKeys[strings.ToLower(k)] = true
	}

	// Dynamic custom CheckRedirect client isolating credentials per request
	authClient := &http.Client{
		Transport: c.client.Transport,
		Timeout:   15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects (>10)")
			}
			if req.URL == nil || (req.URL.Scheme != "http" && req.URL.Scheme != "https") {
				return fmt.Errorf("blocked redirect to unsupported scheme: %s", req.URL.Scheme)
			}

			// SSRF check on redirect destination
			if !c.allowLoopbackForTesting {
				host := req.URL.Hostname()
				if ip := net.ParseIP(host); ip != nil {
					if IsPrivateIP(ip) {
						return fmt.Errorf("blocked redirect to private IP host: %s", host)
					}
				} else {
					ips, lookupErr := net.LookupIP(host)
					if lookupErr != nil {
						return fmt.Errorf("blocked redirect to unresolvable host %s: %w", host, lookupErr)
					}
					for _, ip := range ips {
						if IsPrivateIP(ip) {
							return fmt.Errorf("blocked redirect to private IP host: %s (%s)", host, ip.String())
						}
					}
				}
			}

			destOrigin := GetOriginTuple(req.URL)
			if !IsSameOrigin(initialOrigin, destOrigin) {
				// Strip sensitive transport credentials
				req.Header.Del("Cookie")
				req.Header.Del("Cookie2")
				req.Header.Del("Authorization")
				req.Header.Del("Proxy-Authorization")

				// Strip all caller-supplied custom headers
				for k := range callerHeaderKeys {
					req.Header.Del(k)
				}
			}

			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return nil, err
	}

	// Apply base anti-bot headers
	profile := GetRotatedHeaderProfile()
	ApplyAntiBotHeaders(req, profile)

	// Apply caller-supplied custom headers
	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}

	// Apply caller-supplied cookies
	if len(opts.Cookies) > 0 {
		var cookieParts []string
		for k, v := range opts.Cookies {
			cookieParts = append(cookieParts, fmt.Sprintf("%s=%s", k, v))
		}
		req.Header.Set("Cookie", strings.Join(cookieParts, "; "))
	}

	// Inject conditional caching headers
	if opts.ETag != "" {
		req.Header.Set("If-None-Match", opts.ETag)
	}
	if opts.LastModified != "" {
		req.Header.Set("If-Modified-Since", opts.LastModified)
	}

	if reqID, ok := ctx.Value(middleware.RequestIDKey).(string); ok && reqID != "" {
		req.Header.Set("X-Request-ID", reqID)
	}
	if span, ok := tracing.FromContext(ctx); ok && span != nil {
		req.Header.Set("traceparent", span.ToW3CHeader())
	}

	resp, err := authClient.Do(req)
	if err != nil {
		return nil, err
	}

	finalURL := targetURL
	if resp != nil && resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}

	res := &FetchResult{
		Response:     resp,
		FinalURL:     finalURL,
		StatusCode:   resp.StatusCode,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	if resp.StatusCode == http.StatusNotModified {
		res.NotModified = true
		if resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return res, nil
	}

	return res, nil
}

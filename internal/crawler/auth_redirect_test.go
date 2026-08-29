package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestAuthRedirectSecurity_SameOriginPreservation(t *testing.T) {
	var mu sync.Mutex
	var finalAuth, finalCookie, finalCustomHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/destination", http.StatusFound)
			return
		}
		if r.URL.Path == "/destination" {
			finalAuth = r.Header.Get("Authorization")
			finalCookie = r.Header.Get("Cookie")
			finalCustomHeader = r.Header.Get("X-Custom-Token")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewTestClient(true)
	headers := map[string]string{
		"Authorization":  "Bearer secret-token-123",
		"X-Custom-Token": "custom-value-abc",
	}
	cookies := map[string]string{
		"session_id": "sess_98765",
	}

	res, err := client.FetchWithAuth(context.Background(), server.URL+"/start", FetchOptions{Headers: headers, Cookies: cookies})
	if err != nil {
		t.Fatalf("FetchWithAuth failed: %v", err)
	}
	defer res.Response.Body.Close()

	mu.Lock()
	defer mu.Unlock()

	if finalAuth != "Bearer secret-token-123" {
		t.Errorf("expected Authorization header preserved on same-origin redirect, got %q", finalAuth)
	}
	if !strings.Contains(finalCookie, "session_id=sess_98765") {
		t.Errorf("expected Cookie preserved on same-origin redirect, got %q", finalCookie)
	}
	if finalCustomHeader != "custom-value-abc" {
		t.Errorf("expected custom header preserved on same-origin redirect, got %q", finalCustomHeader)
	}
}

func TestAuthRedirectSecurity_CrossOriginStripping(t *testing.T) {
	var mu sync.Mutex
	var crossAuth, crossCookie, crossCustomHeader string

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		crossAuth = r.Header.Get("Authorization")
		crossCookie = r.Header.Get("Cookie")
		crossCustomHeader = r.Header.Get("X-Custom-Token")
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("cross-origin-ok"))
	}))
	defer destServer.Close()

	startServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Redirect to different host/port (cross-origin)
		http.Redirect(w, r, destServer.URL+"/landing", http.StatusFound)
	}))
	defer startServer.Close()

	client := NewTestClient(true)
	headers := map[string]string{
		"Authorization":  "Bearer secret-token-cross",
		"X-Custom-Token": "secret-custom-key",
	}
	cookies := map[string]string{
		"auth_sess": "val_12345",
	}

	res, err := client.FetchWithAuth(context.Background(), startServer.URL+"/redirect", FetchOptions{Headers: headers, Cookies: cookies})
	if err != nil {
		t.Fatalf("FetchWithAuth cross-origin redirect failed: %v", err)
	}
	defer res.Response.Body.Close()

	mu.Lock()
	defer mu.Unlock()

	if crossAuth != "" {
		t.Errorf("SECURITY VULNERABILITY: Authorization header leaked to cross-origin server: %q", crossAuth)
	}
	if crossCookie != "" {
		t.Errorf("SECURITY VULNERABILITY: Cookie leaked to cross-origin server: %q", crossCookie)
	}
	if crossCustomHeader != "" {
		t.Errorf("SECURITY VULNERABILITY: Caller header leaked to cross-origin server: %q", crossCustomHeader)
	}
}

func TestAuthRedirectSecurity_SubdomainStripping(t *testing.T) {
	origin1 := OriginTuple{Scheme: "https", Host: "auth.company.com", Port: "443"}
	origin2 := OriginTuple{Scheme: "https", Host: "cdn.company.com", Port: "443"}
	origin3 := OriginTuple{Scheme: "https", Host: "app.company.com", Port: "443"}

	if IsSameOrigin(origin1, origin2) {
		t.Errorf("subdomain auth.company.com -> cdn.company.com must NOT be considered same origin")
	}
	if IsSameOrigin(origin1, origin3) {
		t.Errorf("subdomain auth.company.com -> app.company.com must NOT be considered same origin")
	}
}

func TestAuthRedirectSecurity_CleartextDowngradeStripping(t *testing.T) {
	originHTTPS := OriginTuple{Scheme: "https", Host: "example.com", Port: "443"}
	originHTTP := OriginTuple{Scheme: "http", Host: "example.com", Port: "80"}

	if IsSameOrigin(originHTTPS, originHTTP) {
		t.Errorf("HTTPS -> HTTP cleartext downgrade must NEVER be considered same origin")
	}
}

package network

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// TestNewFallbackProxyEmpty: an empty address means "no fallback configured",
// which must yield a nil handler so callers fall back to their reject path.
func TestNewFallbackProxyEmpty(t *testing.T) {
	h, err := NewFallbackProxy("")
	if err != nil {
		t.Fatalf("unexpected error for empty address: %v", err)
	}
	if h != nil {
		t.Fatalf("expected nil handler for empty address, got %T", h)
	}
}

// TestNewFallbackProxyForwards: a configured fallback reverse-proxies a
// non-tunnel request to the decoy backend and preserves the original Host so
// the decoy sees the real hostname.
func TestNewFallbackProxyForwards(t *testing.T) {
	const decoyBody = "<html>totally a real website</html>"
	var gotHost string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		io.WriteString(w, decoyBody)
	}))
	defer backend.Close()

	backendAddr := strings.TrimPrefix(backend.URL, "http://")
	h, err := NewFallbackProxy(backendAddr)
	if err != nil {
		t.Fatalf("NewFallbackProxy: %v", err)
	}
	if h == nil {
		t.Fatal("expected a handler, got nil")
	}

	req := httptest.NewRequest(http.MethodGet, "http://n1.example.sbs/", nil)
	req.Host = "n1.example.sbs"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from decoy, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != decoyBody {
		t.Fatalf("expected decoy body %q, got %q", decoyBody, body)
	}
	if gotHost != "n1.example.sbs" {
		t.Fatalf("expected decoy to see original host n1.example.sbs, got %q", gotHost)
	}
}

// TestNewFallbackProxyFixedTarget: the upstream is fixed at construction time
// by the operator-supplied address. Requests carrying different Host headers,
// paths, and query strings all reach the same configured backend; a second
// loopback fixture must receive no traffic, proving no request field can
// redirect the proxy to another upstream.
func TestNewFallbackProxyFixedTarget(t *testing.T) {
	var hits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	var otherHits atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()

	h, err := NewFallbackProxy(strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatalf("NewFallbackProxy: %v", err)
	}

	cases := []struct{ url, host string }{
		// Request URL and Host both name the other fixture; neither may steer the proxy.
		{other.URL + "/a?x=1", strings.TrimPrefix(other.URL, "http://")},
		{"http://n1.example.sbs/", "n1.example.sbs"},
		{"http://n1.example.sbs/some/path?q=1", "n1.example.sbs"},
		{"http://n2.example.sbs/other", "n2.example.sbs"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, c.url, nil)
		req.Host = c.host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("url=%s host=%s: want 200, got %d", c.url, c.host, rec.Code)
		}
	}
	if got := hits.Load(); got != int32(len(cases)) {
		t.Errorf("configured backend received %d requests, want %d", got, len(cases))
	}
	if got := otherHits.Load(); got != 0 {
		t.Errorf("other server received %d requests, want 0", got)
	}
}

// TestNewFallbackProxyInvalidAddr: a malformed address surfaces an error
// rather than a half-built proxy.
func TestNewFallbackProxyInvalidAddr(t *testing.T) {
	if _, err := NewFallbackProxy("%zz"); err == nil {
		t.Fatal("expected an error for a malformed address, got nil")
	}
	// Sanity: the well-formed control case parses.
	if _, err := url.Parse("http://127.0.0.1:8080"); err != nil {
		t.Fatalf("control url failed to parse: %v", err)
	}
}

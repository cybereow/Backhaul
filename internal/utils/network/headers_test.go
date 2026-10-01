package network

import (
	"strings"
	"testing"

	utls "github.com/refraction-networking/utls"
)

func TestRandomUserAgent(t *testing.T) {
	// Call RandomUserAgent multiple times to ensure it works and doesn't panic
	for i := 0; i < 100; i++ {
		ua := RandomUserAgent()
		found := false
		for _, v := range browserUserAgents {
			if ua == v {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("RandomUserAgent returned an unexpected value: %s", ua)
		}
	}
}

// Every User-Agent must agree with the ClientHello UtlsDialTLS sends. If a utls
// upgrade moves HelloChrome_Auto to another Chrome, this fails: update the pool
// (and the version below) to match.
func TestUserAgentsMatchTLSPreset(t *testing.T) {
	if utls.HelloChrome_Auto != utls.HelloChrome_133 {
		t.Fatalf("HelloChrome_Auto is now %v: update browserUserAgents to that Chrome major", utls.HelloChrome_Auto.Str())
	}
	for _, ua := range browserUserAgents {
		if !strings.Contains(ua, "Chrome/133.") || strings.Contains(ua, "Edg") || strings.Contains(ua, "OPR") || strings.Contains(ua, "Mobile") {
			t.Errorf("User-Agent does not match the Chrome 133 desktop ClientHello: %s", ua)
		}
	}
}

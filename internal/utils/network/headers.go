package network

import (
	"crypto/rand"
	"math/big"
)

// browserUserAgents is the User-Agent pool. Every entry is desktop Chrome at the
// major version the uTLS ClientHello preset impersonates (utls.HelloChrome_Auto,
// currently Chrome 133; see TestUserAgentsMatchTLSPreset): on wss the handshake
// already says "Chrome 133", and a different browser - or a 2009 MSIE - in the
// User-Agent of the same connection is a contradiction a correlating middlebox
// can flag. Desktop platforms share one Chrome ClientHello, so they may differ.
var browserUserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}

// RandomUserAgent returns a random entry from browserUserAgents.
func RandomUserAgent() string {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(browserUserAgents))))
	if err != nil {
		return browserUserAgents[0]
	}
	return browserUserAgents[n.Int64()]
}

package network

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

// The legacy handshake names (MuxSubprotocol, CapHeader/CapHalfCloseV1) carry the
// product name in clear text on every wsmux/wssmux upgrade. Anything that sees
// the HTTP request - a CDN that terminates TLS, a middlebox on a plain ws leg,
// an operator's log - can match on it, whatever the TLS fingerprint looks like.
//
// The stealth handshake replaces them with values derived from the shared auth
// token, so both ends agree without any new configuration and nothing in the
// request names the project:
//
//   - the framed-mux subprotocol becomes one of a handful of ordinary-looking
//     Sec-WebSocket-Protocol values (see stealthSubprotocols), picked by the token;
//   - the half-close capability moves from X-Backhaul-Cap to a generic
//     X-Request-Id header whose value is a per-request nonce plus a MAC over it,
//     so it looks like a request id and never repeats.
//
// A server accepts both forms (so new servers keep serving old clients); a client
// chooses with mux_stealth_handshake.

// stealthSubprotocols are common real-world WebSocket subprotocol names. None of
// them carries any meaning here; they are only what the token picks between.
var stealthSubprotocols = []string{
	"graphql-transport-ws", "v12.stomp", "wamp.2.json", "mqtt",
	"xmpp", "soap", "chat", "json",
}

// stealthCapHeader is the header that carries the stealth capability offer.
const stealthCapHeader = "X-Request-Id"

func tokenMAC(token, label string, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte(label))
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

// StealthMuxSubprotocol is the framed-mux subprotocol token derived from the
// shared auth token. Both ends compute the same value.
func StealthMuxSubprotocol(token string) string {
	return stealthSubprotocols[int(tokenMAC(token, "mux-subprotocol")[0])%len(stealthSubprotocols)]
}

// stealthCapValue returns a fresh X-Request-Id value that offers the half-close
// capability: 8 random bytes followed by 8 bytes of MAC over them, hex encoded
// (32 characters, the shape of an ordinary request id).
func stealthCapValue(token string) (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	tag := tokenMAC(token, "cap-"+CapHalfCloseV1, nonce)[:8]
	return hex.EncodeToString(nonce) + hex.EncodeToString(tag), nil
}

func validStealthCap(token, v string) bool {
	raw, err := hex.DecodeString(strings.TrimSpace(v))
	if err != nil || len(raw) != 16 {
		return false
	}
	want := tokenMAC(token, "cap-"+CapHalfCloseV1, raw[:8])[:8]
	return hmac.Equal(raw[8:], want)
}

// MatchMuxSubprotocol reports which framed-mux subprotocol an upgrade request
// offers - the legacy token or the one derived from token - so the server can
// echo exactly that one. ok is false when it offers neither.
func MatchMuxSubprotocol(h http.Header, token string) (proto string, ok bool) {
	stealth := StealthMuxSubprotocol(token)
	for _, v := range h.Values("Sec-WebSocket-Protocol") {
		for _, tok := range strings.Split(v, ",") {
			switch tok = strings.TrimSpace(tok); tok {
			case stealth, MuxSubprotocol:
				return tok, true
			}
		}
	}
	return "", false
}

// OffersHalfClose reports whether an upgrade request offers the half-close
// capability, in either the legacy (X-Backhaul-Cap) or the stealth (X-Request-Id
// with a valid MAC) form.
func OffersHalfClose(h http.Header, token string) bool {
	if OffersCapability(h, CapHalfCloseV1) {
		return true
	}
	for _, v := range h.Values(stealthCapHeader) {
		if validStealthCap(token, v) {
			return true
		}
	}
	return false
}

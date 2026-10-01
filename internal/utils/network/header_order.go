package network

import (
	"bytes"
	"net"
	"sort"
	"strings"
)

// gobwas/ws writes the fixed upgrade headers (Host, Upgrade, Connection,
// Sec-WebSocket-Version, Sec-WebSocket-Key, ...) in its own order and appends the
// caller's headers after them, so the request does not read like a browser's even
// when every header in it is one a browser sends. Header order is something a TLS
// terminator or WAF can fingerprint, so orderedUpgradeConn rewrites the request
// head, once, into the order Chrome writes a WebSocket upgrade.
//
// The order below was captured from a real Chromium 141 opening a WebSocket (plain
// and TLS) against a local listener:
//
//	Host, Connection, Pragma, Cache-Control, User-Agent, Upgrade, Origin,
//	Sec-WebSocket-Version, Accept-Encoding, [Accept-Language], Sec-WebSocket-Key,
//	Sec-WebSocket-Extensions, Sec-WebSocket-Protocol
//
// Accept-Language is absent from a headless capture but sits there in headed
// Chrome. Headers a browser would not send here (Authorization, the capability
// header) go in the slot where a Cookie header would be, just before the key.
// Sec-WebSocket-Extensions is never sent: the tunnel does not implement
// permessage-deflate.
var (
	upgradeHeadOrder = []string{
		"host", "connection", "pragma", "cache-control", "user-agent", "upgrade",
		"origin", "sec-websocket-version", "accept-encoding", "accept-language",
	}
	upgradeTailOrder = []string{"sec-websocket-key", "sec-websocket-extensions", "sec-websocket-protocol"}
)

const (
	upgradeCustomRank = 500  // between the head and tail groups
	upgradeTailRank   = 1000 // base rank of the tail group
	maxUpgradeHead    = 16 << 10
)

func upgradeRank(key string) int {
	for i, k := range upgradeHeadOrder {
		if k == key {
			return i
		}
	}
	for i, k := range upgradeTailOrder {
		if k == key {
			return upgradeTailRank + i
		}
	}
	return upgradeCustomRank
}

// reorderUpgradeHead rewrites a complete request head (request line, headers, and
// the closing blank line) into Chrome's header order. Headers keep their values and
// their relative order within a rank. Anything it does not understand comes back
// unchanged: it must never be the reason a handshake fails.
func reorderUpgradeHead(head []byte) []byte {
	body, ok := bytes.CutSuffix(head, []byte("\r\n\r\n"))
	if !ok {
		return head
	}
	lines := strings.Split(string(body), "\r\n")
	if len(lines) < 2 || !strings.HasPrefix(lines[0], "GET ") {
		return head
	}
	hdrs := lines[1:]
	keys := make([]string, len(hdrs))
	for i, l := range hdrs {
		k, _, found := strings.Cut(l, ":")
		if !found || k == "" || strings.TrimSpace(k) != k {
			return head
		}
		keys[i] = strings.ToLower(k)
	}
	idx := make([]int, len(hdrs))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return upgradeRank(keys[idx[a]]) < upgradeRank(keys[idx[b]]) })

	var out strings.Builder
	out.WriteString(lines[0])
	out.WriteString("\r\n")
	for _, i := range idx {
		out.WriteString(hdrs[i])
		out.WriteString("\r\n")
	}
	out.WriteString("\r\n")
	return []byte(out.String())
}

// orderedUpgradeConn holds back the writes of the upgrade request until its head
// is complete, emits it reordered, and from then on passes every write straight
// through. Reads are untouched, so gobwas still validates the response itself.
type orderedUpgradeConn struct {
	net.Conn
	buf  []byte
	done bool
}

func (c *orderedUpgradeConn) Write(p []byte) (int, error) {
	if c.done {
		return c.Conn.Write(p)
	}
	c.buf = append(c.buf, p...)
	end := bytes.Index(c.buf, []byte("\r\n\r\n"))
	if end < 0 && len(c.buf) <= maxUpgradeHead {
		return len(p), nil // head not complete yet
	}
	c.done = true
	out := c.buf
	c.buf = nil
	if end >= 0 {
		out = append(reorderUpgradeHead(out[:end+4]), out[end+4:]...)
	}
	if _, err := c.Conn.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

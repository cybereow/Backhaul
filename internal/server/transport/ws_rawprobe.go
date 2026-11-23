package transport

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"time"

	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
)

// The raw flow mode (see handlers.RawFlowPrefix) only works when the whole path
// between the two ends forwards bytes after the 101 response without reading
// them as WebSocket frames. A CDN or WAF that does read them would break every
// flow, so the server does not trust the path: it uses raw flows only while a
// probe has recently shown that they get through, and falls back to the framed
// transport otherwise.
//
// The client opens the probe: a connection on rawProbePath that upgrades like
// any other, turns raw after one text message, and has to echo what the server
// writes. The payload starts with a byte whose reserved bits are set, which no
// WebSocket parser accepts as the start of a frame, so a middlebox that checks
// frames refuses it. A client that never probes (an older build) is never sent a
// raw flow, because it could not read one.
const (
	// defaultRawProbeTTL is how long a successful probe is trusted. The client
	// probes every minute, so this tolerates a missed round.
	defaultRawProbeTTL = 150 * time.Second
)

// errRawProbeMismatch: the echo was not what was sent.
var errRawProbeMismatch = errors.New("raw probe echo differs from what was sent")

// rawProbePath is the path the client probes on, under the tunnel's base path.
func rawProbePath(basePath string) string { return basePath + handlers.RawProbeSuffix }

// rawMode reports whether a flow started now may be raw, and whether it may be
// resumable (moved to another connection, see RawResume).
func (s *WsTransport) rawMode() (raw bool, caps uint32) {
	if !s.config.Raw {
		return false, 0
	}
	ok, bad := s.rawOKAt.Load(), s.rawBadAt.Load()
	ttl := s.config.RawProbeTTL
	if ttl <= 0 {
		ttl = defaultRawProbeTTL
	}
	if ok == 0 || ok < bad || time.Since(time.Unix(0, ok)) > ttl {
		return false, 0
	}
	return true, s.rawCaps.Load()
}

// runRawProbe answers one probe connection and records the verdict.
func (s *WsTransport) runRawProbe(g *wsGeneration, conn *network.WebSocketConn) {
	defer g.release(conn)
	defer conn.Close()
	caps, err := serveRawProbe(conn)
	if err != nil {
		s.rawBadAt.Store(time.Now().UnixNano())
		s.logger.Warnf("raw flows are not usable on this path, using framed flows: %v", err)
		return
	}
	wasUsable, _ := s.rawMode()
	s.rawCaps.Store(caps)
	s.rawOKAt.Store(time.Now().UnixNano())
	if !wasUsable {
		s.logger.Info("raw flows verified on this path")
	}
}

// serveRawProbe runs the server side of one probe and returns the client's
// capabilities.
func serveRawProbe(conn *network.WebSocketConn) (uint32, error) {
	_ = conn.SetDeadline(time.Now().Add(handlers.RawProbeTimeout))
	if err := conn.WriteMessage(network.TextMessage, []byte(handlers.RawProbeMsg)); err != nil {
		return 0, err
	}
	left, raw := conn.Detach()
	payload := make([]byte, handlers.RawProbeLen)
	if _, err := rand.Read(payload); err != nil {
		return 0, err
	}
	payload[0] = 0xF1 // FIN, all reserved bits, opcode 1: never a valid frame start
	if _, err := raw.Write(payload); err != nil {
		return 0, err
	}
	echo := make([]byte, handlers.RawProbeLen+1)
	if _, err := io.ReadFull(io.MultiReader(bytes.NewReader(left), raw), echo); err != nil {
		return 0, err
	}
	if !bytes.Equal(echo[:handlers.RawProbeLen], payload) {
		return 0, errRawProbeMismatch
	}
	return uint32(echo[handlers.RawProbeLen]), nil
}

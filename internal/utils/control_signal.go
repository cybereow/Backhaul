package utils

import (
	crand "crypto/rand"
	"github.com/musix/backhaul/internal/utils/network"
	"math/big"
	"math/rand"
	"time"
)

// maxControlPadding bounds the random padding appended to control-channel
// frames (heartbeat/chan/closed signals). Without it every control frame is
// exactly 1 byte, which under TLS still shows up as a fixed-size message at
// a fixed interval - an easy statistical fingerprint for DPI.
const maxControlPadding = 64

// heartbeatJitterFraction is how far a heartbeat interval may drift from its
// configured value (+/- this fraction), so the control channel doesn't beat
// with perfectly periodic timing.
const heartbeatJitterFraction = 0.3

// WriteControlSignal writes a single control byte to conn, padded with a
// random amount of random bytes so control frames don't all share the same
// on-wire size. Readers only ever look at the first byte of a control
// message, so the padding is transparent to them.
func WriteControlSignal(conn *network.WebSocketConn, signal byte) error {
	padLenBig, err := crand.Int(crand.Reader, big.NewInt(maxControlPadding+1))
	if err != nil {
		return err
	}
	padLen := int(padLenBig.Int64())

	buf := make([]byte, 1+padLen)
	buf[0] = signal
	if padLen > 0 {
		if _, err := crand.Read(buf[1:]); err != nil {
			return err
		}
	}
	return conn.WriteMessage(network.BinaryMessage, buf)
}

// JitterDuration returns base randomly shifted by up to
// +/- heartbeatJitterFraction, so repeated intervals (e.g. heartbeats) don't
// form a perfectly periodic pattern. Falls back to base for non-positive
// durations.
func JitterDuration(base time.Duration) time.Duration {
	return JitterFraction(base, heartbeatJitterFraction)
}

// JitterFraction is JitterDuration with an explicit spread: base shifted by up
// to +/- frac of itself.
func JitterFraction(base time.Duration, frac float64) time.Duration {
	if base <= 0 {
		return base
	}
	spread := float64(base) * frac
	offset := (rand.Float64()*2 - 1) * spread // in [-spread, +spread]
	return base + time.Duration(offset)
}

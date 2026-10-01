package dnsx

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/miekg/dns"
	"github.com/musix/backhaul/internal/transport/dns/sel"
)

// Control is the operator's lever over a client nobody can reach: the server
// publishes it and every client picks it up on its own. The client asks for it
// when a session starts and then every controlPullEvery (the server can only
// answer queries, so "pushing" means being asked), so a change made on the server
// reaches running clients within about half a minute. All fields are limits or
// switches the client already tunes itself under; zero values mean "no opinion".
type Control struct {
	MaxWorkers int           // upper bound on in-flight exchanges per tunnel (0: none)
	DenyTypes  []uint16      // record types the client must not use (e.g. dns.TypeNULL)
	NoHedge    bool          // turn hedging off
	IdlePoll   time.Duration // idle polling interval (0: client default); a larger value costs resolvers less
}

const (
	controlVersion   = 1
	controlLen       = 5 // ver, maxWorkers, denyMask, bits, idlePoll/100ms
	controlPullEvery = 30 * time.Second
)

// controlTypes fixes the bit position of each record type in the deny mask
// (independent of the tunnel domain, unlike the codec registry).
var controlTypes = []uint16{dns.TypeTXT, dns.TypeNULL, dns.TypeA, dns.TypeAAAA, dns.TypeCNAME, dns.TypeMX, dns.TypeSRV, dns.TypePTR}

// Marshal encodes c into controlLen bytes.
func (c Control) Marshal() []byte {
	b := make([]byte, controlLen)
	b[0] = controlVersion
	b[1] = byte(min(max(c.MaxWorkers, 0), 255))
	for _, t := range c.DenyTypes {
		for i, ct := range controlTypes {
			if ct == t {
				b[2] |= 1 << i
			}
		}
	}
	if c.NoHedge {
		b[3] |= 1
	}
	b[4] = byte(min(max(int(c.IdlePoll/(100*time.Millisecond)), 0), 255))
	return b
}

// ParseControl decodes the bytes Marshal produced. An unknown version is an
// error: an old client must ignore a control block it does not understand.
func ParseControl(b []byte) (Control, error) {
	if len(b) < controlLen {
		return Control{}, errors.New("dnsx: short control block")
	}
	if b[0] != controlVersion {
		return Control{}, fmt.Errorf("dnsx: unknown control version %d", b[0])
	}
	c := Control{MaxWorkers: int(b[1]), NoHedge: b[3]&1 != 0, IdlePoll: time.Duration(b[4]) * 100 * time.Millisecond}
	for i, ct := range controlTypes {
		if b[2]&(1<<i) != 0 {
			c.DenyTypes = append(c.DenyTypes, ct)
		}
	}
	return c, nil
}

// ControlTypeCodes turns configured record type names into Control.DenyTypes.
func ControlTypeCodes(names []string) ([]uint16, error) {
	var out []uint16
	for _, n := range names {
		found := false
		for _, ct := range controlTypes {
			if strings.EqualFold(strings.TrimSpace(n), dns.TypeToString[ct]) {
				out = append(out, ct)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown dns record type %q", n)
		}
	}
	return out, nil
}

// SetControl publishes c to every client of this server.
func (s *Server) SetControl(c Control) {
	b := c.Marshal()
	s.ctlMu.Lock()
	s.ctl = append([]byte{byte(len(b))}, b...)
	s.ctlMu.Unlock()
}

// ctlBlock returns the length-prefixed control block, or nil when none is set.
func (s *Server) ctlBlock() []byte {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()
	return s.ctl
}

// applyControl installs a control block received from the server.
func (c *clientConn) applyControl(ctl Control) {
	c.ctrlCap.Store(int32(ctl.MaxWorkers))
	c.hedge.Store(!ctl.NoHedge && !c.noHedgeCfg)
	c.idlePoll.Store(int64(ctl.IdlePoll))

	c.mu.Lock()
	c.lastCtrlReq = time.Now() // delivered: the next request is due in controlPullEvery
	changed := !sameTypes(c.denied, ctl.DenyTypes)
	if changed && len(c.profiles) == 0 && len(c.standby) == 0 {
		// startup, before any profile is measured: just remember it, warm-up and autotune skip denied types
		c.denied = append([]uint16(nil), ctl.DenyTypes...)
		changed = false
	}
	if changed {
		var keep, held []sel.Profile
		for _, p := range append(append([]sel.Profile(nil), c.profiles...), c.denyHeld...) {
			if typeIn(ctl.DenyTypes, p.RRType) {
				held = append(held, p) // not lost: a later control block may lift the deny
			} else {
				keep = append(keep, p)
			}
		}
		var stillStandby []sel.Profile
		if len(keep) == 0 {
			// nothing measured is allowed: warmed standby paths of other types take over
			for _, p := range c.standby {
				if typeIn(ctl.DenyTypes, p.RRType) {
					held = append(held, p)
				} else {
					keep = append(keep, p)
				}
			}
		} else {
			stillStandby = c.standby
		}
		if len(keep) > 0 { // a deny list that leaves nothing to try is ignored as a whole
			c.standby = stillStandby
			c.denied = append([]uint16(nil), ctl.DenyTypes...)
			c.denyHeld, c.profiles = held, keep
			mgr := sel.New(c.selCfg, keep)
			for _, p := range keep {
				pc, ok := c.caps[keyOf(p)]
				if !ok || pc.rtt <= 0 {
					continue // never measured: no evidence to seed, it must earn its place
				}
				for i := 0; i < warmSamples; i++ {
					mgr.Observe(time.Now(), p, true, pc.rtt)
				}
			}
			c.mgr = mgr
		}
	}
	c.mu.Unlock()
	if c.logf != nil {
		c.logf("dns control from server: max_workers=%d deny=%v no_hedge=%v idle_poll=%v", ctl.MaxWorkers, ctl.DenyTypes, ctl.NoHedge, ctl.IdlePoll)
	}
}

func typeIn(s []uint16, t uint16) bool {
	for _, x := range s {
		if x == t {
			return true
		}
	}
	return false
}

func sameTypes(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for _, t := range a {
		if !typeIn(b, t) {
			return false
		}
	}
	return true
}

// effTarget is how many workers may run: the self-tuned target, capped by the
// server's MaxWorkers when it set one.
func (c *clientConn) effTarget() int32 {
	t := c.target.Load()
	if cp := c.ctrlCap.Load(); cp > 0 && cp < t {
		return cp
	}
	return t
}

// idleInterval is the polling interval when there is nothing to move.
func (c *clientConn) idleInterval() time.Duration {
	if d := time.Duration(c.idlePoll.Load()); d > 0 {
		return d
	}
	return pollIdle
}

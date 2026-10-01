// Package sel picks which carrier profile (resolver x record type x transport)
// the next DNS exchange should use, from live measurements only.
//
// It deliberately knows nothing about TXT, NULL, AAAA...: a type is just a
// profile with a measured success rate, RTT and per-exchange capacity. Any type
// can be best today and dead tomorrow, so:
//
//   - score = decayed success rate x capacity / RTT (delivered bytes/second),
//     with a mild TCP preference that is a tie-breaker, never a lock;
//   - the best TopK profiles share load round-robin, but only those scoring at
//     least SpreadFloor of the best (sharing equally with a much weaker profile
//     would just throw capacity away);
//   - a profile that fails FailStreak times in a row is disabled and re-probed
//     on an exponential schedule, so a recovered type is found again;
//   - stats decay with HalfLife, so an idle profile becomes "unproven" and is
//     re-sampled without any timer;
//   - a challenger replaces an incumbent only after beating it by Margin for
//     Hold (hysteresis), so noise does not flap the active set.
//
// Manager is NOT goroutine-safe; guard it with a mutex.
package sel

import (
	"math"
	"sort"
	"time"
)

// Profile identifies one carrier. Cap is the response bytes one exchange can
// carry on this record type (its codec capacity); it is part of the identity.
type Profile struct {
	Resolver  string
	RRType    uint16
	Transport string
	Cap       int
}

// Config tunes the manager. Zero values take the defaults noted.
type Config struct {
	TopK        int           // profiles sharing load (2)
	HalfLife    time.Duration // stats decay (30s)
	TCPBonus    float64       // score multiplier for tcp (1.15)
	Margin      float64       // challenger must beat worst incumbent by this factor (1.3)
	SpreadFloor float64       // a profile joins the active set at >= this fraction of the best score, and leaves below floor/Margin (0.5)
	Hold        time.Duration // ...continuously for this long (5s)
	FailStreak  int           // consecutive failures that disable a profile (4)
	RecheckMin  time.Duration // first re-probe delay for a disabled profile (5s)
	RecheckMax  time.Duration // cap of the exponential re-probe delay (30s)
	ProbeEvery  int           // every Nth Pick probes a non-active profile (10)
	MinWeight   float64       // decayed samples below which a profile is "unproven" (3)
}

func (c *Config) defaults() {
	if c.TopK <= 0 {
		c.TopK = 2
	}
	if c.HalfLife <= 0 {
		c.HalfLife = 30 * time.Second
	}
	if c.TCPBonus <= 0 {
		c.TCPBonus = 1.15
	}
	if c.Margin <= 0 {
		c.Margin = 1.3
	}
	if c.SpreadFloor <= 0 {
		c.SpreadFloor = 0.5
	}
	if c.Hold <= 0 {
		c.Hold = 5 * time.Second
	}
	if c.FailStreak <= 0 {
		c.FailStreak = 4
	}
	if c.RecheckMin <= 0 {
		c.RecheckMin = 5 * time.Second
	}
	if c.RecheckMax <= 0 {
		c.RecheckMax = 30 * time.Second
	}
	if c.ProbeEvery <= 0 {
		c.ProbeEvery = 10
	}
	if c.MinWeight <= 0 {
		c.MinWeight = 3
	}
}

type stat struct {
	w, ok         float64 // decayed attempts / successes
	rtt           time.Duration
	at            time.Time // last decay
	fails         int       // consecutive failures
	backoff       time.Duration
	disabledUntil time.Time // re-probe due time once disabled
	lastUsed      time.Time
}

const (
	reselectEvery  = 20 * time.Millisecond  // the active set is a slow decision; a few ms of lag costs nothing
	probeScanEvery = 100 * time.Millisecond // how often the candidate list is rebuilt
	probeQueueLen  = 32
)

type probeCand struct {
	p    Profile
	tier int
}

// Manager chooses profiles. Switches counts active-set changes (for tests and
// observability); a flapping selector shows up as a large number here.
type Manager struct {
	cfg       Config
	ps        []Profile
	st        map[Profile]*stat
	active    []Profile
	chal      Profile
	chalSince time.Time
	n, rr     int
	lastSel   time.Time   // last full reselect: Pick runs per exchange, ranking every profile each time does not scale
	probeQ    []probeCand // probe candidates in order, rebuilt at most every probeScanEvery
	probeAt   time.Time

	Switches int
}

func New(cfg Config, profiles []Profile) *Manager {
	cfg.defaults()
	m := &Manager{cfg: cfg, ps: append([]Profile(nil), profiles...), st: map[Profile]*stat{}}
	for _, p := range m.ps {
		m.st[p] = &stat{}
	}
	return m
}

func (m *Manager) decay(s *stat, now time.Time) {
	if s.at.IsZero() {
		s.at = now
		return
	}
	if dt := now.Sub(s.at); dt > 0 {
		f := math.Pow(0.5, dt.Seconds()/m.cfg.HalfLife.Seconds())
		s.w *= f
		s.ok *= f
		s.at = now
	}
}

func (m *Manager) disabled(s *stat) bool { return s.fails >= m.cfg.FailStreak }

// Observe records the outcome of one exchange on p.
func (m *Manager) Observe(now time.Time, p Profile, ok bool, rtt time.Duration) {
	s, found := m.st[p]
	if !found {
		return
	}
	m.decay(s, now)
	s.w++
	if ok {
		s.ok++
		if s.rtt == 0 {
			s.rtt = rtt
		} else {
			s.rtt = (4*s.rtt + rtt) / 5
		}
		s.fails, s.backoff, s.disabledUntil = 0, 0, time.Time{}
		return
	}
	s.fails++
	if s.fails >= m.cfg.FailStreak {
		if s.backoff < m.cfg.RecheckMin {
			s.backoff = m.cfg.RecheckMin
		} else if s.backoff *= 2; s.backoff > m.cfg.RecheckMax {
			s.backoff = m.cfg.RecheckMax
		}
		s.disabledUntil = now.Add(s.backoff)
	}
}

// Score is the profile's estimated delivered bytes/second (0 = unusable/unknown).
func (m *Manager) Score(p Profile, now time.Time) float64 {
	s := m.st[p]
	if s == nil || m.disabled(s) {
		return 0
	}
	m.decay(s, now)
	if s.w < 1 {
		return 0
	}
	rate := (s.ok + 1) / (s.w + 2) // Laplace: one lucky sample is not a 100% profile
	rtt := s.rtt.Seconds()
	if rtt <= 0 {
		rtt = 1
	}
	sc := rate * float64(p.Cap) / rtt
	if p.Transport == "tcp" {
		sc *= m.cfg.TCPBonus
	}
	return sc
}

// Active returns the profiles currently sharing load.
func (m *Manager) Active() []Profile { return append([]Profile(nil), m.active...) }

func (m *Manager) isActive(p Profile) bool {
	for _, a := range m.active {
		if a == p {
			return true
		}
	}
	return false
}

// rankedInactive is the enabled, scoring, non-active profiles, best first.
func (m *Manager) rankedInactive(now time.Time) []Profile {
	var out []Profile
	for _, p := range m.ps {
		if !m.isActive(p) && m.Score(p, now) > 0 {
			out = append(out, p)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return m.Score(out[i], now) > m.Score(out[j], now) })
	return out
}

func (m *Manager) reselect(now time.Time) {
	best := 0.0
	for _, p := range m.ps {
		if sc := m.Score(p, now); sc > best {
			best = sc
		}
	}
	join := m.cfg.SpreadFloor * best
	leave := join / m.cfg.Margin // hysteresis band: no flapping right at the join line

	kept := m.active[:0]
	for _, p := range m.active {
		if sc := m.Score(p, now); sc > 0 && sc >= leave {
			kept = append(kept, p)
		} else {
			m.Switches++ // dead or far behind the best: leaves immediately
		}
	}
	m.active = kept

	ranked := m.rankedInactive(now)
	for len(m.active) < m.cfg.TopK && len(ranked) > 0 && m.Score(ranked[0], now) >= join {
		m.active = append(m.active, ranked[0])
		ranked = ranked[1:]
		m.Switches++
	}
	if len(ranked) == 0 || len(m.active) == 0 {
		m.chal, m.chalSince = Profile{}, time.Time{}
		return
	}

	worst := 0
	for i, a := range m.active {
		if m.Score(a, now) < m.Score(m.active[worst], now) {
			worst = i
		}
	}
	c := ranked[0]
	if m.Score(c, now) <= m.cfg.Margin*m.Score(m.active[worst], now) {
		m.chal, m.chalSince = Profile{}, time.Time{}
		return
	}
	if m.chal != c {
		m.chal, m.chalSince = c, now
	}
	if now.Sub(m.chalSince) >= m.cfg.Hold {
		m.active[worst] = c
		m.Switches++
		m.chal, m.chalSince = Profile{}, time.Time{}
	}
}

// probeTier says how worth probing c is: tier 0 unproven, tier 1 disabled and due
// for a re-check, tier 2 the stalest enabled one; ok is false for a profile that
// is disabled and not due yet.
func (m *Manager) probeTier(c Profile, now time.Time) (tier int, ok bool) {
	s := m.st[c]
	m.decay(s, now)
	switch {
	case m.disabled(s):
		if now.Before(s.disabledUntil) {
			return 0, false
		}
		return 1, true
	case s.w < m.cfg.MinWeight:
		return 0, true
	}
	return 2, true
}

// peekProbe returns the next non-active profile worth sampling without consuming
// it. The ordered candidate list is rebuilt at most every probeScanEvery, so a
// busy tunnel does not rank every profile on every exchange.
func (m *Manager) peekProbe(now time.Time) (c probeCand, ok bool) {
	for len(m.probeQ) > 0 {
		f := m.probeQ[0]
		if t, valid := m.probeTier(f.p, now); valid && !m.isActive(f.p) {
			f.tier = t
			return f, true
		}
		m.probeQ = m.probeQ[1:]
	}
	if !m.probeAt.IsZero() && now.Sub(m.probeAt) < probeScanEvery {
		return probeCand{}, false
	}
	m.probeAt = now
	var all []probeCand
	for _, p := range m.ps {
		if m.isActive(p) {
			continue
		}
		if t, valid := m.probeTier(p, now); valid {
			all = append(all, probeCand{p, t})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].tier != all[j].tier {
			return all[i].tier < all[j].tier
		}
		return m.st[all[i].p].lastUsed.Before(m.st[all[j].p].lastUsed)
	})
	if len(all) > probeQueueLen {
		all = all[:probeQueueLen]
	}
	m.probeQ = all
	if len(all) == 0 {
		return probeCand{}, false
	}
	return all[0], true
}

// Pick returns the profile for the next exchange.
func (m *Manager) Pick(now time.Time) Profile {
	m.n++
	if len(m.active) == 0 || m.lastSel.IsZero() || now.Before(m.lastSel) || now.Sub(m.lastSel) >= reselectEvery {
		m.reselect(now)
		m.lastSel = now
	}

	if c, ok := m.peekProbe(now); ok &&
		(len(m.active) == 0 || (c.tier == 0 && m.n%2 == 0) || m.n%m.cfg.ProbeEvery == 0) {
		m.probeQ = m.probeQ[1:]
		m.st[c.p].lastUsed = now
		return c.p
	}
	if len(m.active) > 0 {
		p := m.active[m.rr%len(m.active)]
		m.rr++
		m.st[p].lastUsed = now
		return p
	}
	// Everything is disabled and nothing is due: keep the path alive by trying
	// whichever profile is due soonest.
	best := m.ps[0]
	for _, c := range m.ps {
		if m.st[c].disabledUntil.Before(m.st[best].disabledUntil) {
			best = c
		}
	}
	m.st[best].lastUsed = now
	return best
}

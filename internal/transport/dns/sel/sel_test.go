package sel

import (
	"math/rand"
	"testing"
	"time"
)

// Record types are just numbers here on purpose: the manager must not care
// which one is which. The scenario is "conditions change under our feet".
const (
	tTXT   = 16
	tAAAA  = 28
	tA     = 1
	tCNAME = 5
	tMX    = 15
	tNULL  = 10
)

func prof(t uint16, cap int) Profile {
	return Profile{Resolver: "r1", RRType: t, Transport: "tcp", Cap: cap}
}

// rate is the true success probability of profile p at simulated second t.
func rate(p Profile, t float64) float64 {
	switch {
	case t < 60: // everything but NULL works; TXT/AAAA carry the most
		if p.RRType == tNULL {
			return 0
		}
		return 0.94
	case t < 150: // TXT collapses (the concurrency-degradation we measured, worse)
		switch p.RRType {
		case tTXT:
			return 0.15
		case tNULL:
			return 0
		}
		return 0.94
	case t < 240: // harsh filtering: ONLY NULL works
		if p.RRType == tNULL {
			return 0.9
		}
		return 0
	default: // TXT recovers, AAAA stays dead
		switch p.RRType {
		case tAAAA:
			return 0
		}
		return 0.9
	}
}

type window struct {
	from, to float64
	picks    map[uint16]int
	okN, n   int
}

func (w *window) share(types ...uint16) float64 {
	c := 0
	for _, t := range types {
		c += w.picks[t]
	}
	return float64(c) / float64(w.n)
}

func TestFollowsChangingConditions(t *testing.T) {
	ps := []Profile{prof(tTXT, 512), prof(tAAAA, 512), prof(tA, 149), prof(tCNAME, 111), prof(tMX, 111), prof(tNULL, 512)}
	m := New(Config{}, ps)
	rng := rand.New(rand.NewSource(1))

	wins := []*window{{from: 20, to: 60}, {from: 80, to: 150}, {from: 190, to: 240}, {from: 270, to: 330}}
	for _, w := range wins {
		w.picks = map[uint16]int{}
	}
	now := time.Unix(1_000_000, 0)
	start := now
	for ; now.Sub(start) < 330*time.Second; now = now.Add(150 * time.Millisecond) {
		sec := now.Sub(start).Seconds()
		p := m.Pick(now)
		ok := rng.Float64() < rate(p, sec)
		m.Observe(now, p, ok, 300*time.Millisecond+time.Duration(rng.Intn(200))*time.Millisecond)
		for _, w := range wins {
			if sec >= w.from && sec < w.to {
				w.n++
				w.picks[p.RRType]++
				if ok {
					w.okN++
				}
			}
		}
	}
	for i, w := range wins {
		t.Logf("window %d [%3.0f,%3.0f)s: picks %v, success %.0f%%", i, w.from, w.to, w.picks, 100*float64(w.okN)/float64(w.n))
	}

	// 1. Steady state: the fat types (TXT/AAAA) carry the load; the dead NULL is
	//    only ever a background probe.
	if s := wins[0].share(tTXT, tAAAA); s < 0.75 {
		t.Errorf("steady state: fat types got %.0f%% of exchanges, want >=75%%", 100*s)
	}
	if s := wins[0].share(tNULL); s > 0.12 {
		t.Errorf("steady state: dead type wasted %.0f%% of exchanges on probes, want <=12%%", 100*s)
	}
	// 2. TXT collapses at t=60: traffic must move off it (probes only) onto AAAA.
	if s := wins[1].share(tTXT); s > 0.12 {
		t.Errorf("after TXT collapse it still got %.0f%% of exchanges, want <=12%%", 100*s)
	}
	if s := wins[1].share(tAAAA); s < 0.5 {
		t.Errorf("after TXT collapse AAAA got only %.0f%%, want >=50%%", 100*s)
	}
	// 3. Only NULL works at t=150: it was dead (and probed away) for 150 s and is
	//    named nowhere in the code, yet must be found and carry the load.
	if s := wins[2].share(tNULL); s < 0.7 {
		t.Errorf("only-NULL-works phase: NULL got %.0f%%, want >=70%% (auto-detect failed)", 100*s)
	}
	// 4. Recovery: once things work again, almost nothing is wasted on dead types.
	if s := wins[3].share(tTXT, tNULL, tA, tCNAME, tMX); s < 0.88 {
		t.Errorf("after recovery only %.0f%% went to working types, want >=88%%", 100*s)
	}
	// 5. A recovered type must re-enter while another type still works (only the
	//    disabled-profile re-check can find it; the all-dead fallback cannot).
	if s := wins[3].share(tTXT); s < 0.15 {
		t.Errorf("TXT recovered at t=240 but got only %.0f%% by t=270-330: recovered profiles are not re-probed", 100*s)
	}
	if got := float64(wins[3].okN) / float64(wins[3].n); got < 0.8 {
		t.Errorf("after recovery overall success %.0f%%, want >=80%%", 100*got)
	}
}

// Two equally good profiles with noisy outcomes must not flap the active set.
func TestNoFlappingOnEqualProfiles(t *testing.T) {
	ps := []Profile{prof(tTXT, 512), prof(tAAAA, 512), prof(tA, 512)}
	m := New(Config{TopK: 1}, ps)
	rng := rand.New(rand.NewSource(2))
	now := time.Unix(1_000_000, 0)
	for i := 0; i < 2000; i++ { // ~300 s
		p := m.Pick(now)
		m.Observe(now, p, rng.Float64() < 0.9, 300*time.Millisecond+time.Duration(rng.Intn(300))*time.Millisecond)
		now = now.Add(150 * time.Millisecond)
	}
	if m.Switches > 8 {
		t.Fatalf("active set changed %d times among equal profiles; hysteresis is not holding", m.Switches)
	}
	t.Logf("equal profiles: %d active-set changes in 300 s", m.Switches)
}

// TCP is preferred when otherwise equal, but never required: a UDP profile that
// is clearly better must still win.
func TestTCPBonusIsAPreferenceNotALock(t *testing.T) {
	tcp := Profile{Resolver: "r", RRType: tTXT, Transport: "tcp", Cap: 512}
	udp := Profile{Resolver: "r", RRType: tTXT, Transport: "udp", Cap: 512}
	m := New(Config{}, []Profile{tcp, udp})
	now := time.Unix(1_000_000, 0)
	feed := func(p Profile, n int, okEvery int, rtt time.Duration) {
		for i := 0; i < n; i++ {
			m.Observe(now, p, i%okEvery != 0 || okEvery == 1, rtt)
		}
	}
	feed(tcp, 20, 1, 300*time.Millisecond)
	feed(udp, 20, 1, 300*time.Millisecond)
	if m.Score(tcp, now) <= m.Score(udp, now) {
		t.Fatalf("equal profiles: tcp %.0f should outscore udp %.0f", m.Score(tcp, now), m.Score(udp, now))
	}
	feed(udp, 40, 1, 100*time.Millisecond) // udp now clearly faster
	if m.Score(udp, now) <= m.Score(tcp, now) {
		t.Fatalf("clearly better udp %.0f must beat tcp %.0f", m.Score(udp, now), m.Score(tcp, now))
	}
}

// A second profile sitting right at the join line (half the best one's
// capacity) with noisy stats must not bounce in and out of the active set.
func TestNoFlappingAtTheJoinLine(t *testing.T) {
	ps := []Profile{prof(tTXT, 512), prof(tAAAA, 256)}
	m := New(Config{TopK: 2}, ps)
	rng := rand.New(rand.NewSource(3))
	now := time.Unix(1_000_000, 0)
	for i := 0; i < 2000; i++ { // ~300 s
		p := m.Pick(now)
		m.Observe(now, p, rng.Float64() < 0.92, 250*time.Millisecond+time.Duration(rng.Intn(200))*time.Millisecond)
		now = now.Add(150 * time.Millisecond)
	}
	if m.Switches > 6 {
		t.Fatalf("active set changed %d times around the join line; the hysteresis band is not holding", m.Switches)
	}
	t.Logf("join-line profiles: %d active-set changes in 300 s", m.Switches)
}

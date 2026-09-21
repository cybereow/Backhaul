package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
)

// Plan 018 field contract: see the comment on poolDiagnostics in wsmux_events.go.
// These tests characterize it against the delivered ownership state (generation
// ownership, grace epochs, setup permits, pending reservations, rotation gate).

const diagChannel = 4 // ChannelSize of the fixture: the setup-permit ceiling

func newDiagTransport(t *testing.T) (*WsMuxTransport, *wsGeneration, poolViews) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &WsMuxConfig{
		Mode:             config.WSMUX,
		Path:             "/",
		MuxVersion:       2,
		MuxCon:           8,
		ChannelSize:      diagChannel,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		KeepAlive:        30 * time.Second,
		Heartbeat:        30 * time.Second,
		Nodelay:          true,
	}
	s := NewWSMuxServer(ctx, cfg, logger)
	g := s.gen
	t.Cleanup(func() {
		s.controlMu.Lock()
		s.invalidateGraceLocked()
		s.controlMu.Unlock()
		cancel()
		g.join(lcDeadline)
	})
	return s, g, s.poolViewsOf(g)
}

func diagOf(t *testing.T, s *WsMuxTransport, v poolViews) map[string]interface{} {
	t.Helper()
	raw, err := json.Marshal(s.poolSnapshot(v))
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	d, ok := m["diagnostics"].(map[string]interface{})
	if !ok {
		t.Fatalf("snapshot has no diagnostics object: %s", raw)
	}
	return d
}

func wantDiag(t *testing.T, d map[string]interface{}, want map[string]interface{}) {
	t.Helper()
	for k, w := range want {
		if d[k] != w {
			t.Errorf("diagnostics[%q] = %v, want %v (all: %v)", k, d[k], w, d)
		}
	}
}

func TestPoolDiagnosticsSnapshot(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s, _, v := newDiagTransport(t)
		d := diagOf(t, s, v)
		wantDiag(t, d, map[string]interface{}{
			"control_state": "waiting", "sessions_owned": 0.0, "sessions_eligible": 0.0,
			"sessions_queued": 0.0, "sessions_draining": 0.0, "setups_active": 0.0,
			"setups_queued": 0.0, "setup_limit": float64(diagChannel), "setups_saturated": false,
			"pending_opens": 0.0, "replacement_waiting": false,
		})
		if _, ok := d["control_grace_elapsed_ms"]; ok {
			t.Error("no grace is running: control_grace_elapsed_ms must be omitted")
		}
	})

	t.Run("connected", func(t *testing.T) {
		s, g, v := newDiagTransport(t)
		s.adoptControl(g, graceConn(t))
		a, b := newRotateSession(t), newRotateSession(t)
		g.own(a)
		g.own(b)
		s.registerSession(a, false)
		s.registerSession(b, false)
		d := diagOf(t, s, v)
		wantDiag(t, d, map[string]interface{}{
			"control_state": "connected", "sessions_owned": 2.0, "sessions_eligible": 2.0,
			"sessions_queued": 0.0, "sessions_draining": 0.0,
		})
		if _, ok := d["control_grace_elapsed_ms"]; ok {
			t.Error("connected: control_grace_elapsed_ms must be omitted")
		}
	})

	t.Run("reconnecting", func(t *testing.T) {
		s, g, v := newDiagTransport(t)
		c := graceConn(t)
		s.adoptControl(g, c)
		s.onControlLost(g, c)
		// Backdate the loss on the monotonic clock: no sleeping for elapsed time.
		s.controlMu.Lock()
		s.graceStart = time.Now().Add(-2 * time.Second)
		s.controlMu.Unlock()
		d := diagOf(t, s, v)
		wantDiag(t, d, map[string]interface{}{"control_state": "reconnecting"})
		ms, ok := d["control_grace_elapsed_ms"].(float64)
		if !ok || ms < 2000 || ms > 60000 {
			t.Errorf("control_grace_elapsed_ms = %v, want ~2000+", d["control_grace_elapsed_ms"])
		}
	})

	t.Run("restarting", func(t *testing.T) {
		s, g, v := newDiagTransport(t)
		c := graceConn(t)
		s.adoptControl(g, c)
		s.onControlLost(g, c)
		s.controlMu.Lock()
		s.restartClaim = g
		s.controlMu.Unlock()
		wantDiag(t, diagOf(t, s, v), map[string]interface{}{"control_state": "restarting"})
	})

	t.Run("draining_and_queued", func(t *testing.T) {
		s, g, v := newDiagTransport(t)
		live, aging, queued := newRotateSession(t), newRotateSession(t), newRotateSession(t)
		for _, sess := range []interface{ Close() error }{live, aging, queued} {
			g.own(sess)
		}
		s.registerSession(live, false)
		s.registerSession(aging, false)
		// aging is rotated out: unregistered, still owned, still open.
		s.unregisterSession(aging)
		// queued is upgraded and waiting for admission.
		s.tunnelChannel <- tunnelSession{session: queued}
		d := diagOf(t, s, v)
		wantDiag(t, d, map[string]interface{}{
			"sessions_owned": 3.0, "sessions_eligible": 1.0,
			"sessions_queued": 1.0, "sessions_draining": 1.0,
		})
		// A drained (closed) session leaves the owned/draining counts.
		aging.Close()
		wantDiag(t, diagOf(t, s, v), map[string]interface{}{
			"sessions_owned": 2.0, "sessions_draining": 0.0,
		})
	})

	t.Run("replacement_waiting", func(t *testing.T) {
		s, g, v := newDiagTransport(t)
		g.rotate <- struct{}{}
		wantDiag(t, diagOf(t, s, v), map[string]interface{}{"replacement_waiting": true})
		<-g.rotate
		wantDiag(t, diagOf(t, s, v), map[string]interface{}{"replacement_waiting": false})
	})

	t.Run("saturated_setup", func(t *testing.T) {
		s, _, v := newDiagTransport(t)
		a, b := newRotateSession(t), newRotateSession(t)
		psA := s.registerSession(a, false)
		psB := s.registerSession(b, false)
		atomic.StoreInt32(&s.setupsActive, diagChannel)
		for i := 0; i < diagChannel; i++ {
			s.localChannel <- LocalTCPConn{}
		}
		s.plainSelectMu.Lock()
		psA.pendingOpens, psB.pendingOpens = 2, 1
		s.plainSelectMu.Unlock()
		d := diagOf(t, s, v)
		wantDiag(t, d, map[string]interface{}{
			"setups_active": float64(diagChannel), "setups_queued": float64(diagChannel),
			"setup_limit": float64(diagChannel), "setups_saturated": true, "pending_opens": 3.0,
		})
		// Not saturated one permit below the ceiling.
		atomic.StoreInt32(&s.setupsActive, diagChannel-1)
		wantDiag(t, diagOf(t, s, v), map[string]interface{}{"setups_saturated": false})
	})

	t.Run("untracked_generation_omits_owned_fields", func(t *testing.T) {
		s := &WsMuxTransport{}
		d := diagOf(t, s, poolViews{})
		for _, k := range []string{"sessions_owned", "sessions_draining", "replacement_waiting"} {
			if _, ok := d[k]; ok {
				t.Errorf("%s must be omitted without a tracked generation, got %v", k, d[k])
			}
		}
		wantDiag(t, d, map[string]interface{}{"control_state": "waiting", "setup_limit": 0.0, "setups_saturated": false})
	})
}

// The legacy /pool keys keep their names and meaning; diagnostics is additive.
func TestPoolDiagnosticsLegacyKeysUnchanged(t *testing.T) {
	s, _, v := newDiagTransport(t)
	m := s.poolSnapshot(v)
	for _, k := range []string{"now", "total_sessions", "distinct_cdns", "total_streams", "per_cdn", "diagnostics"} {
		if _, ok := m[k]; !ok {
			t.Errorf("poolSnapshot lost key %q", k)
		}
	}
	if len(m) != 6 {
		t.Errorf("poolSnapshot has %d keys, want the 5 legacy ones plus diagnostics", len(m))
	}
}

// A snapshot is a copy: mutating it changes nothing in the transport, and taking
// snapshots (however many, however saturated) records no event, so polling can
// never evict the ring's history.
func TestPoolDiagnosticsSnapshotIsolationAndNoEvents(t *testing.T) {
	s, g, v := newDiagTransport(t)
	atomic.StoreInt32(&s.setupsActive, diagChannel)
	sess := newRotateSession(t)
	g.own(sess)
	s.registerSession(sess, false)

	m := s.poolSnapshot(v)
	d := m["diagnostics"].(poolDiagnostics)
	*d.SessionsOwned = 99
	d.SetupsActive = 99
	m["per_cdn"] = nil
	again := s.poolSnapshot(v)["diagnostics"].(poolDiagnostics)
	if *again.SessionsOwned != 1 || again.SetupsActive != diagChannel {
		t.Fatalf("mutating a snapshot leaked into the next: owned=%d setups=%d", *again.SessionsOwned, again.SetupsActive)
	}

	for i := 0; i < maxRecordedEvents/2; i++ {
		s.recordEvent("control_lost", "history")
	}
	before := s.snapshotEvents()
	for i := 0; i < 500; i++ {
		s.poolSnapshot(v)
	}
	after := s.snapshotEvents()
	if len(after) != len(before) || after[0] != before[0] || after[len(after)-1] != before[len(before)-1] {
		t.Fatalf("polling the snapshot changed the event history: %d -> %d", len(before), len(after))
	}
}

// End to end over the real listener: unauthorized requests get no diagnostics,
// authorized ones get the additive object with every legacy key intact, and
// nothing secret appears in either body.
func TestPoolDiagnosticsHTTP(t *testing.T) {
	h := newLCHarness(t)
	client := &http.Client{Timeout: lcDeadline}
	get := func(path, auth string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://"+h.addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	for _, path := range []string{"/pool", "/diag"} {
		for _, auth := range []string{"", "Bearer wrong", lcToken} {
			code, body := get(path, auth)
			if code != http.StatusUnauthorized {
				t.Errorf("GET %s auth=%q: status %d, want 401", path, auth, code)
			}
			if strings.Contains(body, "diagnostics") || strings.Contains(body, "control_state") {
				t.Errorf("GET %s auth=%q leaked diagnostics: %s", path, auth, body)
			}
		}
	}

	h.control()
	h.pool()
	auth := "Bearer " + lcToken
	lcWaitFor(t, "the pool session to be eligible", func() bool {
		_, body := get("/pool", auth)
		var m struct {
			Diagnostics map[string]interface{} `json:"diagnostics"`
		}
		return json.Unmarshal([]byte(body), &m) == nil &&
			m.Diagnostics["control_state"] == "connected" && m.Diagnostics["sessions_eligible"] == 1.0
	})

	code, body := get("/pool", auth)
	if code != http.StatusOK {
		t.Fatalf("GET /pool: %d", code)
	}
	var pool map[string]interface{}
	if err := json.Unmarshal([]byte(body), &pool); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"now", "total_sessions", "distinct_cdns", "total_streams", "per_cdn", "diagnostics"} {
		if _, ok := pool[k]; !ok {
			t.Errorf("/pool lost key %q: %s", k, body)
		}
	}
	if pool["total_sessions"] != 1.0 {
		t.Errorf("/pool total_sessions = %v, want 1", pool["total_sessions"])
	}

	code, dbody := get("/diag", auth)
	if code != http.StatusOK {
		t.Fatalf("GET /diag: %d", code)
	}
	var diag map[string]interface{}
	if err := json.Unmarshal([]byte(dbody), &diag); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"now", "count", "events", "diagnostics"} {
		if _, ok := diag[k]; !ok {
			t.Errorf("/diag lost key %q: %s", k, dbody)
		}
	}
	for _, b := range []string{body, dbody} {
		if strings.Contains(b, lcToken) || strings.Contains(strings.ToLower(b), "authorization") {
			t.Errorf("a diagnostics body carries credential material: %s", b)
		}
	}
}

// Snapshot reads race controlled admission, drain, grace and setup changes.
// Counts must never be negative; no cross-field equality is asserted while the
// state is changing.
func TestPoolDiagnosticsRaceWithMutation(t *testing.T) {
	s, g, v := newDiagTransport(t)
	sessions := []*network.WebSocketConn{graceConn(t), graceConn(t)}
	a, b := newRotateSession(t), newRotateSession(t)

	const rounds = 300
	var stop atomic.Bool
	var readers, writers sync.WaitGroup

	check := func(d poolDiagnostics) {
		ints := []int{d.SessionsEligible, d.SessionsQueued, d.SetupsActive, d.SetupsQueued, d.SetupLimit, d.PendingOpens}
		if d.SessionsOwned != nil {
			ints = append(ints, *d.SessionsOwned)
		}
		if d.SessionsDraining != nil {
			ints = append(ints, *d.SessionsDraining)
		}
		for _, n := range ints {
			if n < 0 {
				t.Errorf("negative count in %+v", d)
				return
			}
		}
		switch d.ControlState {
		case "connected", "reconnecting", "restarting", "waiting":
		default:
			t.Errorf("unknown control_state %q", d.ControlState)
		}
		if d.ControlGraceElapsedMS != nil && *d.ControlGraceElapsedMS < 0 {
			t.Errorf("negative grace elapsed in %+v", d)
		}
	}
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for !stop.Load() {
				m := s.poolSnapshot(v)
				check(m["diagnostics"].(poolDiagnostics))
				if _, err := json.Marshal(m); err != nil {
					t.Errorf("marshal: %v", err)
					return
				}
			}
		}()
	}

	run := func(f func(i int)) {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < rounds; i++ {
				f(i)
			}
		}()
	}
	// admission and drain of pool sessions
	run(func(int) {
		g.own(a)
		s.registerSession(a, false)
		g.own(b)
		s.registerSession(b, false)
		s.unregisterSession(a) // rotated out, still owned: draining
		s.unregisterSession(b)
		g.release(a)
		g.release(b)
	})
	// control loss, reattach and restart claim
	run(func(i int) {
		c := sessions[i%2]
		s.controlMu.Lock()
		s.controlChannel, s.handlersStarted = c, true
		s.controlMu.Unlock()
		s.controlMu.Lock()
		s.controlChannel = nil
		s.graceStart = time.Now()
		if i%3 == 0 {
			s.restartClaim = g
		} else {
			s.restartClaim = nil
		}
		s.controlMu.Unlock()
	})
	// setup permits, queue, pending reservations, rotation gate, session queue
	run(func(i int) {
		atomic.AddInt32(&s.setupsActive, 1)
		atomic.AddInt32(&s.setupsActive, -1)
		select {
		case s.localChannel <- LocalTCPConn{}:
			<-s.localChannel
		default:
		}
		select {
		case s.tunnelChannel <- tunnelSession{session: a}:
			<-s.tunnelChannel
		default:
		}
		select {
		case g.rotate <- struct{}{}:
			<-g.rotate
		default:
		}
	})
	writers.Wait()
	stop.Store(true)
	readers.Wait()
}

// The end of a grace period is recorded once, when the adoption ends it - and
// not for a first channel or for one that replaces a still-registered channel.
func TestControlReattachedEvent(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	if n := graceEvents(s, "control_reattached"); n != 0 {
		t.Fatalf("the first control channel is not a reattach; got %d event(s)", n)
	}
	s.onControlLost(g, c1)
	c2 := graceConn(t)
	if _, first, ok := s.adoptControl(g, c2); !ok || first {
		t.Fatalf("reattach: first=%v ok=%v", first, ok)
	}
	if n := graceEvents(s, "control_reattached"); n != 1 {
		t.Fatalf("want one control_reattached event after the reattach, got %d: %+v", n, s.snapshotEvents())
	}
	// Replacing a still-registered channel is control_replaced (the caller's
	// event), not the end of a grace period.
	if stale, _, ok := s.adoptControl(g, graceConn(t)); !ok || stale != c2 {
		t.Fatalf("replace: stale=%v ok=%v", stale, ok)
	}
	if n := graceEvents(s, "control_reattached"); n != 1 {
		t.Fatalf("a replaced channel must not record a second reattach, got %d", n)
	}
	for _, e := range s.snapshotEvents() {
		if strings.Contains(e.Detail, lcToken) || strings.Contains(e.Detail, "Bearer") {
			t.Errorf("event carries credential material: %+v", e)
		}
	}
}

// A completed rotation records its retirement once. Rotation is minutes apart,
// so this stays sparse against the ring's cap.
func TestRotationRetiredEvent(t *testing.T) {
	s, g := newRotateFixture(t)
	old := addRotateSession(t, s)
	got := awaitAsync(s, g, old)
	waitRequest(t, s)
	assertNoResult(t, "retired before a replacement was up", got)
	if n := graceEvents(s, "rotation_retired"); n != 0 {
		t.Fatalf("no retirement yet, got %d event(s)", n)
	}
	addRotateSession(t, s)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("rotation gave up although a replacement joined")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("rotation did not retire after its replacement joined")
	}
	if n := graceEvents(s, "rotation_retired"); n != 1 {
		t.Fatalf("want one rotation_retired event, got %d: %+v", n, s.snapshotEvents())
	}
}

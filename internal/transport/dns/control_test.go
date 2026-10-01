package dnsx

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/musix/backhaul/internal/transport/dns/sel"
)

func TestControlRoundTrip(t *testing.T) {
	in := Control{MaxWorkers: 6, DenyTypes: []uint16{dns.TypeNULL, dns.TypePTR}, NoHedge: true, IdlePoll: 1500 * time.Millisecond}
	got, err := ParseControl(in.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxWorkers != 6 || !got.NoHedge || got.IdlePoll != 1500*time.Millisecond || !sameTypes(got.DenyTypes, in.DenyTypes) {
		t.Fatalf("round trip changed the control block: %+v -> %+v", in, got)
	}
	if !reflect.DeepEqual(Control{}.Marshal()[1:], []byte{0, 0, 0, 0}) {
		t.Error("the zero control block must mean no opinion")
	}
	bad := in.Marshal()
	bad[0] = 99
	if _, err := ParseControl(bad); err == nil {
		t.Error("an unknown control version must be rejected, not half-applied")
	}
	if _, err := ParseControl(in.Marshal()[:3]); err == nil {
		t.Error("a short control block was accepted")
	}
	if _, err := ControlTypeCodes([]string{"txt", "bogus"}); err == nil {
		t.Error("an unknown record type name was accepted")
	}
}

// The server publishes a control block; a running client picks it up and obeys it:
// capped workers, a denied record type is dropped from the selector, hedging off.
func TestClientObeysServerControl(t *testing.T) {
	const domain, key = "t.example.com", "k"
	srv := NewServer(domain, key, newTestLogger())
	defer srv.Close()
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	srv.SetControl(Control{MaxWorkers: 2, DenyTypes: []uint16{dns.TypeMX}, NoHedge: true, IdlePoll: 700 * time.Millisecond})

	go func() {
		c, err := srv.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(c, c)
	}()
	cli, err := Dial(context.Background(), DialParams{
		Domain: domain, Key: key, Workers: 8, NoAutotune: true, Timeout: time.Second,
		Profiles: []sel.Profile{
			{Resolver: addr, RRType: dns.TypeTXT, Transport: "udp", Cap: 700},
			{Resolver: addr, RRType: dns.TypeMX, Transport: "udp", Cap: 150},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	c := cli.(*clientConn)

	deadline := time.Now().Add(5 * time.Second)
	for c.ctrlCap.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if c.ctrlCap.Load() != 2 {
		t.Fatalf("worker cap from the server not applied: %d", c.ctrlCap.Load())
	}
	if c.hedge.Load() {
		t.Error("server asked for no hedging but it is still on")
	}
	if c.idleInterval() != 700*time.Millisecond {
		t.Errorf("idle poll %v, want 700ms", c.idleInterval())
	}
	if c.effTarget() != 2 {
		t.Errorf("effective workers %d with a server cap of 2 (self target %d)", c.effTarget(), c.target.Load())
	}
	c.mu.Lock()
	for _, p := range c.mgr.Active() {
		if p.RRType == dns.TypeMX {
			t.Error("a denied record type is still active in the selector")
		}
	}
	denied := append([]uint16(nil), c.denied...)
	c.mu.Unlock()
	if !typeIn(denied, dns.TypeMX) {
		t.Errorf("deny list not stored: %v", denied)
	}
}

// Denying every type must not leave the client with nothing to try.
func TestControlNeverDeniesEverything(t *testing.T) {
	c := &clientConn{profiles: []sel.Profile{{Resolver: "1.1.1.1:53", RRType: dns.TypeTXT, Transport: "udp", Cap: 700}}}
	c.mgr = sel.New(sel.Config{}, c.profiles)
	before := c.mgr
	c.applyControl(Control{DenyTypes: []uint16{dns.TypeTXT}})
	if c.mgr != before {
		t.Error("the selector was replaced by an empty one")
	}
}

// A change made on the server while clients are running reaches them: the client
// asks again after controlPullEvery, with no restart.
func TestControlChangeReachesARunningClient(t *testing.T) {
	const domain, key = "t.example.com", "k"
	srv := NewServer(domain, key, newTestLogger())
	defer srv.Close()
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	srv.SetControl(Control{MaxWorkers: 8})
	cli, err := Dial(context.Background(), DialParams{
		Domain: domain, Key: key, Workers: 16, NoAutotune: true, Timeout: time.Second,
		Profiles: []sel.Profile{{Resolver: addr, RRType: dns.TypeTXT, Transport: "udp", Cap: 700}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	c := cli.(*clientConn)
	if c.ctrlCap.Load() != 8 {
		t.Fatalf("initial control block not applied: %d", c.ctrlCap.Load())
	}

	srv.SetControl(Control{MaxWorkers: 3}) // the operator tightens the limit
	c.mu.Lock()
	c.lastCtrlReq = time.Now().Add(-2 * controlPullEvery) // the next exchange is due to ask again
	c.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for c.ctrlCap.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if c.ctrlCap.Load() != 3 {
		t.Fatalf("the changed limit never reached the running client: %d", c.ctrlCap.Load())
	}
}

// Unmeasured warm survivors must not sit in the selector (nominal capacities would
// outrank measured profiles and dead ones would cost 2s timeouts): they wait as
// standby, and a total failure of the measured set triggers a failover measurement.
func TestStandbyProfilesStayOutOfTheSelector(t *testing.T) {
	const domain, key = "t.example.com", "k"
	srv := NewServer(domain, key, newTestLogger())
	defer srv.Close()
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	var profiles []sel.Profile
	for _, t := range []uint16{dns.TypeTXT, dns.TypeMX, dns.TypeA, dns.TypeAAAA} {
		profiles = append(profiles, sel.Profile{Resolver: addr, RRType: t, Transport: "udp", Cap: 300})
	}
	cli, err := Dial(context.Background(), DialParams{Domain: domain, Key: key, Timeout: time.Second, Profiles: profiles, Sel: sel.Config{TopK: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	c := cli.(*clientConn)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sp := range c.standby {
		for _, p := range c.profiles {
			if keyOf(sp) == keyOf(p) {
				t.Errorf("standby profile %+v is also in the selector", sp)
			}
		}
	}
	if len(c.profiles) == 0 {
		t.Fatal("no measured profiles")
	}
}
